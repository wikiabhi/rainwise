// Package jobs creates, runs, and checkpoints jobs (§7.3, §9.4, §10.5).
package jobs

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

type ServiceDeps struct {
	Store             ports.Store
	AutomationEnabled bool
	ProviderNames     []string
	Now               func() time.Time
}

// Service is the front door for jobs. The runner picks up what it creates.
type Service struct {
	d ServiceDeps

	// Notifier, if set, is told a job changed so the runner needn't wait for its
	// next poll. Set it before the first call; it must not block.
	Notifier func()

	// mu makes the idempotency and dedup checks atomic with the insert.
	mu sync.Mutex
}

func NewService(d ServiceDeps) *Service {
	if d.Now == nil {
		d.Now = time.Now
	}
	return &Service{d: d}
}

type CreateRequest struct {
	Type           domain.JobType
	Provider       string
	Taxonomy       string
	DryRun         bool
	Params         domain.JobParams
	Scope          domain.Scope
	FromDryRun     string
	IdempotencyKey string
}

// Create returns the new job, or with created=false the existing job that an
// idempotency key or an active sync (§7.3) resolves to.
func (s *Service) Create(ctx context.Context, req CreateRequest) (job domain.Job, created bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	js := s.d.Store.Jobs()

	if j, ok, err := js.FindJobByIdempotencyKey(ctx, req.IdempotencyKey); err != nil {
		return domain.Job{}, false, err
	} else if ok {
		return j, false, nil
	}
	if req, err = s.inheritPlanScope(ctx, req); err != nil {
		return domain.Job{}, false, err
	}
	if err := s.validate(ctx, req); err != nil {
		return domain.Job{}, false, err
	}
	if req.Type == domain.JobSync {
		// The gate comes first: an active dry-run sync must not answer a real one
		// that automation forbids.
		if !req.DryRun && !s.d.AutomationEnabled {
			return domain.Job{}, false, fmt.Errorf("real sync: %w", domain.ErrAutomationDisabled)
		}
		if j, ok, err := js.FindActiveJob(ctx, req.Provider, domain.JobSync); err != nil {
			return domain.Job{}, false, err
		} else if ok {
			return j, false, nil
		}
	}

	now := s.d.Now().UTC()
	id, err := newJobID(now)
	if err != nil {
		return domain.Job{}, false, err
	}
	job = domain.Job{
		ID:             id,
		Type:           req.Type,
		Status:         domain.JobQueued,
		Provider:       req.Provider,
		TaxonomyID:     req.Taxonomy,
		DryRun:         req.DryRun,
		Params:         req.Params,
		Scope:          req.Scope,
		FromDryRun:     req.FromDryRun,
		IdempotencyKey: req.IdempotencyKey,
		CreatedAt:      now,
	}
	if err := js.CreateJob(ctx, job); err != nil {
		return domain.Job{}, false, err
	}
	s.notify()
	return job, true, nil
}

func (s *Service) validate(ctx context.Context, req CreateRequest) error {
	classifies := false
	switch req.Type {
	case domain.JobSync, domain.JobBackfill, domain.JobReclassify:
		classifies = true
	case domain.JobSnapshot:
	case domain.JobRestore:
		if req.Params.SnapshotID == "" {
			return fmt.Errorf("restore needs snapshot_id: %w", domain.ErrValidation)
		}
	default:
		return fmt.Errorf("unknown job type %q: %w", req.Type, domain.ErrValidation)
	}
	if !slices.Contains(s.d.ProviderNames, req.Provider) {
		return fmt.Errorf("unknown provider %q: %w", req.Provider, domain.ErrValidation)
	}
	if req.Type == domain.JobReclassify && req.Scope.IsEmpty() {
		return fmt.Errorf("reclassify needs a scope: %w", domain.ErrValidation)
	}
	// Snapshots and restores don't classify, and §11 posts restores without a
	// taxonomy, so theirs is optional.
	if classifies || req.Taxonomy != "" {
		t, err := s.d.Store.Taxonomies().GetTaxonomy(ctx, req.Taxonomy)
		if errors.Is(err, domain.ErrNotFound) {
			return fmt.Errorf("unknown taxonomy %q: %w", req.Taxonomy, domain.ErrValidation)
		}
		if err != nil {
			return err
		}
		if classifies && !req.DryRun && t.Status == domain.TaxonomyDraft {
			return fmt.Errorf("taxonomy %q is a draft, so only dry runs can use it: %w", req.Taxonomy, domain.ErrConflict)
		}
	}
	if req.FromDryRun != "" {
		return s.validatePlan(ctx, req)
	}
	return nil
}

// inheritPlanScope gives an apply job with no scope its dry run's. An unknown dry
// run is left for validatePlan to report.
func (s *Service) inheritPlanScope(ctx context.Context, req CreateRequest) (CreateRequest, error) {
	if req.FromDryRun == "" || !req.Scope.IsEmpty() {
		return req, nil
	}
	plan, err := s.d.Store.Jobs().GetJob(ctx, req.FromDryRun)
	if errors.Is(err, domain.ErrNotFound) {
		return req, nil
	}
	if err != nil {
		return req, err
	}
	req.Scope = plan.Scope
	return req, nil
}

// sameScope compares scopes as sets of entries, spelled as given.
func sameScope(a, b domain.Scope) bool {
	same := func(x, y []string) bool {
		x, y = slices.Clone(x), slices.Clone(y)
		slices.Sort(x)
		slices.Sort(y)
		return slices.Equal(slices.Compact(x), slices.Compact(y))
	}
	return same(a.Collections, b.Collections) && same(a.Tags, b.Tags) && same(a.IDs, b.IDs)
}

// validatePlan checks that from_dry_run names a finished dry run this job can
// apply: a plan is only meaningful for its own type, provider, taxonomy, and
// scope, and §7.3 applies it as a whole.
func (s *Service) validatePlan(ctx context.Context, req CreateRequest) error {
	if req.DryRun {
		return fmt.Errorf("from_dry_run applies a plan, so it can't be a dry run: %w", domain.ErrValidation)
	}
	plan, err := s.d.Store.Jobs().GetJob(ctx, req.FromDryRun)
	if errors.Is(err, domain.ErrNotFound) {
		return fmt.Errorf("from_dry_run: unknown job %q: %w", req.FromDryRun, domain.ErrValidation)
	}
	if err != nil {
		return err
	}
	switch {
	case !plan.DryRun:
		return fmt.Errorf("from_dry_run: job %q is not a dry run: %w", plan.ID, domain.ErrValidation)
	case plan.Type != req.Type:
		return fmt.Errorf("from_dry_run: job %q is a %s: %w", plan.ID, plan.Type, domain.ErrValidation)
	case plan.Provider != req.Provider:
		return fmt.Errorf("from_dry_run: job %q is for provider %q: %w", plan.ID, plan.Provider, domain.ErrValidation)
	case plan.TaxonomyID != req.Taxonomy:
		return fmt.Errorf("from_dry_run: job %q used taxonomy %q: %w", plan.ID, plan.TaxonomyID, domain.ErrValidation)
	case !sameScope(plan.Scope, req.Scope):
		return fmt.Errorf("from_dry_run: scope differs from that of job %q: %w", plan.ID, domain.ErrValidation)
	case plan.Status != domain.JobSucceeded && plan.Status != domain.JobSucceededWithErrors:
		return fmt.Errorf("from_dry_run: job %q is %s: %w", plan.ID, plan.Status, domain.ErrConflict)
	}
	return nil
}

func (s *Service) Get(ctx context.Context, id string) (domain.Job, error) {
	return s.d.Store.Jobs().GetJob(ctx, id)
}

func (s *Service) List(ctx context.Context, cursor string, limit int) (domain.Page[domain.Job], error) {
	return s.d.Store.Jobs().ListJobs(ctx, cursor, limit)
}

// Cancel moves a queued job straight to canceled, and a running one to cancelling
// for the runner to finish. A finished job gives ErrConflict.
//
// Each write is guarded on the status just read, and a lost race reads again: the
// runner may claim or finish the job in between. A status only moves forward, so
// this ends. The guard doesn't cover other fields, so a write here can carry
// counters a page behind, but the runner's next save restores them from memory.
func (s *Service) Cancel(ctx context.Context, id string) (domain.Job, error) {
	js := s.d.Store.Jobs()
	for {
		j, err := js.GetJob(ctx, id)
		if err != nil {
			return domain.Job{}, err
		}
		from := j.Status
		switch from {
		case domain.JobQueued:
			now := s.d.Now().UTC()
			j.Status, j.FinishedAt = domain.JobCanceled, &now
		case domain.JobRunning:
			j.Status = domain.JobCancelling
		case domain.JobCancelling:
			return j, nil
		default:
			return domain.Job{}, fmt.Errorf("job %s already %s: %w", id, from, domain.ErrConflict)
		}
		ok, err := js.UpdateJobIf(ctx, j, from)
		if err != nil {
			return domain.Job{}, err
		}
		if ok {
			s.notify()
			return j, nil
		}
	}
}

func (s *Service) notify() {
	if s.Notifier != nil {
		s.Notifier()
	}
}

var idEncoding = base32.NewEncoding("0123456789ABCDEFGHJKMNPQRSTVWXYZ").WithPadding(base32.NoPadding)

// newJobID is time-prefixed, so IDs sort roughly by creation.
func newJobID(now time.Time) (string, error) {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], uint64(now.UnixMilli()))
	if _, err := rand.Read(b[8:]); err != nil {
		return "", fmt.Errorf("job id: %w", err)
	}
	return "job_" + idEncoding.EncodeToString(b[:]), nil
}

// newSnapshotID is time-prefixed in the same way.
func newSnapshotID(now time.Time) (string, error) {
	id, err := newJobID(now)
	if err != nil {
		return "", err
	}
	return "snap_" + strings.TrimPrefix(id, "job_"), nil
}
