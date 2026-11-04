package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/wikiabhi/rainwise/internal/core/pipeline"
	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

const defaultSnapshotRetention = 10

type SnapshotDeps struct {
	Provider  ports.Provider
	Store     ports.Store
	Now       func() time.Time
	Retention int // How many snapshots to keep; 10 if unset (§11).
}

// SnapshotService takes and exports snapshots (§11).
type SnapshotService struct{ d SnapshotDeps }

func NewSnapshotService(d SnapshotDeps) *SnapshotService {
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.Retention <= 0 {
		d.Retention = defaultSnapshotRetention
	}
	return &SnapshotService{d: d}
}

// Take records every bookmark in scope and the collection list, along with what
// protects them from later jobs: each one's pin and, for the whole library, the
// first-backfill setting. Then it prunes to retention. It takes no taxonomy, so scope must not name collections by taxonomy
// key; see scopeMatcher.resolvedScope.
//
// The items are gathered first and stored in one call, so a failed listing leaves
// no snapshot behind, and one that exists can't be missing part of a page.
func (s *SnapshotService) Take(ctx context.Context, provider string, scope domain.Scope, jobID string) (string, error) {
	if provider != s.d.Provider.Name() {
		return "", fmt.Errorf("snapshot of provider %q, this one is %q: %w", provider, s.d.Provider.Name(), domain.ErrValidation)
	}
	lookup := Deps{Provider: s.d.Provider}
	match, err := lookup.newScopeMatcher(ctx, scope, domain.Taxonomy{})
	if err != nil {
		return "", err
	}
	collections, err := lookup.listCollections(ctx)
	if err != nil {
		return "", err
	}
	now := s.d.Now().UTC()
	var firstBackfill string
	if scope.IsEmpty() {
		if firstBackfill, _, err = s.d.Store.BookmarkStates().GetSetting(ctx, firstBackfillPrefix+provider); err != nil {
			return "", err
		}
	}
	items, err := s.scopeItems(ctx, match, now)
	if err != nil {
		return "", err
	}
	id, err := newSnapshotID(now)
	if err != nil {
		return "", err
	}
	snaps := s.d.Store.Snapshots()
	if err := snaps.CreateSnapshot(ctx, domain.Snapshot{
		ID:                id,
		Provider:          provider,
		JobID:             jobID,
		CreatedAt:         now,
		Collections:       collections,
		ScopeAll:          scope.IsEmpty(),
		FirstBackfillDone: firstBackfill,
	}); err != nil {
		return "", fmt.Errorf("store snapshot: %w", err)
	}
	if err := snaps.AddSnapshotItems(ctx, id, items); err != nil {
		return "", fmt.Errorf("store snapshot items: %w", err)
	}
	// The snapshot is safe by now, so a failure to prune only leaves an extra one.
	if err := s.prune(ctx, id); err != nil {
		slog.Warn("pruning snapshots", "error", err)
	}
	return id, nil
}

func (s *SnapshotService) scopeItems(ctx context.Context, match scopeMatcher, now time.Time) ([]domain.SnapshotItem, error) {
	var items []domain.SnapshotItem
	seen := map[domain.ID]bool{} // A library changing underneath page numbers can repeat one.
	for token := ""; ; {
		page, err := s.d.Provider.ListBookmarks(ctx, ports.ListQuery{PageToken: token})
		if err != nil {
			return nil, fmt.Errorf("list bookmarks: %w", err)
		}
		for _, b := range page.Items {
			if match.matches(b) && !seen[b.ID] {
				seen[b.ID] = true
				pin, pinnedAt, err := s.pinOf(ctx, b, now)
				if err != nil {
					return nil, err
				}
				items = append(items, domain.SnapshotItem{Bookmark: b.ID, Collection: b.Collection, Tags: b.Tags, PinSource: pin, PinnedAt: pinnedAt})
			}
		}
		if page.NextToken == "" {
			return items, nil
		}
		token = page.NextToken
	}
}

// pinOf is the bookmark's pin as of now. A manual move no job has read yet would be
// pinned by the next one, so it counts: without that, restoring this snapshot would
// put the bookmark back with nothing protecting it. The stored state isn't changed.
func (s *SnapshotService) pinOf(ctx context.Context, b domain.Bookmark, now time.Time) (domain.PinSource, *time.Time, error) {
	st, known, err := s.d.Store.BookmarkStates().GetState(ctx, b.ID)
	if err != nil {
		return "", nil, fmt.Errorf("load state of %s: %w", b.ID, err)
	}
	if !known {
		return domain.PinNone, nil, nil
	}
	pipeline.DetectManualMove(&st, b, now)
	return st.PinSource, st.PinnedAt, nil
}

// prune keeps the newest snapshots, the one just taken, and any a job not yet
// finished refers to: a queued restore is still going to need its target.
func (s *SnapshotService) prune(ctx context.Context, taken string) error {
	protect := []string{taken}
	for cursor := ""; ; {
		page, err := s.d.Store.Jobs().ListJobs(ctx, cursor, recoverPageSize)
		if err != nil {
			return err
		}
		for _, j := range page.Items {
			switch j.Status {
			case domain.JobQueued, domain.JobRunning, domain.JobCancelling:
				for _, id := range []string{j.SnapshotID, j.Params.SnapshotID} {
					if id != "" {
						protect = append(protect, id)
					}
				}
			}
		}
		if page.NextToken == "" {
			break
		}
		cursor = page.NextToken
	}
	return s.d.Store.Snapshots().PruneSnapshots(ctx, s.d.Retention, protect)
}

// Export streams a snapshot as one JSON object: its fields, plus "items".
func (s *SnapshotService) Export(ctx context.Context, id string, w io.Writer) error {
	snaps := s.d.Store.Snapshots()
	snap, err := snaps.GetSnapshot(ctx, id)
	if err != nil {
		return err
	}
	head, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}
	// head is an object with fields, so it ends in "}" and can take another.
	if _, err := w.Write(append(head[:len(head)-1], `,"items":[`...)); err != nil {
		return err
	}
	first := true
	for cursor := ""; ; {
		page, err := snaps.ListSnapshotItems(ctx, id, cursor, decisionPageSize)
		if err != nil {
			return err
		}
		for _, it := range page.Items {
			raw, err := json.Marshal(it)
			if err != nil {
				return fmt.Errorf("encode snapshot item: %w", err)
			}
			if !first {
				raw = append([]byte{','}, raw...)
			}
			first = false
			if _, err := w.Write(raw); err != nil {
				return err
			}
		}
		if page.NextToken == "" {
			break
		}
		cursor = page.NextToken
	}
	_, err = io.WriteString(w, "]}\n")
	return err
}

// listCollections returns the provider's collections, or none if it can't list them.
func (d Deps) listCollections(ctx context.Context) ([]domain.Collection, error) {
	m := d.manager()
	if m == nil {
		return nil, nil
	}
	cols, err := m.ListCollections(ctx)
	if errors.Is(err, domain.ErrCapabilityUnsupported) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list collections: %w", err)
	}
	return cols, nil
}

// snapshotJob is a snapshot on request (§11). A dry run only counts what it would
// cover.
type snapshotJob struct{ Deps }

func (h snapshotJob) Run(ctx context.Context, job *domain.Job, rc RunContext) error {
	d := h.Deps
	d.startTime(job)
	var t domain.Taxonomy
	if job.TaxonomyID != "" {
		var err error
		if t, err = d.Store.Taxonomies().GetTaxonomy(ctx, job.TaxonomyID); err != nil {
			return fmt.Errorf("load taxonomy %q: %w", job.TaxonomyID, err)
		}
	}
	match, err := d.newScopeMatcher(ctx, job.Scope, t)
	if err != nil {
		return err
	}
	if job.DryRun {
		scan, err := d.scanLibrary(ctx, job, rc, match, false)
		if err != nil {
			return err
		}
		job.Progress.Total = scan.matched
		return nil
	}
	if err := d.takeSnapshot(ctx, job, rc, match.resolvedScope(job.Scope)); err != nil {
		return err
	}
	n, err := d.countSnapshotItems(ctx, job.SnapshotID)
	if err != nil {
		return err
	}
	job.Progress.Total, job.Progress.Processed = n, n
	return nil
}

func (d Deps) countSnapshotItems(ctx context.Context, id string) (int, error) {
	n := 0
	for cursor := ""; ; {
		page, err := d.Store.Snapshots().ListSnapshotItems(ctx, id, cursor, decisionPageSize)
		if err != nil {
			return 0, fmt.Errorf("read snapshot %s: %w", id, err)
		}
		n += len(page.Items)
		if page.NextToken == "" {
			return n, nil
		}
		cursor = page.NextToken
	}
}
