package jobs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wikiabhi/rainwise/internal/adapters/store/memory"
	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

var t0 = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

func newTestStore(t *testing.T) ports.Store {
	t.Helper()
	s := memory.New()
	ctx := context.Background()
	for _, tx := range []domain.Taxonomy{
		{ID: "default", Status: domain.TaxonomyActive},
		{ID: "other", Status: domain.TaxonomyActive},
		{ID: "draft", Status: domain.TaxonomyDraft},
	} {
		if _, err := s.Taxonomies().SeedTaxonomy(ctx, tx); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func newTestService(s ports.Store, automation bool) *Service {
	return NewService(ServiceDeps{Store: s, AutomationEnabled: automation, ProviderNames: []string{"p"}, Now: func() time.Time { return t0 }})
}

func backfillReq() CreateRequest {
	return CreateRequest{Type: domain.JobBackfill, Provider: "p", Taxonomy: "default"}
}

func seedJob(t *testing.T, s ports.Store, j domain.Job) domain.Job {
	t.Helper()
	if j.Provider == "" {
		j.Provider = "p"
	}
	if j.TaxonomyID == "" {
		j.TaxonomyID = "default"
	}
	if j.Type == "" {
		j.Type = domain.JobBackfill
	}
	if j.CreatedAt.IsZero() {
		j.CreatedAt = t0
	}
	if err := s.Jobs().CreateJob(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	return j
}

func TestCreateValidation(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	svc := newTestService(s, true)
	seedJob(t, s, domain.Job{ID: "real", Status: domain.JobSucceeded})
	seedJob(t, s, domain.Job{ID: "dry-running", Status: domain.JobRunning, DryRun: true})
	seedJob(t, s, domain.Job{ID: "dry-failed", Status: domain.JobFailed, DryRun: true})
	seedJob(t, s, domain.Job{ID: "dry-ok", Status: domain.JobSucceeded, DryRun: true})
	seedJob(t, s, domain.Job{ID: "dry-errs", Status: domain.JobSucceededWithErrors, DryRun: true})
	seedJob(t, s, domain.Job{ID: "dry-sync", Type: domain.JobSync, Status: domain.JobSucceeded, DryRun: true})
	seedJob(t, s, domain.Job{ID: "dry-other-tax", TaxonomyID: "other", Status: domain.JobSucceeded, DryRun: true})
	seedJob(t, s, domain.Job{ID: "dry-other-prov", Provider: "q", Status: domain.JobSucceeded, DryRun: true})

	mod := func(f func(*CreateRequest)) CreateRequest {
		r := backfillReq()
		f(&r)
		return r
	}
	tests := []struct {
		name string
		req  CreateRequest
		want error
	}{
		{"unknown type", mod(func(r *CreateRequest) { r.Type = "nope" }), domain.ErrValidation},
		{"unknown provider", mod(func(r *CreateRequest) { r.Provider = "nope" }), domain.ErrValidation},
		{"empty provider", mod(func(r *CreateRequest) { r.Provider = "" }), domain.ErrValidation},
		{"unknown taxonomy", mod(func(r *CreateRequest) { r.Taxonomy = "nope" }), domain.ErrValidation},
		{"classifying job without taxonomy", mod(func(r *CreateRequest) { r.Taxonomy = "" }), domain.ErrValidation},
		{"snapshot with unknown taxonomy", CreateRequest{Type: domain.JobSnapshot, Provider: "p", Taxonomy: "nope"}, domain.ErrValidation},
		{"reclassify empty scope", mod(func(r *CreateRequest) { r.Type = domain.JobReclassify }), domain.ErrValidation},
		{"restore without snapshot", CreateRequest{Type: domain.JobRestore, Provider: "p"}, domain.ErrValidation},
		{"from_dry_run missing job", mod(func(r *CreateRequest) { r.FromDryRun = "nope" }), domain.ErrValidation},
		{"from_dry_run real job", mod(func(r *CreateRequest) { r.FromDryRun = "real" }), domain.ErrValidation},
		{"from_dry_run running job", mod(func(r *CreateRequest) { r.FromDryRun = "dry-running" }), domain.ErrConflict},
		{"from_dry_run failed job", mod(func(r *CreateRequest) { r.FromDryRun = "dry-failed" }), domain.ErrConflict},
		{"from_dry_run other type", mod(func(r *CreateRequest) { r.FromDryRun = "dry-sync" }), domain.ErrValidation},
		{"from_dry_run other taxonomy", mod(func(r *CreateRequest) { r.FromDryRun = "dry-other-tax" }), domain.ErrValidation},
		{"from_dry_run other provider", mod(func(r *CreateRequest) { r.FromDryRun = "dry-other-prov" }), domain.ErrValidation},
		{"from_dry_run on a dry run", mod(func(r *CreateRequest) { r.FromDryRun = "dry-ok"; r.DryRun = true }), domain.ErrValidation},
		{"from_dry_run ok", mod(func(r *CreateRequest) { r.FromDryRun = "dry-ok" }), nil},
		{"from_dry_run ok with errors", mod(func(r *CreateRequest) { r.FromDryRun = "dry-errs" }), nil},
		{"reclassify with scope", mod(func(r *CreateRequest) {
			r.Type = domain.JobReclassify
			r.Scope = domain.Scope{Tags: []string{"x"}}
		}), nil},
		{"tags_only on a backfill", mod(func(r *CreateRequest) { r.Params.TagsOnly = true }), domain.ErrValidation},
		{"propose_only on a real backfill", mod(func(r *CreateRequest) { r.Params.ProposeOnly = true }), domain.ErrValidation},
		{"propose_only on a dry-run sync", mod(func(r *CreateRequest) {
			r.Type = domain.JobSync
			r.DryRun = true
			r.Params.ProposeOnly = true
		}), domain.ErrValidation},
		{"propose_only on a dry-run reclassify", mod(func(r *CreateRequest) {
			r.Type = domain.JobReclassify
			r.DryRun = true
			r.Scope = domain.Scope{Tags: []string{"x"}}
			r.Params.ProposeOnly = true
		}), domain.ErrValidation},
		{"propose_only on a dry-run backfill", mod(func(r *CreateRequest) { r.DryRun = true; r.Params.ProposeOnly = true }), nil},
		{"restore needs no taxonomy", CreateRequest{Type: domain.JobRestore, Provider: "p", Params: domain.JobParams{SnapshotID: "s1"}}, nil},
		{"snapshot needs no taxonomy", CreateRequest{Type: domain.JobSnapshot, Provider: "p"}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j, created, err := svc.Create(ctx, tt.req)
			if !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
			if tt.want != nil {
				return
			}
			if !created || j.Status != domain.JobQueued || j.ID == "" || !j.CreatedAt.Equal(t0) {
				t.Fatalf("job = %+v, created = %v", j, created)
			}
		})
	}
}

func TestCreateStoresRequest(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	woken := 0
	svc := newTestService(s, true)
	svc.Notifier = func() { woken++ }
	req := CreateRequest{Type: domain.JobReclassify, Provider: "p", Taxonomy: "default", DryRun: true,
		Params: domain.JobParams{Force: true}, Scope: domain.Scope{IDs: []string{"p:1"}}, IdempotencyKey: "k"}
	j, _, err := svc.Create(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	got, err := svc.Get(ctx, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.TaxonomyID != "default" || !got.DryRun || !got.Params.Force || got.Scope.IDs[0] != "p:1" || got.IdempotencyKey != "k" {
		t.Errorf("stored = %+v", got)
	}
	if woken != 1 {
		t.Errorf("Notifier called %d times, want 1", woken)
	}
	page, err := svc.List(ctx, "", 10)
	if err != nil || len(page.Items) != 1 {
		t.Errorf("List = %+v, %v", page, err)
	}
}

func TestDraftTaxonomy(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(newTestStore(t), true)
	req := backfillReq()
	req.Taxonomy = "draft"
	if _, _, err := svc.Create(ctx, req); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("real backfill on draft: err = %v, want ErrConflict", err)
	}
	req.DryRun = true
	if _, created, err := svc.Create(ctx, req); err != nil || !created {
		t.Fatalf("dry run on draft: created = %v, err = %v", created, err)
	}
}

func TestAutomationGate(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		req  CreateRequest
		want error
	}{
		{"real sync", CreateRequest{Type: domain.JobSync, Provider: "p", Taxonomy: "default"}, domain.ErrAutomationDisabled},
		{"dry sync", CreateRequest{Type: domain.JobSync, Provider: "p", Taxonomy: "default", DryRun: true}, nil},
		{"real backfill", backfillReq(), nil},
		{"reclassify", CreateRequest{Type: domain.JobReclassify, Provider: "p", Taxonomy: "default", Scope: domain.Scope{Tags: []string{"x"}}}, nil},
		{"snapshot", CreateRequest{Type: domain.JobSnapshot, Provider: "p"}, nil},
		{"restore", CreateRequest{Type: domain.JobRestore, Provider: "p", Params: domain.JobParams{SnapshotID: "s1"}}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := newTestService(newTestStore(t), false)
			if _, _, err := svc.Create(ctx, tt.req); !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestAutomationGateBeatsDedup(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	svc := newTestService(s, false)
	if _, _, err := svc.Create(ctx, CreateRequest{Type: domain.JobSync, Provider: "p", Taxonomy: "default", DryRun: true}); err != nil {
		t.Fatal(err)
	}
	_, _, err := svc.Create(ctx, CreateRequest{Type: domain.JobSync, Provider: "p", Taxonomy: "default"})
	if !errors.Is(err, domain.ErrAutomationDisabled) {
		t.Fatalf("err = %v, want ErrAutomationDisabled", err)
	}
}

func TestSyncDedup(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	svc := newTestService(s, true)
	req := CreateRequest{Type: domain.JobSync, Provider: "p", Taxonomy: "default"}
	first, created, err := svc.Create(ctx, req)
	if err != nil || !created {
		t.Fatalf("first: created = %v, err = %v", created, err)
	}
	again, created, err := svc.Create(ctx, req)
	if err != nil || created || again.ID != first.ID {
		t.Fatalf("queued dup: id = %s want %s, created = %v, err = %v", again.ID, first.ID, created, err)
	}

	first.Status = domain.JobRunning
	if err := s.Jobs().UpdateJob(ctx, first); err != nil {
		t.Fatal(err)
	}
	again, created, err = svc.Create(ctx, req)
	if err != nil || created || again.ID != first.ID {
		t.Fatalf("running dup: id = %s want %s, created = %v, err = %v", again.ID, first.ID, created, err)
	}

	first.Status = domain.JobSucceeded
	if err := s.Jobs().UpdateJob(ctx, first); err != nil {
		t.Fatal(err)
	}
	next, created, err := svc.Create(ctx, req)
	if err != nil || !created || next.ID == first.ID {
		t.Fatalf("after finish: id = %s, created = %v, err = %v", next.ID, created, err)
	}
}

func TestIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	woken := 0
	svc := newTestService(newTestStore(t), true)
	svc.Notifier = func() { woken++ }
	req := backfillReq()
	req.IdempotencyKey = "k1"
	first, created, err := svc.Create(ctx, req)
	if err != nil || !created {
		t.Fatalf("first: created = %v, err = %v", created, err)
	}
	again, created, err := svc.Create(ctx, req)
	if err != nil || created || again.ID != first.ID {
		t.Fatalf("repeat: id = %s want %s, created = %v, err = %v", again.ID, first.ID, created, err)
	}
	if woken != 1 {
		t.Errorf("Notifier called %d times, want 1", woken)
	}
	req.IdempotencyKey = "k2"
	other, created, err := svc.Create(ctx, req)
	if err != nil || !created || other.ID == first.ID {
		t.Fatalf("other key: id = %s, created = %v, err = %v", other.ID, created, err)
	}
}

func TestCancel(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		from, want domain.JobStatus
		finished   bool
		err        error
	}{
		{from: domain.JobRunning, want: domain.JobCancelling},
		{from: domain.JobCancelling, want: domain.JobCancelling},
		{from: domain.JobQueued, want: domain.JobCanceled, finished: true},
		{from: domain.JobSucceeded, err: domain.ErrConflict},
		{from: domain.JobSucceededWithErrors, err: domain.ErrConflict},
		{from: domain.JobFailed, err: domain.ErrConflict},
		{from: domain.JobCanceled, err: domain.ErrConflict},
	}
	for _, tt := range tests {
		t.Run(string(tt.from), func(t *testing.T) {
			s := newTestStore(t)
			svc := newTestService(s, true)
			seedJob(t, s, domain.Job{ID: "j", Status: tt.from})
			got, err := svc.Cancel(ctx, "j")
			if !errors.Is(err, tt.err) {
				t.Fatalf("err = %v, want %v", err, tt.err)
			}
			if tt.err != nil {
				return
			}
			stored, _ := s.Jobs().GetJob(ctx, "j")
			if got.Status != tt.want || stored.Status != tt.want {
				t.Errorf("status returned %s, stored %s, want %s", got.Status, stored.Status, tt.want)
			}
			if (stored.FinishedAt != nil) != tt.finished {
				t.Errorf("FinishedAt = %v, want set = %v", stored.FinishedAt, tt.finished)
			}
		})
	}
	t.Run("missing", func(t *testing.T) {
		if _, err := newTestService(newTestStore(t), true).Cancel(ctx, "nope"); !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	})
}

// racingStore changes the job's row right after Cancel reads it, to simulate the
// runner moving the job on between Cancel's read and its write.
type racingStore struct {
	ports.Store
	once func()
}

func (r *racingStore) Jobs() ports.JobStore { return &racingJobs{JobStore: r.Store.Jobs(), r: r} }

type racingJobs struct {
	ports.JobStore
	r *racingStore
}

func (j *racingJobs) GetJob(ctx context.Context, id string) (domain.Job, error) {
	got, err := j.JobStore.GetJob(ctx, id)
	if f := j.r.once; f != nil {
		j.r.once = nil
		f()
	}
	return got, err
}

func TestCancelRaces(t *testing.T) {
	ctx := context.Background()
	flip := func(s ports.Store, to domain.JobStatus) func() {
		return func() {
			j, _ := s.Jobs().GetJob(ctx, "j")
			j.Status = to
			_ = s.Jobs().UpdateJob(ctx, j)
		}
	}
	t.Run("job finishes first", func(t *testing.T) {
		s := newTestStore(t)
		seedJob(t, s, domain.Job{ID: "j", Status: domain.JobRunning})
		svc := newTestService(&racingStore{Store: s, once: flip(s, domain.JobSucceeded)}, true)
		if _, err := svc.Cancel(ctx, "j"); !errors.Is(err, domain.ErrConflict) {
			t.Fatalf("err = %v, want ErrConflict", err)
		}
		if j, _ := s.Jobs().GetJob(ctx, "j"); j.Status != domain.JobSucceeded {
			t.Fatalf("status = %s, a finished job must stay finished", j.Status)
		}
	})
	t.Run("queued job gets claimed first", func(t *testing.T) {
		s := newTestStore(t)
		seedJob(t, s, domain.Job{ID: "j", Status: domain.JobQueued})
		svc := newTestService(&racingStore{Store: s, once: flip(s, domain.JobRunning)}, true)
		got, err := svc.Cancel(ctx, "j")
		if err != nil || got.Status != domain.JobCancelling {
			t.Fatalf("got %s, err = %v, want cancelling", got.Status, err)
		}
	})
}
