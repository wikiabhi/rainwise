package jobs

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	cfake "github.com/wikiabhi/rainwise/internal/adapters/classifier/fake"
	pfake "github.com/wikiabhi/rainwise/internal/adapters/provider/fake"
	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

const firstBackfillKey = "first_backfill_done:fake"

func (e *jobEnv) backfill(mod func(*domain.Job)) *domain.Job {
	e.t.Helper()
	job := e.newJob(domain.JobBackfill, mod)
	e.mustRun(job)
	return job
}

func dryRun(j *domain.Job) { j.DryRun = true }

func (e *jobEnv) decisions(jobID string) []domain.DecisionRecord {
	e.t.Helper()
	page, err := e.store.Jobs().ListDecisions(context.Background(), jobID, "", 1000)
	if err != nil {
		e.t.Fatal(err)
	}
	return page.Items
}

func (e *jobEnv) collectionNames() []string {
	e.t.Helper()
	cols, err := e.provider.ListCollections(context.Background())
	if err != nil {
		e.t.Fatal(err)
	}
	var names []string
	for _, c := range cols {
		names = append(names, c.Name)
	}
	return names
}

func TestBackfillDryRunNoChanges(t *testing.T) {
	e := newJobEnv(t)
	tx := fixedTaxonomy()
	tx.Collections = append(tx.Collections, domain.TaxonomyCollection{Key: "c", Name: "Gamma"})
	e.seedTaxonomy(tx)
	e.addBookmark(1, pfake.Inbox)
	e.addBookmark(2, colBeta)
	collections := e.collectionNames()

	job := e.backfill(dryRun)

	if len(e.provider.Writes()) != 0 {
		t.Errorf("writes = %+v", e.provider.Writes())
	}
	if len(e.snaps.calls) != 0 {
		t.Errorf("snapshots = %+v", e.snaps.calls)
	}
	if len(e.sleeps) != 0 {
		t.Errorf("waited %v", e.sleeps)
	}
	if got := e.collectionNames(); !slices.Equal(got, collections) {
		t.Errorf("collections = %v", got)
	}
	if len(e.decisions(job.ID)) != 2 {
		t.Errorf("decisions = %+v", e.decisions(job.ID))
	}
	if _, ok := e.setting(firstBackfillKey); ok {
		t.Error("first-backfill setting set by a dry run")
	}
	if job.SnapshotID != "" {
		t.Errorf("snapshot id = %q", job.SnapshotID)
	}
	if job.Progress.Total != 2 {
		t.Errorf("progress = %+v", job.Progress)
	}
}

func TestBackfillSequence(t *testing.T) {
	t.Run("snapshot, wait, provision, process", func(t *testing.T) {
		e := newJobEnv(t)
		tx := fixedTaxonomy()
		tx.Collections = append(tx.Collections, domain.TaxonomyCollection{Key: "c", Name: "Gamma"})
		e.seedTaxonomy(tx)
		e.addBookmark(1, pfake.Inbox)

		note := func(what string) {
			e.mu.Lock()
			defer e.mu.Unlock()
			e.order = append(e.order, what)
		}
		e.snaps.on = func() {
			if slices.Contains(e.collectionNames(), "Gamma") {
				t.Error("provisioned before the snapshot")
			}
			note("snapshot")
		}
		e.cls.OnClassify(func(ports.ClassifyRequest) {
			if !slices.Contains(e.collectionNames(), "Gamma") {
				t.Error("classified before provisioning")
			}
			note("classify")
		})
		sleep := e.deps.Sleep
		e.deps.Sleep = func(ctx context.Context, d time.Duration) error {
			if slices.Contains(e.collectionNames(), "Gamma") {
				t.Error("provisioned before the durable wait")
			}
			return sleep(ctx, d)
		}

		job := e.backfill(nil)

		if want := []string{"snapshot", "wait", "classify"}; !slices.Equal(e.order, want) {
			t.Errorf("order = %v, want %v", e.order, want)
		}
		if !slices.Equal(e.sleeps, []time.Duration{30 * time.Second}) {
			t.Errorf("sleeps = %v", e.sleeps)
		}
		if job.SnapshotID != "snap-"+job.ID {
			t.Errorf("snapshot id = %q", job.SnapshotID)
		}
		if saved := e.reload(job.ID); saved.SnapshotID != job.SnapshotID {
			t.Errorf("snapshot id not saved: %q", saved.SnapshotID)
		}
	})

	t.Run("snapshot failure means no writes", func(t *testing.T) {
		e := newJobEnv(t)
		tx := fixedTaxonomy()
		tx.Collections = append(tx.Collections, domain.TaxonomyCollection{Key: "c", Name: "Gamma"})
		e.seedTaxonomy(tx)
		e.addBookmark(1, pfake.Inbox)
		e.snaps.err = errors.New("disk full")
		job := e.newJob(domain.JobBackfill, nil)
		if err := e.runJob(job); err == nil {
			t.Fatal("want an error")
		}
		if len(e.provider.Writes()) != 0 || e.cls.Calls() != 0 {
			t.Errorf("writes = %+v, calls = %d", e.provider.Writes(), e.cls.Calls())
		}
		if slices.Contains(e.collectionNames(), "Gamma") {
			t.Error("provisioned after a failed snapshot")
		}
	})

	t.Run("created collections are recorded on the snapshot", func(t *testing.T) {
		e := newJobEnv(t)
		tx := fixedTaxonomy()
		tx.Collections = append(tx.Collections, domain.TaxonomyCollection{Key: "c", Name: "Gamma"})
		e.seedTaxonomy(tx)
		e.backfill(nil)
		snap, err := e.store.Snapshots().GetSnapshot(context.Background(), "snap-job1")
		if err != nil {
			t.Fatal(err)
		}
		if len(snap.CreatedCollections) != 1 {
			t.Errorf("created = %+v", snap.CreatedCollections)
		}
	})

	t.Run("a stop during the wait resumes without a second snapshot", func(t *testing.T) {
		e := newJobEnv(t)
		e.addBookmark(1, pfake.Inbox)
		stopping := make(chan struct{})
		e.deps.Sleep = func(ctx context.Context, _ time.Duration) error {
			close(stopping)
			<-ctx.Done()
			return ctx.Err()
		}
		job := e.newJob(domain.JobBackfill, nil)
		err := Handlers(e.deps)[domain.JobBackfill].Run(context.Background(), job, RunContext{
			Checkpoint: func(ctx context.Context) error { return e.store.Jobs().UpdateJob(context.WithoutCancel(ctx), *job) },
			Stopping:   stopping,
		})
		if !errors.Is(err, ErrInterrupted) {
			t.Fatalf("err = %v", err)
		}
		if len(e.provider.Writes()) != 0 || e.cls.Calls() != 0 {
			t.Error("worked past an unfinished wait")
		}
		slept := 0
		e.deps.Sleep = func(context.Context, time.Duration) error { slept++; return nil }
		saved := e.reload(job.ID)
		e.mustRun(saved)
		if len(e.snaps.calls) != 1 {
			t.Errorf("snapshots = %d", len(e.snaps.calls))
		}
		if slept != 1 {
			t.Errorf("waits on resume = %d", slept)
		}
		if len(e.provider.Writes()) != 1 {
			t.Errorf("writes = %+v", e.provider.Writes())
		}
	})
}

func TestBackfillMissingCollectionsNoProvision(t *testing.T) {
	e := newJobEnv(t)
	tx := fixedTaxonomy()
	tx.ProvisionMissing = false
	tx.Collections = append(tx.Collections, domain.TaxonomyCollection{Key: "c", Name: "Gamma"})
	e.seedTaxonomy(tx)
	e.addBookmark(1, pfake.Inbox)
	job := e.newJob(domain.JobBackfill, nil)
	if err := e.runJob(job); !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("err = %v", err)
	}
	if len(e.provider.Writes()) != 0 || e.cls.Calls() != 0 || len(e.snaps.calls) != 0 {
		t.Errorf("writes %+v, calls %d, snapshots %d", e.provider.Writes(), e.cls.Calls(), len(e.snaps.calls))
	}
	if slices.Contains(e.collectionNames(), "Gamma") {
		t.Error("created a collection")
	}
}

func TestBackfillNoCollectionSupportFailsBeforeWrites(t *testing.T) {
	e := newJobEnv(t)
	caps := e.provider.Capabilities()
	caps.CreateCollections = false
	e.provider.SetCapabilities(caps)
	e.addBookmark(1, pfake.Inbox)
	job := e.newJob(domain.JobBackfill, nil)
	if err := e.runJob(job); !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("err = %v", err)
	}
	if len(e.snaps.calls) != 0 {
		t.Error("took a snapshot")
	}
}

func TestBackfillResume(t *testing.T) {
	e := newJobEnv(t)
	for i := 1; i <= 4; i++ {
		e.addBookmark(i, pfake.Inbox)
	}
	job := e.newJob(domain.JobBackfill, func(j *domain.Job) { j.Params.BatchSize = 2 })
	// Saves in order: the snapshot id, the start of processing, page 1.
	err := Handlers(e.deps)[domain.JobBackfill].Run(context.Background(), job, e.stopAfter(job, 3))
	if !errors.Is(err, ErrInterrupted) {
		t.Fatalf("err = %v", err)
	}
	if got := e.cls.Calls(); got != 2 {
		t.Fatalf("calls after page 1 = %d", got)
	}
	saved := e.reload(job.ID)
	if saved.Checkpoint.PageToken != "2" || saved.Progress.Processed != 2 {
		t.Fatalf("saved = %+v %+v", saved.Checkpoint, saved.Progress)
	}

	e.mustRun(saved)

	if got := e.cls.Calls(); got != 4 {
		t.Errorf("calls = %d, want 4", got)
	}
	reqs := e.cls.Requests()
	for _, r := range reqs[2:] {
		if r.Subject.Bookmark.ID == bmID(1) || r.Subject.Bookmark.ID == bmID(2) {
			t.Errorf("page 1 classified again: %s", r.Subject.Bookmark.ID)
		}
	}
	if saved.Progress.Processed != 4 || saved.Progress.Total != 4 {
		t.Errorf("progress = %+v", saved.Progress)
	}
	if len(e.snaps.calls) != 1 || saved.SnapshotID != job.SnapshotID {
		t.Errorf("snapshots = %d, id %q", len(e.snaps.calls), saved.SnapshotID)
	}
}

func TestBackfillFailureLeavesResumableCheckpoint(t *testing.T) {
	e := newJobEnv(t)
	e.addBookmark(1, pfake.Inbox)
	job := e.backfill(nil)
	saved := e.reload(job.ID)
	processed := saved.Progress.Processed
	calls := e.cls.Calls()
	e.logged.bookmarks = nil
	// A rerun after the work is done, as if the final write was lost.
	e.mustRun(saved)
	if saved.Progress.Processed != processed || e.cls.Calls() != calls || len(e.logged.bookmarks) != 0 {
		t.Errorf("redid work: %+v", saved.Progress)
	}
}

func TestFromDryRun(t *testing.T) {
	setup := func(t *testing.T) (*jobEnv, domain.ID, *domain.Job) {
		e := newJobEnv(t)
		id := e.addBookmark(1, pfake.Inbox, "mine")
		dry := e.backfill(dryRun)
		e.setNow(t0.Add(time.Hour))
		return e, id, dry
	}
	apply := func(dry *domain.Job, mod func(*domain.Job)) func(*domain.Job) {
		return func(j *domain.Job) {
			j.FromDryRun = dry.ID
			if mod != nil {
				mod(j)
			}
		}
	}

	t.Run("uses saved decisions", func(t *testing.T) {
		e, id, dry := setup(t)
		calls := e.cls.Calls()
		job := e.backfill(apply(dry, nil))
		if e.cls.Calls() != calls {
			t.Errorf("classifier calls = %d, want %d", e.cls.Calls(), calls)
		}
		if got := e.collectionOf(id); got != colAlpha {
			t.Errorf("collection = %v", got)
		}
		if job.Checkpoint.PinCutoff == nil || !job.Checkpoint.PinCutoff.Equal(*dry.StartedAt) {
			t.Errorf("cutoff = %v, want dry run's start %v", job.Checkpoint.PinCutoff, dry.StartedAt)
		}
	})

	t.Run("a move after the dry run is honored", func(t *testing.T) {
		e, id, dry := setup(t)
		e.provider.SetCollection(id, colBeta)
		e.backfill(apply(dry, nil))
		if got := e.collectionOf(id); got != colBeta {
			t.Errorf("collection = %v", got)
		}
		if st := e.state(id); st.PinSource != domain.PinUser {
			t.Errorf("pin = %q", st.PinSource)
		}
	})

	t.Run("a changed bookmark is classified again", func(t *testing.T) {
		e, id, dry := setup(t)
		b, _ := e.provider.Bookmark(id)
		b.Title = "Something else"
		e.provider.Add(b)
		calls := e.cls.Calls()
		e.backfill(apply(dry, nil))
		if e.cls.Calls() != calls+1 {
			t.Errorf("classifier calls = %d, want %d", e.cls.Calls(), calls+1)
		}
	})

	t.Run("a plan made before its collections existed applies", func(t *testing.T) {
		e := newJobEnv(t)
		tx := fixedTaxonomy()
		tx.Collections = append(tx.Collections, domain.TaxonomyCollection{Key: "c", Name: "Gamma"})
		e.seedTaxonomy(tx)
		id := e.addBookmark(1, pfake.Inbox)
		e.cls.Script(cfake.Result{Decision: classified("c", 0.9)})
		dry := e.backfill(dryRun)
		recs := e.decisions(dry.ID)
		if len(recs) != 1 || recs[0].TargetCollection == nil {
			t.Fatalf("plan = %+v", recs)
		}
		if dry.Progress.NeedsReview != 0 || dry.Progress.Changed != 1 {
			t.Errorf("dry run reported %+v", dry.Progress)
		}
		calls := e.cls.Calls()
		e.backfill(apply(dry, nil))
		if e.cls.Calls() != calls {
			t.Errorf("classified again: %d", e.cls.Calls())
		}
		got := e.collectionOf(id)
		found := false
		cols, _ := e.provider.ListCollections(context.Background())
		for _, c := range cols {
			if c.ID == got && c.Name == "Gamma" {
				found = true
			}
		}
		if !found {
			t.Errorf("collection = %v, not the provisioned Gamma", got)
		}
	})

	t.Run("a plan target its taxonomy no longer has is classified again", func(t *testing.T) {
		e := newJobEnv(t)
		tx := fixedTaxonomy()
		tx.Collections = append(tx.Collections, domain.TaxonomyCollection{Key: "c", Name: "Gamma"})
		e.seedTaxonomy(tx)
		e.addBookmark(1, pfake.Inbox)
		e.cls.Script(cfake.Result{Decision: classified("c", 0.9)})
		dry := e.backfill(dryRun)
		e.seedTaxonomy(fixedTaxonomy())
		e.cls.Script(cfake.Result{Decision: classified("a", 0.9)})
		calls := e.cls.Calls()
		e.backfill(apply(dry, nil))
		if e.cls.Calls() != calls+1 {
			t.Errorf("calls = %d, want %d", e.cls.Calls(), calls+1)
		}
		if got := e.collectionOf(bmID(1)); got != colAlpha {
			t.Errorf("collection = %v", got)
		}
	})
}

func TestProposedTagsAndApproval(t *testing.T) {
	e := newJobEnv(t)
	first := e.addBookmark(1, pfake.Inbox, "go")
	second := e.addBookmark(2, pfake.Inbox)
	e.cls.Script(cfake.Result{Decision: classified("a", 0.9, "go", "ai-agents", "postgres", "unapproved")})

	dry := e.backfill(dryRun)

	got, err := ProposedTags(context.Background(), e.store, dry.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := []ProposedTag{{Name: "ai-agents", Count: 2}, {Name: "postgres", Count: 2}, {Name: "unapproved", Count: 2}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("proposed = %+v, want %+v", got, want)
	}
	if _, err := ProposedTags(context.Background(), e.store, "nope"); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("unknown job err = %v", err)
	}

	calls := e.cls.Calls()
	e.backfill(func(j *domain.Job) {
		j.FromDryRun = dry.ID
		j.Params.ApprovedNewTags = map[string]string{"ai-agents": "agents", "postgres": "postgres"}
	})
	if e.cls.Calls() != calls {
		t.Errorf("classified again")
	}
	if got := e.tagsOf(first); !slices.Equal(got, []string{"go", "agents", "postgres"}) {
		t.Errorf("first tags = %v", got)
	}
	if got := e.tagsOf(second); !slices.Equal(got, []string{"go", "agents", "postgres"}) {
		t.Errorf("second tags = %v", got)
	}
}

func TestProposedTagsPageThroughEverything(t *testing.T) {
	e := newJobEnv(t)
	for i := 1; i <= 3; i++ {
		e.addBookmark(i, pfake.Inbox)
	}
	e.cls.Script(cfake.Result{Decision: classified("a", 0.9, "fresh")})
	dry := e.backfill(func(j *domain.Job) { j.DryRun = true; j.Params.BatchSize = 1 })
	got, err := ProposedTags(context.Background(), e.store, dry.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []ProposedTag{{Name: "fresh", Count: 3}}) {
		t.Errorf("proposed = %+v", got)
	}
}

func TestDryRunWithNewTagsOffReportsNone(t *testing.T) {
	e := newJobEnv(t)
	tx := fixedTaxonomy()
	tx.Rules.AllowNewTags = false
	e.seedTaxonomy(tx)
	e.addBookmark(1, pfake.Inbox)
	e.cls.Script(cfake.Result{Decision: classified("a", 0.9, "fresh")})
	dry := e.backfill(dryRun)
	got, err := ProposedTags(context.Background(), e.store, dry.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("proposed = %+v, want none with allow_new_tags off", got)
	}
}

func TestBackfillWithoutPlanNoNewTags(t *testing.T) {
	e := newJobEnv(t)
	id := e.addBookmark(1, pfake.Inbox, "go")
	other := e.addBookmark(2, colBeta)
	e.cls.Script(cfake.Result{Decision: classified("a", 0.9, "go", "brand-new")})
	e.backfill(nil)
	if got := e.tagsOf(id); !slices.Equal(got, []string{"go"}) {
		t.Errorf("tags = %v", got)
	}
	if got := e.tagsOf(other); !slices.Equal(got, []string{"go"}) {
		t.Errorf("other tags = %v, want the existing tag only", got)
	}
}

func TestVocabulary(t *testing.T) {
	t.Run("from the tag lister, most used first and capped", func(t *testing.T) {
		e := newJobEnv(t)
		e.addBookmark(1, colBeta, "rare", "common")
		e.addBookmark(2, colBeta, "common")
		e.deps.TagVocabSize = 1
		e.backfill(nil)
		req := e.cls.Requests()[0]
		if !slices.Equal(req.TagVocab, []string{"common"}) {
			t.Errorf("vocab = %v", req.TagVocab)
		}
		for _, q := range e.logged.bookmarks {
			if q.PageToken != "" {
				t.Errorf("unexpected page: %+v", q)
			}
		}
		if len(e.logged.bookmarks) != 1 {
			t.Errorf("lister available, but listed %d times", len(e.logged.bookmarks))
		}
	})

	t.Run("from a first pass without one", func(t *testing.T) {
		e := newJobEnv(t)
		caps := e.provider.Capabilities()
		caps.TagListing = false
		e.provider.SetCapabilities(caps)
		e.addBookmark(1, colBeta, "rare", "common")
		e.addBookmark(2, colBeta, "common")
		job := e.backfill(func(j *domain.Job) { j.Params.BatchSize = 1 })
		req := e.cls.Requests()[0]
		if !slices.Equal(req.TagVocab, []string{"common", "rare"}) {
			t.Errorf("vocab = %v", req.TagVocab)
		}
		// Two passes of two pages each.
		if len(e.logged.bookmarks) != 4 {
			t.Errorf("lists = %d", len(e.logged.bookmarks))
		}
		if job.Progress.Total != 2 {
			t.Errorf("progress = %+v", job.Progress)
		}
	})

	t.Run("the first pass stops when told to", func(t *testing.T) {
		e := newJobEnv(t)
		caps := e.provider.Capabilities()
		caps.TagListing = false
		e.provider.SetCapabilities(caps)
		e.addBookmark(1, colBeta)
		job := e.newJob(domain.JobBackfill, dryRun)
		stopped := make(chan struct{})
		close(stopped)
		err := Handlers(e.deps)[domain.JobBackfill].Run(context.Background(), job, RunContext{
			Checkpoint: func(context.Context) error { return nil },
			Stopping:   stopped,
		})
		if !errors.Is(err, ErrInterrupted) {
			t.Errorf("err = %v", err)
		}
	})
}

// userPinned is a bookmark the user moved to Beta before the cutoff.
func (e *jobEnv) userPinned(n int, at time.Time) domain.ID {
	e.t.Helper()
	id := e.addBookmark(n, colBeta)
	if err := e.store.BookmarkStates().PutState(context.Background(), domain.BookmarkState{
		ID: id, KnownCollection: colBeta, PinSource: domain.PinUser, PinnedAt: &at, InputHash: "old",
	}); err != nil {
		e.t.Fatal(err)
	}
	return id
}

func TestFirstBackfillIgnoresOldPins(t *testing.T) {
	e := newJobEnv(t)
	id := e.userPinned(1, t0.Add(-time.Hour))
	e.backfill(nil)
	if got := e.collectionOf(id); got != colAlpha {
		t.Errorf("collection = %v, want the first backfill to move it", got)
	}
}

func TestFirstBackfillHonorsPinsAfterCutoff(t *testing.T) {
	e := newJobEnv(t)
	id := e.userPinned(1, t0.Add(time.Minute))
	e.backfill(nil)
	if got := e.collectionOf(id); got != colBeta {
		t.Errorf("collection = %v", got)
	}
}

func TestLaterBackfillRespectsUserPins(t *testing.T) {
	e := newJobEnv(t)
	e.putSetting(firstBackfillKey, t0.Add(-24*time.Hour).Format(time.RFC3339))
	pinned := e.userPinned(1, t0.Add(-time.Hour))
	baseline := e.addBookmark(2, colBeta)
	at := t0.Add(-time.Hour)
	if err := e.store.BookmarkStates().PutState(context.Background(), domain.BookmarkState{
		ID: baseline, KnownCollection: colBeta, PinSource: domain.PinBaseline, PinnedAt: &at,
	}); err != nil {
		t.Fatal(err)
	}
	e.backfill(nil)
	if got := e.collectionOf(pinned); got != colBeta {
		t.Errorf("user-pinned moved to %v", got)
	}
	if got := e.collectionOf(baseline); got != colAlpha {
		t.Errorf("baseline-pinned stayed in %v", got)
	}
}

func TestFirstBackfillFlagSetOnSuccessOnly(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		e := newJobEnv(t)
		e.addBookmark(1, pfake.Inbox)
		job := e.newJob(domain.JobBackfill, nil)
		e.setNow(t0.Add(time.Hour))
		e.mustRun(job)
		v, ok := e.setting(firstBackfillKey)
		if !ok {
			t.Fatal("not set")
		}
		at, err := time.Parse(time.RFC3339, v)
		if err != nil || !at.Equal(t0.Add(time.Hour)) {
			t.Errorf("value %q: %v", v, err)
		}
	})

	t.Run("keeps the first time", func(t *testing.T) {
		e := newJobEnv(t)
		e.backfill(nil)
		first, _ := e.setting(firstBackfillKey)
		e.setNow(t0.Add(time.Hour))
		e.backfill(nil)
		if got, _ := e.setting(firstBackfillKey); got != first {
			t.Errorf("%q became %q", first, got)
		}
	})

	t.Run("interrupted", func(t *testing.T) {
		e := newJobEnv(t)
		for i := 1; i <= 2; i++ {
			e.addBookmark(i, pfake.Inbox)
		}
		job := e.newJob(domain.JobBackfill, func(j *domain.Job) { j.Params.BatchSize = 1 })
		err := Handlers(e.deps)[domain.JobBackfill].Run(context.Background(), job, e.stopAfter(job, 3))
		if !errors.Is(err, ErrInterrupted) {
			t.Fatalf("err = %v", err)
		}
		if _, ok := e.setting(firstBackfillKey); ok {
			t.Error("set by an interrupted job")
		}
	})

	t.Run("failed job", func(t *testing.T) {
		e := newJobEnv(t)
		e.snaps.err = errors.New("disk full")
		job := e.newJob(domain.JobBackfill, nil)
		if err := e.runJob(job); err == nil {
			t.Fatal("want an error")
		}
		if _, ok := e.setting(firstBackfillKey); ok {
			t.Error("set by a failed job")
		}
	})

	t.Run("finished with item failures still counts", func(t *testing.T) {
		e := newJobEnv(t)
		e.addBookmark(1, pfake.Inbox)
		e.cls.Script(cfake.Result{Err: errors.New("boom")})
		job := e.backfill(nil)
		if job.Progress.Failed != 1 {
			t.Fatalf("progress = %+v", job.Progress)
		}
		if _, ok := e.setting(firstBackfillKey); !ok {
			t.Error("a finished backfill must make later user moves final")
		}
	})

	t.Run("a scoped backfill isn't the first backfill", func(t *testing.T) {
		e := newJobEnv(t)
		id := e.addBookmark(1, pfake.Inbox)
		other := e.addBookmark(2, pfake.Inbox)
		e.backfill(func(j *domain.Job) { j.Scope.IDs = []string{id.String()} })
		if _, ok := e.setting(firstBackfillKey); ok {
			t.Error("set by a scoped backfill")
		}
		if got := e.collectionOf(other); got != pfake.Inbox {
			t.Errorf("out-of-scope item moved to %v", got)
		}
	})
}

func draftTaxonomy() domain.Taxonomy {
	return domain.Taxonomy{
		ID: "default", Mode: domain.ModeDynamic, Status: domain.TaxonomyDraft, ProvisionMissing: true,
		Rules:   domain.TaxonomyRules{MinTags: 1, MaxTags: 5, MinConfidence: 0.6, AllowNewTags: true},
		Dynamic: domain.DynamicSettings{MaxCollections: 4, SampleSize: 2},
	}
}

func (e *jobEnv) proposeCollections() {
	e.proposer.out = []domain.CollectionProposal{{Name: "Databases", Description: "DBs", Count: 2}, {Name: "Alpha", Count: 1}}
}

func (e *jobEnv) taxonomy() domain.Taxonomy {
	e.t.Helper()
	tx, err := e.store.Taxonomies().GetTaxonomy(context.Background(), "default")
	if err != nil {
		e.t.Fatal(err)
	}
	return tx
}

func TestDynamicProposeOnly(t *testing.T) {
	e := newJobEnv(t)
	e.seedTaxonomy(draftTaxonomy())
	e.proposeCollections()
	for i := 1; i <= 5; i++ {
		e.addBookmark(i, colBeta)
	}
	job := e.backfill(func(j *domain.Job) { j.DryRun = true; j.Params.ProposeOnly = true; j.Params.BatchSize = 2 })

	if e.cls.Calls() != 0 {
		t.Errorf("classifier calls = %d", e.cls.Calls())
	}
	if len(e.provider.Writes()) != 0 || len(e.decisions(job.ID)) != 0 {
		t.Error("did more than propose")
	}
	tx := e.taxonomy()
	if tx.Status != domain.TaxonomyDraft {
		t.Errorf("status = %q", tx.Status)
	}
	var names []string
	for _, c := range tx.Collections {
		names = append(names, c.Name)
	}
	if !slices.Equal(names, []string{"Databases", "Alpha", "Needs Review"}) {
		t.Errorf("collections = %+v", tx.Collections)
	}

	if len(e.proposer.reqs) != 1 {
		t.Fatalf("proposer calls = %d", len(e.proposer.reqs))
	}
	r := e.proposer.reqs[0]
	if !slices.Equal(r.ExistingCollections, []string{"Alpha", "Beta", "Needs Review"}) {
		t.Errorf("existing = %v", r.ExistingCollections)
	}
	// Two samples spaced evenly through five.
	if len(r.Samples) != 2 || r.Samples[0].Bookmark.ID != bmID(1) || r.Samples[1].Bookmark.ID != bmID(3) {
		t.Errorf("samples = %+v", r.Samples)
	}
	if r.Samples[0].Bookmark.Title == "" || r.Samples[0].Bookmark.Description == "" {
		t.Errorf("sample lost its text: %+v", r.Samples[0])
	}
}

func TestDynamicProposeOnlyNeedsADraft(t *testing.T) {
	e := newJobEnv(t)
	job := e.newJob(domain.JobBackfill, func(j *domain.Job) { j.DryRun = true; j.Params.ProposeOnly = true })
	if err := e.runJob(job); !errors.Is(err, domain.ErrValidation) {
		t.Errorf("err = %v", err)
	}
	if len(e.taxonomy().Collections) != 3 || len(e.proposer.reqs) != 0 {
		t.Error("changed a fixed taxonomy")
	}
}

func TestDynamicDraftNeedsAProposer(t *testing.T) {
	e := newJobEnv(t)
	e.seedTaxonomy(draftTaxonomy())
	e.deps.Proposer = nil
	job := e.newJob(domain.JobBackfill, dryRun)
	if err := e.runJob(job); !errors.Is(err, domain.ErrValidation) {
		t.Errorf("err = %v", err)
	}
}

func TestDynamicDryRunClassifiesAgainstProposal(t *testing.T) {
	e := newJobEnv(t)
	e.seedTaxonomy(draftTaxonomy())
	e.proposeCollections()
	e.addBookmark(1, pfake.Inbox)
	e.cls.Script(cfake.Result{Decision: classified("databases", 0.9)})

	job := e.backfill(dryRun)

	if e.cls.Calls() != 1 {
		t.Fatalf("calls = %d", e.cls.Calls())
	}
	req := e.cls.Requests()[0]
	if !slices.ContainsFunc(req.Taxonomy.Collections, func(c domain.TaxonomyCollection) bool { return c.Name == "Databases" }) {
		t.Errorf("taxonomy = %+v", req.Taxonomy)
	}
	tx := e.taxonomy()
	if req.Taxonomy.Version != tx.Version {
		t.Errorf("version = %d, want %d", req.Taxonomy.Version, tx.Version)
	}
	recs := e.decisions(job.ID)
	if len(recs) != 1 || recs[0].TargetCollection == nil {
		t.Fatalf("decisions = %+v", recs)
	}
	if len(e.provider.Writes()) != 0 || slices.Contains(e.collectionNames(), "Databases") {
		t.Error("dry run changed the provider")
	}

	// Activate, then apply: Databases is provisioned and the plan lands there.
	tx.Status = domain.TaxonomyActive
	if _, err := e.deps.Taxonomies.Put(context.Background(), "default", tx, nil); err != nil {
		t.Fatal(err)
	}
	calls := e.cls.Calls()
	e.backfill(func(j *domain.Job) { j.FromDryRun = job.ID })
	if e.cls.Calls() != calls {
		t.Error("classified again")
	}
	cols, _ := e.provider.ListCollections(context.Background())
	dbs := slices.IndexFunc(cols, func(c domain.Collection) bool { return c.Name == "Databases" })
	if dbs < 0 || e.collectionOf(bmID(1)) != cols[dbs].ID {
		t.Errorf("collection = %v, collections = %+v", e.collectionOf(bmID(1)), cols)
	}
}

func TestDynamicDryRunResumeDoesNotProposeAgain(t *testing.T) {
	e := newJobEnv(t)
	e.seedTaxonomy(draftTaxonomy())
	e.proposeCollections()
	e.addBookmark(1, pfake.Inbox)
	e.addBookmark(2, pfake.Inbox)
	e.cls.Script(cfake.Result{Decision: classified("databases", 0.9)})
	job := e.newJob(domain.JobBackfill, func(j *domain.Job) { j.DryRun = true; j.Params.BatchSize = 1 })
	// Saves in order: the start of processing, page 1.
	err := Handlers(e.deps)[domain.JobBackfill].Run(context.Background(), job, e.stopAfter(job, 2))
	if !errors.Is(err, ErrInterrupted) {
		t.Fatalf("err = %v", err)
	}
	version := e.taxonomy().Version
	saved := e.reload(job.ID)
	e.mustRun(saved)
	if len(e.proposer.reqs) != 1 || e.taxonomy().Version != version {
		t.Errorf("proposer calls = %d, version %d -> %d", len(e.proposer.reqs), version, e.taxonomy().Version)
	}
	if len(e.decisions(job.ID)) != 2 {
		t.Errorf("decisions = %+v", e.decisions(job.ID))
	}
}

func TestDynamicDraftWithCollectionsIsNotProposedAgain(t *testing.T) {
	e := newJobEnv(t)
	tx := draftTaxonomy()
	tx.Collections = []domain.TaxonomyCollection{{Key: "edited", Name: "Edited"}, {Key: "rv", Name: "Rv", Role: "review"}}
	e.seedTaxonomy(tx)
	version := e.taxonomy().Version
	e.proposeCollections()
	e.backfill(dryRun)
	if len(e.proposer.reqs) != 0 || e.taxonomy().Version != version {
		t.Errorf("proposer calls = %d, version %d -> %d", len(e.proposer.reqs), version, e.taxonomy().Version)
	}
}

func TestRealBackfillRefusesDraft(t *testing.T) {
	e := newJobEnv(t)
	e.seedTaxonomy(draftTaxonomy())
	job := e.newJob(domain.JobBackfill, nil)
	if err := e.runJob(job); !errors.Is(err, domain.ErrConflict) {
		t.Errorf("err = %v", err)
	}
	if len(e.snaps.calls) != 0 {
		t.Error("took a snapshot")
	}
}

func TestOldCollectionsNotDeleted(t *testing.T) {
	e := newJobEnv(t)
	e.provider.AddCollection(domain.Collection{ID: colOther, Name: "Old Stuff"})
	id := e.addBookmark(1, colOther)
	e.backfill(nil)
	if got := e.collectionOf(id); got != colAlpha {
		t.Fatalf("collection = %v", got)
	}
	if !slices.Contains(e.collectionNames(), "Old Stuff") {
		t.Errorf("collections = %v", e.collectionNames())
	}
}

func TestHandlersWithRunner(t *testing.T) {
	e := newJobEnv(t)
	e.addBookmark(1, pfake.Inbox)
	e.addBookmark(2, colBeta)
	svc := NewService(ServiceDeps{Store: e.store, AutomationEnabled: true, ProviderNames: []string{"fake"}, Now: e.nowFn})
	r := NewRunner(RunnerDeps{Store: e.store, Handlers: Handlers(e.deps), Provider: "fake", Now: e.nowFn, PollEvery: time.Hour})
	svc.Notifier = r.Wake
	r.Start(context.Background())
	defer shutdown(t, r)

	job, _, err := svc.Create(context.Background(), CreateRequest{Type: domain.JobBackfill, Provider: "fake", Taxonomy: "default"})
	if err != nil {
		t.Fatal(err)
	}
	done := waitStatus(t, e.store, job.ID, domain.JobSucceeded)
	if done.Progress.Processed != 2 || done.Progress.Total != 2 || done.SnapshotID == "" {
		t.Errorf("job = %+v", done)
	}
}

// createApply creates a from_dry_run job through the service, as the API does, after
// marking the dry run finished.
func (e *jobEnv) createApply(dry *domain.Job, scope domain.Scope, params domain.JobParams) (domain.Job, error) {
	e.t.Helper()
	dry.Status = domain.JobSucceeded
	if err := e.store.Jobs().UpdateJob(context.Background(), *dry); err != nil {
		e.t.Fatal(err)
	}
	svc := NewService(ServiceDeps{Store: e.store, ProviderNames: []string{"fake"}, Now: e.nowFn})
	job, _, err := svc.Create(context.Background(), CreateRequest{Type: dry.Type, Provider: "fake", Taxonomy: "default",
		FromDryRun: dry.ID, Scope: scope, Params: params})
	return job, err
}

func TestApplyStaysWithinThePlan(t *testing.T) {
	jobTypes := map[string]domain.JobType{"backfill": domain.JobBackfill, "reclassify": domain.JobReclassify}
	for name, typ := range jobTypes {
		t.Run(name+": trial scope, then an unscoped apply", func(t *testing.T) {
			e := newJobEnv(t)
			planned := e.addBookmark(1, pfake.Inbox)
			other := e.addBookmark(2, pfake.Inbox)
			scope := domain.Scope{IDs: []string{planned.String()}}
			dry := e.newJob(typ, func(j *domain.Job) { j.DryRun = true; j.Scope = scope })
			e.mustRun(dry)
			calls := e.cls.Calls()

			job, err := e.createApply(dry, domain.Scope{}, domain.JobParams{})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(job.Scope, scope) {
				t.Fatalf("scope = %+v, want the dry run's", job.Scope)
			}
			e.mustRun(&job)

			if e.cls.Calls() != calls {
				t.Errorf("classifier calls = %d, want %d", e.cls.Calls(), calls)
			}
			if w := e.provider.Writes(); len(w) != 1 || w[0].ID != planned {
				t.Errorf("writes = %+v", w)
			}
			if got := e.collectionOf(other); got != pfake.Inbox {
				t.Errorf("out-of-plan bookmark moved to %v", got)
			}
			if len(e.snaps.calls) != 1 || !reflect.DeepEqual(e.snaps.calls[0], scope) {
				t.Errorf("snapshot scopes = %+v", e.snaps.calls)
			}
			if _, ok := e.setting(firstBackfillKey); ok {
				t.Error("first_backfill_done set by a scoped apply")
			}
		})

		t.Run(name+": a differing scope is refused", func(t *testing.T) {
			e := newJobEnv(t)
			one, two := e.addBookmark(1, pfake.Inbox), e.addBookmark(2, pfake.Inbox)
			dry := e.newJob(typ, func(j *domain.Job) { j.DryRun = true; j.Scope = domain.Scope{IDs: []string{one.String()}} })
			e.mustRun(dry)
			for _, scope := range []domain.Scope{
				{IDs: []string{two.String()}},
				{IDs: []string{one.String(), two.String()}},
				{IDs: []string{one.String()}, Tags: []string{"x"}},
			} {
				if _, err := e.createApply(dry, scope, domain.JobParams{}); !errors.Is(err, domain.ErrValidation) {
					t.Errorf("scope %+v: err = %v, want ErrValidation", scope, err)
				}
			}
		})

		t.Run(name+": the same scope in another order is accepted", func(t *testing.T) {
			e := newJobEnv(t)
			one, two := e.addBookmark(1, pfake.Inbox), e.addBookmark(2, pfake.Inbox)
			dry := e.newJob(typ, func(j *domain.Job) {
				j.DryRun = true
				j.Scope = domain.Scope{IDs: []string{one.String(), two.String()}}
			})
			e.mustRun(dry)
			if _, err := e.createApply(dry, domain.Scope{IDs: []string{two.String(), one.String()}}, domain.JobParams{}); err != nil {
				t.Errorf("err = %v", err)
			}
		})
	}

	t.Run("a narrower apply of a whole-library plan is refused", func(t *testing.T) {
		e := newJobEnv(t)
		id := e.addBookmark(1, pfake.Inbox)
		dry := e.backfill(dryRun)
		if _, err := e.createApply(dry, domain.Scope{IDs: []string{id.String()}}, domain.JobParams{}); !errors.Is(err, domain.ErrValidation) {
			t.Errorf("err = %v, want ErrValidation", err)
		}
	})

	t.Run("a bookmark added after the dry run is untouched", func(t *testing.T) {
		e := newJobEnv(t)
		planned := e.addBookmark(1, pfake.Inbox)
		dry := e.backfill(dryRun)
		late := e.addBookmark(2, pfake.Inbox)
		calls := e.cls.Calls()

		job, err := e.createApply(dry, domain.Scope{}, domain.JobParams{})
		if err != nil {
			t.Fatal(err)
		}
		e.mustRun(&job)

		if e.cls.Calls() != calls {
			t.Errorf("classifier calls = %d, want %d", e.cls.Calls(), calls)
		}
		if got := e.collectionOf(late); got != pfake.Inbox {
			t.Errorf("late bookmark moved to %v", got)
		}
		if got := e.collectionOf(planned); got != colAlpha {
			t.Errorf("planned bookmark in %v", got)
		}
		if _, known, _ := e.store.BookmarkStates().GetState(context.Background(), late); known {
			t.Error("recorded state for a bookmark outside the plan, which a later sync would read as handled")
		}
		if job.Progress.Skipped != 1 || job.Progress.Processed != 2 || job.Progress.Failed != 0 {
			t.Errorf("progress = %+v", job.Progress)
		}
		// A whole-library apply is still the first backfill.
		if _, ok := e.setting(firstBackfillKey); !ok {
			t.Error("first_backfill_done not set by a whole-library apply")
		}
	})

	t.Run("a planned bookmark that changed is still classified again", func(t *testing.T) {
		e := newJobEnv(t)
		id := e.addBookmark(1, pfake.Inbox)
		dry := e.backfill(dryRun)
		b, _ := e.provider.Bookmark(id)
		b.Title = "Something else"
		e.provider.Add(b)
		calls := e.cls.Calls()
		job, err := e.createApply(dry, domain.Scope{}, domain.JobParams{})
		if err != nil {
			t.Fatal(err)
		}
		e.mustRun(&job)
		if e.cls.Calls() != calls+1 {
			t.Errorf("classifier calls = %d, want %d", e.cls.Calls(), calls+1)
		}
	})
}
