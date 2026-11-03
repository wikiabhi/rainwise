package jobs

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	cfake "github.com/wikiabhi/rainwise/internal/adapters/classifier/fake"
	pfake "github.com/wikiabhi/rainwise/internal/adapters/provider/fake"
	"github.com/wikiabhi/rainwise/internal/adapters/store/memory"
	"github.com/wikiabhi/rainwise/internal/core/pipeline"
	"github.com/wikiabhi/rainwise/internal/core/taxonomy"
	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

const handlerText = "A long and detailed description of how database indexes work under realistic production load."

var (
	colAlpha  = domain.ID{Provider: "fake", Local: "alpha"}
	colBeta   = domain.ID{Provider: "fake", Local: "beta"}
	colReview = domain.ID{Provider: "fake", Local: "review"}
	colOther  = domain.ID{Provider: "fake", Local: "other"}
)

func bmID(n int) domain.ID { return domain.ID{Provider: "fake", Local: "b" + strconv.Itoa(n)} }

// listLog records list queries and otherwise is the fake provider.
type listLog struct {
	*pfake.Provider
	mu         sync.Mutex
	bookmarks  []ports.ListQuery
	highlights []ports.ListQuery
	listErr    error
	listFails  int
}

// failLists makes each bookmark listing fail, and counts them.
func (l *listLog) failLists(err error) *int {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.listErr = err
	return &l.listFails
}

func (l *listLog) ListBookmarks(ctx context.Context, q ports.ListQuery) (domain.Page[domain.Bookmark], error) {
	l.mu.Lock()
	l.bookmarks = append(l.bookmarks, q)
	err := l.listErr
	if err != nil {
		l.listFails++
	}
	l.mu.Unlock()
	if err != nil {
		return domain.Page[domain.Bookmark]{}, err
	}
	return l.Provider.ListBookmarks(ctx, q)
}

func (l *listLog) ListAllHighlights(ctx context.Context, q ports.ListQuery) (domain.Page[domain.Highlight], error) {
	l.mu.Lock()
	l.highlights = append(l.highlights, q)
	l.mu.Unlock()
	return l.Provider.ListAllHighlights(ctx, q)
}

type snapshotStub struct {
	store ports.Store
	mu    sync.Mutex
	err   error
	calls []domain.Scope
	jobs  []string
	on    func()
}

func (s *snapshotStub) Take(_ context.Context, _ string, scope domain.Scope, jobID string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, scope)
	s.jobs = append(s.jobs, jobID)
	if s.on != nil {
		s.on()
	}
	if s.err != nil {
		return "", s.err
	}
	id := "snap-" + jobID
	if err := s.store.Snapshots().CreateSnapshot(context.Background(), domain.Snapshot{ID: id, Provider: "fake", JobID: jobID, CreatedAt: t0}); err != nil {
		return "", err
	}
	return id, nil
}

type stubProposer struct {
	mu   sync.Mutex
	reqs []ports.ProposeRequest
	out  []domain.CollectionProposal
}

func (p *stubProposer) ProposeTaxonomy(_ context.Context, r ports.ProposeRequest) (ports.TaxonomyProposal, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reqs = append(p.reqs, r)
	return ports.TaxonomyProposal{Collections: p.out}, nil
}

// jobEnv runs handlers directly against fakes, with one item at a time so that
// scripted classifier answers line up with bookmarks.
type jobEnv struct {
	t        *testing.T
	provider *pfake.Provider
	logged   *listLog
	cls      *cfake.Classifier
	store    ports.Store
	snaps    *snapshotStub
	proposer *stubProposer
	sleeps   []time.Duration
	now      time.Time
	mu       sync.Mutex
	order    []string
	deps     Deps
	nextJob  int
}

func newJobEnv(t *testing.T) *jobEnv {
	t.Helper()
	e := &jobEnv{
		t:        t,
		provider: pfake.New(),
		cls:      cfake.New("primary", cfake.Result{Decision: classified("a", 0.9)}),
		store:    memory.New(),
		proposer: &stubProposer{},
		now:      t0,
	}
	e.snaps = &snapshotStub{store: e.store}
	e.logged = &listLog{Provider: e.provider}
	for _, c := range []domain.Collection{{ID: colAlpha, Name: "Alpha"}, {ID: colBeta, Name: "Beta"}, {ID: colReview, Name: "Needs Review"}} {
		e.provider.AddCollection(c)
	}
	e.seedTaxonomy(fixedTaxonomy())
	e.build()
	return e
}

func (e *jobEnv) nowFn() time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.now
}

func (e *jobEnv) setNow(at time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.now = at
}

func (e *jobEnv) build() {
	pipe := pipeline.New(pipeline.Deps{Provider: e.logged, Classifier: e.cls, Store: e.store, Now: e.nowFn, ContentFetch: "off"})
	e.deps = Deps{
		Provider:     e.logged,
		Pipeline:     pipe,
		Taxonomies:   taxonomy.New(e.store.Taxonomies()),
		Proposer:     e.proposer,
		Store:        e.store,
		Now:          e.nowFn,
		Concurrency:  1,
		TagVocabSize: 200,
		DurableWait:  30 * time.Second,
		Sleep: func(_ context.Context, d time.Duration) error {
			e.mu.Lock()
			defer e.mu.Unlock()
			e.sleeps = append(e.sleeps, d)
			e.order = append(e.order, "wait")
			return nil
		},
		Snapshots: e.snaps,
	}
}

func fixedTaxonomy() domain.Taxonomy {
	return domain.Taxonomy{
		ID: "default", Mode: domain.ModeFixed, Status: domain.TaxonomyActive, ProvisionMissing: true,
		Collections: []domain.TaxonomyCollection{
			{Key: "a", Name: "Alpha"},
			{Key: "b", Name: "Beta"},
			{Key: "needs_review", Name: "Needs Review", Role: "review"},
		},
		Rules: domain.TaxonomyRules{MinTags: 1, MaxTags: 5, MinConfidence: 0.6, AllowNewTags: true},
	}
}

func (e *jobEnv) seedTaxonomy(tx domain.Taxonomy) {
	e.t.Helper()
	if _, err := e.store.Taxonomies().PutTaxonomy(context.Background(), tx, nil); err != nil {
		e.t.Fatal(err)
	}
}

func classified(key string, conf float64, tags ...string) domain.Decision {
	d := domain.Decision{Collection: domain.CollectionChoice{Key: key}, Confidence: conf, Model: domain.ModelInfo{Model: "m1"}}
	for _, n := range tags {
		d.Tags = append(d.Tags, domain.TagSuggestion{Name: n})
	}
	return d
}

func (e *jobEnv) addBookmark(n int, col domain.ID, tags ...string) domain.ID {
	id := bmID(n)
	e.provider.Add(domain.Bookmark{ID: id, URL: "https://example.test/" + strconv.Itoa(n), Title: "Post " + strconv.Itoa(n),
		Description: handlerText, Tags: tags, Collection: col, UpdatedAt: t0})
	return id
}

func (e *jobEnv) newJob(typ domain.JobType, mod func(*domain.Job)) *domain.Job {
	e.t.Helper()
	e.nextJob++
	started := e.nowFn()
	j := domain.Job{ID: "job" + strconv.Itoa(e.nextJob), Type: typ, Status: domain.JobRunning, Provider: "fake", TaxonomyID: "default", CreatedAt: started, StartedAt: &started}
	if mod != nil {
		mod(&j)
	}
	if err := e.store.Jobs().CreateJob(context.Background(), j); err != nil {
		e.t.Fatal(err)
	}
	return &j
}

// rc saves through the store, as the runner does.
func (e *jobEnv) rc(job *domain.Job) RunContext {
	return RunContext{
		Checkpoint: func(ctx context.Context) error {
			return e.store.Jobs().UpdateJob(context.WithoutCancel(ctx), *job)
		},
		Stopping: make(chan struct{}),
	}
}

func (e *jobEnv) runJob(job *domain.Job) error {
	return Handlers(e.deps)[job.Type].Run(context.Background(), job, e.rc(job))
}

func (e *jobEnv) mustRun(job *domain.Job) {
	e.t.Helper()
	if err := e.runJob(job); err != nil {
		e.t.Fatalf("run %s: %v", job.Type, err)
	}
}

func (e *jobEnv) syncNow() *domain.Job {
	e.t.Helper()
	job := e.newJob(domain.JobSync, nil)
	e.mustRun(job)
	return job
}

func (e *jobEnv) setting(key string) (string, bool) {
	e.t.Helper()
	v, ok, err := e.store.BookmarkStates().GetSetting(context.Background(), key)
	if err != nil {
		e.t.Fatal(err)
	}
	return v, ok
}

func (e *jobEnv) putSetting(key, value string) {
	e.t.Helper()
	if err := e.store.BookmarkStates().PutSetting(context.Background(), key, value); err != nil {
		e.t.Fatal(err)
	}
}

func (e *jobEnv) putWatermark(at time.Time) {
	e.t.Helper()
	e.putSetting("sync_watermark:fake", at.Format(time.RFC3339Nano))
}

func (e *jobEnv) watermark() (time.Time, bool) {
	e.t.Helper()
	v, ok := e.setting("sync_watermark:fake")
	if !ok {
		return time.Time{}, false
	}
	at, err := time.Parse(time.RFC3339, v)
	if err != nil {
		e.t.Fatalf("watermark %q: %v", v, err)
	}
	return at, true
}

func (e *jobEnv) state(id domain.ID) domain.BookmarkState {
	e.t.Helper()
	st, ok, err := e.store.BookmarkStates().GetState(context.Background(), id)
	if err != nil || !ok {
		e.t.Fatalf("state of %s: ok=%v err=%v", id, ok, err)
	}
	return st
}

func (e *jobEnv) collectionOf(id domain.ID) domain.ID {
	e.t.Helper()
	b, ok := e.provider.Bookmark(id)
	if !ok {
		e.t.Fatalf("no bookmark %s", id)
	}
	return b.Collection
}

func (e *jobEnv) tagsOf(id domain.ID) []string {
	e.t.Helper()
	b, _ := e.provider.Bookmark(id)
	return b.Tags
}

func TestSyncFirstRunBaseline(t *testing.T) {
	e := newJobEnv(t)
	inbox := e.addBookmark(1, pfake.Inbox)
	other := e.addBookmark(2, colBeta)

	job := e.syncNow()

	if got := e.cls.Calls(); got != 1 {
		t.Errorf("classifier calls = %d, want 1", got)
	}
	if got := e.collectionOf(inbox); got != colAlpha {
		t.Errorf("inbox item in %v", got)
	}
	if got := e.collectionOf(other); got != colBeta {
		t.Errorf("other item in %v", got)
	}
	if st := e.state(other); st.PinSource != domain.PinBaseline {
		t.Errorf("pin = %q", st.PinSource)
	}
	if len(e.provider.Writes()) != 1 {
		t.Errorf("writes = %+v", e.provider.Writes())
	}
	if job.Progress.Total != 2 || job.Progress.Processed != 2 {
		t.Errorf("progress = %+v", job.Progress)
	}
	if len(e.snaps.calls) != 0 {
		t.Error("sync took a snapshot")
	}
}

func TestSyncWatermark(t *testing.T) {
	wm := t0.Add(-time.Hour)
	incremental := func(e *jobEnv) {
		caps := e.provider.Capabilities()
		caps.IncrementalSince = true
		e.provider.SetCapabilities(caps)
	}

	t.Run("since is watermark minus 48h", func(t *testing.T) {
		e := newJobEnv(t)
		incremental(e)
		e.putWatermark(wm)
		e.syncNow()
		if len(e.logged.bookmarks) == 0 {
			t.Fatal("nothing listed")
		}
		for _, q := range e.logged.bookmarks {
			if q.Since == nil || !q.Since.Equal(wm.Add(-48*time.Hour)) {
				t.Errorf("since = %v", q.Since)
			}
		}
	})

	t.Run("advances to job start on success", func(t *testing.T) {
		e := newJobEnv(t)
		e.addBookmark(1, pfake.Inbox)
		e.putWatermark(wm)
		job := e.newJob(domain.JobSync, nil)
		e.setNow(t0.Add(time.Hour)) // The clock moves on while the job runs.
		e.mustRun(job)
		got, ok := e.watermark()
		if !ok || !got.Equal(t0) {
			t.Errorf("watermark = %v (%v), want job start %v", got, ok, t0)
		}
	})

	t.Run("failed items keep it unchanged", func(t *testing.T) {
		e := newJobEnv(t)
		e.addBookmark(1, pfake.Inbox)
		e.putWatermark(wm)
		e.cls.Script(cfake.Result{Err: errors.New("boom")})
		job := e.syncNow()
		if job.Progress.Failed != 1 {
			t.Fatalf("progress = %+v", job.Progress)
		}
		if got, _ := e.watermark(); !got.Equal(wm) {
			t.Errorf("watermark = %v, want %v", got, wm)
		}
	})

	t.Run("listing failure keeps it unchanged", func(t *testing.T) {
		e := newJobEnv(t)
		e.putWatermark(wm)
		job := e.newJob(domain.JobSync, nil)
		fails := e.logged.failLists(domain.ErrTemporary)
		if err := e.runJob(job); !errors.Is(err, domain.ErrTemporary) || *fails == 0 {
			t.Fatalf("err = %v", err)
		}
		if got, _ := e.watermark(); !got.Equal(wm) {
			t.Errorf("watermark = %v", got)
		}
	})

	t.Run("dry run leaves it alone", func(t *testing.T) {
		e := newJobEnv(t)
		e.addBookmark(1, pfake.Inbox)
		job := e.newJob(domain.JobSync, func(j *domain.Job) { j.DryRun = true })
		e.mustRun(job)
		if _, ok := e.watermark(); ok {
			t.Error("dry run set the watermark")
		}
		if len(e.provider.Writes()) != 0 {
			t.Error("dry run wrote")
		}
	})
}

func TestSyncFullScanWithoutCapability(t *testing.T) {
	e := newJobEnv(t)
	e.putWatermark(t0.Add(-time.Hour))
	e.addBookmark(1, colAlpha)
	e.syncNow()
	if len(e.logged.bookmarks) == 0 {
		t.Fatal("nothing listed")
	}
	for _, q := range e.logged.bookmarks {
		if q.Since != nil {
			t.Errorf("since = %v, want a full list", q.Since)
		}
	}
}

// settled files one bookmark in a first sync, then sets the watermark to t0.
func (e *jobEnv) settledBookmark(n int) domain.ID {
	e.t.Helper()
	id := e.addBookmark(n, pfake.Inbox)
	e.syncNow()
	e.cls.Script(cfake.Result{Decision: classified("", 0.9, "from-highlight")})
	return id
}

func TestSyncHighlights(t *testing.T) {
	t.Run("new highlight queues a tags-only run", func(t *testing.T) {
		e := newJobEnv(t)
		id := e.settledBookmark(1)
		calls, writes := e.cls.Calls(), len(e.provider.Writes())
		focus := domain.ID{Provider: "fake", Local: "h1"}
		e.provider.AddHighlight(domain.Highlight{ID: focus, Bookmark: id, Text: "a highlighted thing", CreatedAt: t0.Add(time.Minute)})
		e.setNow(t0.Add(time.Hour))
		e.syncNow()

		if got := e.cls.Calls(); got != calls+1 {
			t.Fatalf("classifier calls = %d, want %d", got, calls+1)
		}
		req := e.cls.Requests()[calls]
		if req.WantCollection || req.Subject.FocusHighlight == nil || req.Subject.FocusHighlight.ID != focus {
			t.Errorf("request = %+v", req)
		}
		w := e.provider.Writes()
		if len(w) != writes+1 || w[writes].Patch.Collection != nil || !slices.Equal(w[writes].Patch.AddTags, []string{"from-highlight"}) {
			t.Errorf("writes = %+v", w)
		}
		if got := e.collectionOf(id); got != colAlpha {
			t.Errorf("moved to %v", got)
		}
	})

	t.Run("a handled highlight costs nothing on the next sync", func(t *testing.T) {
		e := newJobEnv(t)
		id := e.settledBookmark(1)
		e.provider.AddHighlight(domain.Highlight{ID: domain.ID{Provider: "fake", Local: "h1"}, Bookmark: id, Text: "x", CreatedAt: t0.Add(time.Minute)})
		e.setNow(t0.Add(time.Hour))
		e.syncNow()
		calls := e.cls.Calls()
		e.setNow(t0.Add(2 * time.Hour))
		e.syncNow()
		if got := e.cls.Calls(); got != calls {
			t.Errorf("classifier calls went from %d to %d", calls, got)
		}
	})

	t.Run("no parent reference is skipped", func(t *testing.T) {
		e := newJobEnv(t)
		e.settledBookmark(1)
		calls := e.cls.Calls()
		e.provider.AddHighlight(domain.Highlight{ID: domain.ID{Provider: "fake", Local: "h1"}, Text: "x", CreatedAt: t0.Add(time.Minute)})
		e.setNow(t0.Add(time.Hour))
		job := e.syncNow()
		if e.cls.Calls() != calls || job.Progress.Failed != 0 {
			t.Errorf("calls = %d (was %d), progress = %+v", e.cls.Calls(), calls, job.Progress)
		}
	})

	t.Run("provider without highlights skips it", func(t *testing.T) {
		e := newJobEnv(t)
		e.settledBookmark(1)
		caps := e.provider.Capabilities()
		caps.Highlights = false
		e.provider.SetCapabilities(caps)
		e.setNow(t0.Add(time.Hour))
		job := e.syncNow()
		if len(e.logged.highlights) != 0 {
			t.Errorf("listed highlights: %+v", e.logged.highlights)
		}
		if _, ok := e.watermark(); !ok || job.Progress.Failed != 0 {
			t.Errorf("progress = %+v", job.Progress)
		}
	})

	t.Run("a first sync leaves old highlights alone", func(t *testing.T) {
		e := newJobEnv(t)
		id := e.addBookmark(1, colBeta)
		e.provider.AddHighlight(domain.Highlight{ID: domain.ID{Provider: "fake", Local: "h1"}, Bookmark: id, Text: "x", CreatedAt: t0.Add(-time.Hour)})
		e.syncNow()
		if e.cls.Calls() != 0 || len(e.provider.Writes()) != 0 {
			t.Errorf("calls = %d, writes = %+v", e.cls.Calls(), e.provider.Writes())
		}
	})

	t.Run("failed highlight keeps the watermark", func(t *testing.T) {
		e := newJobEnv(t)
		id := e.settledBookmark(1)
		before, _ := e.watermark()
		e.cls.Script(cfake.Result{Err: errors.New("boom")})
		e.provider.AddHighlight(domain.Highlight{ID: domain.ID{Provider: "fake", Local: "h1"}, Bookmark: id, Text: "x", CreatedAt: t0.Add(time.Minute)})
		e.setNow(t0.Add(time.Hour))
		job := e.syncNow()
		if job.Progress.Failed != 1 {
			t.Fatalf("progress = %+v", job.Progress)
		}
		if got, _ := e.watermark(); !got.Equal(before) {
			t.Errorf("watermark = %v, want %v", got, before)
		}
	})
}

// stopAfter closes the job's Stopping once Checkpoint has been called n times.
func (e *jobEnv) stopAfter(job *domain.Job, n int) RunContext {
	stopping := make(chan struct{})
	calls := 0
	return RunContext{
		Checkpoint: func(ctx context.Context) error {
			calls++
			if calls == n {
				close(stopping)
			}
			return e.store.Jobs().UpdateJob(context.WithoutCancel(ctx), *job)
		},
		Stopping: stopping,
	}
}

// reload is what a restart sees.
func (e *jobEnv) reload(id string) *domain.Job {
	e.t.Helper()
	j, err := e.store.Jobs().GetJob(context.Background(), id)
	if err != nil {
		e.t.Fatal(err)
	}
	return &j
}

func TestSyncResumesAfterBookmarkPass(t *testing.T) {
	e := newJobEnv(t)
	id := e.settledBookmark(1)
	e.addBookmark(2, colBeta)
	e.provider.AddHighlight(domain.Highlight{ID: domain.ID{Provider: "fake", Local: "h1"}, Bookmark: id, Text: "x", CreatedAt: t0.Add(time.Minute)})
	e.setNow(t0.Add(time.Hour))

	job := e.newJob(domain.JobSync, nil)
	err := Handlers(e.deps)[domain.JobSync].Run(context.Background(), job, e.stopAfter(job, 1))
	if !errors.Is(err, ErrInterrupted) {
		t.Fatalf("err = %v", err)
	}
	saved := e.reload(job.ID)
	if saved.Checkpoint.Phase == "" || saved.Progress.Processed != 2 {
		t.Fatalf("saved = %+v %+v", saved.Checkpoint, saved.Progress)
	}
	if _, ok := e.setting("sync_watermark:fake"); !ok {
		t.Fatal("first sync should have set the watermark")
	}
	wmBefore, _ := e.watermark()

	e.logged.bookmarks = nil
	e.mustRun(saved)
	if len(e.logged.bookmarks) != 0 {
		t.Errorf("relisted bookmarks: %+v", e.logged.bookmarks)
	}
	if saved.Progress.Processed != 3 {
		t.Errorf("progress = %+v, want 2 bookmarks and 1 highlight", saved.Progress)
	}
	if got, _ := e.watermark(); !got.After(wmBefore) {
		t.Errorf("watermark didn't advance: %v", got)
	}
}

func TestSyncResumesAfterAllPasses(t *testing.T) {
	e := newJobEnv(t)
	e.addBookmark(1, pfake.Inbox)
	job := e.newJob(domain.JobSync, nil)
	// The last saves are the end of each pass; stop on the final one.
	err := Handlers(e.deps)[domain.JobSync].Run(context.Background(), job, RunContext{
		Checkpoint: func(ctx context.Context) error { return e.store.Jobs().UpdateJob(ctx, *job) },
		Stopping:   make(chan struct{}),
	})
	if err != nil {
		t.Fatal(err)
	}
	saved := e.reload(job.ID)
	if saved.Checkpoint.Phase != phaseDone {
		t.Fatalf("phase = %q", saved.Checkpoint.Phase)
	}
	processed := saved.Progress.Processed
	calls := e.cls.Calls()
	e.logged.bookmarks = nil
	e.mustRun(saved)
	if saved.Progress.Processed != processed || e.cls.Calls() != calls || len(e.logged.bookmarks) != 0 {
		t.Errorf("redid work: %+v, calls %d, lists %d", saved.Progress, e.cls.Calls(), len(e.logged.bookmarks))
	}
}

func TestSyncProvisionsDeclaredCollections(t *testing.T) {
	e := newJobEnv(t)
	tx := fixedTaxonomy()
	tx.Collections = append(tx.Collections, domain.TaxonomyCollection{Key: "c", Name: "Gamma"})
	e.seedTaxonomy(tx)
	e.syncNow()
	cols, _ := e.provider.ListCollections(context.Background())
	if !slices.ContainsFunc(cols, func(c domain.Collection) bool { return c.Name == "Gamma" }) {
		t.Errorf("collections = %+v", cols)
	}
}

func TestSyncMissingCollectionsNoProvision(t *testing.T) {
	e := newJobEnv(t)
	tx := fixedTaxonomy()
	tx.ProvisionMissing = false
	tx.Collections = append(tx.Collections, domain.TaxonomyCollection{Key: "c", Name: "Gamma"})
	e.seedTaxonomy(tx)
	e.addBookmark(1, pfake.Inbox)
	job := e.newJob(domain.JobSync, nil)
	if err := e.runJob(job); !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("err = %v", err)
	}
	if len(e.provider.Writes()) != 0 {
		t.Errorf("writes = %+v", e.provider.Writes())
	}
}

func TestTaxonomyChangeDoesNotResortOnSync(t *testing.T) {
	e := newJobEnv(t)
	id := e.addBookmark(1, pfake.Inbox)
	e.syncNow()
	calls, writes := e.cls.Calls(), len(e.provider.Writes())

	tx, err := e.store.Taxonomies().GetTaxonomy(context.Background(), "default")
	if err != nil {
		t.Fatal(err)
	}
	tx.Collections = tx.Collections[1:] // Alpha, where it sits, is gone from the taxonomy.
	e.seedTaxonomy(tx)
	e.setNow(t0.Add(time.Hour))
	e.syncNow()

	if e.cls.Calls() != calls || len(e.provider.Writes()) != writes {
		t.Errorf("calls %d -> %d, writes %d -> %d", calls, e.cls.Calls(), writes, len(e.provider.Writes()))
	}
	if got := e.collectionOf(id); got != colAlpha {
		t.Errorf("moved to %v", got)
	}
}

func TestSyncUsesAllowNewTags(t *testing.T) {
	for _, allow := range []bool{true, false} {
		e := newJobEnv(t)
		tx := fixedTaxonomy()
		tx.Rules.AllowNewTags = allow
		e.seedTaxonomy(tx)
		e.addBookmark(2, colBeta, "go")
		id := e.addBookmark(1, pfake.Inbox)
		e.cls.Script(cfake.Result{Decision: classified("a", 0.9, "go", "brand-new")})
		e.syncNow()
		want := []string{"go"}
		if allow {
			want = []string{"go", "brand-new"}
		}
		if got := e.tagsOf(id); !slices.Equal(got, want) {
			t.Errorf("allow=%v: tags = %v, want %v", allow, got, want)
		}
	}
}
