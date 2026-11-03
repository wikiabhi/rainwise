package pipeline

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cfake "github.com/wikiabhi/rainwise/internal/adapters/classifier/fake"
	pfake "github.com/wikiabhi/rainwise/internal/adapters/provider/fake"
	"github.com/wikiabhi/rainwise/internal/adapters/store/memory"
	"github.com/wikiabhi/rainwise/internal/core/taxonomy"
	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

var (
	colA   = domain.ID{Provider: "fake", Local: "a"}
	colB   = domain.ID{Provider: "fake", Local: "b"}
	review = domain.ID{Provider: "fake", Local: "review"}
	trash  = domain.ID{Provider: "fake", Local: "trash"}
	bm1    = domain.ID{Provider: "fake", Local: "1"}
	t0     = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
)

const longText = "A long and detailed description of how database indexes work under realistic production load."

type fetcher struct {
	mu    sync.Mutex
	calls int
	text  string
	errs  []error
}

func (f *fetcher) Fetch(context.Context, string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		return "", err
	}
	return f.text, nil
}

type env struct {
	t        *testing.T
	provider *pfake.Provider
	cls      *cfake.Classifier
	store    ports.Store
	fetch    *fetcher
	now      time.Time
	deps     Deps
	pipe     *Pipeline
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{
		t:        t,
		provider: pfake.New(),
		cls:      cfake.New("primary", cfake.Result{Decision: decision("a", 0.9, "go", "brand-new")}),
		store:    memory.New(),
		fetch:    &fetcher{},
		now:      t0,
	}
	e.deps = Deps{
		Provider:     e.provider,
		Classifier:   e.cls,
		Store:        e.store,
		Now:          func() time.Time { return e.now },
		ContentFetch: "off",
		MinChars:     80,
	}
	e.pipe = New(e.deps)
	job := domain.Job{ID: "job", Type: domain.JobBackfill, Status: domain.JobRunning, Provider: "fake", TaxonomyID: "tx", CreatedAt: t0}
	if err := e.store.Jobs().CreateJob(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.Taxonomies().SeedTaxonomy(context.Background(), testTaxonomy()); err != nil {
		t.Fatal(err)
	}
	return e
}

// rebuild applies changes to Deps.
func (e *env) rebuild(f func(*Deps)) {
	f(&e.deps)
	e.pipe = New(e.deps)
}

func decision(key string, conf float64, tags ...string) domain.Decision {
	d := domain.Decision{Collection: domain.CollectionChoice{Key: key}, Confidence: conf, Model: domain.ModelInfo{Model: "m1"}}
	for _, n := range tags {
		d.Tags = append(d.Tags, domain.TagSuggestion{Name: n})
	}
	return d
}

func testTaxonomy() domain.Taxonomy {
	return domain.Taxonomy{
		ID:      "tx",
		Mode:    domain.ModeFixed,
		Status:  domain.TaxonomyActive,
		Version: 3,
		Collections: []domain.TaxonomyCollection{
			{Key: "a", Name: "Alpha"},
			{Key: "b", Name: "Beta"},
			{Key: "needs_review", Name: "Needs Review", Role: "review"},
		},
		Rules: domain.TaxonomyRules{MinTags: 1, MaxTags: 5, MinConfidence: 0.6, AllowNewTags: true},
	}
}

func options(tr Trigger) Options {
	tax := testTaxonomy()
	return Options{
		Trigger:  tr,
		JobID:    "job",
		Taxonomy: tax,
		Resolved: map[string]domain.ID{"a": colA, "b": colB, "needs_review": review},
		Vocab:    taxonomy.NewRunVocab([]string{"go", "agents"}, nil, true),
	}
}

func inboxBookmark() domain.Bookmark {
	return domain.Bookmark{ID: bm1, URL: "https://example.test/post", Title: "Indexes", Description: longText, Tags: []string{"Mine"}, Collection: pfake.Inbox}
}

func (e *env) process(o Options) Outcome {
	e.t.Helper()
	out, err := e.pipe.Process(context.Background(), WorkItem{Bookmark: bm1}, o)
	if err != nil {
		e.t.Fatalf("Process: %v", err)
	}
	return out
}

func (e *env) state() domain.BookmarkState {
	e.t.Helper()
	st, ok, err := e.store.BookmarkStates().GetState(context.Background(), bm1)
	if err != nil || !ok {
		e.t.Fatalf("GetState: ok=%v err=%v", ok, err)
	}
	return st
}

func (e *env) putState(st domain.BookmarkState) {
	e.t.Helper()
	if err := e.store.BookmarkStates().PutState(context.Background(), st); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) bookmark() domain.Bookmark {
	e.t.Helper()
	b, ok := e.provider.Bookmark(bm1)
	if !ok {
		e.t.Fatal("bookmark gone")
	}
	return b
}

func (e *env) moves() int {
	n := 0
	for _, w := range e.provider.Writes() {
		if w.Patch.Collection != nil {
			n++
		}
	}
	return n
}

func (e *env) noSetTags() {
	e.t.Helper()
	for _, w := range e.provider.Writes() {
		if w.Patch.SetTags != nil {
			e.t.Errorf("pipeline used SetTags: %+v", w.Patch)
		}
	}
}

func wantStatus(t *testing.T, got Outcome, want OutcomeStatus) {
	t.Helper()
	if got.Status != want {
		t.Errorf("status = %q, want %q (reason %q)", got.Status, want, got.Record.Reason)
	}
}

func TestProcessNewInboxBookmark(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inboxBookmark())
	out := e.process(options(TriggerSync))
	wantStatus(t, out, Changed)

	b := e.bookmark()
	if b.Collection != colA {
		t.Errorf("collection = %v", b.Collection)
	}
	if !slices.Equal(b.Tags, []string{"Mine", "go", "brand-new"}) {
		t.Errorf("tags = %v", b.Tags)
	}
	e.noSetTags()

	st := e.state()
	if st.KnownCollection != colA || !slices.Equal(st.KnownTags, b.Tags) {
		t.Errorf("known = %v %v", st.KnownCollection, st.KnownTags)
	}
	if st.InputHash != InputHash(inboxBookmark(), nil) || st.TaxonomyVersion != 3 || st.Model != "m1" {
		t.Errorf("state = %+v", st)
	}
	if st.PendingCollection != nil || st.PendingTags != nil || st.PinSource != domain.PinNone || st.ProcessedAt == nil {
		t.Errorf("state = %+v", st)
	}
	d, ok, err := e.store.Jobs().GetDecision(context.Background(), "job", bm1)
	if err != nil || !ok || d.AppliedAt == nil || d.ObservedCollection != pfake.Inbox || !slices.Equal(d.ObservedTags, []string{"Mine"}) {
		t.Errorf("decision = %+v, ok=%v, err=%v", d, ok, err)
	}
}

func TestProcessSkipsSameHash(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inboxBookmark())
	e.process(options(TriggerSync))
	calls, writes := e.cls.Calls(), len(e.provider.Writes())
	wantStatus(t, e.process(options(TriggerSync)), Skipped)
	if e.cls.Calls() != calls || len(e.provider.Writes()) != writes {
		t.Errorf("second run: %d calls, %d writes", e.cls.Calls()-calls, len(e.provider.Writes())-writes)
	}
}

func TestTaxonomyVersionChangeSkipsOnlyForSync(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inboxBookmark())
	e.process(options(TriggerSync))
	calls := e.cls.Calls()

	o := options(TriggerSync)
	o.Taxonomy.Version = 4
	wantStatus(t, e.process(o), Skipped)
	if e.cls.Calls() != calls {
		t.Error("sync classified on a taxonomy change")
	}
	o = options(TriggerBackfill)
	o.Taxonomy.Version = 4
	e.process(o)
	if e.cls.Calls() != calls+1 {
		t.Error("backfill did not act on a taxonomy change")
	}
}

func TestProcessForceBypassesHash(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inboxBookmark())
	e.process(options(TriggerBackfill))
	o := options(TriggerBackfill)
	o.Force = true
	e.process(o)
	if e.cls.Calls() != 2 {
		t.Errorf("calls = %d", e.cls.Calls())
	}
}

func TestProcessDryRunNoWrites(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inboxBookmark())
	o := options(TriggerBackfill)
	o.DryRun = true
	out := e.process(o)
	wantStatus(t, out, Changed)
	if n := len(e.provider.Writes()); n != 0 {
		t.Errorf("writes = %d", n)
	}
	d, ok, err := e.store.Jobs().GetDecision(context.Background(), "job", bm1)
	if err != nil || !ok {
		t.Fatalf("decision missing: %v", err)
	}
	if d.ObservedCollection != pfake.Inbox || d.TargetCollection == nil || *d.TargetCollection != colA {
		t.Errorf("decision = %+v", d)
	}
	st := e.state()
	if st.KnownCollection != pfake.Inbox {
		t.Errorf("known = %v", st.KnownCollection)
	}
	if st.InputHash != "" {
		t.Error("dry run recorded the hash")
	}
}

func dryRunPlan(t *testing.T, e *env, o Options) *domain.DecisionRecord {
	t.Helper()
	o.DryRun = true
	rec := e.process(o).Record
	return &rec
}

func TestCorrectionAfterDryRun(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inboxBookmark())
	plan := dryRunPlan(t, e, options(TriggerBackfill))
	cutoff := e.now

	e.now = e.now.Add(time.Hour)
	e.provider.SetCollection(bm1, colB)

	o := options(TriggerBackfill)
	o.IsFirstBackfill = true
	o.PinCutoff = &cutoff
	o.Plan = plan
	o.Vocab = taxonomy.NewRunVocab([]string{"go"}, nil, false)
	wantStatus(t, e.process(o), Changed)

	if e.moves() != 0 {
		t.Errorf("moves = %d", e.moves())
	}
	if e.cls.Calls() != 1 {
		t.Errorf("plan re-classified: %d calls", e.cls.Calls())
	}
	st := e.state()
	if st.PinSource != domain.PinUser || st.KnownCollection != colB {
		t.Errorf("state = %+v", st)
	}
	b := e.bookmark()
	if b.Collection != colB || !slices.Contains(b.Tags, "go") {
		t.Errorf("bookmark = %+v", b)
	}
	if slices.Contains(b.Tags, "brand-new") {
		t.Errorf("unapproved tag written: %v", b.Tags)
	}
}

func TestPlanAppliedWithoutClassifying(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inboxBookmark())
	plan := dryRunPlan(t, e, options(TriggerBackfill))
	o := options(TriggerBackfill)
	o.Plan = plan
	wantStatus(t, e.process(o), Changed)
	if e.cls.Calls() != 1 {
		t.Errorf("calls = %d", e.cls.Calls())
	}
	if e.bookmark().Collection != colA {
		t.Errorf("collection = %v", e.bookmark().Collection)
	}
	if e.state().Model != "m1" {
		t.Errorf("model = %q", e.state().Model)
	}
}

func TestPlanHashChangedReclassifies(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inboxBookmark())
	plan := dryRunPlan(t, e, options(TriggerBackfill))

	b := e.bookmark()
	b.Title = "A different title"
	e.provider.Add(b)
	e.cls.Script(cfake.Result{Decision: decision("b", 0.9, "go", "other-new")})

	o := options(TriggerBackfill)
	o.Plan = plan
	o.Vocab = taxonomy.NewRunVocab([]string{"go"}, nil, true)
	e.process(o)
	if e.cls.Calls() != 2 {
		t.Errorf("calls = %d", e.cls.Calls())
	}
	got := e.bookmark()
	if got.Collection != colB {
		t.Errorf("collection = %v", got.Collection)
	}
	if slices.Contains(got.Tags, "other-new") {
		t.Errorf("unapproved tag written on re-classification: %v", got.Tags)
	}
	if e.state().InputHash != InputHash(b, nil) {
		t.Error("hash not the changed one")
	}
}

func TestApprovedNewTagsMapping(t *testing.T) {
	cases := []struct {
		name     string
		existing []string
		plan     []string
		approved map[string]string
		want     []string
	}{
		{name: "mapped value written", plan: []string{"ai-agents"}, approved: map[string]string{"ai-agents": "agents"}, want: []string{"Mine", "agents"}},
		{name: "unmapped new tag dropped", plan: []string{"unapproved", "go"}, want: []string{"Mine", "go"}},
		{name: "new tag new this run isn't existing", plan: []string{"proposed"}, want: []string{"Mine"}},
		{name: "mapping collapses duplicates", plan: []string{"ai-agents", "agent-ai", "agents"}, approved: map[string]string{"ai-agents": "agents", "agent-ai": "agents"}, want: []string{"Mine", "agents"}},
		{name: "mapping onto a tag already on the bookmark", plan: []string{"x"}, approved: map[string]string{"x": "mine"}, want: []string{"Mine"}},
		{
			name:     "cap applies after mapping",
			plan:     []string{"go", "agents", "n1", "n2", "n3"},
			approved: map[string]string{"n1": "one", "n2": "two", "n3": "three"},
			want:     []string{"Mine", "go", "agents", "one"},
		},
		{name: "lock at five existing", existing: []string{"a1", "a2", "a3", "a4", "a5"}, plan: []string{"go"}, want: []string{"a1", "a2", "a3", "a4", "a5"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			b := inboxBookmark()
			if c.existing != nil {
				b.Tags = c.existing
			}
			e.provider.Add(b)
			o := options(TriggerBackfill)
			o.Taxonomy.Rules.MaxTags = 4
			if c.existing != nil {
				o.Taxonomy.Rules.MaxTags = 8 // the 5-tag lock, not the cap, is under test
			}
			o.Vocab = taxonomy.NewRunVocab([]string{"go", "agents"}, nil, true)
			o.Vocab.Resolve("proposed")
			o.ApprovedNewTags = c.approved
			target := colA
			o.Plan = &domain.DecisionRecord{InputHash: InputHash(b, nil), ObservedCollection: b.Collection, TargetCollection: &target, AddTags: c.plan}
			e.process(o)
			if got := e.bookmark().Tags; !slices.Equal(got, c.want) {
				t.Errorf("tags = %v, want %v", got, c.want)
			}
			e.noSetTags()
		})
	}
}

func TestPlanObservedElsewhereDoesNotMove(t *testing.T) {
	e := newEnv(t) // state lost since the dry run: nothing else reveals the move
	b := inboxBookmark()
	b.Collection = colB
	e.provider.Add(b)
	target := colA
	o := options(TriggerBackfill)
	o.Plan = &domain.DecisionRecord{InputHash: InputHash(b, nil), ObservedCollection: pfake.Inbox, TargetCollection: &target}
	e.process(o)
	if e.moves() != 0 {
		t.Errorf("moves = %d", e.moves())
	}
}

// onSecondGet runs f on the re-read at step 8.
func onSecondGet(p *pfake.Provider, f func()) {
	var gets atomic.Int32
	p.OnGet(func(domain.ID) {
		if gets.Add(1) == 2 {
			f()
		}
	})
}

func TestUserMoveDuringClassify(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inboxBookmark())
	onSecondGet(e.provider, func() { e.provider.SetCollection(bm1, colB) })
	wantStatus(t, e.process(options(TriggerSync)), Unchanged)
	if e.moves() != 0 {
		t.Errorf("moves = %d", e.moves())
	}
	st := e.state()
	if st.PinSource != domain.PinUser || st.KnownCollection != colB {
		t.Errorf("state = %+v", st)
	}
	if e.bookmark().Collection != colB {
		t.Errorf("collection = %v", e.bookmark().Collection)
	}
}

func TestUserMoveDuringClassifyTagsArriveNextRun(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inboxBookmark())
	onSecondGet(e.provider, func() { e.provider.SetCollection(bm1, colB) })
	e.process(options(TriggerSync))
	e.provider.OnGet(nil)
	wantStatus(t, e.process(options(TriggerSync)), Changed)
	b := e.bookmark()
	if b.Collection != colB || !slices.Contains(b.Tags, "go") {
		t.Errorf("bookmark = %+v", b)
	}
	if e.moves() != 0 {
		t.Errorf("moves = %d", e.moves())
	}
}

func TestTrashedBetweenReadAndWrite(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inboxBookmark())
	onSecondGet(e.provider, func() { e.provider.SetCollection(bm1, trash) })
	wantStatus(t, e.process(options(TriggerBackfill)), Unchanged)
	if n := len(e.provider.Writes()); n != 0 {
		t.Errorf("writes = %d", n)
	}
	if e.bookmark().Collection != trash {
		t.Errorf("pulled out of trash: %v", e.bookmark().Collection)
	}
	if st := e.state(); st.PinSource != domain.PinUser || st.PendingCollection != nil {
		t.Errorf("state = %+v", st)
	}
}

func TestDeletedBookmark(t *testing.T) {
	t.Run("first read", func(t *testing.T) {
		e := newEnv(t)
		wantStatus(t, e.process(options(TriggerSync)), Deleted)
		if e.cls.Calls() != 0 {
			t.Errorf("calls = %d", e.cls.Calls())
		}
		if st := e.state(); st.Status != string(Deleted) {
			t.Errorf("status = %q", st.Status)
		}
	})
	t.Run("re-read", func(t *testing.T) {
		e := newEnv(t)
		e.provider.Add(inboxBookmark())
		onSecondGet(e.provider, func() { e.provider.Remove(bm1) })
		wantStatus(t, e.process(options(TriggerSync)), Deleted)
		if n := len(e.provider.Writes()); n != 0 {
			t.Errorf("writes = %d", n)
		}
	})
	t.Run("write", func(t *testing.T) {
		e := newEnv(t)
		e.provider.Add(inboxBookmark())
		e.deps.Provider = writeFails{e.provider}
		e.pipe = New(e.deps)
		wantStatus(t, e.process(options(TriggerSync)), Deleted)
		st := e.state()
		if st.Status != string(Deleted) || st.PendingCollection != nil || st.PendingTags != nil {
			t.Errorf("state = %+v", st)
		}
		d, ok, err := e.store.Jobs().GetDecision(context.Background(), "job", bm1)
		if err != nil || !ok || d.Result != resultDeleted {
			t.Errorf("decision = %+v, ok=%v, err=%v", d, ok, err)
		}
	})
}

// writeFails is a provider whose bookmark disappears at the moment of the write.
type writeFails struct{ *pfake.Provider }

func (w writeFails) UpdateBookmark(_ context.Context, id domain.ID, _ ports.BookmarkPatch) (domain.Bookmark, error) {
	w.Remove(id)
	return domain.Bookmark{}, domain.ErrNotFound
}

func TestNoUsableTextGoesToReview(t *testing.T) {
	bare := domain.Bookmark{ID: bm1, URL: "https://example.test/doc.pdf", Tags: []string{"Mine"}, Collection: pfake.Inbox}
	cases := []struct {
		name string
		edit func(*domain.Bookmark)
	}{
		{name: "empty fields"},
		{name: "whitespace only", edit: func(b *domain.Bookmark) { b.Title, b.Description, b.Excerpt = "  ", "\n", "\t" }},
		{name: "title repeats the url", edit: func(b *domain.Bookmark) { b.Title = b.URL }},
		{name: "title is only the domain", edit: func(b *domain.Bookmark) { b.Title = "www.Example.test" }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			e.rebuild(func(d *Deps) { d.ContentFetch = "auto"; d.Fetcher = e.fetch })
			e.fetch.errs = []error{domain.ErrNotFound}
			b := bare
			if c.edit != nil {
				c.edit(&b)
			}
			e.provider.Add(b)
			wantStatus(t, e.process(options(TriggerSync)), NeedsReview)
			if e.cls.Calls() != 0 {
				t.Errorf("classifier called %d times", e.cls.Calls())
			}
			got := e.bookmark()
			if got.Collection != review {
				t.Errorf("collection = %v", got.Collection)
			}
			if !slices.Equal(got.Tags, []string{"Mine"}) {
				t.Errorf("tags = %v", got.Tags)
			}
			if st := e.state(); st.InputHash == "" {
				t.Error("hash not recorded")
			}
		})
	}
}

func TestDomainOnlyNeverEnough(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(domain.Bookmark{ID: bm1, URL: "https://news.example.test/articles/some-long-slug-with-words", Collection: pfake.Inbox})
	wantStatus(t, e.process(options(TriggerSync)), NeedsReview) // content_fetch off
	if e.cls.Calls() != 0 {
		t.Errorf("classifier called %d times", e.cls.Calls())
	}
	if e.bookmark().Collection != review {
		t.Errorf("collection = %v", e.bookmark().Collection)
	}
}

func TestNoUsableTextStaysInAGoodHome(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(domain.Bookmark{ID: bm1, URL: "https://example.test/doc.pdf", Collection: colA})
	wantStatus(t, e.process(options(TriggerBackfill)), Unchanged)
	if n := len(e.provider.Writes()); n != 0 {
		t.Errorf("writes = %d", n)
	}
}

func TestClassifierErrorFailsItem(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inboxBookmark())
	e.cls.Script(cfake.Result{Err: errors.New("down")})
	_, err := e.pipe.Process(context.Background(), WorkItem{Bookmark: bm1}, options(TriggerSync))
	if err == nil {
		t.Fatal("want an error")
	}
	if n := len(e.provider.Writes()); n != 0 {
		t.Errorf("writes = %d", n)
	}
	if e.bookmark().Collection != pfake.Inbox {
		t.Errorf("moved: %v", e.bookmark().Collection)
	}
	if st := e.state(); st.InputHash != "" {
		t.Error("hash recorded on failure")
	}
}

func TestInvalidOutputRetriesOnce(t *testing.T) {
	t.Run("then success", func(t *testing.T) {
		e := newEnv(t)
		e.provider.Add(inboxBookmark())
		e.cls.Script(cfake.Result{Err: domain.ErrInvalidOutput}, cfake.Result{Decision: decision("a", 0.9)})
		wantStatus(t, e.process(options(TriggerSync)), Changed)
		if e.cls.Calls() != 2 {
			t.Errorf("calls = %d", e.cls.Calls())
		}
	})
	t.Run("twice is an error", func(t *testing.T) {
		e := newEnv(t)
		e.provider.Add(inboxBookmark())
		e.cls.Script(cfake.Result{Err: domain.ErrInvalidOutput})
		_, err := e.pipe.Process(context.Background(), WorkItem{Bookmark: bm1}, options(TriggerSync))
		if !errors.Is(err, domain.ErrInvalidOutput) {
			t.Errorf("err = %v", err)
		}
		if e.cls.Calls() != 2 {
			t.Errorf("calls = %d", e.cls.Calls())
		}
	})
}

func TestFallbackOnError(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inboxBookmark())
	e.cls.Script(cfake.Result{Err: errors.New("down")})
	fb := cfake.New("backup", cfake.Result{Decision: decision("b", 0.9)})
	e.rebuild(func(d *Deps) { d.Fallback = fb })
	out := e.process(options(TriggerSync))
	wantStatus(t, out, Changed)
	if fb.Calls() != 1 {
		t.Errorf("fallback calls = %d", fb.Calls())
	}
	if out.Record.Decision.Model.Classifier != "backup" || !strings.Contains(out.Record.Reason, "fallback") {
		t.Errorf("record = %+v", out.Record)
	}
	if e.bookmark().Collection != colB {
		t.Errorf("collection = %v", e.bookmark().Collection)
	}
}

func TestFallbackFailureFailsItem(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inboxBookmark())
	e.cls.Script(cfake.Result{Err: errors.New("down")})
	e.rebuild(func(d *Deps) { d.Fallback = cfake.New("backup", cfake.Result{Err: errors.New("also down")}) })
	if _, err := e.pipe.Process(context.Background(), WorkItem{Bookmark: bm1}, options(TriggerSync)); err == nil {
		t.Fatal("want an error")
	}
	if e.bookmark().Collection != pfake.Inbox {
		t.Error("moved")
	}
}

func TestFallbackNotUsedForLowConfidence(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inboxBookmark())
	e.cls.Script(cfake.Result{Decision: decision("a", 0.1)})
	fb := cfake.New("backup", cfake.Result{Decision: decision("b", 0.9)})
	e.rebuild(func(d *Deps) { d.Fallback = fb })
	wantStatus(t, e.process(options(TriggerSync)), NeedsReview)
	if fb.Calls() != 0 {
		t.Errorf("fallback calls = %d", fb.Calls())
	}
	if e.bookmark().Collection != review {
		t.Errorf("collection = %v", e.bookmark().Collection)
	}
}

func TestContentFetch(t *testing.T) {
	short := domain.Bookmark{ID: bm1, URL: "https://example.test/p", Title: "Short title", Collection: pfake.Inbox}
	long := inboxBookmark()
	cases := []struct {
		mode  string
		b     domain.Bookmark
		calls int
	}{
		{"off", short, 0},
		{"off", long, 0},
		{"auto", short, 1},
		{"auto", long, 0},
		{"always", short, 1},
		{"always", long, 1},
		{"", short, 1}, // default is auto
		{"", long, 0},
	}
	for _, c := range cases {
		t.Run(c.mode+"/"+c.b.Title, func(t *testing.T) {
			e := newEnv(t)
			e.fetch.text = "fetched page text"
			e.rebuild(func(d *Deps) { d.ContentFetch = c.mode; d.Fetcher = e.fetch })
			e.provider.Add(c.b)
			e.process(options(TriggerSync))
			if e.fetch.calls != c.calls {
				t.Errorf("fetch calls = %d, want %d", e.fetch.calls, c.calls)
			}
			wantContent := ""
			if c.calls > 0 {
				wantContent = "fetched page text"
			}
			if got := e.cls.Requests()[0].Content; got != wantContent {
				t.Errorf("content = %q", got)
			}
		})
	}
}

func TestFetchTemporaryFailure(t *testing.T) {
	e := newEnv(t)
	e.rebuild(func(d *Deps) { d.ContentFetch = "auto"; d.Fetcher = e.fetch })
	temp := errors.Join(domain.ErrTemporary, errors.New("503"))
	e.fetch.errs = []error{temp, temp, temp}
	e.provider.Add(domain.Bookmark{ID: bm1, URL: "https://example.test/p", Collection: pfake.Inbox})

	for attempt := 1; attempt <= 2; attempt++ {
		_, err := e.pipe.Process(context.Background(), WorkItem{Bookmark: bm1}, options(TriggerSync))
		if !errors.Is(err, domain.ErrTemporary) {
			t.Fatalf("attempt %d: err = %v", attempt, err)
		}
		st := e.state()
		if st.FetchAttempts != attempt || st.InputHash != "" {
			t.Errorf("attempt %d: state = %+v", attempt, st)
		}
		if e.bookmark().Collection != pfake.Inbox {
			t.Errorf("attempt %d: moved to %v", attempt, e.bookmark().Collection)
		}
	}
	wantStatus(t, e.process(options(TriggerSync)), NeedsReview) // 3rd counts as permanent
	if e.bookmark().Collection != review {
		t.Errorf("collection = %v", e.bookmark().Collection)
	}
	if st := e.state(); st.FetchAttempts != 0 {
		t.Errorf("attempts not reset: %d", st.FetchAttempts)
	}
}

func TestFetchCancelDoesNotCountAnAttempt(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(domain.Bookmark{ID: bm1, URL: "https://example.test/p", Collection: pfake.Inbox})
	ctx, cancel := context.WithCancel(context.Background())
	e.rebuild(func(d *Deps) { d.ContentFetch = "auto"; d.Fetcher = cancelingFetcher{cancel} })
	if _, err := e.pipe.Process(ctx, WorkItem{Bookmark: bm1}, options(TriggerSync)); err == nil {
		t.Fatal("want an error")
	}
	if st := e.state(); st.FetchAttempts != 0 {
		t.Errorf("attempts = %d", st.FetchAttempts)
	}
}

type cancelingFetcher struct{ cancel func() }

func (c cancelingFetcher) Fetch(context.Context, string) (string, error) {
	c.cancel()
	return "", domain.ErrTemporary
}

func TestFetchPermanentFailure(t *testing.T) {
	t.Run("enough text classifies", func(t *testing.T) {
		e := newEnv(t)
		e.rebuild(func(d *Deps) { d.ContentFetch = "auto"; d.Fetcher = e.fetch })
		e.fetch.errs = []error{domain.ErrNotFound}
		e.provider.Add(domain.Bookmark{ID: bm1, URL: "https://example.test/p", Title: "Short but real title", Collection: pfake.Inbox})
		wantStatus(t, e.process(options(TriggerSync)), Changed)
		if e.cls.Calls() != 1 {
			t.Errorf("calls = %d", e.cls.Calls())
		}
	})
	t.Run("too little goes to review", func(t *testing.T) {
		e := newEnv(t)
		e.rebuild(func(d *Deps) { d.ContentFetch = "auto"; d.Fetcher = e.fetch })
		e.fetch.errs = []error{domain.ErrNotFound}
		e.provider.Add(domain.Bookmark{ID: bm1, URL: "https://example.test/p", Collection: pfake.Inbox})
		wantStatus(t, e.process(options(TriggerSync)), NeedsReview)
		if e.cls.Calls() != 0 {
			t.Errorf("calls = %d", e.cls.Calls())
		}
	})
}

func TestBaselineRecordsOnly(t *testing.T) {
	e := newEnv(t)
	b := inboxBookmark()
	b.Collection = colB
	e.provider.Add(b)
	o := options(TriggerSync)
	o.Baseline = true
	wantStatus(t, e.process(o), Skipped)
	if e.cls.Calls() != 0 || len(e.provider.Writes()) != 0 {
		t.Errorf("calls = %d, writes = %d", e.cls.Calls(), len(e.provider.Writes()))
	}
	st := e.state()
	if st.PinSource != domain.PinBaseline || st.KnownCollection != colB || st.InputHash == "" {
		t.Errorf("state = %+v", st)
	}
	wantStatus(t, e.process(options(TriggerSync)), Skipped) // and later syncs leave it alone
	if e.cls.Calls() != 0 {
		t.Errorf("calls = %d", e.cls.Calls())
	}
}

func TestBaselineProcessesInbox(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inboxBookmark())
	o := options(TriggerSync)
	o.Baseline = true
	wantStatus(t, e.process(o), Changed)
	if e.bookmark().Collection != colA {
		t.Errorf("collection = %v", e.bookmark().Collection)
	}
}

func TestBaselineKeepsUserPin(t *testing.T) {
	e := newEnv(t)
	b := inboxBookmark()
	b.Collection = colB
	e.provider.Add(b)
	e.putState(domain.BookmarkState{ID: bm1, KnownCollection: colA}) // moved by the user
	o := options(TriggerSync)
	o.Baseline = true
	e.process(o)
	if st := e.state(); st.PinSource != domain.PinUser {
		t.Errorf("pin = %q", st.PinSource)
	}
}

func TestSyncNewInCollectionPinsUserTagsOnly(t *testing.T) {
	e := newEnv(t)
	b := inboxBookmark()
	b.Collection = colB
	e.provider.Add(b)
	wantStatus(t, e.process(options(TriggerSync)), Changed)
	if e.moves() != 0 {
		t.Errorf("moves = %d", e.moves())
	}
	if !slices.Contains(e.bookmark().Tags, "go") {
		t.Errorf("tags = %v", e.bookmark().Tags)
	}
	if st := e.state(); st.PinSource != domain.PinUser {
		t.Errorf("pin = %q", st.PinSource)
	}
	if e.cls.Requests()[0].WantCollection {
		t.Error("asked for a collection")
	}
}

func TestSyncChangedInput(t *testing.T) {
	seed := func(t *testing.T, loc domain.ID) *env {
		e := newEnv(t)
		b := inboxBookmark()
		b.Collection = loc
		e.provider.Add(b)
		e.putState(domain.BookmarkState{ID: bm1, KnownCollection: loc, InputHash: "old", TaxonomyVersion: 3})
		return e
	}
	t.Run("elsewhere records the hash only", func(t *testing.T) {
		e := seed(t, colB)
		wantStatus(t, e.process(options(TriggerSync)), Skipped)
		if e.cls.Calls() != 0 || len(e.provider.Writes()) != 0 {
			t.Errorf("calls = %d, writes = %d", e.cls.Calls(), len(e.provider.Writes()))
		}
		if st := e.state(); st.InputHash != InputHash(e.bookmark(), nil) {
			t.Errorf("hash = %q", st.InputHash)
		}
	})
	t.Run("needs review classifies again", func(t *testing.T) {
		e := seed(t, review)
		wantStatus(t, e.process(options(TriggerSync)), Changed)
		if e.bookmark().Collection != colA {
			t.Errorf("collection = %v", e.bookmark().Collection)
		}
	})
}

func TestRestoredBookmarkSkippedBySync(t *testing.T) {
	seed := func(t *testing.T) *env {
		e := newEnv(t)
		b := inboxBookmark()
		e.provider.Add(b)
		at := t0.Add(-time.Hour)
		e.putState(domain.BookmarkState{ID: bm1, KnownCollection: b.Collection, RestoredAt: &at, InputHash: InputHash(b, nil), TaxonomyVersion: 3})
		return e
	}
	t.Run("sync skips", func(t *testing.T) {
		e := seed(t)
		b := e.bookmark()
		b.Title = "edited"
		e.provider.Add(b)
		wantStatus(t, e.process(options(TriggerSync)), Skipped)
		if e.cls.Calls() != 0 || len(e.provider.Writes()) != 0 {
			t.Errorf("calls = %d, writes = %d", e.cls.Calls(), len(e.provider.Writes()))
		}
		if e.state().RestoredAt == nil {
			t.Error("RestoredAt cleared by sync")
		}
	})
	t.Run("backfill processes despite the same hash", func(t *testing.T) {
		e := seed(t)
		wantStatus(t, e.process(options(TriggerBackfill)), Changed)
		if e.cls.Calls() != 1 {
			t.Errorf("calls = %d", e.cls.Calls())
		}
		if e.state().RestoredAt != nil {
			t.Error("RestoredAt not cleared by a real backfill")
		}
	})
	t.Run("dry-run backfill keeps it", func(t *testing.T) {
		e := seed(t)
		o := options(TriggerBackfill)
		o.DryRun = true
		e.process(o)
		if e.state().RestoredAt == nil {
			t.Error("RestoredAt cleared by a dry run")
		}
	})
}

func highlightEnv(t *testing.T, conf float64) *env {
	e := newEnv(t)
	e.provider.Add(inboxBookmark())
	e.provider.AddHighlight(domain.Highlight{ID: domain.ID{Provider: "fake", Local: "h1"}, Bookmark: bm1, Text: "highlighted words"})
	e.cls.Script(cfake.Result{Decision: decision("a", conf, "go")})
	return e
}

func TestHighlightTagsOnly(t *testing.T) {
	hl := domain.ID{Provider: "fake", Local: "h1"}
	item := WorkItem{Bookmark: bm1, FocusHighlight: &hl}
	t.Run("tags added, no move", func(t *testing.T) {
		e := highlightEnv(t, 0.9)
		out, err := e.pipe.Process(context.Background(), item, options(TriggerWebhook))
		if err != nil {
			t.Fatal(err)
		}
		wantStatus(t, out, Changed)
		if e.moves() != 0 {
			t.Errorf("moves = %d", e.moves())
		}
		if !slices.Contains(e.bookmark().Tags, "go") {
			t.Errorf("tags = %v", e.bookmark().Tags)
		}
		req := e.cls.Requests()[0]
		if req.WantCollection || req.Subject.FocusHighlight == nil || req.Subject.FocusHighlight.ID != hl {
			t.Errorf("request = %+v", req)
		}
	})
	t.Run("below min confidence, no tags", func(t *testing.T) {
		e := highlightEnv(t, 0.1)
		out, err := e.pipe.Process(context.Background(), item, options(TriggerWebhook))
		if err != nil {
			t.Fatal(err)
		}
		wantStatus(t, out, Unchanged)
		if n := len(e.provider.Writes()); n != 0 {
			t.Errorf("writes = %d", n)
		}
	})
	t.Run("leaves a new inbox bookmark to be filed later", func(t *testing.T) {
		e := highlightEnv(t, 0.9)
		if _, err := e.pipe.Process(context.Background(), item, options(TriggerWebhook)); err != nil {
			t.Fatal(err)
		}
		e.cls.Script(cfake.Result{Decision: decision("a", 0.9)})
		wantStatus(t, e.process(options(TriggerSync)), Changed)
		if e.bookmark().Collection != colA {
			t.Errorf("collection = %v", e.bookmark().Collection)
		}
	})
	t.Run("provider without highlights skips", func(t *testing.T) {
		e := highlightEnv(t, 0.9)
		e.provider.SetCapabilities(ports.ProviderCapabilities{})
		out, err := e.pipe.Process(context.Background(), item, options(TriggerWebhook))
		if err != nil {
			t.Fatal(err)
		}
		wantStatus(t, out, Skipped)
		if e.cls.Calls() != 0 {
			t.Errorf("calls = %d", e.cls.Calls())
		}
	})
}

func TestCrashAfterWrite(t *testing.T) {
	t.Run("moved collection is finalized without a pin", func(t *testing.T) {
		e := newEnv(t)
		b := inboxBookmark()
		b.Collection = colA
		e.provider.Add(b)
		pending := colA
		e.putState(domain.BookmarkState{ID: bm1, KnownCollection: pfake.Inbox, PendingCollection: &pending, PendingTags: []string{"go"}, Model: "m1"})
		wantStatus(t, e.process(options(TriggerSync)), Changed)
		st := e.state()
		if st.PinSource != domain.PinNone || st.PendingCollection != nil || st.PendingTags != nil {
			t.Errorf("state = %+v", st)
		}
		if st.KnownCollection != colA || st.InputHash != InputHash(b, nil) || st.TaxonomyVersion != 3 || st.Model != "m1" {
			t.Errorf("state = %+v", st)
		}
		if e.cls.Calls() != 0 || len(e.provider.Writes()) != 0 {
			t.Errorf("calls = %d, writes = %d", e.cls.Calls(), len(e.provider.Writes()))
		}
	})
	t.Run("tags-only write that landed is finalized", func(t *testing.T) {
		e := newEnv(t)
		b := inboxBookmark()
		b.Collection = colB
		b.Tags = []string{"Mine", "go"}
		e.provider.Add(b)
		e.putState(domain.BookmarkState{ID: bm1, KnownCollection: colB, PendingTags: []string{"GO"}})
		wantStatus(t, e.process(options(TriggerBackfill)), Changed)
		if e.cls.Calls() != 0 || e.state().PendingTags != nil {
			t.Errorf("calls = %d, state = %+v", e.cls.Calls(), e.state())
		}
	})
	t.Run("write that never landed is redone", func(t *testing.T) {
		e := newEnv(t)
		e.provider.Add(inboxBookmark())
		pending := colB
		e.putState(domain.BookmarkState{ID: bm1, KnownCollection: pfake.Inbox, PendingCollection: &pending})
		wantStatus(t, e.process(options(TriggerSync)), Changed)
		if e.cls.Calls() != 1 || e.bookmark().Collection != colA {
			t.Errorf("calls = %d, collection = %v", e.cls.Calls(), e.bookmark().Collection)
		}
		if st := e.state(); st.PinSource != domain.PinNone || st.PendingCollection != nil {
			t.Errorf("state = %+v", st)
		}
	})
	t.Run("user moved elsewhere after the crash pins", func(t *testing.T) {
		e := newEnv(t)
		b := inboxBookmark()
		b.Collection = colB
		e.provider.Add(b)
		pending := colA
		e.putState(domain.BookmarkState{ID: bm1, KnownCollection: pfake.Inbox, PendingCollection: &pending})
		e.process(options(TriggerSync))
		if st := e.state(); st.PinSource != domain.PinUser || st.PendingCollection != nil {
			t.Errorf("state = %+v", st)
		}
		if e.moves() != 0 {
			t.Errorf("moves = %d", e.moves())
		}
	})
	t.Run("failed write leaves the pending write", func(t *testing.T) {
		e := newEnv(t)
		e.provider.Add(inboxBookmark())
		boom := errors.New("boom")
		e.deps.Provider = writeErr{e.provider, boom}
		e.pipe = New(e.deps)
		if _, err := e.pipe.Process(context.Background(), WorkItem{Bookmark: bm1}, options(TriggerSync)); !errors.Is(err, boom) {
			t.Fatalf("err = %v", err)
		}
		if st := e.state(); st.PendingCollection == nil || *st.PendingCollection != colA || st.InputHash != "" {
			t.Errorf("state = %+v", st)
		}
	})
}

type writeErr struct {
	*pfake.Provider
	err error
}

func (w writeErr) UpdateBookmark(context.Context, domain.ID, ports.BookmarkPatch) (domain.Bookmark, error) {
	return domain.Bookmark{}, w.err
}

func TestManualMoveSavedEvenWhenClassifierFails(t *testing.T) {
	e := newEnv(t)
	b := inboxBookmark()
	b.Collection = colB
	e.provider.Add(b)
	e.putState(domain.BookmarkState{ID: bm1, KnownCollection: colA})
	e.cls.Script(cfake.Result{Err: errors.New("down")})
	if _, err := e.pipe.Process(context.Background(), WorkItem{Bookmark: bm1}, options(TriggerBackfill)); err == nil {
		t.Fatal("want an error")
	}
	if st := e.state(); st.PinSource != domain.PinUser || st.KnownCollection != colB {
		t.Errorf("state = %+v", st)
	}
}

func TestOurMoveClearsAnOverriddenPin(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inboxBookmark())
	before := t0.Add(-time.Hour)
	cutoff := t0
	e.putState(domain.BookmarkState{ID: bm1, KnownCollection: pfake.Inbox, PinSource: domain.PinUser, PinnedAt: &before})
	o := options(TriggerBackfill)
	o.IsFirstBackfill = true
	o.PinCutoff = &cutoff
	e.process(o)
	if e.bookmark().Collection != colA {
		t.Fatalf("collection = %v", e.bookmark().Collection)
	}
	if st := e.state(); st.PinSource != domain.PinNone || st.PinnedAt != nil {
		t.Errorf("state = %+v", st)
	}
}

func TestPinnedBookmarkKeepsItsPlace(t *testing.T) {
	e := newEnv(t)
	b := inboxBookmark()
	b.Collection = colB
	e.provider.Add(b)
	before := t0.Add(-time.Hour)
	e.putState(domain.BookmarkState{ID: bm1, KnownCollection: colB, PinSource: domain.PinUser, PinnedAt: &before, InputHash: "old"})
	wantStatus(t, e.process(options(TriggerBackfill)), Changed) // tags only
	if e.moves() != 0 {
		t.Errorf("moves = %d", e.moves())
	}
	if st := e.state(); st.PinSource != domain.PinUser {
		t.Errorf("pin = %q", st.PinSource)
	}
}

func TestFiveTagsLockedAndNotSentToClassifierWhenNothingToDecide(t *testing.T) {
	e := newEnv(t)
	b := inboxBookmark()
	b.Collection = colB
	b.Tags = []string{"t1", "t2", "t3", "t4", "t5"}
	e.provider.Add(b)
	wantStatus(t, e.process(options(TriggerSync)), Unchanged) // pinned as new-in-collection
	if e.cls.Calls() != 0 || len(e.provider.Writes()) != 0 {
		t.Errorf("calls = %d, writes = %d", e.cls.Calls(), len(e.provider.Writes()))
	}
}

func TestTagsAddedDuringClassifyRespectTheCap(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inboxBookmark())
	onSecondGet(e.provider, func() {
		b := e.bookmark()
		b.Tags = []string{"Mine", "u2", "u3", "u4", "u5"}
		e.provider.Add(b)
	})
	e.process(options(TriggerSync))
	if got := e.bookmark().Tags; len(got) != 5 {
		t.Errorf("tags = %v", got)
	}
	if e.moves() != 1 {
		t.Errorf("moves = %d", e.moves())
	}
}

func TestRequestContents(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inboxBookmark())
	e.provider.AddHighlight(domain.Highlight{ID: domain.ID{Provider: "fake", Local: "h1"}, Bookmark: bm1, Text: "words"})
	e.process(options(TriggerSync))
	r := e.cls.Requests()[0]
	if !r.WantCollection || r.TagsWanted != 4 || len(r.Subject.Highlights) != 1 {
		t.Errorf("request = %+v", r)
	}
	if r.Taxonomy.ID != "tx" || r.Taxonomy.Version != 3 || len(r.Taxonomy.Collections) != 3 {
		t.Errorf("taxonomy = %+v", r.Taxonomy)
	}
	if !slices.Equal(r.TagVocab[:2], []string{"go", "agents"}) {
		t.Errorf("vocab = %v", r.TagVocab)
	}
}

func TestPerBookmarkLock(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inboxBookmark())
	var active, peak atomic.Int32
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	e.cls.OnClassify(func(ports.ClassifyRequest) {
		n := active.Add(1)
		if n > peak.Load() {
			peak.Store(n)
		}
		entered <- struct{}{}
		<-release
		active.Add(-1)
	})
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := e.pipe.Process(context.Background(), WorkItem{Bookmark: bm1}, options(TriggerSync)); err != nil {
				t.Error(err)
			}
		}()
	}
	<-entered
	close(release)
	wg.Wait()
	if peak.Load() != 1 {
		t.Errorf("peak concurrency = %d", peak.Load())
	}
	if e.cls.Calls() != 1 {
		t.Errorf("calls = %d, the second run should skip on the hash", e.cls.Calls())
	}
	if n := len(e.pipe.locks); n != 0 {
		t.Errorf("lock table leaked %d entries", n)
	}
}

func TestLockWaitHonorsContext(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inboxBookmark())
	release := make(chan struct{})
	entered := make(chan struct{})
	e.cls.OnClassify(func(ports.ClassifyRequest) { close(entered); <-release })
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.pipe.Process(context.Background(), WorkItem{Bookmark: bm1}, options(TriggerSync))
	}()
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := e.pipe.Process(ctx, WorkItem{Bookmark: bm1}, options(TriggerSync)); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v", err)
	}
	close(release)
	<-done
}

func dynamicOptions() Options {
	o := options(TriggerSync)
	o.Taxonomy.Mode = domain.ModeDynamic
	o.Taxonomy.Collections = []domain.TaxonomyCollection{
		{Key: "a", Name: "Alpha"},
		{Key: "needs_review", Name: "Needs Review", Role: "review"},
	}
	o.Resolved = map[string]domain.ID{"a": colA, "needs_review": review}
	o.Taxonomy.Dynamic = domain.DynamicSettings{MaxCollections: 3, AutoCreate: true, AutoCreateMinConfidence: 0.8}
	return o
}

func proposal(name string, conf float64) cfake.Result {
	d := decision("", conf)
	d.Collection = domain.CollectionChoice{ProposedName: name}
	return cfake.Result{Decision: d}
}

func addInbox(e *env, local string) domain.ID {
	id := domain.ID{Provider: "fake", Local: local}
	b := inboxBookmark()
	b.ID = id
	b.URL += "/" + local
	e.provider.Add(b)
	return id
}

func TestAutoCreateCollection(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	if err := e.store.Snapshots().CreateSnapshot(ctx, domain.Snapshot{ID: "snap", Provider: "fake", JobID: "job", CreatedAt: t0}); err != nil {
		t.Fatal(err)
	}
	o := dynamicOptions()
	o.SnapshotID = "snap"

	// First proposal: created, recorded in the snapshot, and the bookmark filed.
	first := addInbox(e, "1")
	e.cls.Script(proposal("New Home", 0.9))
	out, err := e.pipe.Process(ctx, WorkItem{Bookmark: first}, o)
	if err != nil {
		t.Fatal(err)
	}
	wantStatus(t, out, Changed)
	got, _ := e.provider.Bookmark(first)
	if got.Collection.Local != "created-1" {
		t.Fatalf("collection = %v", got.Collection)
	}
	snap, err := e.store.Snapshots().GetSnapshot(ctx, "snap")
	if err != nil || !slices.Equal(snap.CreatedCollections, []domain.ID{got.Collection}) {
		t.Fatalf("snapshot = %+v, %v", snap, err)
	}

	// Same name again, in different case: reused, not created twice.
	second := addInbox(e, "2")
	e.cls.Script(proposal("new home", 0.9))
	if _, err := e.pipe.Process(ctx, WorkItem{Bookmark: second}, o); err != nil {
		t.Fatal(err)
	}
	got2, _ := e.provider.Bookmark(second)
	if got2.Collection != got.Collection {
		t.Fatalf("second = %v, want %v", got2.Collection, got.Collection)
	}
	if req := e.cls.Requests()[1]; len(req.Taxonomy.Collections) != 3 {
		t.Errorf("classifier wasn't told about the created collection: %+v", req.Taxonomy.Collections)
	}

	// A different name is over the cap of 3: review, nothing created.
	third := addInbox(e, "3")
	e.cls.Script(proposal("Another", 0.9))
	out, err = e.pipe.Process(ctx, WorkItem{Bookmark: third}, o)
	if err != nil {
		t.Fatal(err)
	}
	wantStatus(t, out, NeedsReview)
	got3, _ := e.provider.Bookmark(third)
	if got3.Collection != review {
		t.Errorf("third = %v", got3.Collection)
	}
	if !strings.Contains(out.Record.Reason, "cap") {
		t.Errorf("reason = %q", out.Record.Reason)
	}
	if out.Record.Decision.Collection.ProposedName != "Another" {
		t.Errorf("proposal not recorded: %+v", out.Record.Decision.Collection)
	}
	cols, _ := e.provider.ListCollections(ctx)
	if len(cols) != 1 {
		t.Errorf("collections = %+v", cols)
	}

	// The cap survives a restart.
	fresh := New(e.deps)
	fourth := addInbox(e, "4")
	e.cls.Script(proposal("Yet Another", 0.9))
	if _, err := fresh.Process(ctx, WorkItem{Bookmark: fourth}, o); err != nil {
		t.Fatal(err)
	}
	if cols, _ := e.provider.ListCollections(ctx); len(cols) != 1 {
		t.Errorf("created past the cap after restart: %+v", cols)
	}
}

func TestAutoCreateConcurrentItemsCannotPassTheCap(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	first, second := addInbox(e, "1"), addInbox(e, "2")
	e.cls.Script(proposal("One", 0.9), proposal("Two", 0.9))
	// Hold both items in classification, so both pass the validator's cap check
	// before either has created anything.
	var arrived sync.WaitGroup
	arrived.Add(2)
	e.cls.OnClassify(func(ports.ClassifyRequest) {
		arrived.Done()
		arrived.Wait()
	})
	o := dynamicOptions() // room for exactly one more collection
	outs := make([]Outcome, 2)
	var wg sync.WaitGroup
	for i, id := range []domain.ID{first, second} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := e.pipe.Process(ctx, WorkItem{Bookmark: id}, o)
			if err != nil {
				t.Error(err)
			}
			outs[i] = out
		}()
	}
	wg.Wait()
	cols, err := e.provider.ListCollections(ctx)
	if err != nil || len(cols) != 1 {
		t.Fatalf("collections = %+v, %v", cols, err)
	}
	statuses := []OutcomeStatus{outs[0].Status, outs[1].Status}
	if !slices.Contains(statuses, Changed) || !slices.Contains(statuses, NeedsReview) {
		t.Errorf("statuses = %v", statuses)
	}
}

func TestAutoCreateWithoutSnapshot(t *testing.T) {
	e := newEnv(t)
	e.cls.Script(proposal("New Home", 0.9))
	e.provider.Add(inboxBookmark())
	wantStatus(t, e.process(dynamicOptions()), Changed)
	if e.bookmark().Collection.Local != "created-1" {
		t.Errorf("collection = %v", e.bookmark().Collection)
	}
}

func TestAutoCreateReusesExistingProviderCollection(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	existing := domain.ID{Provider: "fake", Local: "existing"}
	e.provider.AddCollection(domain.Collection{ID: existing, Name: "New Home"})
	if err := e.store.Snapshots().CreateSnapshot(ctx, domain.Snapshot{ID: "snap", Provider: "fake", JobID: "job", CreatedAt: t0}); err != nil {
		t.Fatal(err)
	}
	o := dynamicOptions()
	o.SnapshotID = "snap"
	e.cls.Script(proposal("new home", 0.9))
	e.provider.Add(inboxBookmark())
	e.process(o)
	if e.bookmark().Collection != existing {
		t.Errorf("collection = %v", e.bookmark().Collection)
	}
	snap, _ := e.store.Snapshots().GetSnapshot(ctx, "snap")
	if len(snap.CreatedCollections) != 0 {
		t.Errorf("a pre-existing collection was recorded as created: %v", snap.CreatedCollections)
	}
}

func TestAutoCreateDryRunCreatesNothingAndPlanCreates(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	e.cls.Script(proposal("New Home", 0.9))
	e.provider.Add(inboxBookmark())
	o := dynamicOptions()
	o.Trigger = TriggerBackfill
	dry := o
	dry.DryRun = true
	out := e.process(dry)
	wantStatus(t, out, Changed)
	if cols, _ := e.provider.ListCollections(ctx); len(cols) != 0 {
		t.Fatalf("dry run created: %+v", cols)
	}
	if !strings.Contains(out.Record.Reason, "New Home") {
		t.Errorf("reason = %q", out.Record.Reason)
	}
	rec := out.Record
	o.Plan = &rec
	wantStatus(t, e.process(o), Changed)
	if e.cls.Calls() != 1 {
		t.Errorf("calls = %d", e.cls.Calls())
	}
	if e.bookmark().Collection.Local != "created-1" {
		t.Errorf("collection = %v", e.bookmark().Collection)
	}
}

func TestProposalWithoutAutoCreateGoesToReview(t *testing.T) {
	e := newEnv(t)
	e.cls.Script(proposal("New Home", 0.9))
	e.provider.Add(inboxBookmark())
	o := dynamicOptions()
	o.Taxonomy.Dynamic.AutoCreate = false
	out := e.process(o)
	wantStatus(t, out, NeedsReview)
	if e.bookmark().Collection != review {
		t.Errorf("collection = %v", e.bookmark().Collection)
	}
	if out.Record.Decision.Collection.ProposedName != "New Home" {
		t.Errorf("record = %+v", out.Record.Decision.Collection)
	}
}

func (e *env) recordedProposals() []domain.CollectionProposal {
	e.t.Helper()
	tx, err := e.store.Taxonomies().GetTaxonomy(context.Background(), "tx")
	if err != nil {
		e.t.Fatal(err)
	}
	return tx.Proposals
}

func wantProposal(t *testing.T, got []domain.CollectionProposal, name string, count int) {
	t.Helper()
	if len(got) != 1 || got[0].Name != name || got[0].Count != count {
		t.Errorf("proposals = %+v, want %s x%d", got, name, count)
	}
}

func TestDeclinedProposalIsRecorded(t *testing.T) {
	e := newEnv(t)
	e.cls.Script(proposal("New Home", 0.9))
	e.provider.Add(inboxBookmark())
	o := dynamicOptions()
	o.Taxonomy.Dynamic.AutoCreate = false
	e.process(o)
	wantProposal(t, e.recordedProposals(), "New Home", 1)

	// Same name from a second bookmark, and the version doesn't move.
	b2 := inboxBookmark()
	b2.ID = domain.ID{Provider: "fake", Local: "2"}
	e.provider.Add(b2)
	if _, err := e.pipe.Process(context.Background(), WorkItem{Bookmark: b2.ID}, o); err != nil {
		t.Fatal(err)
	}
	wantProposal(t, e.recordedProposals(), "New Home", 2)
	tx, _ := e.store.Taxonomies().GetTaxonomy(context.Background(), "tx")
	if tx.Version != testTaxonomy().Version {
		t.Errorf("version = %d", tx.Version)
	}
}

func TestDeclinedProposalAlreadyInReviewIsRecorded(t *testing.T) {
	e := newEnv(t)
	e.cls.Script(proposal("New Home", 0.9))
	b := inboxBookmark()
	b.Collection = review
	e.provider.Add(b)
	o := dynamicOptions()
	o.Trigger = TriggerBackfill
	o.Taxonomy.Dynamic.AutoCreate = false
	e.process(o)
	wantProposal(t, e.recordedProposals(), "New Home", 1)
}

func TestDryRunDoesNotRecordProposal(t *testing.T) {
	e := newEnv(t)
	e.cls.Script(proposal("New Home", 0.9))
	e.provider.Add(inboxBookmark())
	o := dynamicOptions()
	o.Taxonomy.Dynamic.AutoCreate = false
	o.DryRun = true
	e.process(o)
	if got := e.recordedProposals(); len(got) != 0 {
		t.Errorf("proposals = %+v", got)
	}
}

func TestPlanApplyRecordsDeclinedProposal(t *testing.T) {
	e := newEnv(t)
	e.cls.Script(proposal("New Home", 0.9))
	e.provider.Add(inboxBookmark())
	o := dynamicOptions()
	o.Trigger = TriggerBackfill
	o.Taxonomy.Dynamic.AutoCreate = false
	o.DryRun = true
	rec := e.process(o).Record

	o.DryRun = false
	o.Plan = &rec
	calls := e.cls.Calls()
	e.process(o)
	if e.cls.Calls() != calls {
		t.Errorf("classified again")
	}
	if e.bookmark().Collection != review {
		t.Errorf("collection = %v", e.bookmark().Collection)
	}
	wantProposal(t, e.recordedProposals(), "New Home", 1)
}

func TestAutoCreateCapAtWriteTimeRecordsProposal(t *testing.T) {
	e := newEnv(t)
	e.cls.Script(proposal("New Home", 0.9))
	e.provider.Add(inboxBookmark())
	o := dynamicOptions()
	// Another item takes the last slot after this one passes validation.
	e.cls.OnClassify(func(ports.ClassifyRequest) {
		if err := e.store.BookmarkStates().PutSetting(context.Background(), createdSettingPrefix+"fake:tx",
			`[{"key":"other","name":"Other","id":{"provider":"fake","local":"other"}}]`); err != nil {
			t.Error(err)
		}
	})
	wantStatus(t, e.process(o), NeedsReview)
	wantProposal(t, e.recordedProposals(), "New Home", 1)
}

func TestDryRunMarksNewTagsOnEveryUse(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inboxBookmark())
	b2 := inboxBookmark()
	b2.ID = domain.ID{Provider: "fake", Local: "2"}
	e.provider.Add(b2)
	o := options(TriggerBackfill)
	o.DryRun = true
	first := e.process(o).Record
	second, err := e.pipe.Process(context.Background(), WorkItem{Bookmark: b2.ID}, o)
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range []domain.DecisionRecord{first, second.Record} {
		if !slices.Equal(rec.NewTags, []string{"brand-new"}) {
			t.Errorf("new tags = %v (add %v)", rec.NewTags, rec.AddTags)
		}
	}
}

func TestNoJobIDSavesNoDecision(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inboxBookmark())
	o := options(TriggerWebhook)
	o.JobID = ""
	wantStatus(t, e.process(o), Changed)
	if _, ok, _ := e.store.Jobs().GetDecision(context.Background(), "", bm1); ok {
		t.Error("decision saved under an empty job")
	}
}

func TestWriteNotFoundOnLiveBookmarkIsAnItemError(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inboxBookmark())
	e.deps.Provider = writeErr{e.provider, domain.ErrNotFound} // a stale target collection
	e.pipe = New(e.deps)
	_, err := e.pipe.Process(context.Background(), WorkItem{Bookmark: bm1}, options(TriggerSync))
	if err == nil {
		t.Fatal("want an item error")
	}
	st := e.state()
	if st.Status == string(Deleted) || st.PendingCollection != nil || st.PendingTags != nil || st.InputHash != "" {
		t.Errorf("state = %+v", st)
	}
	d, ok, err := e.store.Jobs().GetDecision(context.Background(), "job", bm1)
	if err != nil || !ok || d.Result != resultFailed || d.AppliedAt != nil {
		t.Errorf("decision = %+v, ok=%v, err=%v", d, ok, err)
	}
	// The next run isn't confused by it: the provider now accepts the write.
	e.deps.Provider = e.provider
	e.pipe = New(e.deps)
	wantStatus(t, e.process(options(TriggerSync)), Changed)
	if got := e.state(); got.PinSource != domain.PinNone {
		t.Errorf("pin = %q", got.PinSource)
	}
}

func TestWriteNotFoundWithFailedReadIsAnItemError(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inboxBookmark())
	boom := errors.New("read failed")
	e.deps.Provider = writeThenReadFails{e.provider, boom}
	e.pipe = New(e.deps)
	_, err := e.pipe.Process(context.Background(), WorkItem{Bookmark: bm1}, options(TriggerSync))
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if st := e.state(); st.Status == string(Deleted) || st.PendingCollection != nil {
		t.Errorf("state = %+v", st)
	}
}

// writeThenReadFails refuses the write as not found, then fails the next read.
type writeThenReadFails struct {
	*pfake.Provider
	err error
}

func (w writeThenReadFails) UpdateBookmark(context.Context, domain.ID, ports.BookmarkPatch) (domain.Bookmark, error) {
	w.FailNext(w.err)
	return domain.Bookmark{}, domain.ErrNotFound
}

func TestBaselineThenBackfillIgnoresThePin(t *testing.T) {
	e := newEnv(t)
	b := inboxBookmark()
	b.Collection = colB
	e.provider.Add(b)
	o := options(TriggerSync)
	o.Baseline = true
	wantStatus(t, e.process(o), Skipped)

	wantStatus(t, e.process(options(TriggerSync)), Skipped) // §10.2: version-only difference
	if e.cls.Calls() != 0 {
		t.Fatalf("sync classified a baseline bookmark: %d calls", e.cls.Calls())
	}
	wantStatus(t, e.process(options(TriggerBackfill)), Changed)
	if e.cls.Calls() != 1 || e.bookmark().Collection != colA {
		t.Errorf("calls = %d, collection = %v", e.cls.Calls(), e.bookmark().Collection)
	}
	st := e.state()
	if st.PinSource != domain.PinNone || st.TaxonomyVersion != 3 {
		t.Errorf("state = %+v", st)
	}
}

func TestFirstBackfillIgnoresPreCutoffPinDespiteSameHash(t *testing.T) {
	// A sync pins the bookmark saved directly into colB and records its hash.
	seed := func(t *testing.T) *env {
		e := newEnv(t)
		b := inboxBookmark()
		b.Collection = colB
		e.provider.Add(b)
		wantStatus(t, e.process(options(TriggerSync)), Changed)
		if st := e.state(); st.PinSource != domain.PinUser || st.InputHash == "" {
			t.Fatalf("seed state = %+v", st)
		}
		e.now = e.now.Add(time.Hour)
		return e
	}
	first := func(e *env) Options {
		cutoff := e.now
		o := options(TriggerBackfill)
		o.IsFirstBackfill = true
		o.PinCutoff = &cutoff
		return o
	}
	t.Run("may move", func(t *testing.T) {
		e := seed(t)
		wantStatus(t, e.process(first(e)), Changed)
		if e.cls.Calls() != 2 || e.bookmark().Collection != colA {
			t.Errorf("calls = %d, collection = %v", e.cls.Calls(), e.bookmark().Collection)
		}
	})
	t.Run("staying put settles the pin, so a resume doesn't classify again", func(t *testing.T) {
		e := seed(t)
		e.cls.Script(cfake.Result{Decision: decision("b", 0.9)})
		o := first(e)
		e.process(o)
		if e.moves() != 0 {
			t.Errorf("moves = %d", e.moves())
		}
		if st := e.state(); st.PinSource != domain.PinNone || st.PinnedAt != nil {
			t.Errorf("state = %+v", st)
		}
		calls := e.cls.Calls()
		wantStatus(t, e.process(o), Skipped)
		if e.cls.Calls() != calls {
			t.Errorf("resume classified again: %d calls", e.cls.Calls()-calls)
		}
	})
	t.Run("dry run keeps the pin", func(t *testing.T) {
		e := seed(t)
		o := first(e)
		o.DryRun = true
		e.process(o)
		if e.cls.Calls() != 2 {
			t.Errorf("calls = %d", e.cls.Calls())
		}
		if st := e.state(); st.PinSource != domain.PinUser {
			t.Errorf("state = %+v", st)
		}
	})
	t.Run("pin after the cutoff binds", func(t *testing.T) {
		e := seed(t)
		o := first(e)
		early := e.now.Add(-2 * time.Hour)
		o.PinCutoff = &early
		wantStatus(t, e.process(o), Skipped)
		if e.cls.Calls() != 1 {
			t.Errorf("calls = %d", e.cls.Calls())
		}
		if st := e.state(); st.PinSource != domain.PinUser {
			t.Errorf("state = %+v", st)
		}
	})
	t.Run("later backfill respects it", func(t *testing.T) {
		e := seed(t)
		wantStatus(t, e.process(options(TriggerBackfill)), Skipped)
		if e.cls.Calls() != 1 {
			t.Errorf("calls = %d", e.cls.Calls())
		}
	})
}

func TestHighlightKeepsNeedsReviewSecondChance(t *testing.T) {
	hl := domain.ID{Provider: "fake", Local: "h1"}
	e := newEnv(t)
	b := inboxBookmark()
	b.Collection = review
	e.provider.Add(b)
	e.putState(domain.BookmarkState{ID: bm1, KnownCollection: review, InputHash: InputHash(b, nil), TaxonomyVersion: 3})
	e.provider.AddHighlight(domain.Highlight{ID: hl, Bookmark: bm1, Text: "highlighted words"})

	out, err := e.pipe.Process(context.Background(), WorkItem{Bookmark: bm1, FocusHighlight: &hl}, options(TriggerWebhook))
	if err != nil {
		t.Fatal(err)
	}
	wantStatus(t, out, Changed)
	if e.moves() != 0 {
		t.Errorf("moves = %d", e.moves())
	}

	wantStatus(t, e.process(options(TriggerSync)), Changed)
	if e.bookmark().Collection != colA {
		t.Errorf("collection = %v", e.bookmark().Collection)
	}
	if e.state().InputHash == InputHash(b, nil) {
		t.Error("hash not advanced by the collection decision")
	}
}

func TestHighlightOnFiledBookmark(t *testing.T) {
	hl := domain.ID{Provider: "fake", Local: "h1"}
	e := newEnv(t)
	b := inboxBookmark()
	b.Collection = colB
	e.provider.Add(b)
	e.putState(domain.BookmarkState{ID: bm1, KnownCollection: colB, InputHash: InputHash(b, nil), TaxonomyVersion: 2})
	e.provider.AddHighlight(domain.Highlight{ID: hl, Bookmark: bm1, Text: "highlighted words"})

	if _, err := e.pipe.Process(context.Background(), WorkItem{Bookmark: bm1, FocusHighlight: &hl}, options(TriggerWebhook)); err != nil {
		t.Fatal(err)
	}
	calls := e.cls.Calls()
	st := e.state()
	if st.InputHash == InputHash(b, nil) {
		t.Error("filed bookmark's hash not recorded")
	}
	if st.TaxonomyVersion != 2 {
		t.Errorf("version = %d: a tags-only run made no decision under the new taxonomy", st.TaxonomyVersion)
	}
	wantStatus(t, e.process(options(TriggerSync)), Skipped)
	if e.cls.Calls() != calls {
		t.Error("sync classified again")
	}
	e.process(options(TriggerBackfill))
	if e.cls.Calls() != calls+1 {
		t.Error("backfill did not act on the taxonomy change")
	}
}

func TestDryRunMeetingALandedWrite(t *testing.T) {
	e := newEnv(t)
	b := inboxBookmark()
	b.Collection = colA
	e.provider.Add(b)
	pending := colA
	before := t0.Add(-time.Hour)
	e.putState(domain.BookmarkState{ID: bm1, KnownCollection: pfake.Inbox, PendingCollection: &pending, RestoredAt: &before, Model: "m1"})
	o := options(TriggerBackfill)
	o.DryRun = true
	out := e.process(o)
	wantStatus(t, out, Unchanged)
	if out.Record.Result != resultNoChange || out.Record.AppliedAt != nil || out.Record.TargetCollection != nil {
		t.Errorf("record = %+v", out.Record)
	}
	d, ok, err := e.store.Jobs().GetDecision(context.Background(), "job", bm1)
	if err != nil || !ok || d.Result != resultNoChange || d.AppliedAt != nil {
		t.Errorf("decision = %+v, ok=%v, err=%v", d, ok, err)
	}
	st := e.state()
	if st.PendingCollection != nil || st.KnownCollection != colA || st.PinSource != domain.PinNone {
		t.Errorf("state = %+v", st)
	}
	if st.RestoredAt == nil {
		t.Error("dry run cleared RestoredAt")
	}
	if e.cls.Calls() != 0 || len(e.provider.Writes()) != 0 {
		t.Errorf("calls = %d, writes = %d", e.cls.Calls(), len(e.provider.Writes()))
	}
}
