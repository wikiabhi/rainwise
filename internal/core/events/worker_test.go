package events

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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

var (
	colA   = domain.ID{Provider: "fake", Local: "a"}
	colB   = domain.ID{Provider: "fake", Local: "b"}
	review = domain.ID{Provider: "fake", Local: "review"}
	bm1    = domain.ID{Provider: "fake", Local: "1"}
	t0     = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
)

const longText = "A long and detailed description of how database indexes work under realistic production load."

// spyEvents records the last update of each event.
type spyEvents struct {
	ports.EventStore
	mu   sync.Mutex
	last map[string]domain.StoredEvent
}

func (s *spyEvents) UpdateEvent(ctx context.Context, e domain.StoredEvent) error {
	s.mu.Lock()
	s.last[e.DedupKey] = e
	s.mu.Unlock()
	return s.EventStore.UpdateEvent(ctx, e)
}

type spyStore struct {
	ports.Store
	ev *spyEvents
}

func (s spyStore) Events() ports.EventStore { return s.ev }

type resolver struct {
	*pfake.Provider
	urls map[string]domain.ID
}

func (r resolver) ResolveURL(_ context.Context, u string) (domain.ID, error) {
	if id, ok := r.urls[u]; ok {
		return id, nil
	}
	return domain.ID{}, fmt.Errorf("url %s: %w", u, domain.ErrNotFound)
}

type env struct {
	t        *testing.T
	provider *pfake.Provider
	cls      *cfake.Classifier
	store    spyStore
	now      time.Time
	pipe     *pipeline.Pipeline
	seq      int
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{t: t, provider: pfake.New(), now: t0,
		cls: cfake.New("c", cfake.Result{Decision: decision("a", 0.9, "go")})}
	e.store = spyStore{Store: memory.New(), ev: nil}
	e.store.ev = &spyEvents{EventStore: e.store.Store.Events(), last: map[string]domain.StoredEvent{}}
	for _, c := range []domain.Collection{{ID: colA, Name: "Alpha"}, {ID: colB, Name: "Beta"}, {ID: review, Name: "Needs Review"}} {
		e.provider.AddCollection(c)
	}
	if _, err := e.store.Taxonomies().SeedTaxonomy(context.Background(), testTaxonomy()); err != nil {
		t.Fatal(err)
	}
	e.pipe = pipeline.New(pipeline.Deps{
		Provider: e.provider, Classifier: e.cls, Store: e.store,
		Now: func() time.Time { return e.now }, ContentFetch: "off",
	})
	return e
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
		ID: "tx", Mode: domain.ModeFixed, Status: domain.TaxonomyActive, Version: 1,
		Collections: []domain.TaxonomyCollection{
			{Key: "a", Name: "Alpha"}, {Key: "b", Name: "Beta"},
			{Key: "needs_review", Name: "Needs Review", Role: "review"},
		},
		Rules: domain.TaxonomyRules{MinTags: 1, MaxTags: 5, MinConfidence: 0.6, AllowNewTags: true},
	}
}

func (e *env) worker(p ports.Provider, retag bool) *Worker {
	return NewWorker(WorkerDeps{
		Store: e.store, Pipeline: e.pipe, Provider: p,
		Taxonomies: taxonomy.New(e.store.Taxonomies()), TaxonomyID: "tx",
		OnMoveRetag: retag, Now: func() time.Time { return e.now },
		Sleep: func(context.Context, time.Duration) error { return nil },
	})
}

func (e *env) ingestor(enabled bool) *Ingestor {
	return NewIngestor(e.store.Events(), enabled, func() time.Time { return e.now })
}

func (e *env) event(typ domain.EventType, ref domain.ResourceRef) domain.Event {
	e.seq++
	return domain.Event{Provider: "fake", Type: typ, Resource: ref, OccurredAt: e.now, DedupKey: fmt.Sprintf("k%d", e.seq)}
}

func (e *env) ingest(evs ...domain.Event) {
	e.t.Helper()
	if err := e.ingestor(true).Ingest(context.Background(), evs); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) tick(w *Worker) int {
	e.t.Helper()
	n, err := w.tick(context.Background())
	if err != nil {
		e.t.Fatal(err)
	}
	return n
}

func (e *env) status(key string) domain.StoredEvent {
	e.t.Helper()
	got, ok := e.store.ev.last[key]
	if !ok {
		e.t.Fatalf("event %s was never updated", key)
	}
	return got
}

func (e *env) bookmark() domain.Bookmark {
	b, ok := e.provider.Bookmark(bm1)
	if !ok {
		e.t.Fatal("bookmark gone")
	}
	return b
}

func (e *env) state() domain.BookmarkState {
	st, _, err := e.store.BookmarkStates().GetState(context.Background(), bm1)
	if err != nil {
		e.t.Fatal(err)
	}
	return st
}

func inbox() domain.Bookmark {
	return domain.Bookmark{ID: bm1, URL: "https://example.test/post", Title: "Indexes", Description: longText, Collection: pfake.Inbox}
}

func ref(id string) domain.ResourceRef { return domain.ResourceRef{ID: id} }

func TestIngestDedup(t *testing.T) {
	e := newEnv(t)
	ev := e.event(domain.EventBookmarkCreated, ref("1"))
	e.ingest(ev, ev)
	e.ingest(ev)
	got, err := e.store.Events().ClaimEvents(context.Background(), e.now, 10)
	if err != nil || len(got) != 1 {
		t.Fatalf("claimed %d events, err %v; want 1", len(got), err)
	}
}

func TestAutomationOffHolds(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inbox())
	if err := e.ingestor(false).Ingest(context.Background(), []domain.Event{e.event(domain.EventBookmarkCreated, ref("1"))}); err != nil {
		t.Fatal(err)
	}
	if n := e.tick(e.worker(e.provider, false)); n != 0 {
		t.Fatalf("claimed %d held events", n)
	}
	if e.cls.Calls() != 0 || len(e.provider.Writes()) != 0 {
		t.Error("held event must not reach the pipeline")
	}
	// It is stored as held.
	n, err := e.store.Events().DropHeldBefore(context.Background(), e.now.Add(time.Hour))
	if err != nil || n != 1 {
		t.Errorf("held events = %d, err %v; want 1", n, err)
	}
}

func TestHeldDropped(t *testing.T) {
	e := newEnv(t)
	if err := e.ingestor(false).Ingest(context.Background(), []domain.Event{e.event(domain.EventBookmarkCreated, ref("1"))}); err != nil {
		t.Fatal(err)
	}
	w := e.worker(e.provider, false)
	e.now = e.now.Add(6 * 24 * time.Hour)
	e.tick(w)
	if n, _ := e.store.Events().DropHeldBefore(context.Background(), e.now.Add(time.Hour)); n != 1 {
		t.Fatalf("event under 7 days old was dropped")
	}
	// Put it back, then let it age past 7 days.
	if err := e.ingestor(false).Ingest(context.Background(), []domain.Event{e.event(domain.EventBookmarkCreated, ref("1"))}); err != nil {
		t.Fatal(err)
	}
	e.now = e.now.Add(8 * 24 * time.Hour)
	e.tick(w)
	if n, _ := e.store.Events().DropHeldBefore(context.Background(), e.now.Add(time.Hour)); n != 0 {
		t.Errorf("%d held events survived past 7 days", n)
	}
}

func TestBookmarkCreated(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inbox())
	ev := e.event(domain.EventBookmarkCreated, ref("1"))
	e.ingest(ev)
	if n := e.tick(e.worker(e.provider, false)); n != 1 {
		t.Fatalf("claimed %d", n)
	}
	if got := e.bookmark(); got.Collection != colA || !slices.Contains(got.Tags, "go") {
		t.Errorf("bookmark = %+v", got)
	}
	if st := e.status(ev.DedupKey); st.Status != domain.EventDone {
		t.Errorf("status = %s", st.Status)
	}
	reqs := e.cls.Requests()
	if len(reqs) != 1 || !reqs[0].WantCollection {
		t.Errorf("requests = %+v", reqs)
	}
}

func TestHighlightCreated(t *testing.T) {
	e := newEnv(t)
	b := inbox()
	b.Collection = colB
	b.Tags = []string{"mine"}
	e.provider.Add(b)
	hl := domain.Highlight{ID: domain.ID{Provider: "fake", Local: "h1"}, Bookmark: bm1, Text: "an insight", CreatedAt: e.now}
	e.provider.AddHighlight(hl)
	ev := e.event(domain.EventHighlightCreated, ref("h1"))
	ev.Bookmark = &domain.ResourceRef{ID: "1"}
	e.ingest(ev)
	e.tick(e.worker(e.provider, false))

	reqs := e.cls.Requests()
	if len(reqs) != 1 || reqs[0].WantCollection || reqs[0].Subject.FocusHighlight == nil || reqs[0].Subject.FocusHighlight.ID != hl.ID {
		t.Fatalf("requests = %+v", reqs)
	}
	got := e.bookmark()
	if got.Collection != colB || !slices.Contains(got.Tags, "go") || !slices.Contains(got.Tags, "mine") {
		t.Errorf("bookmark = %+v", got)
	}
	if e.status(ev.DedupKey).Status != domain.EventDone {
		t.Error("event not done")
	}
}

func TestOwnMoveIgnored(t *testing.T) {
	e := newEnv(t)
	b := inbox()
	b.Collection = colA
	e.provider.Add(b)
	if err := e.store.BookmarkStates().PutState(context.Background(), domain.BookmarkState{ID: bm1, KnownCollection: colA}); err != nil {
		t.Fatal(err)
	}
	ev := e.event(domain.EventBookmarkMoved, ref("1"))
	e.ingest(ev)
	e.tick(e.worker(e.provider, true))
	if st := e.state(); st.PinSource != domain.PinNone {
		t.Errorf("pin = %q", st.PinSource)
	}
	if len(e.provider.Writes()) != 0 || e.cls.Calls() != 0 {
		t.Error("own move must not write or classify")
	}
	if e.status(ev.DedupKey).Status != domain.EventDone {
		t.Error("event not done")
	}
}

func TestUserMovePins(t *testing.T) {
	for _, retag := range []bool{false, true} {
		t.Run(fmt.Sprintf("retag=%v", retag), func(t *testing.T) {
			e := newEnv(t)
			b := inbox()
			b.Collection = colB // the user moved it here
			e.provider.Add(b)
			if err := e.store.BookmarkStates().PutState(context.Background(), domain.BookmarkState{ID: bm1, KnownCollection: pfake.Inbox}); err != nil {
				t.Fatal(err)
			}
			e.ingest(e.event(domain.EventBookmarkMoved, ref("1")))
			e.tick(e.worker(e.provider, retag))

			st := e.state()
			if st.PinSource != domain.PinUser || st.KnownCollection != colB {
				t.Errorf("state = %+v", st)
			}
			got := e.bookmark()
			if got.Collection != colB {
				t.Errorf("pinned bookmark moved to %s", got.Collection)
			}
			if retag != (e.cls.Calls() == 1) {
				t.Errorf("classifier calls = %d with retag=%v", e.cls.Calls(), retag)
			}
			if retag != slices.Contains(got.Tags, "go") {
				t.Errorf("tags = %v with retag=%v", got.Tags, retag)
			}
			if retag && e.cls.Requests()[0].WantCollection {
				t.Error("retag must not ask for a collection")
			}
		})
	}
}

func TestResolveByURL(t *testing.T) {
	url := "https://example.test/post"
	t.Run("resolves", func(t *testing.T) {
		e := newEnv(t)
		e.provider.Add(inbox())
		ev := e.event(domain.EventBookmarkCreated, domain.ResourceRef{URL: url})
		e.ingest(ev)
		e.tick(e.worker(resolver{e.provider, map[string]domain.ID{url: bm1}}, false))
		if e.status(ev.DedupKey).Status != domain.EventDone || e.bookmark().Collection != colA {
			t.Errorf("event %+v bookmark %+v", e.status(ev.DedupKey), e.bookmark())
		}
	})
	t.Run("no resolver", func(t *testing.T) {
		e := newEnv(t)
		e.provider.Add(inbox())
		ev := e.event(domain.EventBookmarkCreated, domain.ResourceRef{URL: url})
		e.ingest(ev)
		e.tick(e.worker(e.provider, false))
		if st := e.status(ev.DedupKey); st.Status != domain.EventFailed {
			t.Errorf("status = %s", st.Status)
		}
		if e.cls.Calls() != 0 {
			t.Error("classified an unresolved event")
		}
	})
	t.Run("no match", func(t *testing.T) {
		e := newEnv(t)
		ev := e.event(domain.EventBookmarkMoved, domain.ResourceRef{URL: url})
		e.ingest(ev)
		e.tick(e.worker(resolver{e.provider, nil}, false))
		if st := e.status(ev.DedupKey); st.Status != domain.EventFailed {
			t.Errorf("status = %s", st.Status)
		}
	})
}

func TestRetryBackoff(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inbox())
	ev := e.event(domain.EventBookmarkCreated, ref("1"))
	e.ingest(ev)
	w := e.worker(e.provider, false)
	failure := fmt.Errorf("down: %w", domain.ErrTemporary)

	for attempt := 1; attempt <= 5; attempt++ {
		e.provider.FailNext(failure)
		if n := e.tick(w); n != 1 {
			t.Fatalf("attempt %d claimed %d", attempt, n)
		}
		st := e.status(ev.DedupKey)
		if st.Attempts != attempt {
			t.Fatalf("attempts = %d, want %d", st.Attempts, attempt)
		}
		if attempt == 5 {
			if st.Status != domain.EventFailed {
				t.Fatalf("status = %s after 5 attempts", st.Status)
			}
			break
		}
		if st.Status != domain.EventPending || !st.NextAttempt.After(e.now) {
			t.Fatalf("attempt %d: %+v", attempt, st)
		}
		if n := e.tick(w); n != 0 {
			t.Fatalf("event retried before its backoff (claimed %d)", n)
		}
		e.now = st.NextAttempt
	}
	e.now = e.now.Add(24 * time.Hour)
	if n := e.tick(w); n != 0 {
		t.Error("failed event was claimed again")
	}
	if e.cls.Calls() != 0 {
		t.Error("classifier called despite read failures")
	}
}

func TestRestartReclaims(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inbox())
	ev := e.event(domain.EventBookmarkCreated, ref("1"))
	e.ingest(ev)
	// A previous process claimed the event and died.
	if got, err := e.store.Events().ClaimEvents(context.Background(), e.now, 10); err != nil || len(got) != 1 {
		t.Fatalf("setup claim: %v %v", got, err)
	}
	w := e.worker(e.provider, false)
	if n := e.tick(w); n != 0 {
		t.Fatalf("claimed event was claimable before a restart (%d)", n)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.d.Sleep = func(context.Context, time.Duration) error { cancel(); return ctx.Err() }
	w.Run(ctx)
	if e.status(ev.DedupKey).Status != domain.EventDone || e.bookmark().Collection != colA {
		t.Errorf("event not processed after restart: %+v", e.status(ev.DedupKey))
	}
}

func TestForgedPayloadIgnored(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inbox())
	ev := e.event(domain.EventBookmarkCreated, ref("1"))
	ev.Metadata = map[string]string{"title": "FORGED TITLE", "tags": "evil", "collection": "b", "description": "FORGED BODY"}
	e.ingest(ev)
	e.tick(e.worker(e.provider, false))

	reqs := e.cls.Requests()
	if len(reqs) != 1 {
		t.Fatalf("requests = %d", len(reqs))
	}
	if dump := fmt.Sprintf("%+v", reqs[0]); strings.Contains(dump, "FORGED") || strings.Contains(dump, "evil") {
		t.Errorf("forged fields reached the classifier: %s", dump)
	}
	if reqs[0].Subject.Bookmark.Title != "Indexes" {
		t.Errorf("title = %q", reqs[0].Subject.Bookmark.Title)
	}
	if got := e.bookmark(); slices.Contains(got.Tags, "evil") || got.Collection != colA {
		t.Errorf("bookmark = %+v", got)
	}
}

// A bookmark pass may already have recorded the hash that includes the new
// highlight; the highlight event must still run (Force).
func TestHighlightForcedPastRecordedHash(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inbox())
	e.provider.AddHighlight(domain.Highlight{ID: domain.ID{Provider: "fake", Local: "h1"}, Bookmark: bm1, Text: "an insight", CreatedAt: e.now})
	w := e.worker(e.provider, false)
	e.ingest(e.event(domain.EventBookmarkCreated, ref("1")))
	e.tick(w)
	if e.cls.Calls() != 1 {
		t.Fatalf("setup: classifier calls = %d", e.cls.Calls())
	}
	e.cls.Script(cfake.Result{Decision: decision("a", 0.9, "insight")})
	ev := e.event(domain.EventHighlightCreated, ref("h1"))
	ev.Bookmark = &domain.ResourceRef{ID: "1"}
	e.ingest(ev)
	e.tick(w)
	if e.cls.Calls() != 2 || !slices.Contains(e.bookmark().Tags, "insight") {
		t.Errorf("calls = %d, tags = %v", e.cls.Calls(), e.bookmark().Tags)
	}
}

type scanCounter struct {
	*pfake.Provider
	scans atomic.Int32
}

func (s *scanCounter) ListBookmarks(ctx context.Context, q ports.ListQuery) (domain.Page[domain.Bookmark], error) {
	s.scans.Add(1)
	return s.Provider.ListBookmarks(ctx, q)
}

// noListing switches off the provider's tag listing, so the worker has to use
// the persisted vocabulary.
func (e *env) noListing(allowNew bool) {
	e.t.Helper()
	caps := e.provider.Capabilities()
	caps.TagListing = false
	e.provider.SetCapabilities(caps)
	tx := testTaxonomy()
	tx.Rules.AllowNewTags = allowNew
	if _, err := e.store.Taxonomies().PutTaxonomy(context.Background(), tx, nil); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) persisted() []string {
	e.t.Helper()
	terms, _, err := taxonomy.LoadVocab(context.Background(), e.store.BookmarkStates(), "fake")
	if err != nil {
		e.t.Fatal(err)
	}
	return terms
}

func (e *env) savePersisted(terms ...string) {
	e.t.Helper()
	if err := taxonomy.SaveVocab(context.Background(), e.store.BookmarkStates(), "fake", terms); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) addSecond() {
	second := inbox()
	second.ID = domain.ID{Provider: "fake", Local: "2"}
	second.URL = "https://example.test/two"
	e.provider.Add(second)
}

func TestPersistedVocabWithoutNewTags(t *testing.T) {
	e := newEnv(t)
	e.noListing(false)
	e.savePersisted("go")
	e.provider.Add(inbox())
	e.cls.Script(cfake.Result{Decision: decision("a", 0.9, "go", "rust")})
	e.ingest(e.event(domain.EventBookmarkCreated, ref("1")))
	e.tick(e.worker(e.provider, false))
	got := e.bookmark()
	if !slices.Contains(got.Tags, "go") || slices.Contains(got.Tags, "rust") {
		t.Errorf("tags = %v, want the persisted go and not rust", got.Tags)
	}
}

func TestPersistedVocabGrowsWithNewTags(t *testing.T) {
	e := newEnv(t)
	e.noListing(true)
	e.savePersisted("go")
	e.provider.Add(inbox())
	e.addSecond()
	e.cls.Script(cfake.Result{Decision: decision("a", 0.9, "Brand-New")})
	w := e.worker(e.provider, false)
	e.ingest(e.event(domain.EventBookmarkCreated, ref("1")), e.event(domain.EventBookmarkCreated, ref("2")))
	e.tick(w)

	reqs := e.cls.Requests()
	if len(reqs) != 2 {
		t.Fatalf("requests = %d", len(reqs))
	}
	if !slices.Contains(reqs[0].TagVocab, "go") || slices.Contains(reqs[0].TagVocab, "brand-new") {
		t.Errorf("first vocab = %v", reqs[0].TagVocab)
	}
	if !slices.Contains(reqs[1].TagVocab, "brand-new") {
		t.Errorf("tag accepted for one event wasn't offered for the next: %v", reqs[1].TagVocab)
	}
	if got := e.persisted(); !slices.Contains(got, "brand-new") || !slices.Contains(got, "go") {
		t.Errorf("persisted = %v", got)
	}
}

func TestVocabScansOnce(t *testing.T) {
	e := newEnv(t)
	e.noListing(true)
	b := inbox()
	b.Tags = []string{"existing"}
	e.provider.Add(b)
	e.addSecond()
	sc := &scanCounter{Provider: e.provider}
	e.ingest(e.event(domain.EventBookmarkCreated, ref("1")), e.event(domain.EventBookmarkCreated, ref("2")))
	e.tick(e.worker(sc, false))
	if n := sc.scans.Load(); n != 1 {
		t.Errorf("library scans = %d, want 1", n)
	}
	reqs := e.cls.Requests()
	if len(reqs) != 2 || !slices.Contains(reqs[0].TagVocab, "existing") {
		t.Errorf("vocab = %+v", reqs)
	}
	if !slices.Contains(e.persisted(), "existing") {
		t.Errorf("persisted = %v", e.persisted())
	}
}

func TestWebhookSavesNoDecision(t *testing.T) {
	e := newEnv(t)
	e.provider.Add(inbox())
	e.ingest(e.event(domain.EventBookmarkCreated, ref("1")))
	e.tick(e.worker(e.provider, false))
	if e.bookmark().Collection != colA {
		t.Fatal("event not processed")
	}
	page, err := e.store.Jobs().ListDecisions(context.Background(), "", "", 10)
	if err != nil || len(page.Items) != 0 {
		t.Errorf("decisions = %+v, err %v; want none", page.Items, err)
	}
}

func TestLowConfidenceHighlightAddsNoTags(t *testing.T) {
	e := newEnv(t)
	b := inbox()
	b.Collection = colB
	b.Tags = []string{"mine"}
	e.provider.Add(b)
	e.provider.AddHighlight(domain.Highlight{ID: domain.ID{Provider: "fake", Local: "h1"}, Bookmark: bm1, Text: "x", CreatedAt: e.now})
	e.cls.Script(cfake.Result{Decision: decision("a", 0.3, "go")}) // below 0.6
	ev := e.event(domain.EventHighlightCreated, ref("h1"))
	ev.Bookmark = &domain.ResourceRef{ID: "1"}
	e.ingest(ev)
	e.tick(e.worker(e.provider, false))
	if e.cls.Calls() != 1 {
		t.Fatalf("classifier calls = %d", e.cls.Calls())
	}
	if got := e.bookmark(); !slices.Equal(got.Tags, []string{"mine"}) || got.Collection != colB {
		t.Errorf("bookmark = %+v", got)
	}
	if e.status(ev.DedupKey).Status != domain.EventDone {
		t.Error("event not done")
	}
}
