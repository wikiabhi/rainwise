package raindrop

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wikiabhi/rainwise/internal/adapters/provider/providertest"
	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

var t0 = time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)

type clock struct {
	mu     sync.Mutex
	now    time.Time
	sleeps []time.Duration
	adv    bool
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Sleep(_ context.Context, d time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if d <= 0 {
		return nil
	}
	c.sleeps = append(c.sleeps, d)
	if c.adv {
		c.now = c.now.Add(d)
	}
	return nil
}

func setup(t *testing.T, h http.HandlerFunc, mod func(*Config)) (*Provider, *clock) {
	t.Helper()
	s := httptest.NewServer(h)
	t.Cleanup(s.Close)
	clk := &clock{now: t0, adv: true}
	cfg := Config{Token: "secret-token", BaseURL: s.URL, RequestsPerMinute: 1000, Sleep: clk.Sleep, Now: clk.Now}
	if mod != nil {
		mod(&cfg)
	}
	return New(cfg), clk
}

func rid(n int) domain.ID { return domain.ID{Provider: "raindrop", Local: strconv.Itoa(n)} }

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func item(id, coll int, tags ...string) map[string]any {
	return map[string]any{"_id": id, "link": fmt.Sprintf("https://example.test/%d", id), "title": "T", "excerpt": "ex",
		"note": "my note", "tags": tags, "collection": map[string]any{"$id": coll},
		"created": "2026-10-01T08:00:00.000Z", "lastUpdate": "2026-10-02T09:30:00.000Z"}
}

func TestListPaging(t *testing.T) {
	var queries []url.Values
	var paths []string
	p, _ := setup(t, func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.Query())
		paths = append(paths, r.URL.Path)
		if r.Header.Get("Authorization") != "Bearer secret-token" {
			t.Errorf("auth header = %q", r.Header.Get("Authorization"))
		}
		n := 50
		if r.URL.Query().Get("page") == "1" {
			n = 3
		}
		items := make([]any, n)
		for i := range items {
			items[i] = item(i+1, 5)
		}
		writeJSON(w, map[string]any{"result": true, "items": items})
	}, nil)
	pg, err := p.ListBookmarks(context.Background(), ports.ListQuery{Limit: 50})
	if err != nil || pg.NextToken != "1" || len(pg.Items) != 50 {
		t.Fatalf("page 0: %v next=%q n=%d", err, pg.NextToken, len(pg.Items))
	}
	pg, err = p.ListBookmarks(context.Background(), ports.ListQuery{Limit: 50, PageToken: pg.NextToken})
	if err != nil || pg.NextToken != "" || len(pg.Items) != 3 {
		t.Fatalf("page 1: %v next=%q n=%d", err, pg.NextToken, len(pg.Items))
	}
	q := queries[0]
	if paths[0] != "/raindrops/0" || q.Get("page") != "0" || q.Get("perpage") != "50" || q.Get("sort") != "created" || q.Has("search") {
		t.Fatalf("first request: %s %v", paths[0], q)
	}
	if queries[1].Get("page") != "1" {
		t.Fatalf("second request: %v", queries[1])
	}
}

func TestListLimitCapped(t *testing.T) {
	p, _ := setup(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("perpage") != "50" {
			t.Errorf("perpage = %s", r.URL.Query().Get("perpage"))
		}
		writeJSON(w, map[string]any{"items": []any{}})
	}, nil)
	if _, err := p.ListBookmarks(context.Background(), ports.ListQuery{Limit: 500}); err != nil {
		t.Fatal(err)
	}
}

func TestListSince(t *testing.T) {
	since := time.Date(2026, 11, 1, 15, 0, 0, 0, time.UTC)
	for _, inc := range []bool{true, false} {
		var got url.Values
		p, _ := setup(t, func(w http.ResponseWriter, r *http.Request) {
			got = r.URL.Query()
			writeJSON(w, map[string]any{"items": []any{}})
		}, func(c *Config) { c.IncrementalSince = inc })
		if _, err := p.ListBookmarks(context.Background(), ports.ListQuery{Since: &since}); err != nil {
			t.Fatal(err)
		}
		if inc && got.Get("search") != "lastUpdate:>2026-11-01" {
			t.Errorf("search = %q", got.Get("search"))
		}
		if !inc && got.Has("search") {
			t.Errorf("Since sent without IncrementalSince: %q", got.Get("search"))
		}
		if p.Capabilities().IncrementalSince != inc {
			t.Error("capability mismatch")
		}
	}
}

func TestListNeverTrash(t *testing.T) {
	var paths []string
	p, _ := setup(t, func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		if r.URL.Path == "/raindrop/9" {
			writeJSON(w, map[string]any{"item": item(9, -99)})
			return
		}
		writeJSON(w, map[string]any{"items": []any{}})
	}, nil)
	trash := rid(-99)
	if _, err := p.ListBookmarks(context.Background(), ports.ListQuery{Collection: &trash}); !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("err = %v", err)
	}
	if _, err := p.ListBookmarks(context.Background(), ports.ListQuery{}); err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		if strings.Contains(path, "-99") {
			t.Errorf("requested %s", path)
		}
	}
	b, err := p.GetBookmark(context.Background(), rid(9))
	if err != nil || b.Collection != rid(-99) {
		t.Fatalf("trashed: %v %+v", err, b.Collection)
	}
}

func TestMapping(t *testing.T) {
	p, _ := setup(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"result": true, "item": item(42, 7, "a", "B")})
	}, nil)
	b, err := p.GetBookmark(context.Background(), rid(42))
	if err != nil {
		t.Fatal(err)
	}
	if b.ID != rid(42) || b.URL != "https://example.test/42" || b.Title != "T" || b.Excerpt != "ex" ||
		b.Collection != rid(7) || !slices.Equal(b.Tags, []string{"a", "B"}) || b.Metadata["note"] != "my note" ||
		!b.CreatedAt.Equal(time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)) || !b.UpdatedAt.Equal(time.Date(2026, 10, 2, 9, 30, 0, 0, time.UTC)) {
		t.Fatalf("mapped = %+v", b)
	}
	if _, err := p.GetBookmark(context.Background(), domain.ID{Provider: "other", Local: "1"}); !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("foreign id err = %v", err)
	}
}

type putRecorder struct {
	mu   sync.Mutex
	puts []map[string]any
}

func (r *putRecorder) handler(cur map[string]any) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		defer r.mu.Unlock()
		if req.Method == http.MethodPut {
			var body map[string]any
			_ = json.NewDecoder(req.Body).Decode(&body)
			r.puts = append(r.puts, body)
			for k, v := range body {
				cur[k] = v
			}
		}
		writeJSON(w, map[string]any{"item": cur})
	}
}

func TestAddTagsMerges(t *testing.T) {
	rec := &putRecorder{}
	p, _ := setup(t, rec.handler(item(1, 5, "Keep", "other")), nil)
	b, err := p.UpdateBookmark(context.Background(), rid(1), ports.BookmarkPatch{AddTags: []string{"keep", "New", "new", " "}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.puts) != 1 {
		t.Fatalf("puts = %v", rec.puts)
	}
	if got := fmt.Sprint(rec.puts[0]["tags"]); got != "[Keep other New]" {
		t.Fatalf("sent tags %s", got)
	}
	if _, has := rec.puts[0]["collection"]; has {
		t.Error("collection sent for a tag-only patch")
	}
	if !slices.Equal(b.Tags, []string{"Keep", "other", "New"}) {
		t.Fatalf("returned tags %v", b.Tags)
	}
}

func TestAddTagsNothingNewSkipsWrite(t *testing.T) {
	rec := &putRecorder{}
	p, _ := setup(t, rec.handler(item(1, 5, "Keep")), nil)
	if _, err := p.UpdateBookmark(context.Background(), rid(1), ports.BookmarkPatch{AddTags: []string{"KEEP"}}); err != nil {
		t.Fatal(err)
	}
	if len(rec.puts) != 0 {
		t.Fatalf("unexpected PUT %v", rec.puts)
	}
}

func TestSetTagsReplaces(t *testing.T) {
	rec := &putRecorder{}
	p, _ := setup(t, rec.handler(item(1, 5, "a", "b")), nil)
	b, err := p.UpdateBookmark(context.Background(), rid(1), ports.BookmarkPatch{SetTags: []string{"z"}})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(rec.puts[0]["tags"]) != "[z]" || !slices.Equal(b.Tags, []string{"z"}) {
		t.Fatalf("puts %v tags %v", rec.puts, b.Tags)
	}
}

func TestMoveOnly(t *testing.T) {
	rec := &putRecorder{}
	p, _ := setup(t, rec.handler(item(1, 5, "a")), nil)
	to := rid(8)
	b, err := p.UpdateBookmark(context.Background(), rid(1), ports.BookmarkPatch{Collection: &to})
	if err != nil {
		t.Fatal(err)
	}
	if _, has := rec.puts[0]["tags"]; has {
		t.Fatalf("tags sent on move: %v", rec.puts[0])
	}
	if c := rec.puts[0]["collection"].(map[string]any); c["$id"].(float64) != 8 || b.Collection != to {
		t.Fatalf("put %v bookmark %v", rec.puts[0], b.Collection)
	}
}

func TestUpdateNotFound(t *testing.T) {
	p, _ := setup(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(404) }, nil)
	to := rid(8)
	for _, patch := range []ports.BookmarkPatch{{Collection: &to}, {AddTags: []string{"x"}}} {
		if _, err := p.UpdateBookmark(context.Background(), rid(1), patch); !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("err = %v", err)
		}
	}
}

func TestOtherStatusNotMapped(t *testing.T) {
	p, _ := setup(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(403) }, nil)
	_, err := p.GetBookmark(context.Background(), rid(1))
	if err == nil || errors.Is(err, domain.ErrNotFound) || errors.Is(err, domain.ErrTemporary) || strings.Contains(err.Error(), "secret-token") {
		t.Fatalf("err = %v", err)
	}
}

func TestRateLimitBackoff(t *testing.T) {
	for _, spelling := range []string{"RateLimit-Remaining", "X-RateLimit-Remaining"} {
		t.Run("429/"+spelling, func(t *testing.T) {
			calls := 0
			p, clk := setup(t, func(w http.ResponseWriter, r *http.Request) {
				calls++
				if calls == 1 {
					w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(t0.Add(10*time.Second).Unix(), 10))
					w.Header().Set(spelling, "0")
					w.WriteHeader(429)
					return
				}
				writeJSON(w, map[string]any{"item": item(1, 5)})
			}, nil)
			if _, err := p.GetBookmark(context.Background(), rid(1)); err != nil {
				t.Fatal(err)
			}
			if calls != 2 || !slices.Equal(clk.sleeps, []time.Duration{10 * time.Second}) {
				t.Fatalf("calls=%d sleeps=%v", calls, clk.sleeps)
			}
		})
		t.Run("remaining0/"+spelling, func(t *testing.T) {
			p, clk := setup(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(t0.Add(7*time.Second).Unix(), 10))
				w.Header().Set(spelling, "0")
				writeJSON(w, map[string]any{"item": item(1, 5)})
			}, nil)
			for range 2 {
				if _, err := p.GetBookmark(context.Background(), rid(1)); err != nil {
					t.Fatal(err)
				}
			}
			if !slices.Equal(clk.sleeps, []time.Duration{7 * time.Second}) {
				t.Fatalf("sleeps=%v", clk.sleeps)
			}
		})
	}
}

func TestRateLimitExhausted(t *testing.T) {
	p, _ := setup(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(429) }, nil)
	if _, err := p.GetBookmark(context.Background(), rid(1)); !errors.Is(err, domain.ErrRateLimited) {
		t.Fatalf("err = %v", err)
	}
}

func TestTokenBucket(t *testing.T) {
	p, clk := setup(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"item": item(1, 5)})
	}, func(c *Config) { c.RequestsPerMinute = 2 })
	clk.adv = false // the clock stays still, so every reservation is judged at t0
	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := p.GetBookmark(context.Background(), rid(1)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	slices.Sort(clk.sleeps)
	want := []time.Duration{30 * time.Second, 60 * time.Second, 90 * time.Second}
	if !slices.Equal(clk.sleeps, want) {
		t.Fatalf("sleeps = %v, want %v", clk.sleeps, want)
	}
}

func Test5xxRetry(t *testing.T) {
	for _, tc := range []struct {
		fails   int
		wantErr bool
	}{{3, false}, {6, true}} {
		calls := 0
		p, _ := setup(t, func(w http.ResponseWriter, r *http.Request) {
			calls++
			if calls <= tc.fails {
				w.WriteHeader(503)
				return
			}
			writeJSON(w, map[string]any{"item": item(1, 5)})
		}, nil)
		_, err := p.GetBookmark(context.Background(), rid(1))
		if tc.wantErr != errors.Is(err, domain.ErrTemporary) || (!tc.wantErr && err != nil) {
			t.Fatalf("fails=%d err=%v", tc.fails, err)
		}
		if tc.wantErr && calls > 6 {
			t.Fatalf("calls = %d", calls)
		}
	}
}

func TestCollections(t *testing.T) {
	var created map[string]any
	p, _ := setup(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /collections":
			writeJSON(w, map[string]any{"items": []any{map[string]any{"_id": 1, "title": "Root", "description": "d"}}})
		case "GET /collections/childrens":
			writeJSON(w, map[string]any{"items": []any{map[string]any{"_id": 2, "title": "Kid", "parent": map[string]any{"$id": 1}}}})
		case "POST /collection":
			_ = json.NewDecoder(r.Body).Decode(&created)
			writeJSON(w, map[string]any{"item": map[string]any{"_id": 3, "title": created["title"]}})
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}, nil)
	cols, err := p.ListCollections(context.Background())
	if err != nil || len(cols) != 2 {
		t.Fatalf("%v %v", err, cols)
	}
	if cols[0].Parent != nil || cols[0].Description != "d" || cols[1].Parent == nil || *cols[1].Parent != rid(1) || cols[1].Name != "Kid" {
		t.Fatalf("cols = %+v", cols)
	}
	c, err := p.CreateCollection(context.Background(), domain.Collection{Name: "New"})
	if err != nil || c.ID != rid(3) || created["title"] != "New" {
		t.Fatalf("%v %+v %v", err, c, created)
	}
	if _, has := created["parent"]; has {
		t.Error("parent sent for root collection")
	}
}

func TestDeleteIfEmpty(t *testing.T) {
	deleted := []string{}
	p, _ := setup(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "DELETE":
			deleted = append(deleted, r.URL.Path)
			writeJSON(w, map[string]any{"result": true})
		case r.URL.Path == "/raindrops/10":
			writeJSON(w, map[string]any{"items": []any{item(1, 10)}, "count": 1})
		case strings.HasPrefix(r.URL.Path, "/raindrops/"):
			writeJSON(w, map[string]any{"items": []any{}, "count": 0})
		case r.URL.Path == "/collections":
			writeJSON(w, map[string]any{"items": []any{map[string]any{"_id": 11, "title": "Parent"}}})
		case r.URL.Path == "/collections/childrens":
			writeJSON(w, map[string]any{"items": []any{map[string]any{"_id": 12, "title": "Kid", "parent": map[string]any{"$id": 11}}}})
		}
	}, nil)
	ctx := context.Background()
	for _, tc := range []struct {
		id   int
		want bool
	}{{10, false}, {11, false}, {13, true}} {
		got, err := p.DeleteCollectionIfEmpty(ctx, rid(tc.id))
		if err != nil || got != tc.want {
			t.Fatalf("id %d: got %v err %v", tc.id, got, err)
		}
	}
	if !slices.Equal(deleted, []string{"/collection/13"}) {
		t.Fatalf("deleted = %v", deleted)
	}
	for _, sys := range []int{0, -1, -99} {
		if _, err := p.DeleteCollectionIfEmpty(ctx, rid(sys)); !errors.Is(err, domain.ErrValidation) {
			t.Errorf("system collection %d: %v", sys, err)
		}
	}
}

func TestHighlights(t *testing.T) {
	var globalPages []string
	p, _ := setup(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/raindrop/5":
			it := item(5, 1)
			it["highlights"] = []any{map[string]any{"_id": "h1", "text": "quote", "note": "n", "created": "2026-10-03T00:00:00.000Z", "tags": []string{"x"}}}
			writeJSON(w, map[string]any{"item": it})
		case "/highlights":
			pg := r.URL.Query().Get("page")
			globalPages = append(globalPages, pg+"/"+r.URL.Query().Get("perpage"))
			if pg == "0" {
				writeJSON(w, map[string]any{"items": []any{
					map[string]any{"_id": "h2", "text": "a", "raindropRef": 7},
					map[string]any{"_id": "h3", "text": "b"},
				}})
				return
			}
			writeJSON(w, map[string]any{"items": []any{map[string]any{"_id": "h4", "text": "c", "raindropRef": 8}}})
		}
	}, nil)
	ctx := context.Background()
	hs, err := p.ListHighlights(ctx, rid(5))
	if err != nil || len(hs) != 1 || hs[0].ID.Local != "h1" || hs[0].Bookmark != rid(5) || hs[0].Text != "quote" || hs[0].Note != "n" || hs[0].Tags[0] != "x" || hs[0].CreatedAt.IsZero() {
		t.Fatalf("%v %+v", err, hs)
	}
	pg, err := p.ListAllHighlights(ctx, ports.ListQuery{Limit: 2})
	if err != nil || pg.NextToken != "1" || len(pg.Items) != 2 || pg.Items[0].Bookmark != rid(7) || !pg.Items[1].Bookmark.IsZero() {
		t.Fatalf("%v %+v", err, pg)
	}
	pg, err = p.ListAllHighlights(ctx, ports.ListQuery{Limit: 2, PageToken: pg.NextToken})
	if err != nil || pg.NextToken != "" || len(pg.Items) != 1 {
		t.Fatalf("%v %+v", err, pg)
	}
	if !slices.Equal(globalPages, []string{"0/2", "1/2"}) {
		t.Fatalf("pages = %v", globalPages)
	}
}

func TestResolveURL(t *testing.T) {
	p, _ := setup(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("search") == "" {
			t.Error("no search param")
		}
		writeJSON(w, map[string]any{"items": []any{item(1, 5), item(2, 5)}})
	}, nil)
	id, err := p.ResolveURL(context.Background(), "https://example.test/2")
	if err != nil || id != rid(2) {
		t.Fatalf("%v %v", id, err)
	}
	if _, err := p.ResolveURL(context.Background(), "https://example.test/2/"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
}

func TestListTagsGatedByFlag(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"items": []any{map[string]any{"_id": "go", "count": 3}}})
	}
	p, _ := setup(t, handler, nil)
	if _, err := p.ListTags(context.Background()); !errors.Is(err, domain.ErrCapabilityUnsupported) {
		t.Fatalf("err = %v", err)
	}
	p, _ = setup(t, handler, func(c *Config) { c.TagListing = true })
	tags, err := p.ListTags(context.Background())
	if err != nil || len(tags) != 1 || tags[0] != (domain.TagCount{Name: "go", Count: 3}) {
		t.Fatalf("%v %v", tags, err)
	}
}

func TestCapabilities(t *testing.T) {
	c := New(Config{}).Capabilities()
	if !c.Highlights || !c.CreateCollections || c.NativeWebhooks || c.CursorPagination || c.TagListing || c.IncrementalSince ||
		c.InboxCollection == nil || *c.InboxCollection != rid(-1) {
		t.Fatalf("caps = %+v", c)
	}
}

// stateful is a small in-memory Raindrop.
type stateful struct {
	mu    sync.Mutex
	items map[int]map[string]any
	nextC int
}

func (s *stateful) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case r.Method == "GET" && r.URL.Path == "/raindrops/0":
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		per, _ := strconv.Atoi(r.URL.Query().Get("perpage"))
		ids := []int{}
		for id := range s.items {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		out := []any{}
		for i := page * per; i < min(len(ids), (page+1)*per); i++ {
			out = append(out, s.items[ids[i]])
		}
		writeJSON(w, map[string]any{"result": true, "items": out, "count": len(ids)})
	case strings.HasPrefix(r.URL.Path, "/raindrop/"):
		id, _ := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/raindrop/"))
		it, ok := s.items[id]
		if !ok {
			w.WriteHeader(404)
			return
		}
		if r.Method == "PUT" {
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			for k, v := range body {
				it[k] = v
			}
		}
		writeJSON(w, map[string]any{"result": true, "item": it})
	case r.Method == "POST" && r.URL.Path == "/collection":
		s.nextC++
		writeJSON(w, map[string]any{"item": map[string]any{"_id": 100 + s.nextC, "title": "x"}})
	default:
		w.WriteHeader(500)
	}
}

func TestConformance(t *testing.T) {
	providertest.Run(t, func(t *testing.T) ports.Provider {
		st := &stateful{items: map[int]map[string]any{}}
		for i := 1; i <= 5; i++ {
			it := item(i, 10+i%2, "Keep", "other")
			st.items[i] = it
		}
		p, _ := setup(t, st.ServeHTTP, nil)
		return p
	})
}
