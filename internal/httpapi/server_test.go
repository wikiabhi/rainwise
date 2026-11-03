package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/wikiabhi/rainwise/internal/adapters/provider/fake"
	"github.com/wikiabhi/rainwise/internal/adapters/store/memory"
	"github.com/wikiabhi/rainwise/internal/core/events"
	"github.com/wikiabhi/rainwise/internal/core/jobs"
	"github.com/wikiabhi/rainwise/internal/core/taxonomy"
	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

const (
	apiToken  = "api-token"
	hookToken = "hook-secret"
)

type env struct {
	t        *testing.T
	store    ports.Store
	provider *fake.Provider
	h        http.Handler
	logs     *bytes.Buffer
}

func canonical(ctx context.Context, h http.Header, body []byte) ([]domain.Event, error) {
	return events.NormalizeCanonical("fake", body)
}

type normalizerFunc func(context.Context, http.Header, []byte) ([]domain.Event, error)

func (f normalizerFunc) Normalize(ctx context.Context, h http.Header, b []byte) ([]domain.Event, error) {
	return f(ctx, h, b)
}

func newEnv(t *testing.T, mods ...func(*Deps)) *env {
	t.Helper()
	store := memory.New()
	p := fake.New()
	ctx := context.Background()
	tx := domain.Taxonomy{
		ID: "default", Mode: domain.ModeFixed, Status: domain.TaxonomyActive,
		Collections: []domain.TaxonomyCollection{
			{Key: "tech", Name: "Tech", Role: "normal"},
			{Key: "review", Name: "Needs Review", Role: "review"},
		},
		Rules: domain.TaxonomyRules{MinTags: 1, MaxTags: 3},
	}
	if _, err := store.Taxonomies().SeedTaxonomy(ctx, tx); err != nil {
		t.Fatal(err)
	}
	logs := &bytes.Buffer{}
	clock := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
	d := Deps{
		APIToken:       apiToken,
		WebhookSecrets: map[string]string{"fake": hookToken, "nonorm": hookToken},
		Normalizers:    map[string]ports.EventNormalizer{"fake": normalizerFunc(canonical)},
		Jobs: jobs.NewService(jobs.ServiceDeps{
			Store: store, AutomationEnabled: true, ProviderNames: []string{"fake"},
			Now: func() time.Time { clock = clock.Add(time.Second); return clock },
		}),
		Taxonomies: taxonomy.New(store.Taxonomies()),
		Snapshots:  jobs.NewSnapshotService(jobs.SnapshotDeps{Provider: p, Store: store}),
		Ingestor:   events.NewIngestor(store.Events(), true, nil),
		Store:      store,
		Providers: []ProviderInfo{{Name: "fake", Capabilities: ports.ProviderCapabilities{
			Highlights: true, TagListing: true,
		}}},
		Classifiers: []ClassifierInfo{{Name: "llm", Roles: []string{"classifier", "proposer"},
			Capabilities: ports.ClassifierCapabilities{FixedChoice: true, MaxChoices: 30}}},
		DefaultProvider: "fake", DefaultTaxonomy: "default",
		Log: slog.New(slog.NewTextHandler(logs, nil)),
	}
	for _, m := range mods {
		m(&d)
	}
	return &env{t: t, store: store, provider: p, h: New(d), logs: logs}
}

type resp struct {
	*httptest.ResponseRecorder
}

func (r resp) json() map[string]any {
	var m map[string]any
	if err := json.Unmarshal(r.Body.Bytes(), &m); err != nil {
		panic(fmt.Sprintf("body not JSON: %q", r.Body.String()))
	}
	return m
}

func (e *env) do(method, path, body string, hdr ...string) resp {
	e.t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+apiToken)
	for i := 0; i+1 < len(hdr); i += 2 {
		if hdr[i+1] == "" {
			req.Header.Del(hdr[i])
		} else {
			req.Header.Set(hdr[i], hdr[i+1])
		}
	}
	rec := httptest.NewRecorder()
	e.h.ServeHTTP(rec, req)
	return resp{rec}
}

func (e *env) hook(path, body string, hdr ...string) resp {
	h := append([]string{"Authorization", "Bearer " + hookToken}, hdr...)
	return e.do("POST", path, body, h...)
}

func want(t *testing.T, r resp, status int) {
	t.Helper()
	if r.Code != status {
		t.Fatalf("status = %d, want %d; body %s", r.Code, status, r.Body.String())
	}
}

func checkErrorShape(t *testing.T, r resp, code string) {
	t.Helper()
	var b struct {
		Error map[string]json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(r.Body.Bytes(), &b); err != nil {
		t.Fatalf("error body %q: %v", r.Body.String(), err)
	}
	for _, k := range []string{"code", "message", "details"} {
		if _, ok := b.Error[k]; !ok {
			t.Fatalf("error body %q lacks %q", r.Body.String(), k)
		}
	}
	if string(b.Error["details"]) == "null" {
		t.Fatalf("details is null")
	}
	if code != "" {
		var got string
		_ = json.Unmarshal(b.Error["code"], &got)
		if got != code {
			t.Fatalf("code = %q, want %q", got, code)
		}
	}
	if ct := r.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("content type %q", ct)
	}
}

const canonicalBody = `{"type":"bookmark.created","resource":{"id":"12345"},"event_id":"e1"}`

func TestAuth(t *testing.T) {
	e := newEnv(t)
	for _, tc := range []struct {
		name   string
		call   func() resp
		status int
	}{
		{"no token", func() resp { return e.do("GET", "/v1/jobs", "", "Authorization", "") }, 401},
		{"wrong token", func() resp { return e.do("GET", "/v1/jobs", "", "Authorization", "Bearer nope") }, 401},
		{"wrong scheme", func() resp { return e.do("GET", "/v1/jobs", "", "Authorization", "Basic "+apiToken) }, 401},
		{"good token", func() resp { return e.do("GET", "/v1/jobs", "") }, 200},
		{"healthz needs none", func() resp { return e.do("GET", "/healthz", "", "Authorization", "") }, 200},
		{"webhook with api token", func() resp {
			return e.do("POST", "/v1/webhooks/fake", canonicalBody)
		}, 401},
		{"webhook no secret", func() resp {
			return e.do("POST", "/v1/webhooks/fake", canonicalBody, "Authorization", "")
		}, 401},
		{"webhook bearer", func() resp { return e.hook("/v1/webhooks/fake", canonicalBody) }, 202},
		{"webhook header", func() resp {
			return e.do("POST", "/v1/webhooks/fake", strings.Replace(canonicalBody, "e1", "e2", 1),
				"Authorization", "", "X-Rainwise-Secret", hookToken)
		}, 202},
		{"webhook wrong header", func() resp {
			return e.do("POST", "/v1/webhooks/fake", canonicalBody, "Authorization", "", "X-Rainwise-Secret", "x")
		}, 401},
		{"job route with webhook secret", func() resp {
			return e.do("GET", "/v1/jobs", "", "Authorization", "Bearer "+hookToken)
		}, 401},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.call()
			want(t, r, tc.status)
			if tc.status == 401 {
				checkErrorShape(t, r, "unauthorized")
			}
		})
	}
	t.Run("empty configured token denies", func(t *testing.T) {
		e := newEnv(t, func(d *Deps) { d.APIToken = "" })
		want(t, e.do("GET", "/v1/jobs", "", "Authorization", "Bearer "), 401)
	})
	t.Run("log has no secrets", func(t *testing.T) {
		if s := e.logs.String(); strings.Contains(s, apiToken) || strings.Contains(s, hookToken) {
			t.Fatalf("log leaks a secret: %s", s)
		}
		if !strings.Contains(e.logs.String(), "/v1/jobs") {
			t.Fatalf("no request log: %s", e.logs.String())
		}
	})
}

func TestErrorShape(t *testing.T) {
	e := newEnv(t)
	for _, tc := range []struct {
		name, method, path, body string
	}{
		{"unknown route", "GET", "/v1/nothing", ""},
		{"method not allowed", "PATCH", "/v1/jobs", ""},
		{"not found", "GET", "/v1/jobs/job_missing", ""},
		{"bad json", "POST", "/v1/jobs", "{"},
		{"unknown field", "POST", "/v1/jobs", `{"type":"sync","bogus":1}`},
		{"validation", "POST", "/v1/jobs", `{"type":"nope"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := e.do(tc.method, tc.path, tc.body)
			if r.Code < 400 {
				t.Fatalf("status %d", r.Code)
			}
			checkErrorShape(t, r, "")
		})
	}
}

func TestStatusMapping(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		status int
		code   string
		hidden string
	}{
		{"not found", fmt.Errorf("x: %w", domain.ErrNotFound), 404, "not_found", ""},
		{"conflict", fmt.Errorf("x: %w", domain.ErrConflict), 409, "conflict", ""},
		{"validation", fmt.Errorf("x: %w", domain.ErrValidation), 422, "validation_failed", ""},
		{"automation", fmt.Errorf("x: %w", domain.ErrAutomationDisabled), 409, "automation_disabled", ""},
		{"capability", fmt.Errorf("x: %w", domain.ErrCapabilityUnsupported), 422, "capability_unsupported", ""},
		{"unknown", errors.New("sqlite: disk on fire /var/db/secret"), 500, "internal", "disk on fire"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			(&server{log: slog.New(slog.NewTextHandler(io.Discard, nil))}).fail(rec, tc.err)
			r := resp{rec}
			want(t, r, tc.status)
			checkErrorShape(t, r, tc.code)
			if tc.hidden != "" && strings.Contains(r.Body.String(), tc.hidden) {
				t.Fatalf("leaks internals: %s", r.Body.String())
			}
		})
	}

	e := newEnv(t)
	t.Run("malformed json", func(t *testing.T) { want(t, e.do("POST", "/v1/jobs", `{"type":`), 400) })
	t.Run("empty body", func(t *testing.T) { want(t, e.do("POST", "/v1/jobs", ``), 400) })
	t.Run("trailing data", func(t *testing.T) { want(t, e.do("POST", "/v1/jobs", `{"type":"sync"} {}`), 400) })
	t.Run("body over 1MB", func(t *testing.T) {
		big := `{"type":"sync","scope":{"tags":["` + strings.Repeat("a", 1<<20) + `"]}}`
		r := e.do("POST", "/v1/jobs", big)
		want(t, r, 413)
		checkErrorShape(t, r, "payload_too_large")
	})
	t.Run("webhook over 1MB", func(t *testing.T) {
		want(t, e.hook("/v1/webhooks/fake", strings.Repeat("a", 1<<20+1)), 413)
	})
	t.Run("store failure is 500 and hidden", func(t *testing.T) {
		e := newEnv(t, func(d *Deps) { d.Jobs = nil })
		r := e.do("GET", "/v1/jobs", "")
		want(t, r, 500)
		checkErrorShape(t, r, "internal")
	})
}

func TestWebhook(t *testing.T) {
	e := newEnv(t)
	count := func() int {
		n := 0
		evs, _ := e.store.Events().ClaimEvents(context.Background(), time.Now().Add(time.Hour), 100)
		n = len(evs)
		return n
	}
	want(t, e.hook("/v1/webhooks/fake", canonicalBody), 202)
	want(t, e.hook("/v1/webhooks/fake", canonicalBody), 202) // duplicate
	if n := count(); n != 1 {
		t.Fatalf("stored %d events, want 1", n)
	}
	for _, tc := range []struct {
		name, path, body string
		status           int
	}{
		{"unknown provider", "/v1/webhooks/other", canonicalBody, 404},
		{"no normalizer", "/v1/webhooks/nonorm", canonicalBody, 404},
		{"malformed", "/v1/webhooks/fake", `{"type":`, 400},
		{"unknown type", "/v1/webhooks/fake", `{"type":"bookmark.exploded","resource":{"id":"1"}}`, 422},
		{"no resource", "/v1/webhooks/fake", `{"type":"bookmark.created","resource":{}}`, 422},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := e.hook(tc.path, tc.body)
			want(t, r, tc.status)
			checkErrorShape(t, r, "")
		})
	}
}

func TestCreateJob(t *testing.T) {
	e := newEnv(t)
	r := e.do("POST", "/v1/jobs", `{"type":"backfill","dry_run":true,"params":{"batch_size":5},"scope":{"tags":["x"]}}`)
	want(t, r, 202)
	j := r.json()
	id, _ := j["id"].(string)
	if !strings.HasPrefix(id, "job_") || r.Header().Get("Location") != "/v1/jobs/"+id {
		t.Fatalf("id %q, Location %q", id, r.Header().Get("Location"))
	}
	if j["provider"] != "fake" || j["taxonomy"] != "default" || j["status"] != "queued" || j["dry_run"] != true {
		t.Fatalf("defaults not applied: %v", j)
	}
	if _, ok := j["progress"].(map[string]any); !ok || j["snapshot_id"] != nil || j["error"] != nil {
		t.Fatalf("shape: %v", j)
	}

	// Duplicate sync returns the active one.
	a := e.do("POST", "/v1/jobs", `{"type":"sync"}`)
	want(t, a, 202)
	b := e.do("POST", "/v1/jobs", `{"type":"sync"}`)
	want(t, b, 200)
	if a.json()["id"] != b.json()["id"] {
		t.Fatalf("duplicate sync made a new job")
	}

	// Idempotency-Key.
	k1 := e.do("POST", "/v1/jobs", `{"type":"backfill","dry_run":true}`, "Idempotency-Key", "k")
	k2 := e.do("POST", "/v1/jobs", `{"type":"backfill","dry_run":true}`, "Idempotency-Key", "k")
	want(t, k1, 202)
	want(t, k2, 200)
	if k1.json()["id"] != k2.json()["id"] {
		t.Fatal("idempotency key made a new job")
	}

	want(t, e.do("POST", "/v1/jobs", `{"type":"backfill","bogus":1}`), 400)
	want(t, e.do("POST", "/v1/jobs", `{"type":"backfill","params":{"bogus":1}}`), 400)
	want(t, e.do("POST", "/v1/jobs", `{"type":"reclassify"}`), 422)
	want(t, e.do("POST", "/v1/jobs", `{"type":"backfill","provider":"zzz"}`), 422)
}

func TestCreateJobDefaults(t *testing.T) {
	e := newEnv(t)
	r := e.do("POST", "/v1/jobs", `{"type":"restore","params":{"snapshot_id":"snap_1"},"dry_run":true}`)
	want(t, r, 202)
	j := r.json()
	if j["provider"] != "fake" {
		t.Fatalf("provider = %v", j["provider"])
	}
	if tx, ok := j["taxonomy"]; ok && tx != "" {
		t.Fatalf("restore got a taxonomy: %v", tx)
	}
	s := e.do("POST", "/v1/jobs", `{"type":"snapshot"}`)
	want(t, s, 202)
	if tx, ok := s.json()["taxonomy"]; ok && tx != "" {
		t.Fatalf("snapshot got a taxonomy: %v", tx)
	}
	if j := e.do("POST", "/v1/jobs", `{"type":"sync","provider":"fake","taxonomy":"default","dry_run":true}`).json(); j["taxonomy"] != "default" {
		t.Fatal("explicit taxonomy lost")
	}
}

func TestGetListCancelJob(t *testing.T) {
	e := newEnv(t)
	ids := map[string]bool{}
	for i := 0; i < 3; i++ {
		r := e.do("POST", "/v1/jobs", `{"type":"backfill","dry_run":true}`)
		want(t, r, 202)
		ids[r.json()["id"].(string)] = true
	}
	seen := map[string]bool{}
	path := "/v1/jobs?limit=2"
	pages := 0
	for path != "" {
		r := e.do("GET", path, "")
		want(t, r, 200)
		m := r.json()
		items := m["items"].([]any)
		if len(items) > 2 {
			t.Fatalf("limit ignored: %d", len(items))
		}
		for _, it := range items {
			seen[it.(map[string]any)["id"].(string)] = true
		}
		pages++
		path = ""
		if c, _ := m["next_cursor"].(string); c != "" {
			path = "/v1/jobs?limit=2&cursor=" + c
		}
	}
	if pages != 2 || len(seen) != 3 {
		t.Fatalf("pages %d, seen %d", pages, len(seen))
	}
	want(t, e.do("GET", "/v1/jobs?limit=abc", ""), 400)
	want(t, e.do("GET", "/v1/jobs?limit=-1", ""), 400)

	var id string
	for id = range ids {
		break
	}
	r := e.do("GET", "/v1/jobs/"+id, "")
	want(t, r, 200)
	if r.json()["id"] != id {
		t.Fatal("wrong job")
	}
	want(t, e.do("GET", "/v1/jobs/job_nope", ""), 404)

	c := e.do("DELETE", "/v1/jobs/"+id, "")
	want(t, c, 202)
	if c.json()["status"] != "canceled" {
		t.Fatalf("status %v", c.json()["status"])
	}
	want(t, e.do("DELETE", "/v1/jobs/"+id, ""), 409)
	want(t, e.do("DELETE", "/v1/jobs/job_nope", ""), 404)
}

func id(s string) domain.ID { i, _ := domain.ParseID(s); return i }

func TestDecisionsAndProposedTags(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	j := e.do("POST", "/v1/jobs", `{"type":"backfill","dry_run":true}`).json()["id"].(string)
	planned := id("planned:cooking")
	recs := []domain.DecisionRecord{
		{JobID: j, Bookmark: id("fake:1"), ObservedCollection: id("fake:0"), TargetCollection: &planned,
			AddTags: []string{"go"}, NewTags: []string{"go"}, Result: "proposed"},
		{JobID: j, Bookmark: id("fake:2"), ObservedCollection: id("fake:0"),
			AddTags: []string{"go", "rust"}, NewTags: []string{"go", "rust"}, Result: "proposed"},
		{JobID: j, Bookmark: id("fake:3"), ObservedCollection: id("fake:0"), Result: "proposed"},
	}
	for _, r := range recs {
		if err := e.store.Jobs().SaveDecision(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	r := e.do("GET", "/v1/jobs/"+j+"/decisions?limit=2", "")
	want(t, r, 200)
	m := r.json()
	items := m["items"].([]any)
	if len(items) != 2 || m["next_cursor"] == nil || m["next_cursor"] == "" {
		t.Fatalf("paging: %v", m)
	}
	first := items[0].(map[string]any)
	if first["bookmark"] != "fake:1" || first["target_collection"] != "planned:cooking" {
		t.Fatalf("ids: %v", first)
	}
	r = e.do("GET", "/v1/jobs/"+j+"/decisions?cursor="+m["next_cursor"].(string), "")
	last := r.json()["items"].([]any)[0].(map[string]any)
	for _, k := range []string{"observed_tags", "add_tags"} {
		if s, ok := last[k].([]any); !ok || len(s) != 0 {
			t.Fatalf("%s = %v, want []", k, last[k])
		}
	}
	if strings.Contains(r.Body.String(), "null") && strings.Contains(r.Body.String(), `"tags":null`) {
		t.Fatalf("null tags: %s", r.Body.String())
	}
	want(t, e.do("GET", "/v1/jobs/job_nope/decisions", ""), 404)

	p := e.do("GET", "/v1/jobs/"+j+"/proposed-tags", "")
	want(t, p, 200)
	tags := p.json()["items"].([]any)
	if len(tags) != 2 || tags[0].(map[string]any)["name"] != "go" || tags[0].(map[string]any)["count"] != float64(2) {
		t.Fatalf("proposed tags: %v", tags)
	}
	want(t, e.do("GET", "/v1/jobs/job_nope/proposed-tags", ""), 404)

	// A job without new tags gives [], not null.
	k := e.do("POST", "/v1/jobs", `{"type":"snapshot"}`).json()["id"].(string)
	q := e.do("GET", "/v1/jobs/"+k+"/proposed-tags", "")
	if !strings.Contains(q.Body.String(), `"items":[]`) {
		t.Fatalf("body %s", q.Body.String())
	}
}

func TestRestoreDecisions(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	j := e.do("POST", "/v1/jobs", `{"type":"restore","params":{"snapshot_id":"s"},"dry_run":true}`).json()["id"].(string)
	to := id("fake:9")
	_ = e.store.Jobs().SaveDecision(ctx, domain.DecisionRecord{JobID: j, Bookmark: id("fake:1"),
		ObservedCollection: id("fake:2"), TargetCollection: &to, ObservedTags: []string{"a"}, Result: "proposed", Reason: "tags differ"})
	_ = e.store.Jobs().SaveDecision(ctx, domain.DecisionRecord{JobID: j, Bookmark: id("fake:3"),
		ObservedCollection: id("fake:2"), Result: "skipped", Reason: "deleted"})
	r := e.do("GET", "/v1/jobs/"+j+"/decisions", "")
	want(t, r, 200)
	items := r.json()["items"].([]any)
	d := items[0].(map[string]any)
	if d["from_collection"] != "fake:2" || d["to_collection"] != "fake:9" {
		t.Fatalf("diff: %v", d)
	}
	if s, ok := d["to_tags"].([]any); !ok || len(s) != 0 {
		t.Fatalf("to_tags = %v, want []", d["to_tags"])
	}
	if s := d["from_tags"].([]any); len(s) != 1 || d["result"] != "proposed" {
		t.Fatalf("diff: %v", d)
	}
	sk := items[1].(map[string]any)
	if sk["to_collection"] != nil || sk["reason"] != "deleted" {
		t.Fatalf("skipped: %v", sk)
	}
}

func TestProviders(t *testing.T) {
	e := newEnv(t)
	r := e.do("GET", "/v1/providers", "")
	want(t, r, 200)
	it := r.json()["items"].([]any)[0].(map[string]any)
	caps := it["capabilities"].(map[string]any)
	if it["name"] != "fake" || caps["highlights"] != true || caps["tag_listing"] != true || caps["create_collections"] != false {
		t.Fatalf("provider: %v", it)
	}
	if strings.Contains(strings.ToLower(r.Body.String()), "secret") || strings.Contains(r.Body.String(), apiToken) {
		t.Fatal("secret in body")
	}
}

func TestClassifiers(t *testing.T) {
	e := newEnv(t)
	r := e.do("GET", "/v1/classifiers", "")
	want(t, r, 200)
	it := r.json()["items"].([]any)[0].(map[string]any)
	caps := it["capabilities"].(map[string]any)
	if it["name"] != "llm" || len(it["roles"].([]any)) != 2 || caps["fixed_choice"] != true || caps["max_choices"] != float64(30) {
		t.Fatalf("classifier: %v", it)
	}
}

func TestTaxonomies(t *testing.T) {
	e := newEnv(t)
	l := e.do("GET", "/v1/taxonomies", "")
	want(t, l, 200)
	if n := len(l.json()["items"].([]any)); n != 1 {
		t.Fatalf("%d taxonomies", n)
	}
	g := e.do("GET", "/v1/taxonomies/default", "")
	want(t, g, 200)
	if g.json()["version"] != float64(1) || g.Header().Get("ETag") != `"1"` {
		t.Fatalf("get: %v etag %q", g.json(), g.Header().Get("ETag"))
	}
	want(t, e.do("GET", "/v1/taxonomies/nope", ""), 404)

	body := func(mod func(map[string]any)) string {
		m := g.json()
		mod(m)
		b, _ := json.Marshal(m)
		return string(b)
	}
	put := e.do("PUT", "/v1/taxonomies/default", body(func(m map[string]any) { m["rules"].(map[string]any)["max_tags"] = 4 }), "If-Match", `"1"`)
	want(t, put, 200)
	if put.json()["version"] != float64(2) || put.Header().Get("ETag") != `"2"` {
		t.Fatalf("put: %v", put.json())
	}
	// stale
	r := e.do("PUT", "/v1/taxonomies/default", body(func(m map[string]any) {}), "If-Match", "1")
	want(t, r, 409)
	checkErrorShape(t, r, "conflict")
	// no If-Match is fine
	want(t, e.do("PUT", "/v1/taxonomies/default", body(func(m map[string]any) {})), 200)
	// bad If-Match
	want(t, e.do("PUT", "/v1/taxonomies/default", body(func(m map[string]any) {}), "If-Match", "abc"), 400)
	// invalid
	r = e.do("PUT", "/v1/taxonomies/default", body(func(m map[string]any) { m["rules"].(map[string]any)["min_tags"] = 9 }))
	want(t, r, 422)
	checkErrorShape(t, r, "validation_failed")
	want(t, e.do("PUT", "/v1/taxonomies/default", `{"bogus":1}`), 400)
	want(t, e.do("PUT", "/v1/taxonomies/default", `{`), 400)
}

func TestProposals(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	r := e.do("GET", "/v1/taxonomies/default/proposals", "")
	want(t, r, 200)
	if !strings.Contains(r.Body.String(), `"items":[]`) {
		t.Fatalf("body %s", r.Body.String())
	}
	_ = e.store.Taxonomies().RecordProposal(ctx, "default", domain.CollectionProposal{Name: "Cooking", Count: 2})
	r = e.do("GET", "/v1/taxonomies/default/proposals", "")
	it := r.json()["items"].([]any)
	if len(it) != 1 || it[0].(map[string]any)["name"] != "Cooking" || it[0].(map[string]any)["count"] != float64(2) {
		t.Fatalf("proposals: %v", it)
	}
	want(t, e.do("GET", "/v1/taxonomies/nope/proposals", ""), 404)
}

func TestSnapshotsAndExport(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.provider.Add(domain.Bookmark{ID: id("fake:1"), URL: "https://a", Tags: []string{"x"}, Collection: id("fake:0")})
	sid, err := e.Snapshots().Take(ctx, "fake", domain.Scope{}, "job_1")
	if err != nil {
		t.Fatal(err)
	}
	l := e.do("GET", "/v1/snapshots", "")
	want(t, l, 200)
	items := l.json()["items"].([]any)
	if len(items) != 1 || items[0].(map[string]any)["id"] != sid {
		t.Fatalf("list: %v", items)
	}
	if cc, ok := items[0].(map[string]any)["created_collections"].([]any); !ok || len(cc) != 0 {
		t.Fatalf("created_collections = %v, want []", items[0].(map[string]any)["created_collections"])
	}
	g := e.do("GET", "/v1/snapshots/"+sid, "")
	want(t, g, 200)
	want(t, e.do("GET", "/v1/snapshots/snap_nope", ""), 404)

	x := e.do("GET", "/v1/snapshots/"+sid+"/export", "")
	want(t, x, 200)
	if !strings.HasPrefix(x.Header().Get("Content-Type"), "application/json") ||
		!strings.Contains(x.Header().Get("Content-Disposition"), sid+".json") {
		t.Fatalf("headers: %v", x.Header())
	}
	m := x.json()
	if m["id"] != sid || len(m["items"].([]any)) != 1 {
		t.Fatalf("export: %v", m)
	}
	r := e.do("GET", "/v1/snapshots/snap_nope/export", "")
	want(t, r, 404)
	checkErrorShape(t, r, "not_found")
	want(t, e.do("GET", "/v1/snapshots?limit=0x", ""), 400)
}

func (e *env) Snapshots() *jobs.SnapshotService {
	return jobs.NewSnapshotService(jobs.SnapshotDeps{Provider: e.provider, Store: e.store})
}

type brokenStore struct{ ports.Store }

func (brokenStore) Jobs() ports.JobStore { return brokenJobs{} }

type brokenJobs struct{ ports.JobStore }

func (brokenJobs) ListJobs(context.Context, string, int) (domain.Page[domain.Job], error) {
	return domain.Page[domain.Job]{}, errors.New("db down")
}

func TestHealthz(t *testing.T) {
	e := newEnv(t)
	r := e.do("GET", "/healthz", "", "Authorization", "")
	want(t, r, 200)
	b := newEnv(t, func(d *Deps) { d.Store = brokenStore{d.Store} })
	r = b.do("GET", "/healthz", "", "Authorization", "")
	want(t, r, 503)
	checkErrorShape(t, r, "unavailable")
	if strings.Contains(r.Body.String(), "db down") {
		t.Fatal("leaks detail")
	}
}

func TestListActiveJobs(t *testing.T) {
	e := newEnv(t)
	r := e.do("GET", "/v1/jobs?active=true", "")
	want(t, r, 200)
	if items, ok := r.json()["items"].([]any); !ok || len(items) != 0 {
		t.Fatalf("idle items = %v", r.json()["items"])
	}
	a := e.do("POST", "/v1/jobs", `{"type":"backfill","dry_run":true}`)
	want(t, a, 202)
	b := e.do("POST", "/v1/jobs", `{"type":"snapshot"}`)
	want(t, b, 202)
	// Only the first becomes running; the other stays queued.
	ctx := context.Background()
	j, err := e.store.Jobs().GetJob(ctx, a.json()["id"].(string))
	if err != nil {
		t.Fatal(err)
	}
	j.Status = domain.JobRunning
	if err := e.store.Jobs().UpdateJob(ctx, j); err != nil {
		t.Fatal(err)
	}
	m := e.do("GET", "/v1/jobs?active=true&limit=1", "").json()
	items := m["items"].([]any)
	if len(items) != 1 || items[0].(map[string]any)["id"] != j.ID {
		t.Fatalf("items = %v", items)
	}
	if m["next_cursor"] != nil {
		t.Fatalf("next_cursor = %v", m["next_cursor"])
	}
	want(t, e.do("GET", "/v1/jobs?active=false", ""), 400)
}
