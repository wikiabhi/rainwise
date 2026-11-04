package jev

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wikiabhi/rainwise/internal/adapters/classifier/classifiertest"
	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

type recorded struct {
	Path   string
	Header http.Header
	Body   map[string]any
}

type reply struct {
	Status int
	Raw    string
}

type backend struct {
	mu      sync.Mutex
	srv     *httptest.Server
	reqs    []recorded
	replies []reply
}

func newBackend(t *testing.T, replies ...reply) *backend {
	t.Helper()
	b := &backend{replies: replies}
	b.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		b.mu.Lock()
		b.reqs = append(b.reqs, recorded{r.URL.Path, r.Header.Clone(), body})
		rep := b.replies[min(len(b.reqs)-1, len(b.replies)-1)]
		b.mu.Unlock()
		if rep.Status == 0 {
			rep.Status = 200
		}
		w.WriteHeader(rep.Status)
		_, _ = io.WriteString(w, rep.Raw)
	}))
	t.Cleanup(b.srv.Close)
	return b
}

func (b *backend) requests() []recorded {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]recorded(nil), b.reqs...)
}

func newClassifier(b *backend, mod func(*Config)) (*Classifier, *[]time.Duration) {
	cfg := Config{Name: "jev", BaseURL: b.srv.URL, Model: "jev-1", APIKey: "sekret", HTTP: b.srv.Client()}
	if mod != nil {
		mod(&cfg)
	}
	c := New(cfg)
	var slept []time.Duration
	c.sleep = func(_ context.Context, d time.Duration) error { slept = append(slept, d); return nil }
	return c, &slept
}

func request(wantCollection bool, tagsWanted int) ports.ClassifyRequest {
	return ports.ClassifyRequest{
		Subject: ports.Subject{Bookmark: domain.Bookmark{
			ID: domain.ID{Provider: "test", Local: "1"}, URL: "https://example.test/p", Title: "A post",
		}},
		Content: "page text",
		Taxonomy: domain.TaxonomySnapshot{
			ID: "t", Version: 1,
			Collections: []domain.TaxonomyCollection{
				{Key: "dev", Name: "Dev", Description: "programming articles"},
				{Key: "cook", Name: "Cooking", Description: "recipes"},
				{Key: "needs_review", Name: "Needs Review", Role: "review"},
			},
			Rules: domain.TaxonomyRules{MaxTags: 5},
		},
		TagVocab:       []string{"golang", "sqlite", "rust", "bread"},
		TagsWanted:     tagsWanted,
		WantCollection: wantCollection,
	}
}

func answers(m map[string]any) string {
	out, _ := json.Marshal(map[string]any{
		"model": "jev-1.13.0", "answers": m,
		"usage": map[string]any{"input_tokens": 428, "output_tokens": 73},
	})
	return string(out)
}

func choice(k string, conf float64) map[string]any {
	return map[string]any{"type": "choice", "choice": k, "confidence": conf}
}

func noul(p float64) map[string]any { return map[string]any{"type": "noul", "noul": p} }

func TestRequestShape(t *testing.T) {
	b := newBackend(t, reply{Raw: answers(map[string]any{"collection": choice("dev", 1)})})
	c, _ := newClassifier(b, nil)
	if _, err := c.Classify(context.Background(), request(true, 0)); err != nil {
		t.Fatal(err)
	}
	r := b.requests()[0]
	if r.Path != "/v1/systemone" {
		t.Errorf("path = %q", r.Path)
	}
	if r.Header.Get("Authorization") != "Bearer sekret" {
		t.Errorf("auth = %q", r.Header.Get("Authorization"))
	}
	if r.Body["model"] != "jev-1" {
		t.Errorf("model = %v", r.Body["model"])
	}
	if s, _ := r.Body["state"].(string); !strings.Contains(s, "A post") || !strings.Contains(s, "page text") {
		t.Errorf("state = %v", r.Body["state"])
	}
	qs := r.Body["questions"].(map[string]any)
	if len(qs) != 1 {
		t.Fatalf("questions = %v", qs)
	}
	q := qs["collection"].(map[string]any)
	if q["type"] != "choice" || q["instructions"] == "" {
		t.Errorf("question = %v", q)
	}
	crit := q["criteria"].(map[string]any)
	if len(crit) != 2 || crit["dev"] != "programming articles" || crit["cook"] != "recipes" {
		t.Errorf("criteria = %v (review collection must be excluded)", crit)
	}
}

func TestTagQuestions(t *testing.T) {
	b := newBackend(t, reply{Raw: answers(map[string]any{
		"collection": choice("dev", 1), "tag_0": noul(0.1), "tag_1": noul(0.95), "tag_2": noul(0.8),
	})})
	c, _ := newClassifier(b, func(cfg *Config) { cfg.MaxTagCandidates = 3 })
	d, err := c.Classify(context.Background(), request(true, 5))
	if err != nil {
		t.Fatal(err)
	}
	qs := b.requests()[0].Body["questions"].(map[string]any)
	if len(qs) != 4 {
		t.Fatalf("want collection + 3 tag questions, got %v", qs)
	}
	for _, k := range []string{"tag_0", "tag_1", "tag_2"} {
		q := qs[k].(map[string]any)
		if q["type"] != "noul" {
			t.Errorf("%s = %v", k, q)
		}
	}
	if !strings.Contains(qs["tag_1"].(map[string]any)["instructions"].(string), "sqlite") {
		t.Error("tag_1 instructions should name sqlite")
	}
	if _, ok := qs["tag_3"]; ok {
		t.Error("tag_3 exceeds MaxTagCandidates")
	}
	if len(d.Tags) != 2 || d.Tags[0].Name != "sqlite" || d.Tags[1].Name != "rust" {
		t.Errorf("tags = %v", d.Tags)
	}
}

func TestNoTagQuestionsWhenNoneWanted(t *testing.T) {
	b := newBackend(t, reply{Raw: answers(map[string]any{"collection": choice("dev", 1)})})
	c, _ := newClassifier(b, nil)
	if _, err := c.Classify(context.Background(), request(true, 0)); err != nil {
		t.Fatal(err)
	}
	if n := len(b.requests()[0].Body["questions"].(map[string]any)); n != 1 {
		t.Errorf("questions = %d", n)
	}
}

func TestParse(t *testing.T) {
	b := newBackend(t, reply{Raw: answers(map[string]any{
		"collection": choice("cook", 0.83),
		"tag_0":      noul(0.75), "tag_1": noul(0.99), "tag_2": noul(0.7), "tag_3": noul(0.9),
	})})
	c, _ := newClassifier(b, nil)
	d, err := c.Classify(context.Background(), request(true, 2))
	if err != nil {
		t.Fatal(err)
	}
	if d.Collection.Key != "cook" || d.Confidence != 0.83 {
		t.Errorf("collection/confidence = %+v %v", d.Collection, d.Confidence)
	}
	if len(d.Tags) != 2 || d.Tags[0].Name != "sqlite" || d.Tags[1].Name != "bread" {
		t.Fatalf("tags = %v (0.7 is not above threshold; cap is 2)", d.Tags)
	}
	if d.Tags[0].Score == nil || *d.Tags[0].Score != 0.99 {
		t.Errorf("score = %v", d.Tags[0].Score)
	}
	m := d.Model
	if m.Classifier != "jev" || m.Model != "jev-1" || m.Version != "jev-1.13.0" || m.InputTokens != 428 || m.OutputTokens != 73 {
		t.Errorf("model = %+v", m)
	}
}

func TestParseRejectsBadAnswers(t *testing.T) {
	cases := map[string]string{
		"unknown key":    answers(map[string]any{"collection": choice("nope", 1)}),
		"review key":     answers(map[string]any{"collection": choice("needs_review", 1)}),
		"missing":        answers(map[string]any{}),
		"wrong type":     answers(map[string]any{"collection": noul(0.5)}),
		"bad confidence": answers(map[string]any{"collection": choice("dev", 1.5)}),
		"not json":       `nope`,
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			b := newBackend(t, reply{Raw: raw})
			c, _ := newClassifier(b, nil)
			_, err := c.Classify(context.Background(), request(true, 0))
			if !errors.Is(err, domain.ErrInvalidOutput) {
				t.Errorf("err = %v", err)
			}
		})
	}
	t.Run("missing tag answer", func(t *testing.T) {
		b := newBackend(t, reply{Raw: answers(map[string]any{"collection": choice("dev", 1)})})
		c, _ := newClassifier(b, nil)
		if _, err := c.Classify(context.Background(), request(true, 2)); !errors.Is(err, domain.ErrInvalidOutput) {
			t.Errorf("err = %v", err)
		}
	})
}

func TestWantCollectionFalse(t *testing.T) {
	b := newBackend(t, reply{Raw: answers(map[string]any{"tag_0": noul(0.5), "tag_1": noul(0.95), "tag_2": noul(0.1), "tag_3": noul(0.5)})})
	c, _ := newClassifier(b, nil)
	d, err := c.Classify(context.Background(), request(false, 3))
	if err != nil {
		t.Fatal(err)
	}
	qs := b.requests()[0].Body["questions"].(map[string]any)
	if _, ok := qs["collection"]; ok {
		t.Error("collection question must be absent")
	}
	// |2*0.1-1| = 0.8, |2*0.95-1| = 0.9, |2*0.5-1| = 0
	if d.Confidence < 0.8999 || d.Confidence > 0.9001 {
		t.Errorf("confidence = %v, want 0.9", d.Confidence)
	}
	if d.Collection.Key != "" {
		t.Errorf("collection = %+v", d.Collection)
	}
}

func TestTooManyChoices(t *testing.T) {
	b := newBackend(t, reply{Raw: "{}"})
	c, _ := newClassifier(b, nil)
	r := request(true, 0)
	r.Taxonomy.Collections = nil
	for i := range 256 {
		r.Taxonomy.Collections = append(r.Taxonomy.Collections, domain.TaxonomyCollection{Key: fmt.Sprint("k", i)})
	}
	_, err := c.Classify(context.Background(), r)
	if !errors.Is(err, domain.ErrValidation) {
		t.Errorf("err = %v", err)
	}
	if len(b.requests()) != 0 {
		t.Error("must not call the API")
	}
}

func TestHTTPErrorKinds(t *testing.T) {
	cases := []struct {
		status    int
		rate, tmp bool
		attempts  int
	}{
		{429, true, false, 4},
		{529, false, true, 4},
		{500, false, true, 4},
		{401, false, false, 1},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprint(tc.status), func(t *testing.T) {
			b := newBackend(t, reply{Status: tc.status, Raw: `{"error":"nope"}`})
			c, slept := newClassifier(b, nil)
			_, err := c.Classify(context.Background(), request(true, 0))
			if err == nil {
				t.Fatal("expected error")
			}
			if errors.Is(err, domain.ErrRateLimited) != tc.rate || errors.Is(err, domain.ErrTemporary) != tc.tmp {
				t.Errorf("err = %v", err)
			}
			if errors.Is(err, domain.ErrInvalidOutput) {
				t.Error("http failure must not be invalid output")
			}
			if n := len(b.requests()); n != tc.attempts {
				t.Errorf("attempts = %d, want %d", n, tc.attempts)
			}
			if len(*slept) != tc.attempts-1 {
				t.Errorf("sleeps = %v", *slept)
			}
			if strings.Contains(err.Error(), "sekret") {
				t.Error("error leaks the API key")
			}
		})
	}
}

func TestRetryRecovers(t *testing.T) {
	b := newBackend(t, reply{Status: 429, Raw: `{}`}, reply{Status: 529, Raw: `{}`},
		reply{Raw: answers(map[string]any{"collection": choice("dev", 1)})})
	c, slept := newClassifier(b, nil)
	if _, err := c.Classify(context.Background(), request(true, 0)); err != nil {
		t.Fatal(err)
	}
	if len(b.requests()) != 3 || len(*slept) != 2 || (*slept)[1] != 2*(*slept)[0] {
		t.Errorf("requests=%d slept=%v", len(b.requests()), *slept)
	}
}

func TestMaxTokensExceeded(t *testing.T) {
	body := `{"error":{"code":"max_tokens_exceeded","message":"too long"}}`
	t.Run("retries once with half the state", func(t *testing.T) {
		b := newBackend(t, reply{Status: 400, Raw: body},
			reply{Raw: answers(map[string]any{"collection": choice("dev", 1)})})
		c, slept := newClassifier(b, nil)
		r := request(true, 0)
		r.Content = strings.Repeat("x", 4000)
		if _, err := c.Classify(context.Background(), r); err != nil {
			t.Fatal(err)
		}
		reqs := b.requests()
		if len(reqs) != 2 || len(*slept) != 0 {
			t.Fatalf("requests=%d slept=%v", len(reqs), *slept)
		}
		first := len(reqs[0].Body["state"].(string))
		second := len(reqs[1].Body["state"].(string))
		if second != first/2 {
			t.Errorf("state %d -> %d, want half", first, second)
		}
	})
	t.Run("matches code in a string error field", func(t *testing.T) {
		b := newBackend(t, reply{Status: 400, Raw: `{"error":"max_tokens_exceeded"}`},
			reply{Raw: answers(map[string]any{"collection": choice("dev", 1)})})
		c, _ := newClassifier(b, nil)
		if _, err := c.Classify(context.Background(), request(true, 0)); err != nil {
			t.Fatal(err)
		}
		if len(b.requests()) != 2 {
			t.Errorf("requests = %d", len(b.requests()))
		}
	})
	t.Run("gives up after one retry", func(t *testing.T) {
		b := newBackend(t, reply{Status: 400, Raw: body})
		c, _ := newClassifier(b, nil)
		_, err := c.Classify(context.Background(), request(true, 0))
		if err == nil || errors.Is(err, domain.ErrTemporary) || errors.Is(err, domain.ErrRateLimited) {
			t.Errorf("err = %v", err)
		}
		if len(b.requests()) != 2 {
			t.Errorf("requests = %d", len(b.requests()))
		}
	})
	t.Run("other 400 is not retried", func(t *testing.T) {
		b := newBackend(t, reply{Status: 400, Raw: `{"error":{"code":"bad_request"}}`})
		c, _ := newClassifier(b, nil)
		if _, err := c.Classify(context.Background(), request(true, 0)); err == nil {
			t.Fatal("expected error")
		}
		if len(b.requests()) != 1 {
			t.Errorf("requests = %d", len(b.requests()))
		}
	})
}

func TestCapabilities(t *testing.T) {
	c := New(Config{Name: "jev", BaseURL: "http://x"})
	want := ports.ClassifierCapabilities{FixedChoice: true, CalibratedConfidence: true, MaxChoices: 255}
	if c.Capabilities() != want {
		t.Errorf("caps = %+v", c.Capabilities())
	}
	if _, ok := any(c).(ports.TaxonomyProposer); ok {
		t.Error("must not implement TaxonomyProposer")
	}
}

func TestConformance(t *testing.T) {
	var mu sync.Mutex
	var want domain.Decision
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Questions map[string]struct {
				Type string `json:"type"`
			} `json:"questions"`
		}
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		mu.Lock()
		d := want
		mu.Unlock()
		picked := map[string]bool{}
		for _, tg := range d.Tags {
			picked[tg.Name] = true
		}
		vocab := []string{"databases", "storage"} // classifiertest's vocabulary
		ans := map[string]any{}
		if _, ok := body.Questions["collection"]; ok {
			key := d.Collection.Key
			if key == "" {
				key = "alpha" // Jev cannot propose; it still must pick a listed key
			}
			ans["collection"] = choice(key, d.Confidence)
		}
		for i, name := range vocab {
			if _, ok := body.Questions[fmt.Sprint("tag_", i)]; ok {
				p := 0.01
				if picked[name] {
					p = 0.99
				}
				ans[fmt.Sprint("tag_", i)] = noul(p)
			}
		}
		_, _ = io.WriteString(w, answers(ans))
	}))
	t.Cleanup(srv.Close)
	c := New(Config{Name: "jev", BaseURL: srv.URL, Model: "jev-1", HTTP: srv.Client()})
	classifiertest.Run(t, c, func(d domain.Decision) {
		mu.Lock()
		want = d
		mu.Unlock()
	})
}
