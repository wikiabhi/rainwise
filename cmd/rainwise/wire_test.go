package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	cfake "github.com/wikiabhi/rainwise/internal/adapters/classifier/fake"
	pfake "github.com/wikiabhi/rainwise/internal/adapters/provider/fake"
	"github.com/wikiabhi/rainwise/internal/adapters/store/sqlite"
	"github.com/wikiabhi/rainwise/internal/config"
	"github.com/wikiabhi/rainwise/internal/domain"
)

const baseYAML = `
server: {addr: "127.0.0.1:0", api_token: ${API_TOKEN}}
state: {sqlite_path: ${DB}}
processing: {provider: raindrop, taxonomy: default}
providers:
  raindrop: {type: raindrop, token: ${RD_TOKEN}, webhook_secret: ${HOOK_SECRET}}
classifiers:
  llm1: {type: llm, base_url: "http://llm.invalid", model: m, api_key: ${LLM_KEY}}
  jev: {type: jev, base_url: "http://jev.invalid", model: j, api_key: ${JEV_KEY}}
roles: {classifier: llm1, proposer: llm1, fallback: null}
taxonomies:
  default:
    mode: fixed
    collections:
      - {key: backend, name: Backend}
      - {key: needs_review, name: Needs Review, role: review}
`

func testEnv(t *testing.T) func(string) string {
	t.Helper()
	db := filepath.Join(t.TempDir(), "r.db")
	vals := map[string]string{
		"API_TOKEN": "api-secret-value", "RD_TOKEN": "rd-secret-value", "HOOK_SECRET": "hook-secret-value",
		"LLM_KEY": "llm-secret-value", "JEV_KEY": "jev-secret-value", "DB": db,
	}
	return func(k string) string { return vals[k] }
}

func writeConfig(t *testing.T, y string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(y), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func baseConfig(t *testing.T) *config.Config {
	t.Helper()
	cfg, err := config.Load(writeConfig(t, baseYAML), testEnv(t))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func dynamic(id string, autoCreate bool) domain.Taxonomy {
	return domain.Taxonomy{
		ID: id, Mode: domain.ModeDynamic, Status: domain.TaxonomyDraft,
		Rules:   domain.TaxonomyRules{MinTags: 2, MaxTags: 5, MinConfidence: 0.6},
		Dynamic: domain.DynamicSettings{MaxCollections: 12, SampleSize: 100, AutoCreate: autoCreate, AutoCreateMinConfidence: 0.85},
	}
}

func fixedWith(id string, n int) domain.Taxonomy {
	t := domain.Taxonomy{ID: id, Mode: domain.ModeFixed, Status: domain.TaxonomyActive,
		Rules: domain.TaxonomyRules{MinTags: 2, MaxTags: 5, MinConfidence: 0.6}}
	for i := range n - 1 {
		t.Collections = append(t.Collections, domain.TaxonomyCollection{Key: fmt.Sprintf("c%d", i), Name: fmt.Sprintf("C%d", i)})
	}
	t.Collections = append(t.Collections, domain.TaxonomyCollection{Key: "review", Name: "Needs Review", Role: "review"})
	return t
}

func shutdown(t *testing.T, a *App) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := a.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestBuildRejects(t *testing.T) {
	jev := "jev"
	tests := []struct {
		name string
		mod  func(*config.Config)
		want []string
	}{
		{"dynamic taxonomy with Jev as proposer", func(c *config.Config) {
			c.Taxonomies["auto"] = dynamic("auto", false)
			c.Roles.Proposer = "jev"
		}, []string{"roles.proposer", `"jev"`, "taxonomies.auto"}},
		{"auto_create with Jev as classifier", func(c *config.Config) {
			c.Taxonomies["auto"] = dynamic("auto", true)
			c.Roles.Classifier = "jev"
		}, []string{"roles.classifier", `"jev"`, "taxonomies.auto", "auto_create"}},
		{"256 fixed collections with Jev", func(c *config.Config) {
			c.Taxonomies["big"] = fixedWith("big", 256)
			c.Roles.Classifier = "jev"
		}, []string{"roles.classifier", `"jev"`, "taxonomies.big", "256", "255"}},
		{"256 fixed collections with Jev as fallback", func(c *config.Config) {
			c.Taxonomies["big"] = fixedWith("big", 256)
			c.Roles.Fallback = &jev
		}, []string{"roles.fallback", "taxonomies.big"}},
		{"unknown provider type", func(c *config.Config) {
			p := c.Providers["raindrop"]
			p.Type = "pocket"
			c.Providers["raindrop"] = p
		}, []string{"providers.raindrop.type", `"pocket"`}},
		{"unknown type on a provider not in use", func(c *config.Config) {
			c.Providers["other"] = config.ProviderConfig{Type: "pocket"}
		}, []string{"providers.other.type"}},
		{"unknown classifier type", func(c *config.Config) {
			k := c.Classifiers["llm1"]
			k.Type = "rules"
			c.Classifiers["llm1"] = k
		}, []string{"classifiers.llm1.type", `"rules"`}},
		{"unknown content_fetch", func(c *config.Config) {
			c.Processing.ContentFetch = "sometimes"
		}, []string{"processing.content_fetch", `"sometimes"`}},
		{"dynamic max_collections above what Jev can choose from", func(c *config.Config) {
			tx := dynamic("auto", false)
			tx.Dynamic.MaxCollections = 256
			c.Taxonomies["auto"] = tx
			c.Roles.Classifier = "jev"
		}, []string{"roles.classifier", `"jev"`, "taxonomies.auto", "max_collections", "256", "255"}},
		{"provider named apart from its adapter", func(c *config.Config) {
			c.Providers["mine"] = c.Providers["raindrop"]
			c.Processing.Provider = "mine"
		}, []string{"processing.provider", `"mine"`}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := baseConfig(t)
			tc.mod(cfg)
			a, err := Build(cfg, BuildDeps{})
			if err == nil {
				shutdown(t, a)
				t.Fatal("Build should fail")
			}
			for _, w := range tc.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q should contain %q", err, w)
				}
			}
			if _, statErr := os.Stat(cfg.State.SQLitePath); !os.IsNotExist(statErr) {
				t.Errorf("a rejected config should leave no database: %v", statErr)
			}
		})
	}
}

// §8.1: the database wins over config, so the startup rules must read it too.
func TestBuildRejectsStoredTaxonomy(t *testing.T) {
	cfg := baseConfig(t)
	cfg.Taxonomies["auto"] = dynamic("auto", false)
	a, err := Build(cfg, BuildDeps{})
	if err != nil {
		t.Fatal(err)
	}
	shutdown(t, a)

	delete(cfg.Taxonomies, "auto")
	cfg.Roles.Proposer = "jev"
	a, err = Build(cfg, BuildDeps{})
	if err == nil {
		shutdown(t, a)
		t.Fatal("Build should fail on the stored dynamic taxonomy")
	}
	if !strings.Contains(err.Error(), "roles.proposer") || !strings.Contains(err.Error(), "taxonomies.auto") {
		t.Fatalf("err = %v", err)
	}
	// A failed Build must let go of the database, or the retry can't open it.
	cfg.Roles.Proposer = "llm1"
	if a, err = Build(cfg, BuildDeps{}); err != nil {
		t.Fatal(err)
	}
	shutdown(t, a)
}

func get(t *testing.T, a *App, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Authorization", "Bearer api-secret-value")
	rec := httptest.NewRecorder()
	a.Handler.ServeHTTP(rec, req)
	return rec
}

func TestBuildAccepts(t *testing.T) {
	tests := []struct {
		name string
		mod  func(*config.Config)
	}{
		{"fixed taxonomy with Jev", func(c *config.Config) {
			c.Roles.Classifier, c.Roles.Proposer = "jev", "jev"
		}},
		{"dynamic with LLM proposer and Jev classifier", func(c *config.Config) {
			c.Taxonomies["auto"] = dynamic("auto", false)
			c.Roles.Classifier, c.Roles.Proposer = "jev", "llm1"
		}},
		{"255 fixed collections with Jev", func(c *config.Config) {
			c.Taxonomies["big"] = fixedWith("big", 255)
			c.Roles.Classifier = "jev"
		}},
		{"auto_create with LLM classifier", func(c *config.Config) {
			c.Taxonomies["auto"] = dynamic("auto", true)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := baseConfig(t)
			tc.mod(cfg)
			a, err := Build(cfg, BuildDeps{})
			if err != nil {
				t.Fatal(err)
			}
			defer shutdown(t, a)
			if rec := get(t, a, "/healthz"); rec.Code != http.StatusOK {
				t.Fatalf("healthz = %d", rec.Code)
			}
		})
	}
}

// The info routes show what's wired, and no secret.
func TestAPIShowsWiring(t *testing.T) {
	cfg := baseConfig(t)
	p := cfg.Providers["raindrop"]
	p.IncrementalSince = true
	cfg.Providers["raindrop"] = p
	a, err := Build(cfg, BuildDeps{})
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(t, a)

	providers, classifiers := get(t, a, "/v1/providers").Body.String(), get(t, a, "/v1/classifiers").Body.String()
	if !strings.Contains(providers, `"name":"raindrop"`) || !strings.Contains(providers, `"incremental_since":true`) ||
		!strings.Contains(providers, `"tag_listing":false`) {
		t.Fatalf("providers = %s", providers)
	}
	var list struct {
		Items []struct {
			Name  string   `json:"name"`
			Roles []string `json:"roles"`
		} `json:"items"`
	}
	if err := json.Unmarshal([]byte(classifiers), &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 2 || list.Items[0].Name != "jev" || len(list.Items[0].Roles) != 0 ||
		list.Items[1].Name != "llm1" || strings.Join(list.Items[1].Roles, ",") != "classifier,proposer" {
		t.Fatalf("classifiers = %s", classifiers)
	}
	for _, body := range []string{providers, classifiers} {
		if strings.Contains(body, "secret-value") {
			t.Fatalf("a secret leaked: %s", body)
		}
	}
	// A request that names no provider gets the configured one.
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader(`{"type":"snapshot"}`))
	req.Header.Set("Authorization", "Bearer api-secret-value")
	rec := httptest.NewRecorder()
	a.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted || !strings.Contains(rec.Body.String(), `"provider":"raindrop"`) {
		t.Fatalf("POST /v1/jobs = %d %s", rec.Code, rec.Body)
	}
}

func captureDefaultLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

func fakeConfig(t *testing.T) *config.Config {
	t.Helper()
	cfg := baseConfig(t)
	cfg.Providers["fake"] = config.ProviderConfig{Type: "fake", WebhookSecret: "hook"}
	delete(cfg.Providers, "raindrop")
	cfg.Processing.Provider = "fake"
	cfg.Processing.ContentFetch = "off"
	cfg.Scheduler = config.Scheduler{Enabled: true, SyncEvery: time.Hour}
	return cfg
}

type recordingTicker struct {
	asked chan time.Duration
	ticks chan time.Time
}

func newRecordingTicker() *recordingTicker {
	return &recordingTicker{asked: make(chan time.Duration, 1), ticks: make(chan time.Time)}
}

func (r *recordingTicker) ticker(d time.Duration) (<-chan time.Time, func()) {
	r.asked <- d
	return r.ticks, func() {}
}

func TestSchedulerNeedsAutomation(t *testing.T) {
	logs := captureDefaultLog(t)
	cfg := fakeConfig(t)
	cfg.Automation.Enabled = false
	rt := newRecordingTicker()
	a, err := Build(cfg, BuildDeps{Provider: pfake.New(), Classifier: cfake.New("llm1"), Ticker: rt.ticker})
	if err != nil {
		t.Fatal(err)
	}
	a.Start(context.Background())
	// Shutdown waits for whatever Start began, so a scheduler would have asked for
	// its ticker by the time it returns.
	jobs := get(t, a, "/v1/jobs").Body.String()
	shutdown(t, a)

	select {
	case <-rt.asked:
		t.Fatal("the scheduler ran with automation off")
	default:
	}
	if !strings.Contains(jobs, `"items":[]`) {
		t.Fatalf("jobs = %s", jobs)
	}
	if out := logs.String(); !strings.Contains(out, "level=WARN") || !strings.Contains(out, "scheduler.enabled") ||
		!strings.Contains(out, "automation.enabled") {
		t.Fatalf("no warning naming the settings: %q", out)
	}
}

// The other half of TestSchedulerNeedsAutomation: it's the automation setting that
// keeps the scheduler quiet, and a tick really does make a sync job.
func TestSchedulerRunsWithAutomation(t *testing.T) {
	logs := captureDefaultLog(t)
	cfg := fakeConfig(t)
	cfg.Automation.Enabled = true
	rt := newRecordingTicker()
	a, err := Build(cfg, BuildDeps{Provider: pfake.New(), Classifier: cfake.New("llm1"), Ticker: rt.ticker, PollEvery: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	a.Start(context.Background())
	defer shutdown(t, a)

	select {
	case d := <-rt.asked:
		if d != time.Hour {
			t.Fatalf("every = %v", d)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the scheduler never started")
	}
	rt.ticks <- time.Now()
	deadline := time.Now().Add(30 * time.Second)
	for !strings.Contains(get(t, a, "/v1/jobs").Body.String(), `"type":"sync"`) {
		if time.Now().After(deadline) {
			t.Fatal("no sync job after a tick")
		}
		runtime.Gosched()
	}
	shutdown(t, a) // Jobs log too, so it's only safe to read the log after this.
	if strings.Contains(logs.String(), "scheduler.enabled") {
		t.Fatalf("unexpected warning: %q", logs.String())
	}
}

// A new job starts at once, not at the next poll.
func TestNewJobWakesRunner(t *testing.T) {
	cfg := fakeConfig(t)
	cfg.Scheduler.Enabled = false
	a, err := Build(cfg, BuildDeps{Provider: pfake.New(), Classifier: cfake.New("llm1"), PollEvery: 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	a.Start(context.Background())
	defer shutdown(t, a)

	// The first job may be picked up by the runner's first look, so it's the second
	// that can only start by being woken.
	for range 2 {
		req := httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader(`{"type":"snapshot"}`))
		req.Header.Set("Authorization", "Bearer api-secret-value")
		rec := httptest.NewRecorder()
		a.Handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("POST /v1/jobs = %d %s", rec.Code, rec.Body)
		}
		id := jobID(t, rec.Body.Bytes())
		deadline := time.Now().Add(10 * time.Second)
		for !strings.Contains(get(t, a, "/v1/jobs/"+id).Body.String(), `"status":"succeeded"`) {
			if time.Now().After(deadline) {
				t.Fatalf("the job never ran: %s", get(t, a, "/v1/jobs/"+id).Body)
			}
			runtime.Gosched()
		}
	}
}

func jobID(t *testing.T, body []byte) string {
	t.Helper()
	var j struct {
		ID string `json:"id"`
	}
	err := json.Unmarshal(body, &j)
	if err != nil || j.ID == "" {
		t.Fatalf("no job id in %s: %v", body, err)
	}
	return j.ID
}

func cli(t *testing.T, env func(string) string, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), args, &stdout, &stderr, env)
	return code, stdout.String(), stderr.String()
}

func TestConfigValidateCommand(t *testing.T) {
	env := testEnv(t)
	good := writeConfig(t, baseYAML)
	if code, _, stderr := cli(t, env, "config", "validate", "--config", good); code != 0 {
		t.Fatalf("code = %d, stderr = %q", code, stderr)
	}
	if _, err := os.Stat(env("DB")); !os.IsNotExist(err) {
		t.Fatalf("validate should not create the database: %v", err)
	}

	tests := []struct {
		name, yaml, want string
		env              func(string) string
	}{
		{"unset env", baseYAML, "API_TOKEN", func(string) string { return "" }},
		{"unknown key", baseYAML + "\nbogus: 1\n", "bogus", env},
		{"bad classifier type", strings.Replace(baseYAML, "{type: llm,", "{type: rules,", 1), "classifiers.llm1.type", env},
		{"startup rule", strings.Replace(baseYAML, "proposer: llm1", "proposer: jev", 1) +
			"  auto: {mode: dynamic}\n", "roles.proposer", env},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			code, _, stderr := cli(t, tc.env, "config", "validate", "--config", writeConfig(t, tc.yaml))
			if code != 1 || !strings.Contains(stderr, tc.want) {
				t.Fatalf("code = %d, stderr = %q, want %q", code, stderr, tc.want)
			}
			for _, s := range []string{"rd-secret-value", "llm-secret-value", "api-secret-value"} {
				if strings.Contains(stderr, s) {
					t.Fatalf("stderr leaked a secret: %q", stderr)
				}
			}
		})
	}
	t.Run("missing file", func(t *testing.T) {
		if code, _, stderr := cli(t, env, "config", "validate", "--config", filepath.Join(t.TempDir(), "no.yaml")); code != 1 || stderr == "" {
			t.Fatalf("code = %d, stderr = %q", code, stderr)
		}
	})
}

func TestCLIUsage(t *testing.T) {
	for _, args := range [][]string{nil, {"nonsense"}, {"config"}, {"config", "nonsense"}, {"serve"}, {"config", "validate"}} {
		code, _, stderr := cli(t, testEnv(t), args...)
		if code != 2 || stderr == "" {
			t.Errorf("args %v: code = %d, stderr = %q", args, code, stderr)
		}
	}
}

func TestServeConfigError(t *testing.T) {
	code, _, stderr := cli(t, testEnv(t), "serve", "--config", writeConfig(t, baseYAML+"\nbogus: 1\n"))
	if code != 1 || !strings.Contains(stderr, "bogus") {
		t.Fatalf("code = %d, stderr = %q", code, stderr)
	}
}

// serve stops cleanly when its signal context ends, logging JSON to stdout from
// the start and never a secret.
func TestServeStopsOnSignal(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	y := baseYAML + "scheduler: {enabled: true, sync_every: 1h}\n"
	env := testEnv(t)
	path := writeConfig(t, y)
	sig, stop := context.WithCancel(context.Background())
	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() { done <- run(sig, []string{"serve", "--config", path}, &stdout, &stderr, env) }()
	stop()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("code = %d, stderr = %q", code, stderr.String())
		}
	case <-time.After(30 * time.Second):
		t.Fatal("serve did not stop")
	}
	first, _, _ := strings.Cut(stdout.String(), "\n")
	var line map[string]any
	if err := json.Unmarshal([]byte(first), &line); err != nil {
		t.Fatalf("log line %q is not JSON: %v", first, err)
	}
	if line["level"] != "WARN" {
		t.Fatalf("first line = %v, want the scheduler warning", line)
	}
	if strings.Contains(stdout.String()+stderr.String(), "secret-value") {
		t.Fatal("a secret reached the output")
	}
}

// A port that's taken must stop serve before it touches any job.
func TestServeListensBeforeRecoveringJobs(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	env := testEnv(t)
	ctx := context.Background()
	store, err := sqlite.Open(ctx, env("DB"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	// Running this one would fail it without any call to the provider.
	job := domain.Job{ID: "job_q", Type: domain.JobRestore, Status: domain.JobQueued, Provider: "raindrop",
		Params: domain.JobParams{SnapshotID: "no-such-snapshot"}, CreatedAt: time.Now()}
	if err := store.Jobs().CreateJob(ctx, job); err != nil {
		t.Fatal(err)
	}

	y := strings.Replace(baseYAML, `"127.0.0.1:0"`, `"`+taken.Addr().String()+`"`, 1)
	code, _, stderr := cli(t, env, "serve", "--config", writeConfig(t, y))
	if code != 1 || stderr == "" {
		t.Fatalf("code = %d, stderr = %q", code, stderr)
	}
	got, err := store.Jobs().GetJob(ctx, "job_q")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domain.JobQueued {
		t.Fatalf("job = %s, want it untouched", got.Status)
	}
}

func TestServeReportsListenFailure(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	y := strings.Replace(baseYAML, `"127.0.0.1:0"`, `"not an address"`, 1)
	code, _, stderr := cli(t, testEnv(t), "serve", "--config", writeConfig(t, y))
	if code != 1 || stderr == "" {
		t.Fatalf("code = %d, stderr = %q", code, stderr)
	}
}
