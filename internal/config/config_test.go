package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const baseYAML = `
server: {addr: ":8080", api_token: ${API_TOKEN}}
state: {sqlite_path: /data/r.db}
processing: {provider: raindrop, taxonomy: default}
providers:
  raindrop: {type: raindrop, token: ${RAINDROP_TOKEN}}
classifiers:
  llm1: {type: llm, base_url: http://x, model: m, api_key: ${KEY}}
roles: {classifier: llm1, proposer: llm1, fallback: null}
taxonomies:
  default:
    mode: fixed
    collections:
      - {key: backend, name: Backend}
      - {key: needs_review, name: Needs Review, role: review}
`

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

var goodEnv = map[string]string{"API_TOKEN": "api-secret", "RAINDROP_TOKEN": "rd-secret-123", "KEY": "k-secret"}

func write(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "c.yaml")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func load(t *testing.T, y string) (*Config, error) {
	t.Helper()
	return Load(write(t, y), env(goodEnv))
}

func TestLoadExpandsEnv(t *testing.T) {
	cfg, err := load(t, baseYAML)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Providers["raindrop"].Token.Value(); got != "rd-secret-123" {
		t.Fatalf("token = %q", got)
	}
	if cfg.Server.APIToken.Value() != "api-secret" {
		t.Fatal("api token not expanded")
	}
}

func TestLoadRejectsMissingEnv(t *testing.T) {
	_, err := Load(write(t, baseYAML), env(map[string]string{"API_TOKEN": "a", "KEY": "k"}))
	if err == nil || !strings.Contains(err.Error(), "RAINDROP_TOKEN") {
		t.Fatalf("err = %v, want mention of RAINDROP_TOKEN", err)
	}
}

func TestLoadRejectsUnknownKeys(t *testing.T) {
	if _, err := load(t, "servr: {addr: x}\n"+baseYAML); err == nil {
		t.Fatal("expected error for unknown key")
	}
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := load(t, baseYAML)
	if err != nil {
		t.Fatal(err)
	}
	p := cfg.Processing
	if p.Concurrency != 4 || p.ContentFetch != "auto" || p.ContentFetchMinChars != 80 || p.TagVocabSize != 200 {
		t.Fatalf("processing defaults: %+v", p)
	}
	if cfg.State.SnapshotRetention != 10 {
		t.Fatalf("retention = %d", cfg.State.SnapshotRetention)
	}
	if cfg.Providers["raindrop"].RequestsPerMinute != 100 {
		t.Fatal("requests_per_minute default")
	}
	c := cfg.Classifiers["llm1"]
	if c.MaxTagCandidates != 100 || c.TagThreshold != 0.7 {
		t.Fatalf("classifier defaults: %+v", c)
	}
	tx := cfg.Taxonomies["default"]
	if tx.ID != "default" || tx.Status != "active" {
		t.Fatalf("taxonomy id/status: %+v", tx)
	}
	if tx.Rules.MinTags != 2 || tx.Rules.MaxTags != 5 || tx.Rules.MinConfidence != 0.6 {
		t.Fatalf("rules defaults: %+v", tx.Rules)
	}
	d := tx.Dynamic
	if d.MaxCollections != 12 || d.SampleSize != 1000 || d.AutoCreateMinConfidence != 0.85 {
		t.Fatalf("dynamic defaults: %+v", d)
	}
	if cfg.Automation.Enabled || cfg.Scheduler.Enabled {
		t.Fatal("automation and scheduler must default off")
	}
	if cfg.Roles.Fallback != nil {
		t.Fatal("fallback should be nil")
	}
}

func TestLoadDynamicTaxonomyIsDraft(t *testing.T) {
	y := strings.Replace(baseYAML, "mode: fixed", "mode: dynamic", 1)
	cfg, err := load(t, y)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Taxonomies["default"].Status; got != "draft" {
		t.Fatalf("status = %q", got)
	}
}

func TestLoadParsesDurations(t *testing.T) {
	y := "scheduler: {enabled: true, sync_every: 30m}\n" + baseYAML
	cfg, err := load(t, y)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Scheduler.SyncEvery != 30*time.Minute {
		t.Fatalf("sync_every = %v", cfg.Scheduler.SyncEvery)
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name, from, to, want string
	}{
		{"unknown classifier role", "roles: {classifier: llm1", "roles: {classifier: nope", `unknown classifier "nope"`},
		{"unknown fallback", "fallback: null", "fallback: ghost", `unknown classifier "ghost"`},
		{"unknown provider", "provider: raindrop", "provider: other", `processing.provider "other"`},
		{"zero concurrency", "taxonomy: default}", "taxonomy: default, concurrency: 0}", "concurrency must be at least 1"},
		{"min_tags > max_tags", "mode: fixed", "mode: fixed\n    rules: {min_tags: 6, max_tags: 5}", "min_tags (6) must not exceed max_tags (5)"},
		{"confidence out of range", "mode: fixed", "mode: fixed\n    rules: {min_confidence: 1.5}", "min_confidence must be between 0 and 1"},
		{"fixed with only review", "      - {key: backend, name: Backend}\n", "", "at least one collection that is not role: review"},
		{"two review collections", "      - {key: backend, name: Backend}\n", "      - {key: r2, name: R2, role: review}\n", "found 2"},
		{"duplicate keys", "      - {key: backend, name: Backend}\n", "      - {key: backend, name: A}\n      - {key: backend, name: B}\n", `duplicate collection key "backend"`},
		{"no review collection", "      - {key: needs_review, name: Needs Review, role: review}\n", "", "found none"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if !strings.Contains(baseYAML, tc.from) {
				t.Fatalf("fixture missing %q", tc.from)
			}
			y := strings.Replace(baseYAML, tc.from, tc.to, 1)
			_, err := load(t, y)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestStringRedactsSecrets(t *testing.T) {
	cfg, err := load(t, baseYAML)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"%+v", "%v", "%#v", "%s"} {
		out := fmt.Sprintf(f, cfg)
		for _, s := range goodEnv {
			if strings.Contains(out, s) {
				t.Fatalf("format %s leaked %q", f, s)
			}
		}
	}
}

func TestValidateDynamic(t *testing.T) {
	head := baseYAML[:strings.Index(baseYAML, "taxonomies:")]
	tx := func(cols string) string { return head + "taxonomies:\n  default:\n    mode: dynamic\n" + cols }

	cfg, err := load(t, tx("    collections: []\n"))
	if err != nil {
		t.Fatalf("empty dynamic taxonomy should load: %v", err)
	}
	if got := cfg.Taxonomies["default"].Status; got != "draft" {
		t.Fatalf("status = %q", got)
	}
	if _, err := load(t, tx("")); err != nil {
		t.Fatalf("omitted collections should load: %v", err)
	}
	if _, err := load(t, tx("    collections:\n      - {key: a, name: A}\n")); err == nil || !strings.Contains(err.Error(), "found none") {
		t.Fatalf("err = %v, want review-missing", err)
	}
	two := "    collections:\n      - {key: a, name: A, role: review}\n      - {key: b, name: B, role: review}\n"
	if _, err := load(t, tx(two)); err == nil || !strings.Contains(err.Error(), "found 2") {
		t.Fatalf("err = %v, want two-reviews", err)
	}
}

func TestProviderFlags(t *testing.T) {
	cfg, err := load(t, baseYAML)
	if err != nil {
		t.Fatal(err)
	}
	if p := cfg.Providers["raindrop"]; p.IncrementalSince || p.TagListing {
		t.Fatalf("flags should default to false: %+v", p)
	}
	y := strings.Replace(baseYAML, "{type: raindrop,", "{type: raindrop, incremental_since: true, tag_listing: true,", 1)
	if cfg, err = load(t, y); err != nil {
		t.Fatal(err)
	}
	if p := cfg.Providers["raindrop"]; !p.IncrementalSince || !p.TagListing {
		t.Fatalf("flags not read: %+v", p)
	}
}

// YAML 1.1 reads a bare off as a boolean; here the setting must stay the word.
func TestContentFetchOffIsAString(t *testing.T) {
	y := strings.Replace(baseYAML, "taxonomy: default}", "taxonomy: default, content_fetch: off}", 1)
	cfg, err := load(t, y)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Processing.ContentFetch != "off" {
		t.Fatalf("content_fetch = %q", cfg.Processing.ContentFetch)
	}
}
