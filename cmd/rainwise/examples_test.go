package main

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wikiabhi/rainwise/internal/config"
	"github.com/wikiabhi/rainwise/internal/domain"
)

// exampleEnv gives every variable the shipped configs reference a fake value.
func exampleEnv(string) func(string) string {
	return func(k string) string { return "fake-" + strings.ToLower(k) }
}

func loadExample(t *testing.T, rel string) *config.Config {
	t.Helper()
	cfg, err := config.Load(filepath.Join("..", "..", rel), exampleEnv(rel))
	if err != nil {
		t.Fatalf("%s: %v", rel, err)
	}
	return cfg
}

func TestExampleConfigsValid(t *testing.T) {
	for _, rel := range []string{"examples/config.yaml", "deploy/docker-compose/config.yaml", "deploy/cloudflare/config.yaml"} {
		t.Run(rel, func(t *testing.T) {
			cfg := loadExample(t, rel)
			// A rejected config leaves no database, and a good one must not touch /data.
			cfg.State.SQLitePath = filepath.Join(t.TempDir(), "r.db")
			a, err := Build(cfg, BuildDeps{})
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			shutdown(t, a)
		})
	}
}

func TestExampleFixedNineCollections(t *testing.T) {
	cfg := loadExample(t, "examples/config.yaml")
	if cfg.Automation.Enabled {
		t.Error("automation.enabled must be false")
	}
	tax, ok := cfg.Taxonomies[cfg.Processing.Taxonomy]
	if !ok {
		t.Fatalf("taxonomy %q missing", cfg.Processing.Taxonomy)
	}
	if tax.Mode != domain.ModeFixed {
		t.Errorf("mode = %q", tax.Mode)
	}
	want := []string{
		"AI Engineering & Agents", "System Design & Architecture", "Backend & Software Engineering",
		"Interview Preparation", "Career & Opportunities", "Developer Tools & Open Source",
		"Learning & Reading Lists", "Personal & Life", "Needs Review",
	}
	if len(tax.Collections) != len(want) {
		t.Fatalf("got %d collections, want %d", len(tax.Collections), len(want))
	}
	for i, c := range tax.Collections {
		if c.Name != want[i] {
			t.Errorf("collection %d = %q, want %q", i, c.Name, want[i])
		}
		if (c.Name == "Needs Review") != (c.Role == "review") {
			t.Errorf("%q has role %q", c.Name, c.Role)
		}
	}
	if tax.Rules.MinTags != 2 || tax.Rules.MaxTags != 5 {
		t.Errorf("tags = %d..%d", tax.Rules.MinTags, tax.Rules.MaxTags)
	}
}

func TestComposeDefaults(t *testing.T) {
	cfg := loadExample(t, "deploy/docker-compose/config.yaml")
	if cfg.Automation.Enabled || cfg.Scheduler.Enabled {
		t.Error("compose config must start with automation and scheduler off")
	}
	if cfg.State.SQLitePath != "/data/rainwise.db" {
		t.Errorf("sqlite_path = %q", cfg.State.SQLitePath)
	}
}

func TestCloudflareDefaults(t *testing.T) {
	cfg := loadExample(t, "deploy/cloudflare/config.yaml")
	if cfg.Scheduler.Enabled || cfg.Automation.Enabled {
		t.Error("scheduler and automation must be off")
	}
	if cfg.State.DurableWait != 30*time.Second {
		t.Errorf("durable_wait = %v", cfg.State.DurableWait)
	}
}

// TestNoAuthorCollectionsInCode keeps the example taxonomy out of the program.
func TestNoAuthorCollectionsInCode(t *testing.T) {
	names := []string{
		"AI Engineering", "System Design & Architecture", "Backend & Software Engineering",
		"Interview Preparation", "Career & Opportunities", "Developer Tools & Open Source",
		"Learning & Reading Lists", "Personal & Life",
	}
	root := filepath.Join("..", "..")
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == ".git" || d.Name() == ".superpowers") {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		for _, n := range names {
			if strings.Contains(string(b), n) {
				t.Errorf("%s contains %q", p, n)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
