// Package config loads and validates Rainwise's YAML configuration.
package config

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/wikiabhi/rainwise/internal/domain"
)

// Secret is a string that never prints its value. Use Value for the real one.
type Secret string

const redacted = "[redacted]"

func (s Secret) Value() string                { return string(s) }
func (s Secret) String() string               { return redacted }
func (s Secret) GoString() string             { return `config.Secret("` + redacted + `")` }
func (s Secret) Format(f fmt.State, _ rune)   { _, _ = io.WriteString(f, redacted) }
func (s Secret) MarshalText() ([]byte, error) { return []byte(redacted), nil }

type Server struct {
	Addr     string `yaml:"addr"`
	APIToken Secret `yaml:"api_token"`
}

type State struct {
	SQLitePath        string        `yaml:"sqlite_path"`
	DurableWait       time.Duration `yaml:"durable_wait"`
	SnapshotRetention int           `yaml:"snapshot_retention"`
}

type Automation struct {
	Enabled bool `yaml:"enabled"`
}

type Scheduler struct {
	Enabled   bool          `yaml:"enabled"`
	SyncEvery time.Duration `yaml:"sync_every"`
}

type Processing struct {
	Provider             string `yaml:"provider"`
	Taxonomy             string `yaml:"taxonomy"`
	Concurrency          int    `yaml:"concurrency"`
	ContentFetch         string `yaml:"content_fetch"`
	ContentFetchMinChars int    `yaml:"content_fetch_min_chars"`
	OnMoveRetag          bool   `yaml:"on_move_retag"`
	TagVocabSize         int    `yaml:"tag_vocab_size"`
}

type ProviderConfig struct {
	Type              string `yaml:"type"`
	Token             Secret `yaml:"token"`
	WebhookSecret     Secret `yaml:"webhook_secret"`
	RequestsPerMinute int    `yaml:"requests_per_minute"`
}

type ClassifierConfig struct {
	Type             string         `yaml:"type"`
	BaseURL          string         `yaml:"base_url"`
	Model            string         `yaml:"model"`
	APIKey           Secret         `yaml:"api_key"`
	ExtraBody        map[string]any `yaml:"extra_body"`
	MaxTagCandidates int            `yaml:"max_tag_candidates"`
	TagThreshold     float64        `yaml:"tag_threshold"`
}

type Roles struct {
	Classifier string  `yaml:"classifier"`
	Proposer   string  `yaml:"proposer"`
	Fallback   *string `yaml:"fallback"`
}

type Config struct {
	Server      Server                      `yaml:"server"`
	State       State                       `yaml:"state"`
	Automation  Automation                  `yaml:"automation"`
	Scheduler   Scheduler                   `yaml:"scheduler"`
	Processing  Processing                  `yaml:"processing"`
	Providers   map[string]ProviderConfig   `yaml:"providers"`
	Classifiers map[string]ClassifierConfig `yaml:"classifiers"`
	Roles       Roles                       `yaml:"roles"`
	Taxonomies  map[string]domain.Taxonomy  `yaml:"-"`

	RawTaxonomies map[string]rawTaxonomy `yaml:"taxonomies"`
}

// rawTaxonomy mirrors the §8 YAML. Pointer fields carry defaults.
type rawTaxonomy struct {
	Mode        string                      `yaml:"mode"`
	Status      string                      `yaml:"status"`
	Collections []domain.TaxonomyCollection `yaml:"collections"`
	Rules       struct {
		MinTags       *int              `yaml:"min_tags"`
		MaxTags       *int              `yaml:"max_tags"`
		MinConfidence *float64          `yaml:"min_confidence"`
		AllowNewTags  bool              `yaml:"allow_new_tags"`
		TagAliases    map[string]string `yaml:"tag_aliases"`
		PromptExtra   string            `yaml:"prompt_extra"`
	} `yaml:"rules"`
	ProvisionMissing bool `yaml:"provision_missing_collections"`
	Dynamic          struct {
		MaxCollections          *int     `yaml:"max_collections"`
		SampleSize              *int     `yaml:"sample_size"`
		AutoCreate              bool     `yaml:"auto_create"`
		AutoCreateMinConfidence *float64 `yaml:"auto_create_min_confidence"`
	} `yaml:"dynamic"`
}

var envRef = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// Load reads path, expands ${ENV} references in values via getenv, applies
// defaults, and validates. Unknown keys and unset variables are errors.
func Load(path string, getenv func(string) string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	expanded, err := expandEnv(string(data), getenv)
	if err != nil {
		return nil, err
	}

	cfg := defaults()
	dec := yaml.NewDecoder(strings.NewReader(expanded))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil && err != io.EOF {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}
	cfg.buildTaxonomies()
	applyMapDefaults(cfg)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func defaults() *Config {
	return &Config{
		State: State{SnapshotRetention: 10},
		Processing: Processing{
			Concurrency:          4,
			ContentFetch:         "auto",
			ContentFetchMinChars: 80,
			TagVocabSize:         200,
		},
	}
}

// expandEnv replaces ${VAR} in the raw text, line by line. Comments (a line
// starting with # or the text after " #") are left alone. An unset or empty
// variable is an error naming the variable.
func expandEnv(text string, getenv func(string) string) (string, error) {
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		code, comment := line, ""
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		if j := strings.Index(line, " #"); j >= 0 {
			code, comment = line[:j], line[j:]
		}
		var missing string
		code = envRef.ReplaceAllStringFunc(code, func(m string) string {
			name := envRef.FindStringSubmatch(m)[1]
			v := getenv(name)
			if v == "" && missing == "" {
				missing = name
			}
			return v
		})
		if missing != "" {
			return "", fmt.Errorf("config: environment variable %s is not set (line %d)", missing, i+1)
		}
		lines[i] = code + comment
	}
	return strings.Join(lines, "\n"), nil
}

func applyMapDefaults(cfg *Config) {
	for k, p := range cfg.Providers {
		if p.RequestsPerMinute == 0 {
			p.RequestsPerMinute = 100
		}
		cfg.Providers[k] = p
	}
	for k, c := range cfg.Classifiers {
		if c.MaxTagCandidates == 0 {
			c.MaxTagCandidates = 100
		}
		if c.TagThreshold == 0 {
			c.TagThreshold = 0.7
		}
		cfg.Classifiers[k] = c
	}
}

func orInt(p *int, d int) int {
	if p == nil {
		return d
	}
	return *p
}

func orFloat(p *float64, d float64) float64 {
	if p == nil {
		return d
	}
	return *p
}

func (c *Config) buildTaxonomies() {
	c.Taxonomies = make(map[string]domain.Taxonomy, len(c.RawTaxonomies))
	for id, r := range c.RawTaxonomies {
		status := r.Status
		if status == "" {
			status = domain.TaxonomyDraft
			if r.Mode == domain.ModeFixed {
				status = domain.TaxonomyActive
			}
		}
		c.Taxonomies[id] = domain.Taxonomy{
			ID:          id,
			Mode:        r.Mode,
			Status:      status,
			Collections: r.Collections,
			Rules: domain.TaxonomyRules{
				MinTags:       orInt(r.Rules.MinTags, 2),
				MaxTags:       orInt(r.Rules.MaxTags, 5),
				MinConfidence: orFloat(r.Rules.MinConfidence, 0.6),
				AllowNewTags:  r.Rules.AllowNewTags,
				TagAliases:    r.Rules.TagAliases,
				PromptExtra:   r.Rules.PromptExtra,
			},
			ProvisionMissing: r.ProvisionMissing,
			Dynamic: domain.DynamicSettings{
				MaxCollections:          orInt(r.Dynamic.MaxCollections, 12),
				SampleSize:              orInt(r.Dynamic.SampleSize, 1000),
				AutoCreate:              r.Dynamic.AutoCreate,
				AutoCreateMinConfidence: orFloat(r.Dynamic.AutoCreateMinConfidence, 0.85),
			},
		}
	}
	c.RawTaxonomies = nil
}
