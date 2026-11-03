package config

import (
	"errors"
	"fmt"
	"sort"

	"github.com/wikiabhi/rainwise/internal/domain"
)

const roleReview = "review"

// Validate checks cross references and value ranges, returning every problem found.
func (c *Config) Validate() error {
	var errs []error
	add := func(format string, a ...any) { errs = append(errs, fmt.Errorf("config: "+format, a...)) }

	if _, ok := c.Providers[c.Processing.Provider]; !ok {
		add("processing.provider %q is not defined under providers", c.Processing.Provider)
	}
	if _, ok := c.Taxonomies[c.Processing.Taxonomy]; !ok {
		add("processing.taxonomy %q is not defined under taxonomies", c.Processing.Taxonomy)
	}
	if c.Processing.Concurrency < 1 {
		add("processing.concurrency must be at least 1, got %d", c.Processing.Concurrency)
	}
	if c.State.SnapshotRetention < 1 {
		add("state.snapshot_retention must be at least 1, got %d", c.State.SnapshotRetention)
	}
	if c.Scheduler.Enabled && c.Scheduler.SyncEvery <= 0 {
		add("scheduler.sync_every must be positive when the scheduler is enabled")
	}

	checkRole := func(name, val string) {
		if _, ok := c.Classifiers[val]; !ok {
			add("roles.%s names unknown classifier %q", name, val)
		}
	}
	checkRole("classifier", c.Roles.Classifier)
	checkRole("proposer", c.Roles.Proposer)
	if c.Roles.Fallback != nil {
		checkRole("fallback", *c.Roles.Fallback)
	}

	ids := make([]string, 0, len(c.Taxonomies))
	for id := range c.Taxonomies {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		errs = append(errs, validateTaxonomy(id, c.Taxonomies[id])...)
	}
	return errors.Join(errs...)
}

func validateTaxonomy(id string, t domain.Taxonomy) []error {
	var errs []error
	add := func(format string, a ...any) {
		errs = append(errs, fmt.Errorf("config: taxonomies.%s: "+format, append([]any{id}, a...)...))
	}
	if t.Mode != domain.ModeFixed && t.Mode != domain.ModeDynamic {
		add("mode must be %q or %q, got %q", domain.ModeFixed, domain.ModeDynamic, t.Mode)
	}
	r := t.Rules
	if r.MinTags < 0 || r.MaxTags < 0 {
		add("min_tags and max_tags must not be negative")
	}
	if r.MinTags > r.MaxTags {
		add("min_tags (%d) must not exceed max_tags (%d)", r.MinTags, r.MaxTags)
	}
	if r.MinConfidence < 0 || r.MinConfidence > 1 {
		add("min_confidence must be between 0 and 1, got %v", r.MinConfidence)
	}
	if t.Dynamic.AutoCreateMinConfidence < 0 || t.Dynamic.AutoCreateMinConfidence > 1 {
		add("dynamic.auto_create_min_confidence must be between 0 and 1, got %v", t.Dynamic.AutoCreateMinConfidence)
	}

	seen := map[string]bool{}
	reviews, others := 0, 0
	for _, col := range t.Collections {
		if col.Key == "" {
			add("collection %q has no key", col.Name)
		} else if seen[col.Key] {
			add("duplicate collection key %q", col.Key)
		}
		seen[col.Key] = true
		if col.Role == roleReview {
			reviews++
		} else {
			others++
		}
	}
	// A dynamic taxonomy starts with no collections (§8.3); once it lists
	// any, the review rule applies.
	if t.Mode != domain.ModeDynamic || len(t.Collections) > 0 {
		switch {
		case reviews == 0:
			add("needs exactly one collection with role: review, found none")
		case reviews > 1:
			add("needs exactly one collection with role: review, found %d", reviews)
		}
	}
	if t.Mode == domain.ModeFixed && others == 0 {
		add("fixed taxonomy needs at least one collection that is not role: review")
	}
	return errs
}
