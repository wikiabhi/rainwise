package jobs

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/wikiabhi/rainwise/internal/core/pipeline"
	"github.com/wikiabhi/rainwise/internal/core/taxonomy"
	"github.com/wikiabhi/rainwise/internal/domain"
)

// reclassifyJob is §9.5: a backfill of a required scope, bypassing the hash.
type reclassifyJob struct{ Deps }

func (h reclassifyJob) Run(ctx context.Context, job *domain.Job, rc RunContext) error {
	if job.Scope.IsEmpty() {
		return fmt.Errorf("reclassify needs a scope: %w", domain.ErrValidation)
	}
	return h.runBatch(ctx, job, rc, pipeline.TriggerReclassify)
}

// scopeMatcher tests bookmarks against a domain.Scope. Each kind that's set must
// match, and within a kind any entry will do. The zero value matches everything.
type scopeMatcher struct {
	ids, collections map[domain.ID]bool
	tags             map[string]bool
}

func (m scopeMatcher) matches(b domain.Bookmark) bool {
	if m.ids != nil && !m.ids[b.ID] {
		return false
	}
	if m.collections != nil && !m.collections[b.Collection] {
		return false
	}
	if m.tags == nil {
		return true
	}
	for _, t := range b.Tags {
		if m.tags[taxonomy.ComparisonKey(t)] {
			return true
		}
	}
	return false
}

// newScopeMatcher reads a scope. An ID is "provider:local" or just the local part.
// A collection is one of those, or a root collection's name, or a taxonomy key. A
// collection that can't be found is an error, since a typo would otherwise run a
// snapshot and a job over nothing.
func (d Deps) newScopeMatcher(ctx context.Context, s domain.Scope, t domain.Taxonomy) (scopeMatcher, error) {
	var m scopeMatcher
	if len(s.IDs) > 0 {
		m.ids = map[domain.ID]bool{}
		for _, id := range s.IDs {
			m.ids[d.qualify(id)] = true
		}
	}
	if len(s.Tags) > 0 {
		m.tags = map[string]bool{}
		for _, tag := range s.Tags {
			m.tags[taxonomy.ComparisonKey(tag)] = true
		}
	}
	if len(s.Collections) > 0 {
		cols, err := d.scopeCollections(ctx, s.Collections, t)
		if err != nil {
			return scopeMatcher{}, err
		}
		m.collections = cols
	}
	return m, nil
}

func (d Deps) qualify(s string) domain.ID {
	if id, err := domain.ParseID(s); err == nil && id.Provider == d.Provider.Name() {
		return id
	}
	return domain.ID{Provider: d.Provider.Name(), Local: s}
}

func (d Deps) scopeCollections(ctx context.Context, entries []string, t domain.Taxonomy) (map[domain.ID]bool, error) {
	var cols []domain.Collection
	if m := d.manager(); m != nil {
		var err error
		if cols, err = m.ListCollections(ctx); err != nil && !errors.Is(err, domain.ErrCapabilityUnsupported) {
			return nil, fmt.Errorf("list collections: %w", err)
		}
	}
	out := map[domain.ID]bool{}
	for _, entry := range entries {
		name := entry
		for _, c := range t.Collections {
			if c.Key == entry {
				name = c.Name
			}
		}
		found := false
		for _, c := range cols {
			if c.ID.String() == entry || c.ID.Local == entry || (c.Parent == nil && strings.EqualFold(c.Name, name)) {
				out[c.ID] = true
				found = true
			}
		}
		// An inbox isn't a listed collection, so a full ID stands on its own.
		if id, err := domain.ParseID(entry); !found && err == nil && id.Provider == d.Provider.Name() {
			out[id] = true
			found = true
		}
		if !found {
			return nil, fmt.Errorf("scope: unknown collection %q: %w", entry, domain.ErrValidation)
		}
	}
	return out, nil
}
