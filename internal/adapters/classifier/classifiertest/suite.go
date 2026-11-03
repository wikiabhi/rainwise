// Package classifiertest holds the conformance suite every ports.Classifier must pass.
package classifiertest

import (
	"context"
	"testing"

	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

const (
	keyAlpha = "alpha"
	keyBeta  = "beta"
)

// Stub programs whatever backs the classifier so its next Classify call
// answers with want. A real adapter's test maps this onto its stubbed backend.
type Stub func(want domain.Decision)

func request(wantCollection bool, tagsWanted int) ports.ClassifyRequest {
	return ports.ClassifyRequest{
		Subject: ports.Subject{Bookmark: domain.Bookmark{
			ID:    domain.ID{Provider: "test", Local: "1"},
			URL:   "https://example.test/post",
			Title: "A post about databases",
		}},
		Taxonomy: domain.TaxonomySnapshot{
			ID:      "test",
			Version: 1,
			Collections: []domain.TaxonomyCollection{
				{Key: keyAlpha, Name: "Alpha"},
				{Key: keyBeta, Name: "Beta"},
			},
			Rules: domain.TaxonomyRules{MinTags: 1, MaxTags: 5, MinConfidence: 0.5, AllowNewTags: true},
		},
		TagVocab:       []string{"databases", "storage"},
		TagsWanted:     tagsWanted,
		WantCollection: wantCollection,
	}
}

// Run checks that c's declared capabilities match its behavior.
func Run(t *testing.T, c ports.Classifier, stub Stub) {
	ctx := context.Background()
	caps := c.Capabilities()
	base := domain.Decision{Collection: domain.CollectionChoice{Key: keyAlpha}, Confidence: 0.9}

	classify := func(t *testing.T, want domain.Decision, r ports.ClassifyRequest) domain.Decision {
		t.Helper()
		stub(want)
		d, err := c.Classify(ctx, r)
		if err != nil {
			t.Fatalf("Classify: %v", err)
		}
		return d
	}

	t.Run("Identity", func(t *testing.T) {
		if c.Name() == "" {
			t.Error("Name is empty")
		}
		if caps.MaxChoices < 0 {
			t.Errorf("MaxChoices = %d", caps.MaxChoices)
		}
		if d := classify(t, base, request(true, 0)); d.Model.Classifier == "" {
			t.Error("decision doesn't name its classifier")
		}
	})

	t.Run("ConfidenceInRange", func(t *testing.T) {
		d := classify(t, base, request(true, 0))
		if d.Confidence < 0 || d.Confidence > 1 {
			t.Errorf("confidence = %v", d.Confidence)
		}
	})

	t.Run("FixedChoice", func(t *testing.T) {
		if !caps.FixedChoice {
			t.Skip("not declared")
		}
		d := classify(t, base, request(true, 0))
		if d.Collection.Key != keyAlpha {
			t.Errorf("key = %q, want %q", d.Collection.Key, keyAlpha)
		}
	})

	t.Run("ProposeCollections", func(t *testing.T) {
		want := base
		want.Collection = domain.CollectionChoice{ProposedName: "Gamma"}
		got := classify(t, want, request(true, 0)).Collection.ProposedName
		if caps.ProposeCollections && got != "Gamma" {
			t.Errorf("declared, but proposed name = %q", got)
		}
		if !caps.ProposeCollections && got != "" {
			t.Errorf("not declared, but proposed %q", got)
		}
	})

	t.Run("ProposeTags", func(t *testing.T) {
		// The backend offers two vocabulary tags and one invented tag.
		want := base
		want.Tags = []domain.TagSuggestion{{Name: "databases"}, {Name: "storage"}, {Name: "invented-tag"}}
		got := classify(t, want, request(true, 3)).Tags
		if len(got) > 3 {
			t.Errorf("TagsWanted 3, got %v", got)
		}
		inVocab := func(n string) bool { return n == "databases" || n == "storage" }
		var novel int
		for _, tg := range got {
			if !inVocab(tg.Name) {
				novel++
			}
		}
		if caps.ProposeTags && len(got) == 0 {
			t.Error("declared, but no tags returned")
		}
		// Not declared: tags chosen from the vocabulary are fine; inventing is not.
		if !caps.ProposeTags && novel > 0 {
			t.Errorf("not declared, but returned tags outside the vocabulary: %v", got)
		}
	})

	t.Run("NoTagsWhenNoneWanted", func(t *testing.T) {
		want := base
		want.Tags = []domain.TagSuggestion{{Name: "databases"}}
		if got := classify(t, want, request(true, 0)).Tags; len(got) != 0 {
			t.Errorf("TagsWanted 0, got %v", got)
		}
	})
}
