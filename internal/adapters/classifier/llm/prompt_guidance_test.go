package llm

import (
	"strings"
	"testing"

	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

func guidanceRequest() ports.ClassifyRequest {
	r := ports.ClassifyRequest{WantCollection: true, TagsWanted: 5, TagVocab: []string{"postgres"}}
	r.Taxonomy.Rules = domain.TaxonomyRules{MinTags: 2, MaxTags: 5, AllowNewTags: true}
	return r
}

func TestClassifySystemGuidance(t *testing.T) {
	got := classifySystem(guidanceRequest())
	for _, want := range []string{
		"what the bookmark is for",
		"specific",
		"Never use a collection's name or key as a tag",
		"at least 2 tags",
		"do not pad",
		"too little to tell",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("prompt lacks %q:\n%s", want, got)
		}
	}
}

func TestClassifySystemMinimumStaysWithinRoom(t *testing.T) {
	r := guidanceRequest()
	r.TagsWanted = 1
	if got := classifySystem(r); strings.Contains(got, "at least 2 tags") {
		t.Errorf("asked for more tags than there is room for:\n%s", got)
	}
}

func TestClassifySystemVocabularyOnlyStillForbidsNewTags(t *testing.T) {
	r := guidanceRequest()
	r.Taxonomy.Rules.AllowNewTags = false
	if got := classifySystem(r); !strings.Contains(got, "Use only tags from the vocabulary") {
		t.Errorf("prompt lacks the vocabulary-only rule:\n%s", got)
	}
}

func TestFixedTaxonomyCannotProposeCollections(t *testing.T) {
	r := guidanceRequest()
	r.Taxonomy.Mode = domain.ModeFixed
	r.Taxonomy.Collections = []domain.TaxonomyCollection{{Key: "dev", Name: "Dev"}}
	if got := classifySystem(r); strings.Contains(got, "proposed_name") || !strings.Contains(got, "closest") {
		t.Errorf("prompt offers a new collection:\n%s", got)
	}
	schema := decisionSchema(r)["properties"].(map[string]any)
	if _, ok := schema["proposed_name"]; ok {
		t.Error("schema offers proposed_name")
	}
	if enum := schema["collection"].(map[string]any)["enum"].([]any); len(enum) != 1 || enum[0] != "dev" {
		t.Errorf("enum = %v", enum)
	}
}

func TestDynamicTaxonomyCanProposeCollections(t *testing.T) {
	r := guidanceRequest()
	r.Taxonomy.Mode = domain.ModeDynamic
	if !strings.Contains(classifySystem(r), "proposed_name") {
		t.Error("prompt lost proposed_name")
	}
	if _, ok := decisionSchema(r)["properties"].(map[string]any)["proposed_name"]; !ok {
		t.Error("schema lost proposed_name")
	}
}

func TestClassifySystemAsksForTagsBeyondExistingOnes(t *testing.T) {
	r := guidanceRequest()
	r.TagVocab = []string{"postgres", "kafka"}
	r.Subject.Bookmark.Tags = []string{"Postgres"}
	got := classifySystem(r)
	if !strings.Contains(got, "already has these tags: Postgres") || !strings.Contains(got, "Do not repeat them") {
		t.Errorf("prompt doesn't rule out existing tags:\n%s", got)
	}
	if strings.Contains(got, "vocabulary: postgres") || !strings.Contains(got, "vocabulary: kafka") {
		t.Errorf("vocabulary still offers an existing tag:\n%s", got)
	}
	if !strings.Contains(got, "at least 1 tag,") {
		t.Errorf("wrong minimum:\n%s", got)
	}
}
