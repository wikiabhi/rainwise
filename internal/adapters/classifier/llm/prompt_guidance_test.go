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
