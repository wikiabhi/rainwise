package pipeline

import (
	"testing"

	"github.com/wikiabhi/rainwise/internal/domain"
)

func TestInputHash(t *testing.T) {
	b := domain.Bookmark{URL: "https://x", Title: "T", Description: "D", Excerpt: "E"}
	hl := []domain.Highlight{{Text: "a"}, {Text: "b"}}
	base := InputHash(b, hl)

	b2 := b
	b2.Tags = []string{"x"}
	b2.Collection = domain.ID{Provider: "p", Local: "1"}
	if InputHash(b2, hl) != base {
		t.Error("tags/collection changed hash")
	}
	if InputHash(b, []domain.Highlight{{Text: "b"}, {Text: "a"}}) != base {
		t.Error("highlight order changed hash")
	}
	b3 := b
	b3.Title = "T2"
	if InputHash(b3, hl) == base {
		t.Error("title change did not alter hash")
	}
	if InputHash(b, []domain.Highlight{{Text: "a"}, {Text: "c"}}) == base {
		t.Error("highlight change did not alter hash")
	}
}
