// Package pipeline holds the decision validator and input hash.
package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/wikiabhi/rainwise/internal/domain"
)

// InputHash hashes what the classifier sees: URL, title, description, excerpt
// and highlight text (order-independent). Tags and collection are excluded.
func InputHash(b domain.Bookmark, highlights []domain.Highlight) string {
	texts := make([]string, len(highlights))
	for i, h := range highlights {
		texts[i] = h.Text
	}
	sort.Strings(texts)
	h := sha256.New()
	write := func(s string) { fmt.Fprintf(h, "%d:%s|", len(s), s) }
	for _, s := range []string{b.URL, b.Title, b.Description, b.Excerpt} {
		write(s)
	}
	write(fmt.Sprint(len(texts)))
	for _, s := range texts {
		write(s)
	}
	return hex.EncodeToString(h.Sum(nil))
}
