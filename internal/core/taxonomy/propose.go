package taxonomy

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

const (
	reviewKey         = "needs_review"
	reviewName        = "Needs Review"
	reviewDescription = "Reserved for low-confidence items."
)

// Propose asks p for collections and returns t with them in place of its
// non-review collections, plus a review collection if it has none. MaxCollections
// covers the review collection too, as the validator's cap does. The caller
// stores the result.
func Propose(ctx context.Context, p ports.TaxonomyProposer, samples []ports.Subject, existing []string, t domain.Taxonomy) (domain.Taxonomy, error) {
	slots := t.Dynamic.MaxCollections
	if slots > 0 {
		slots--
	}
	req := ports.ProposeRequest{Samples: samples, ExistingCollections: existing, MaxCollections: slots, Extra: t.Rules.PromptExtra}
	out, err := p.ProposeTaxonomy(ctx, req)
	if errors.Is(err, domain.ErrInvalidOutput) && ctx.Err() == nil {
		out, err = p.ProposeTaxonomy(ctx, req)
	}
	if err != nil {
		return domain.Taxonomy{}, fmt.Errorf("propose taxonomy %q: %w", t.ID, err)
	}

	var reviews []domain.TaxonomyCollection
	for _, c := range t.Collections {
		if c.Role == roleReview {
			reviews = append(reviews, c)
		}
	}
	if len(reviews) == 0 {
		reviews = []domain.TaxonomyCollection{{Key: reviewKey, Name: reviewName, Description: reviewDescription, Role: roleReview}}
	}
	names, keys := map[string]bool{}, map[string]bool{}
	for _, c := range reviews {
		names[nameKey(c.Name)] = true
		keys[c.Key] = true
	}
	var cols []domain.TaxonomyCollection
	for _, c := range out.Collections {
		name := strings.TrimSpace(c.Name)
		if name == "" || names[nameKey(name)] || (t.Dynamic.MaxCollections > 0 && len(cols) >= slots) {
			continue
		}
		names[nameKey(name)] = true
		key := uniqueKey(name, keys)
		cols = append(cols, domain.TaxonomyCollection{Key: key, Name: name, Description: c.Description})
	}
	t.Collections = append(cols, reviews...)
	return t, nil
}

// uniqueKey makes a key from a name and notes it in taken.
func uniqueKey(name string, taken map[string]bool) string {
	var b strings.Builder
	gap := false
	for _, r := range strings.ToLower(name) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			if gap && b.Len() > 0 {
				b.WriteByte('_')
			}
			b.WriteRune(r)
			gap = false
		} else {
			gap = true
		}
	}
	base := b.String()
	if base == "" {
		base = "collection"
	}
	key := base
	for i := 2; taken[key]; i++ {
		key = fmt.Sprintf("%s_%d", base, i)
	}
	taken[key] = true
	return key
}
