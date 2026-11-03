package taxonomy

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

// ListedTags asks the provider for tag counts, reporting whether it could.
func ListedTags(ctx context.Context, p ports.Provider) (map[string]int, bool, error) {
	lister, ok := p.(ports.TagLister)
	if !ok || !p.Capabilities().TagListing {
		return nil, false, nil
	}
	listed, err := lister.ListTags(ctx)
	if errors.Is(err, domain.ErrCapabilityUnsupported) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("list tags: %w", err)
	}
	counts := make(map[string]int, len(listed))
	for _, t := range listed {
		counts[t.Name] = t.Count
	}
	return counts, true, nil
}

// TopTerms returns the n most-used tags, ties broken by name.
func TopTerms(counts map[string]int, n int) []string {
	terms := make([]string, 0, len(counts))
	for name := range counts {
		terms = append(terms, name)
	}
	slices.SortFunc(terms, func(a, b string) int {
		return cmp.Or(cmp.Compare(counts[b], counts[a]), strings.Compare(a, b))
	})
	if len(terms) > n {
		terms = terms[:n]
	}
	return terms
}
