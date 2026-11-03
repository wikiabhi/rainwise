package taxonomy

import (
	"cmp"
	"context"
	"encoding/json"
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

const vocabKeyPrefix = "tag_vocab:"

// VocabKey is the setting that holds a provider's persisted tag vocabulary, for
// providers that can't list their tags.
func VocabKey(provider string) string { return vocabKeyPrefix + provider }

// LoadVocab reads the persisted vocabulary, reporting whether there is one.
func LoadVocab(ctx context.Context, s ports.StateStore, provider string) ([]string, bool, error) {
	raw, ok, err := s.GetSetting(ctx, VocabKey(provider))
	if err != nil || !ok {
		return nil, false, err
	}
	var terms []string
	if err := json.Unmarshal([]byte(raw), &terms); err != nil {
		return nil, false, fmt.Errorf("parse %s: %w", VocabKey(provider), err)
	}
	return terms, true, nil
}

// SaveVocab persists a vocabulary as a JSON list.
func SaveVocab(ctx context.Context, s ports.StateStore, provider string, terms []string) error {
	if terms == nil {
		terms = []string{}
	}
	raw, err := json.Marshal(terms)
	if err != nil {
		return err
	}
	return s.PutSetting(ctx, VocabKey(provider), string(raw))
}

// AddVocab appends new terms the vocabulary lacks. When the result exceeds max,
// the tail of the old terms (the least used, as they're kept by use) makes room.
func AddVocab(terms, added []string, max int) []string {
	have := make(map[string]bool, len(terms))
	for _, t := range terms {
		have[ComparisonKey(t)] = true
	}
	var fresh []string
	for _, t := range added {
		if k := ComparisonKey(t); !have[k] {
			have[k] = true
			fresh = append(fresh, t)
		}
	}
	if len(fresh) == 0 {
		return terms
	}
	if max > 0 && len(fresh) > max {
		fresh = fresh[len(fresh)-max:]
	}
	if max > 0 && len(terms)+len(fresh) > max {
		terms = terms[:max-len(fresh)]
	}
	return append(slices.Clone(terms), fresh...)
}

// ScanLibrary lists every bookmark, page by page, calling visit for each. stop,
// if set, is checked before each page.
func ScanLibrary(ctx context.Context, p ports.Provider, batch int, stop func() error, visit func(domain.Bookmark)) error {
	for token := ""; ; {
		if stop != nil {
			if err := stop(); err != nil {
				return err
			}
		}
		page, err := p.ListBookmarks(ctx, ports.ListQuery{PageToken: token, Limit: batch})
		if err != nil {
			return fmt.Errorf("scan bookmarks: %w", err)
		}
		for _, b := range page.Items {
			visit(b)
		}
		if page.NextToken == "" {
			return nil
		}
		token = page.NextToken
	}
}
