package taxonomy

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

const roleReview = "review"

func nameKey(name string) string { return strings.ToLower(strings.TrimSpace(name)) }

// Resolve maps the taxonomy's collection keys to provider collections, matching
// root collections by name, ignoring case (§8.2). Among root collections sharing
// a name, the lowest ID in sorted order wins. With provision, missing collections
// are created and passed to onCreate, which may be nil; otherwise their names are
// returned. A nil manager finds nothing and can create nothing.
func Resolve(ctx context.Context, m ports.CollectionManager, t domain.Taxonomy, provision bool, onCreate func(domain.ID)) (map[string]domain.ID, []string, error) {
	roots := map[string]domain.ID{}
	if m != nil {
		var err error
		if roots, err = rootsByName(ctx, m); err != nil {
			return nil, nil, err
		}
	} else {
		provision = false
	}
	resolved := make(map[string]domain.ID, len(t.Collections))
	var missing []string
	for _, c := range t.Collections {
		k := nameKey(c.Name)
		id, ok := roots[k]
		switch {
		case ok:
			resolved[c.Key] = id
		case provision:
			made, err := m.CreateCollection(ctx, domain.Collection{Name: c.Name, Description: c.Description})
			if err != nil {
				return nil, nil, fmt.Errorf("create collection %q: %w", c.Name, err)
			}
			roots[k] = made.ID
			resolved[c.Key] = made.ID
			if onCreate != nil {
				onCreate(made.ID)
			}
		default:
			missing = append(missing, c.Name)
		}
	}
	return resolved, missing, nil
}

func rootsByName(ctx context.Context, m ports.CollectionManager) (map[string]domain.ID, error) {
	cols, err := m.ListCollections(ctx)
	if err != nil {
		return nil, fmt.Errorf("list collections: %w", err)
	}
	byName := map[string][]domain.ID{}
	for _, c := range cols {
		if c.Parent == nil {
			k := nameKey(c.Name)
			byName[k] = append(byName[k], c.ID)
		}
	}
	roots := make(map[string]domain.ID, len(byName))
	for k, ids := range byName {
		slices.SortFunc(ids, func(a, b domain.ID) int { return strings.Compare(a.String(), b.String()) })
		roots[k] = ids[0]
		if len(ids) > 1 {
			slog.Warn("several root collections share a name; using the lowest id", "name", k, "id", ids[0].String(), "count", len(ids))
		}
	}
	return roots, nil
}
