package taxonomy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

// Service is the front door for taxonomies (§8.1).
type Service struct {
	store ports.TaxonomyStore
}

func New(store ports.TaxonomyStore) *Service { return &Service{store: store} }

func (s *Service) Get(ctx context.Context, id string) (domain.Taxonomy, error) {
	return s.store.GetTaxonomy(ctx, id)
}

func (s *Service) List(ctx context.Context) ([]domain.Taxonomy, error) {
	return s.store.ListTaxonomies(ctx)
}

// Put validates and stores a taxonomy under id. ifVersion, if set, must match the
// stored version, or ErrConflict results. Proposals are kept as stored: the
// pipeline records them, so a caller's copy may be behind or missing them.
func (s *Service) Put(ctx context.Context, id string, t domain.Taxonomy, ifVersion *int) (domain.Taxonomy, error) {
	t.ID = id
	if err := validate(t); err != nil {
		return domain.Taxonomy{}, err
	}
	cur, err := s.store.GetTaxonomy(ctx, id)
	if err != nil && !errors.Is(err, domain.ErrNotFound) {
		return domain.Taxonomy{}, err
	}
	t.Proposals = cur.Proposals
	return s.store.PutTaxonomy(ctx, t, ifVersion)
}

func validate(t domain.Taxonomy) error {
	if t.Rules.MinTags > t.Rules.MaxTags {
		return fmt.Errorf("min_tags %d exceeds max_tags %d: %w", t.Rules.MinTags, t.Rules.MaxTags, domain.ErrValidation)
	}
	if t.Status != domain.TaxonomyActive {
		return nil
	}
	reviews := 0
	for _, c := range t.Collections {
		if c.Role == roleReview {
			reviews++
		}
	}
	if len(t.Collections) == reviews {
		return fmt.Errorf("an active taxonomy needs collections to choose from: %w", domain.ErrValidation)
	}
	if reviews != 1 {
		return fmt.Errorf("an active taxonomy needs exactly one review collection, found %d: %w", reviews, domain.ErrValidation)
	}
	return nil
}

// Seed stores each taxonomy not yet in the database. The database wins after
// that, so a difference only logs a warning (§8.1). A nil log means the default.
func (s *Service) Seed(ctx context.Context, taxonomies []domain.Taxonomy, log *slog.Logger) error {
	if log == nil {
		log = slog.Default()
	}
	for _, t := range taxonomies {
		seeded, err := s.store.SeedTaxonomy(ctx, t)
		if err != nil {
			return fmt.Errorf("seed taxonomy %q: %w", t.ID, err)
		}
		if seeded {
			continue
		}
		cur, err := s.store.GetTaxonomy(ctx, t.ID)
		if err != nil {
			return fmt.Errorf("seed taxonomy %q: %w", t.ID, err)
		}
		same, err := sameDefinition(cur, t)
		if err != nil {
			return fmt.Errorf("seed taxonomy %q: %w", t.ID, err)
		}
		if !same {
			log.Warn("taxonomy differs from configuration; keeping the stored one", "taxonomy", t.ID, "version", cur.Version)
		}
	}
	return nil
}

// sameDefinition compares what configuration can set, leaving out what only
// changes at runtime.
func sameDefinition(a, b domain.Taxonomy) (bool, error) {
	a.Version, b.Version = 0, 0
	a.Proposals, b.Proposals = nil, nil
	x, err := json.Marshal(a)
	if err != nil {
		return false, err
	}
	y, err := json.Marshal(b)
	if err != nil {
		return false, err
	}
	return string(x) == string(y), nil
}
