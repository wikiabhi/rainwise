package taxonomy

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	pfake "github.com/wikiabhi/rainwise/internal/adapters/provider/fake"
	"github.com/wikiabhi/rainwise/internal/adapters/store/memory"
	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

func id(local string) domain.ID { return domain.ID{Provider: "fake", Local: local} }

func taxWith(cols ...domain.TaxonomyCollection) domain.Taxonomy {
	return domain.Taxonomy{ID: "tx", Mode: domain.ModeFixed, Status: domain.TaxonomyActive, Collections: cols,
		Rules: domain.TaxonomyRules{MinTags: 1, MaxTags: 5}}
}

func TestResolve(t *testing.T) {
	ctx := context.Background()
	alpha := domain.TaxonomyCollection{Key: "a", Name: "Alpha"}
	beta := domain.TaxonomyCollection{Key: "b", Name: "Beta"}

	t.Run("case-insensitive match", func(t *testing.T) {
		p := pfake.New()
		p.AddCollection(domain.Collection{ID: id("1"), Name: "aLPHA"})
		got, missing, err := Resolve(ctx, p, taxWith(alpha), false, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got["a"] != id("1") || len(missing) != 0 {
			t.Errorf("got %v, missing %v", got, missing)
		}
	})

	t.Run("nested same-name ignored", func(t *testing.T) {
		p := pfake.New()
		parent := id("9")
		p.AddCollection(domain.Collection{ID: parent, Name: "Parent"})
		p.AddCollection(domain.Collection{ID: id("1"), Name: "Alpha", Parent: &parent})
		got, missing, err := Resolve(ctx, p, taxWith(alpha), false, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := got["a"]; ok || !reflect.DeepEqual(missing, []string{"Alpha"}) {
			t.Errorf("got %v, missing %v", got, missing)
		}
	})

	t.Run("duplicate roots deterministic", func(t *testing.T) {
		for _, order := range [][]string{{"3", "1", "2"}, {"2", "3", "1"}, {"1", "2", "3"}} {
			p := pfake.New()
			for _, l := range order {
				p.AddCollection(domain.Collection{ID: id(l), Name: "Alpha"})
			}
			got, _, err := Resolve(ctx, p, taxWith(alpha), false, nil)
			if err != nil {
				t.Fatal(err)
			}
			if got["a"] != id("1") {
				t.Errorf("order %v: got %v", order, got["a"])
			}
		}
	})

	t.Run("missing without provision", func(t *testing.T) {
		p := pfake.New()
		created := 0
		got, missing, err := Resolve(ctx, p, taxWith(alpha, beta), false, func(domain.ID) { created++ })
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 || !reflect.DeepEqual(missing, []string{"Alpha", "Beta"}) || created != 0 {
			t.Errorf("got %v, missing %v, created %d", got, missing, created)
		}
		if cols, _ := p.ListCollections(ctx); len(cols) != 0 {
			t.Errorf("created %v", cols)
		}
	})

	t.Run("provision creates and reports", func(t *testing.T) {
		p := pfake.New()
		p.AddCollection(domain.Collection{ID: id("1"), Name: "Alpha"})
		var created []domain.ID
		got, missing, err := Resolve(ctx, p, taxWith(alpha, beta), true, func(c domain.ID) { created = append(created, c) })
		if err != nil {
			t.Fatal(err)
		}
		if len(missing) != 0 || got["a"] != id("1") || got["b"].IsZero() {
			t.Fatalf("got %v, missing %v", got, missing)
		}
		if !reflect.DeepEqual(created, []domain.ID{got["b"]}) {
			t.Errorf("created %v", created)
		}
	})

	t.Run("two collections sharing a name share one creation", func(t *testing.T) {
		p := pfake.New()
		var created int
		got, _, err := Resolve(ctx, p, taxWith(alpha, domain.TaxonomyCollection{Key: "a2", Name: "alpha"}), true, func(domain.ID) { created++ })
		if err != nil {
			t.Fatal(err)
		}
		if created != 1 || got["a"] != got["a2"] {
			t.Errorf("created %d, got %v", created, got)
		}
	})

	t.Run("no manager", func(t *testing.T) {
		got, missing, err := Resolve(ctx, nil, taxWith(alpha), true, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 || !reflect.DeepEqual(missing, []string{"Alpha"}) {
			t.Errorf("got %v, missing %v", got, missing)
		}
	})

	t.Run("provider error", func(t *testing.T) {
		p := pfake.New()
		p.FailNext(domain.ErrTemporary)
		if _, _, err := Resolve(ctx, p, taxWith(alpha), false, nil); !errors.Is(err, domain.ErrTemporary) {
			t.Errorf("err = %v", err)
		}
	})
}

func TestSeedDoesNotOverwrite(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	svc := New(store.Taxonomies())
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	orig := taxWith(domain.TaxonomyCollection{Key: "a", Name: "Alpha"})
	if err := svc.Seed(ctx, []domain.Taxonomy{orig}, log); err != nil {
		t.Fatal(err)
	}
	if err := svc.Seed(ctx, []domain.Taxonomy{orig}, log); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 0 {
		t.Errorf("warned on no difference: %s", buf.String())
	}
	changed := orig
	changed.Rules.MaxTags = 9
	if err := svc.Seed(ctx, []domain.Taxonomy{changed}, log); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "level=WARN") || !strings.Contains(buf.String(), "tx") {
		t.Errorf("no warning: %q", buf.String())
	}
	got, err := svc.Get(ctx, "tx")
	if err != nil {
		t.Fatal(err)
	}
	if got.Rules.MaxTags != 5 {
		t.Errorf("seed overwrote: %+v", got.Rules)
	}
}

func TestSeedIgnoresRecordedProposals(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	svc := New(store.Taxonomies())
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	orig := taxWith(domain.TaxonomyCollection{Key: "a", Name: "Alpha"})
	if err := svc.Seed(ctx, []domain.Taxonomy{orig}, log); err != nil {
		t.Fatal(err)
	}
	if err := store.Taxonomies().RecordProposal(ctx, "tx", domain.CollectionProposal{Name: "New"}); err != nil {
		t.Fatal(err)
	}
	if err := svc.Seed(ctx, []domain.Taxonomy{orig}, log); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 0 {
		t.Errorf("warned about runtime data: %s", buf.String())
	}
}

func TestServicePut(t *testing.T) {
	ctx := context.Background()
	review := domain.TaxonomyCollection{Key: "r", Name: "Review", Role: "review"}
	alpha := domain.TaxonomyCollection{Key: "a", Name: "Alpha"}

	bad := map[string]domain.Taxonomy{
		"min above max":         {Status: domain.TaxonomyDraft, Rules: domain.TaxonomyRules{MinTags: 3, MaxTags: 2}},
		"active no collections": {Status: domain.TaxonomyActive},
		"active no review":      {Status: domain.TaxonomyActive, Collections: []domain.TaxonomyCollection{alpha}},
		"active two reviews":    {Status: domain.TaxonomyActive, Collections: []domain.TaxonomyCollection{alpha, review, {Key: "r2", Name: "R2", Role: "review"}}},
		"active only review":    {Status: domain.TaxonomyActive, Collections: []domain.TaxonomyCollection{review}},
	}
	for name, tx := range bad {
		t.Run(name, func(t *testing.T) {
			svc := New(memory.New().Taxonomies())
			if _, err := svc.Put(ctx, "tx", tx, nil); !errors.Is(err, domain.ErrValidation) {
				t.Errorf("err = %v, want ErrValidation", err)
			}
		})
	}

	t.Run("version guard and id from path", func(t *testing.T) {
		svc := New(memory.New().Taxonomies())
		tx := domain.Taxonomy{ID: "ignored", Status: domain.TaxonomyActive, Collections: []domain.TaxonomyCollection{alpha, review}}
		put, err := svc.Put(ctx, "tx", tx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if put.ID != "tx" || put.Version != 1 {
			t.Errorf("put = %+v", put)
		}
		stale := 0
		if _, err := svc.Put(ctx, "tx", tx, &stale); !errors.Is(err, domain.ErrConflict) {
			t.Errorf("err = %v, want ErrConflict", err)
		}
	})

	t.Run("recorded proposals survive a put", func(t *testing.T) {
		store := memory.New()
		svc := New(store.Taxonomies())
		tx := domain.Taxonomy{Status: domain.TaxonomyDraft}
		if _, err := svc.Put(ctx, "tx", tx, nil); err != nil {
			t.Fatal(err)
		}
		if err := store.Taxonomies().RecordProposal(ctx, "tx", domain.CollectionProposal{Name: "New"}); err != nil {
			t.Fatal(err)
		}
		put, err := svc.Put(ctx, "tx", tx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(put.Proposals) != 1 || put.Proposals[0].Name != "New" {
			t.Errorf("proposals = %+v", put.Proposals)
		}
	})
}

var _ ports.CollectionManager = (*pfake.Provider)(nil)
