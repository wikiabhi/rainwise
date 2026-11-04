package taxonomy

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

type stubProposer struct {
	reqs []ports.ProposeRequest
	errs []error
	out  []domain.CollectionProposal
}

func (s *stubProposer) ProposeTaxonomy(_ context.Context, r ports.ProposeRequest) (ports.TaxonomyProposal, error) {
	s.reqs = append(s.reqs, r)
	if len(s.errs) > 0 {
		err := s.errs[0]
		s.errs = s.errs[1:]
		if err != nil {
			return ports.TaxonomyProposal{}, err
		}
	}
	return ports.TaxonomyProposal{Collections: s.out}, nil
}

func draft(max int) domain.Taxonomy {
	return domain.Taxonomy{ID: "tx", Mode: domain.ModeDynamic, Status: domain.TaxonomyDraft,
		Rules:   domain.TaxonomyRules{PromptExtra: "be brief"},
		Dynamic: domain.DynamicSettings{MaxCollections: max}}
}

func proposals(names ...string) []domain.CollectionProposal {
	var out []domain.CollectionProposal
	for _, n := range names {
		out = append(out, domain.CollectionProposal{Name: n, Description: n + " things", Count: 1})
	}
	return out
}

func TestPropose(t *testing.T) {
	ctx := context.Background()

	t.Run("capped, with a review collection", func(t *testing.T) {
		p := &stubProposer{out: proposals("One", "Two", "Three", "Four", "Five")}
		got, err := Propose(ctx, p, nil, nil, draft(3))
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Collections) > 3 {
			t.Errorf("%d collections, cap 3: %+v", len(got.Collections), got.Collections)
		}
		reviews := 0
		for _, c := range got.Collections {
			if c.Role == "review" {
				reviews++
				if c.Key != "needs_review" {
					t.Errorf("review key = %q", c.Key)
				}
			}
		}
		if reviews != 1 {
			t.Errorf("reviews = %d", reviews)
		}
		if p.reqs[0].MaxCollections > 3 || p.reqs[0].MaxCollections <= 0 {
			t.Errorf("asked for %d", p.reqs[0].MaxCollections)
		}
		if got.Status != domain.TaxonomyDraft {
			t.Errorf("status = %q", got.Status)
		}
	})

	t.Run("passes samples, existing names, and the prompt extra", func(t *testing.T) {
		p := &stubProposer{out: proposals("One")}
		samples := []ports.Subject{{Bookmark: domain.Bookmark{Title: "x"}}}
		if _, err := Propose(ctx, p, samples, []string{"Old", "Older"}, draft(5)); err != nil {
			t.Fatal(err)
		}
		r := p.reqs[0]
		if !reflect.DeepEqual(r.ExistingCollections, []string{"Old", "Older"}) || len(r.Samples) != 1 || r.Extra != "be brief" {
			t.Errorf("request = %+v", r)
		}
	})

	t.Run("keeps an existing review collection and replaces the rest", func(t *testing.T) {
		tx := draft(5)
		tx.Collections = []domain.TaxonomyCollection{{Key: "stale", Name: "Stale"}, {Key: "rv", Name: "Later", Role: "review"}}
		got, err := Propose(ctx, &stubProposer{out: proposals("One")}, nil, nil, tx)
		if err != nil {
			t.Fatal(err)
		}
		want := []domain.TaxonomyCollection{{Key: "one", Name: "One", Description: "One things"}, {Key: "rv", Name: "Later", Role: "review"}}
		if !reflect.DeepEqual(got.Collections, want) {
			t.Errorf("collections = %+v", got.Collections)
		}
	})

	t.Run("drops duplicates, blanks, and review-named proposals, with unique keys", func(t *testing.T) {
		got, err := Propose(ctx, &stubProposer{out: proposals("Dev Ops", "dev ops", " ", "Dev-Ops", "Needs Review")}, nil, nil, draft(10))
		if err != nil {
			t.Fatal(err)
		}
		want := []domain.TaxonomyCollection{
			{Key: "dev_ops", Name: "Dev Ops", Description: "Dev Ops things"},
			{Key: "dev_ops_2", Name: "Dev-Ops", Description: "Dev-Ops things"},
			{Key: "needs_review", Name: "Needs Review", Description: reviewDescription, Role: "review"},
		}
		if !reflect.DeepEqual(got.Collections, want) {
			t.Errorf("collections = %+v", got.Collections)
		}
	})

	t.Run("retries once on invalid output", func(t *testing.T) {
		p := &stubProposer{out: proposals("One"), errs: []error{domain.ErrInvalidOutput, nil}}
		if _, err := Propose(ctx, p, nil, nil, draft(3)); err != nil {
			t.Fatal(err)
		}
		if len(p.reqs) != 2 {
			t.Errorf("calls = %d", len(p.reqs))
		}
	})

	t.Run("gives up after a second invalid output", func(t *testing.T) {
		p := &stubProposer{errs: []error{domain.ErrInvalidOutput, domain.ErrInvalidOutput, nil}}
		if _, err := Propose(ctx, p, nil, nil, draft(3)); !errors.Is(err, domain.ErrInvalidOutput) {
			t.Errorf("err = %v", err)
		}
		if len(p.reqs) != 2 {
			t.Errorf("calls = %d", len(p.reqs))
		}
	})

	t.Run("no retry on other errors", func(t *testing.T) {
		p := &stubProposer{errs: []error{domain.ErrTemporary, nil}}
		if _, err := Propose(ctx, p, nil, nil, draft(3)); !errors.Is(err, domain.ErrTemporary) {
			t.Errorf("err = %v", err)
		}
		if len(p.reqs) != 1 {
			t.Errorf("calls = %d", len(p.reqs))
		}
	})
}

func TestProposeNothingUsableIsInvalid(t *testing.T) {
	withReview := draft(5)
	withReview.Collections = []domain.TaxonomyCollection{{Key: "r", Name: "Later", Role: roleReview}}
	cases := []struct {
		name string
		t    domain.Taxonomy
		out  []domain.CollectionProposal
	}{
		{"nothing proposed", draft(5), nil},
		{"only blank names", draft(5), proposals("  ")},
		{"only the review collection", draft(5), proposals("needs review")},
		{"only the existing review name", withReview, proposals("LATER")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Propose(context.Background(), &stubProposer{out: c.out}, nil, nil, c.t)
			if !errors.Is(err, domain.ErrInvalidOutput) {
				t.Fatalf("err = %v, want ErrInvalidOutput", err)
			}
		})
	}
}
