package raindrop

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/wikiabhi/rainwise/internal/domain"
)

type rawCollection struct {
	ID          int64  `json:"_id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Parent      *refID `json:"parent"`
}

func (c rawCollection) collection() domain.Collection {
	out := domain.Collection{ID: rdID(c.ID), Name: c.Title, Description: c.Description}
	if c.Parent != nil && c.Parent.ID != 0 {
		pid := rdID(c.Parent.ID)
		out.Parent = &pid
	}
	return out
}

// ListCollections merges root and nested collections.
func (p *Provider) ListCollections(ctx context.Context) ([]domain.Collection, error) {
	var out []domain.Collection
	seen := map[int64]bool{}
	for _, path := range []string{"/collections", "/collections/childrens"} {
		var resp struct {
			Items []rawCollection `json:"items"`
		}
		if err := p.do(ctx, http.MethodGet, path, nil, nil, &resp); err != nil {
			return nil, err
		}
		for _, c := range resp.Items {
			if !seen[c.ID] {
				seen[c.ID] = true
				out = append(out, c.collection())
			}
		}
	}
	return out, nil
}

func (p *Provider) CreateCollection(ctx context.Context, c domain.Collection) (domain.Collection, error) {
	body := map[string]any{"title": c.Name}
	if c.Parent != nil {
		l, err := localID(*c.Parent)
		if err != nil {
			return domain.Collection{}, err
		}
		n, err := strconv.ParseInt(l, 10, 64)
		if err != nil {
			return domain.Collection{}, fmt.Errorf("parent id %q: %w", l, domain.ErrValidation)
		}
		body["parent"] = refID{ID: n}
	}
	var resp struct {
		Item rawCollection `json:"item"`
	}
	if err := p.do(ctx, http.MethodPost, "/collection", nil, body, &resp); err != nil {
		return domain.Collection{}, err
	}
	return resp.Item.collection(), nil
}

// DeleteCollectionIfEmpty deletes only a user collection with no bookmarks and
// no child collections. System collections (0, -1, -99) are refused.
func (p *Provider) DeleteCollectionIfEmpty(ctx context.Context, id domain.ID) (bool, error) {
	local, err := localID(id)
	if err != nil {
		return false, err
	}
	if n, err := strconv.ParseInt(local, 10, 64); err != nil || n <= 0 {
		return false, fmt.Errorf("collection %q cannot be deleted: %w", id, domain.ErrValidation)
	}
	var resp struct {
		Items []rawItem `json:"items"`
		Count int       `json:"count"`
	}
	if err := p.do(ctx, http.MethodGet, "/raindrops/"+local, url.Values{"perpage": {"1"}}, nil, &resp); err != nil {
		return false, err
	}
	if len(resp.Items) > 0 || resp.Count > 0 {
		return false, nil
	}
	all, err := p.ListCollections(ctx)
	if err != nil {
		return false, err
	}
	for _, c := range all {
		if c.Parent != nil && *c.Parent == id {
			return false, nil
		}
	}
	if err := p.do(ctx, http.MethodDelete, "/collection/"+local, nil, nil, nil); err != nil {
		return false, err
	}
	return true, nil
}
