package raindrop

import (
	"context"
	"net/http"
	"net/url"
	"strconv"

	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

func (h rawHighlight) highlight(bookmark domain.ID) domain.Highlight {
	if bookmark.IsZero() && h.RaindropRef != 0 {
		bookmark = rdID(h.RaindropRef)
	}
	return domain.Highlight{
		ID:        domain.ID{Provider: providerName, Local: h.ID},
		Bookmark:  bookmark,
		Text:      h.Text,
		Note:      h.Note,
		Tags:      h.Tags,
		CreatedAt: parseTime(h.Created),
	}
}

func (p *Provider) ListHighlights(ctx context.Context, bookmark domain.ID) ([]domain.Highlight, error) {
	local, err := localID(bookmark)
	if err != nil {
		return nil, err
	}
	var resp struct {
		Item rawItem `json:"item"`
	}
	if err := p.do(ctx, http.MethodGet, "/raindrop/"+local, nil, nil, &resp); err != nil {
		return nil, err
	}
	out := make([]domain.Highlight, 0, len(resp.Item.Highlights))
	for _, h := range resp.Item.Highlights {
		out = append(out, h.highlight(bookmark))
	}
	return out, nil
}

// ListAllHighlights pages the global highlight feed. q.Collection is ignored.
func (p *Provider) ListAllHighlights(ctx context.Context, q ports.ListQuery) (domain.Page[domain.Highlight], error) {
	page, perPage, err := pageParams(q)
	if err != nil {
		return domain.Page[domain.Highlight]{}, err
	}
	v := url.Values{"page": {strconv.Itoa(page)}, "perpage": {strconv.Itoa(perPage)}}
	var resp struct {
		Items []rawHighlight `json:"items"`
	}
	if err := p.do(ctx, http.MethodGet, "/highlights", v, nil, &resp); err != nil {
		return domain.Page[domain.Highlight]{}, err
	}
	out := domain.Page[domain.Highlight]{Items: make([]domain.Highlight, 0, len(resp.Items))}
	for _, h := range resp.Items {
		out.Items = append(out.Items, h.highlight(domain.ID{}))
	}
	out.NextToken = nextToken(page, perPage, len(resp.Items), 0)
	return out, nil
}
