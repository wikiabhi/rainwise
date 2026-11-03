package raindrop

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

type refID struct {
	ID int64 `json:"$id"`
}

type rawHighlight struct {
	ID          string   `json:"_id"`
	Text        string   `json:"text"`
	Note        string   `json:"note"`
	Created     string   `json:"created"`
	Tags        []string `json:"tags"`
	RaindropRef int64    `json:"raindropRef"`
}

type rawItem struct {
	ID         int64          `json:"_id"`
	Link       string         `json:"link"`
	Title      string         `json:"title"`
	Excerpt    string         `json:"excerpt"`
	Note       string         `json:"note"`
	Tags       []string       `json:"tags"`
	Collection refID          `json:"collection"`
	Created    string         `json:"created"`
	LastUpdate string         `json:"lastUpdate"`
	Highlights []rawHighlight `json:"highlights"`
}

func parseTime(s string) time.Time {
	t, _ := time.Parse(time.RFC3339, s)
	return t
}

func rdID(n int64) domain.ID {
	return domain.ID{Provider: providerName, Local: strconv.FormatInt(n, 10)}
}

func (r rawItem) bookmark() domain.Bookmark {
	b := domain.Bookmark{
		ID:         rdID(r.ID),
		URL:        r.Link,
		Title:      r.Title,
		Excerpt:    r.Excerpt,
		Tags:       r.Tags,
		Collection: rdID(r.Collection.ID),
		CreatedAt:  parseTime(r.Created),
		UpdatedAt:  parseTime(r.LastUpdate),
	}
	if r.Note != "" {
		b.Metadata = map[string]string{"note": r.Note}
	}
	return b
}

func (p *Provider) GetBookmark(ctx context.Context, id domain.ID) (domain.Bookmark, error) {
	local, err := localID(id)
	if err != nil {
		return domain.Bookmark{}, err
	}
	var resp struct {
		Item rawItem `json:"item"`
	}
	if err := p.do(ctx, http.MethodGet, "/raindrop/"+local, nil, nil, &resp); err != nil {
		return domain.Bookmark{}, err
	}
	return resp.Item.bookmark(), nil
}

func pageParams(q ports.ListQuery) (page, perPage int, err error) {
	perPage = q.Limit
	if perPage <= 0 || perPage > maxPerPage {
		perPage = maxPerPage
	}
	if q.PageToken != "" {
		if page, err = strconv.Atoi(q.PageToken); err != nil || page < 0 {
			return 0, 0, fmt.Errorf("invalid page token: %w", domain.ErrValidation)
		}
	}
	return page, perPage, nil
}

func nextToken(page, perPage, got, count int) string {
	if got < perPage || (count > 0 && (page+1)*perPage >= count) {
		return ""
	}
	return strconv.Itoa(page + 1)
}

func (p *Provider) ListBookmarks(ctx context.Context, q ports.ListQuery) (domain.Page[domain.Bookmark], error) {
	coll := "0"
	if q.Collection != nil {
		var err error
		if coll, err = localID(*q.Collection); err != nil {
			return domain.Page[domain.Bookmark]{}, err
		}
		if coll == "-99" {
			return domain.Page[domain.Bookmark]{}, fmt.Errorf("trash is never listed: %w", domain.ErrValidation)
		}
	}
	page, perPage, err := pageParams(q)
	if err != nil {
		return domain.Page[domain.Bookmark]{}, err
	}
	v := url.Values{
		"page":    {strconv.Itoa(page)},
		"perpage": {strconv.Itoa(perPage)},
		"sort":    {"created"},
	}
	if p.cfg.IncrementalSince && q.Since != nil {
		v.Set("search", "lastUpdate:>"+q.Since.UTC().Format("2006-01-02"))
	}
	var resp struct {
		Items []rawItem `json:"items"`
		Count int       `json:"count"`
	}
	if err := p.do(ctx, http.MethodGet, "/raindrops/"+coll, v, nil, &resp); err != nil {
		return domain.Page[domain.Bookmark]{}, err
	}
	out := domain.Page[domain.Bookmark]{Items: make([]domain.Bookmark, 0, len(resp.Items))}
	for _, it := range resp.Items {
		out.Items = append(out.Items, it.bookmark())
	}
	out.NextToken = nextToken(page, perPage, len(resp.Items), resp.Count)
	return out, nil
}

// UpdateBookmark never sends a tag list narrower than the bookmark's current
// one unless SetTags was asked for.
func (p *Provider) UpdateBookmark(ctx context.Context, id domain.ID, patch ports.BookmarkPatch) (domain.Bookmark, error) {
	local, err := localID(id)
	if err != nil {
		return domain.Bookmark{}, err
	}
	if len(patch.AddTags) > 0 && patch.SetTags != nil {
		return domain.Bookmark{}, fmt.Errorf("AddTags and SetTags together: %w", domain.ErrValidation)
	}
	body := map[string]any{}
	if patch.Collection != nil {
		c, err := localID(*patch.Collection)
		if err != nil {
			return domain.Bookmark{}, err
		}
		n, err := strconv.ParseInt(c, 10, 64)
		if err != nil {
			return domain.Bookmark{}, fmt.Errorf("collection id %q: %w", c, domain.ErrValidation)
		}
		body["collection"] = refID{ID: n}
	}
	var current *domain.Bookmark
	switch {
	case patch.SetTags != nil:
		body["tags"] = patch.SetTags
	case len(patch.AddTags) > 0:
		cur, err := p.GetBookmark(ctx, id)
		if err != nil {
			return domain.Bookmark{}, err
		}
		current = &cur
		if merged, changed := mergeTags(cur.Tags, patch.AddTags); changed {
			body["tags"] = merged
		}
	}
	if len(body) == 0 {
		if current != nil {
			return *current, nil
		}
		return p.GetBookmark(ctx, id)
	}
	var resp struct {
		Item rawItem `json:"item"`
	}
	if err := p.do(ctx, http.MethodPut, "/raindrop/"+local, nil, body, &resp); err != nil {
		return domain.Bookmark{}, err
	}
	return resp.Item.bookmark(), nil
}

// mergeTags appends add to existing, skipping case-insensitive duplicates.
func mergeTags(existing, add []string) ([]string, bool) {
	seen := make(map[string]bool, len(existing)+len(add))
	out := append([]string{}, existing...)
	for _, t := range existing {
		seen[strings.ToLower(t)] = true
	}
	changed := false
	for _, t := range add {
		t = strings.TrimSpace(t)
		if t == "" || seen[strings.ToLower(t)] {
			continue
		}
		seen[strings.ToLower(t)] = true
		out = append(out, t)
		changed = true
	}
	return out, changed
}

// ResolveURL finds the bookmark whose link equals rawURL exactly.
func (p *Provider) ResolveURL(ctx context.Context, rawURL string) (domain.ID, error) {
	v := url.Values{"search": {rawURL}, "perpage": {strconv.Itoa(maxPerPage)}, "page": {"0"}}
	var resp struct {
		Items []rawItem `json:"items"`
	}
	if err := p.do(ctx, http.MethodGet, "/raindrops/0", v, nil, &resp); err != nil {
		return domain.ID{}, err
	}
	for _, it := range resp.Items {
		if it.Link == rawURL {
			return rdID(it.ID), nil
		}
	}
	return domain.ID{}, fmt.Errorf("no bookmark with that url: %w", domain.ErrNotFound)
}
