// Package fake is an in-memory ports.Provider for tests. It also implements
// the optional highlight, collection and tag ports; each returns
// domain.ErrCapabilityUnsupported while its capability is switched off.
package fake

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

const (
	providerName = "fake"
	defaultLimit = 50
)

// Inbox is the inbox collection a new Provider declares.
var Inbox = domain.ID{Provider: providerName, Local: "inbox"}

// Write is one successful UpdateBookmark call.
type Write struct {
	ID    domain.ID
	Patch ports.BookmarkPatch
}

type Provider struct {
	mu          sync.Mutex
	caps        ports.ProviderCapabilities
	bookmarks   map[domain.ID]domain.Bookmark
	order       []domain.ID
	highlights  []domain.Highlight
	collections []domain.Collection
	writes      []Write
	failures    []error
	onGet       func(domain.ID)
	nextCol     int
	listed      bool
	calls       Calls
}

// Calls counts the provider's read requests, failed ones too.
type Calls struct {
	Get, List, Highlights, HighlightFeed, Tags int
}

// New returns an empty provider with highlights, collection creation and tag
// listing on, and the Inbox collection declared.
func New() *Provider {
	inbox := Inbox
	return &Provider{
		caps: ports.ProviderCapabilities{
			Highlights:        true,
			CreateCollections: true,
			TagListing:        true,
			InboxCollection:   &inbox,
		},
		bookmarks: map[domain.ID]domain.Bookmark{},
	}
}

// SetCapabilities overrides the declared capabilities.
func (p *Provider) SetCapabilities(c ports.ProviderCapabilities) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.caps = c
}

// SetListedHighlights makes reads carry each bookmark's highlights, as a provider
// whose listings include them does.
func (p *Provider) SetListedHighlights(on bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.listed = on
}

// Calls returns how many read requests have been made.
func (p *Provider) Calls() Calls {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// read returns a copy of b as a read reports it. The caller holds p.mu.
func (p *Provider) read(b domain.Bookmark) domain.Bookmark {
	b = cloneBookmark(b)
	if p.listed {
		b.Highlights = []domain.Highlight{}
		for _, h := range p.highlights {
			if h.Bookmark == b.ID {
				b.Highlights = append(b.Highlights, h)
			}
		}
	}
	return b
}

// Add stores or replaces a bookmark.
func (p *Provider) Add(b domain.Bookmark) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.bookmarks[b.ID]; !ok {
		p.order = append(p.order, b.ID)
	}
	p.bookmarks[b.ID] = cloneBookmark(b)
}

// AddHighlight stores a highlight.
func (p *Provider) AddHighlight(h domain.Highlight) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.highlights = append(p.highlights, h)
}

// AddCollection stores a collection.
func (p *Provider) AddCollection(c domain.Collection) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.collections = append(p.collections, c)
}

// SetCollection moves a bookmark without recording a write, as a user would.
func (p *Provider) SetCollection(id, collection domain.ID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if b, ok := p.bookmarks[id]; ok {
		b.Collection = collection
		p.bookmarks[id] = b
	}
}

// Remove deletes a bookmark without recording a write.
func (p *Provider) Remove(id domain.ID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.bookmarks, id)
	p.order = slices.DeleteFunc(p.order, func(x domain.ID) bool { return x == id })
}

// Bookmark returns current state without consuming failures or firing hooks.
func (p *Provider) Bookmark(id domain.ID) (domain.Bookmark, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	b, ok := p.bookmarks[id]
	return cloneBookmark(b), ok
}

// Writes returns every successful write, in order.
func (p *Provider) Writes() []Write {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.writes)
}

// FailNext makes the next provider call return err. Calls queue up in order.
func (p *Provider) FailNext(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failures = append(p.failures, err)
}

// OnGet registers a hook run at the start of each GetBookmark, for changing
// state between reads. No lock is held, so it may call back into the provider.
func (p *Provider) OnGet(f func(domain.ID)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onGet = f
}

// takeFailure pops the next queued failure. The caller holds p.mu.
func (p *Provider) takeFailure() error {
	if len(p.failures) == 0 {
		return nil
	}
	err := p.failures[0]
	p.failures = p.failures[1:]
	return err
}

func (p *Provider) Name() string { return providerName }

func (p *Provider) Capabilities() ports.ProviderCapabilities {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.caps
}

func (p *Provider) GetBookmark(_ context.Context, id domain.ID) (domain.Bookmark, error) {
	p.mu.Lock()
	hook := p.onGet
	p.mu.Unlock()
	if hook != nil {
		hook(id)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls.Get++
	if err := p.takeFailure(); err != nil {
		return domain.Bookmark{}, err
	}
	b, ok := p.bookmarks[id]
	if !ok {
		return domain.Bookmark{}, fmt.Errorf("bookmark %s: %w", id, domain.ErrNotFound)
	}
	return p.read(b), nil
}

func (p *Provider) ListBookmarks(_ context.Context, q ports.ListQuery) (domain.Page[domain.Bookmark], error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls.List++
	if err := p.takeFailure(); err != nil {
		return domain.Page[domain.Bookmark]{}, err
	}
	var all []domain.Bookmark
	for _, id := range p.order {
		b := p.bookmarks[id]
		if q.Collection != nil && b.Collection != *q.Collection {
			continue
		}
		if q.Since != nil && p.caps.IncrementalSince && b.UpdatedAt.Before(*q.Since) {
			continue
		}
		all = append(all, p.read(b))
	}
	return paginate(all, q)
}

func (p *Provider) UpdateBookmark(_ context.Context, id domain.ID, patch ports.BookmarkPatch) (domain.Bookmark, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.takeFailure(); err != nil {
		return domain.Bookmark{}, err
	}
	b, ok := p.bookmarks[id]
	if !ok {
		return domain.Bookmark{}, fmt.Errorf("bookmark %s: %w", id, domain.ErrNotFound)
	}
	if patch.Collection != nil {
		b.Collection = *patch.Collection
	}
	if patch.SetTags != nil {
		b.Tags = slices.Clone(patch.SetTags)
	}
	b.Tags = mergeTags(b.Tags, patch.AddTags)
	p.bookmarks[id] = b
	recorded := patch
	recorded.AddTags = slices.Clone(patch.AddTags)
	recorded.SetTags = slices.Clone(patch.SetTags)
	if patch.Collection != nil {
		c := *patch.Collection
		recorded.Collection = &c
	}
	p.writes = append(p.writes, Write{ID: id, Patch: recorded})
	return cloneBookmark(b), nil
}

func (p *Provider) ListHighlights(_ context.Context, bookmark domain.ID) ([]domain.Highlight, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.caps.Highlights {
		return nil, domain.ErrCapabilityUnsupported
	}
	p.calls.Highlights++
	if err := p.takeFailure(); err != nil {
		return nil, err
	}
	var out []domain.Highlight
	for _, h := range p.highlights {
		if h.Bookmark == bookmark {
			out = append(out, h)
		}
	}
	return out, nil
}

func (p *Provider) ListAllHighlights(_ context.Context, q ports.ListQuery) (domain.Page[domain.Highlight], error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.caps.Highlights {
		return domain.Page[domain.Highlight]{}, domain.ErrCapabilityUnsupported
	}
	p.calls.HighlightFeed++
	if err := p.takeFailure(); err != nil {
		return domain.Page[domain.Highlight]{}, err
	}
	return paginate(slices.Clone(p.highlights), q)
}

func (p *Provider) ListCollections(_ context.Context) ([]domain.Collection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.caps.CreateCollections {
		return nil, domain.ErrCapabilityUnsupported
	}
	if err := p.takeFailure(); err != nil {
		return nil, err
	}
	return slices.Clone(p.collections), nil
}

func (p *Provider) CreateCollection(_ context.Context, c domain.Collection) (domain.Collection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.caps.CreateCollections {
		return domain.Collection{}, domain.ErrCapabilityUnsupported
	}
	if err := p.takeFailure(); err != nil {
		return domain.Collection{}, err
	}
	p.nextCol++
	c.ID = domain.ID{Provider: providerName, Local: "created-" + strconv.Itoa(p.nextCol)}
	p.collections = append(p.collections, c)
	return c, nil
}

func (p *Provider) DeleteCollectionIfEmpty(_ context.Context, id domain.ID) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.caps.CreateCollections {
		return false, domain.ErrCapabilityUnsupported
	}
	if err := p.takeFailure(); err != nil {
		return false, err
	}
	for _, b := range p.bookmarks {
		if b.Collection == id {
			return false, nil
		}
	}
	before := len(p.collections)
	p.collections = slices.DeleteFunc(p.collections, func(c domain.Collection) bool { return c.ID == id })
	return len(p.collections) != before, nil
}

func (p *Provider) ListTags(_ context.Context) ([]domain.TagCount, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.caps.TagListing {
		return nil, domain.ErrCapabilityUnsupported
	}
	p.calls.Tags++
	if err := p.takeFailure(); err != nil {
		return nil, err
	}
	counts := map[string]int{}
	for _, b := range p.bookmarks {
		for _, t := range b.Tags {
			counts[t]++
		}
	}
	out := make([]domain.TagCount, 0, len(counts))
	for name, n := range counts {
		out = append(out, domain.TagCount{Name: name, Count: n})
	}
	slices.SortFunc(out, func(a, b domain.TagCount) int {
		if a.Count != b.Count {
			return b.Count - a.Count
		}
		return strings.Compare(a.Name, b.Name)
	})
	return out, nil
}

func paginate[T any](all []T, q ports.ListQuery) (domain.Page[T], error) {
	start := 0
	if q.PageToken != "" {
		n, err := strconv.Atoi(q.PageToken)
		if err != nil || n < 0 {
			return domain.Page[T]{}, fmt.Errorf("page token %q: %w", q.PageToken, domain.ErrValidation)
		}
		start = n
	}
	limit := q.Limit
	if limit <= 0 {
		limit = defaultLimit
	}
	if start >= len(all) {
		return domain.Page[T]{}, nil
	}
	end := min(start+limit, len(all))
	page := domain.Page[T]{Items: all[start:end]}
	if end < len(all) {
		page.NextToken = strconv.Itoa(end)
	}
	return page, nil
}

func mergeTags(existing, add []string) []string {
	out := slices.Clone(existing)
	for _, t := range add {
		if !slices.ContainsFunc(out, func(e string) bool { return strings.EqualFold(e, t) }) {
			out = append(out, t)
		}
	}
	return out
}

func cloneBookmark(b domain.Bookmark) domain.Bookmark {
	b.Tags = slices.Clone(b.Tags)
	b.Highlights = slices.Clone(b.Highlights)
	if b.Metadata != nil {
		m := make(map[string]string, len(b.Metadata))
		for k, v := range b.Metadata {
			m[k] = v
		}
		b.Metadata = m
	}
	return b
}
