package ports

import (
	"context"
	"net/http"
	"time"

	"github.com/wikiabhi/rainwise/internal/domain"
)

type BookmarkReader interface {
	GetBookmark(ctx context.Context, id domain.ID) (domain.Bookmark, error)
	ListBookmarks(ctx context.Context, q ListQuery) (domain.Page[domain.Bookmark], error)
}

type BookmarkWriter interface {
	UpdateBookmark(ctx context.Context, id domain.ID, p BookmarkPatch) (domain.Bookmark, error)
}

// BookmarkPatch makes additive intent explicit. AddTags is the only tag
// operation the pipeline uses. SetTags is reserved for restore.
type BookmarkPatch struct {
	Collection *domain.ID
	AddTags    []string
	SetTags    []string
}

type Provider interface {
	Name() string
	Capabilities() ProviderCapabilities
	BookmarkReader
	BookmarkWriter
}

// Optional ports, discovered by type assertion and declared in capabilities.

type HighlightReader interface {
	ListHighlights(ctx context.Context, bookmark domain.ID) ([]domain.Highlight, error)
	ListAllHighlights(ctx context.Context, q ListQuery) (domain.Page[domain.Highlight], error)
}

type CollectionManager interface {
	ListCollections(ctx context.Context) ([]domain.Collection, error)
	CreateCollection(ctx context.Context, c domain.Collection) (domain.Collection, error)
	DeleteCollectionIfEmpty(ctx context.Context, id domain.ID) (bool, error)
}

type TagLister interface {
	ListTags(ctx context.Context) ([]domain.TagCount, error)
}

type EventNormalizer interface {
	Normalize(ctx context.Context, headers http.Header, body []byte) ([]domain.Event, error)
}

type ProviderCapabilities struct {
	Highlights, CreateCollections, NativeWebhooks  bool
	IncrementalSince, CursorPagination, TagListing bool
	InboxCollection                                *domain.ID // nil if the provider has no inbox concept
}

type ListQuery struct {
	Collection *domain.ID
	Since      *time.Time // honored only with IncrementalSince
	PageToken  string     // opaque: a page number or a cursor
	Limit      int
}

// URLResolver is optional. It resolves a bookmark URL to its ID, for events
// that carry only a URL.
type URLResolver interface {
	ResolveURL(ctx context.Context, url string) (domain.ID, error)
}
