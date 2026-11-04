package ports

import (
	"context"

	"github.com/wikiabhi/rainwise/internal/domain"
)

type Classifier interface {
	Name() string
	Capabilities() ClassifierCapabilities
	Classify(ctx context.Context, r ClassifyRequest) (domain.Decision, error)
}

// TaxonomyProposer is optional: needed only for AI mode's proposal step.
type TaxonomyProposer interface {
	ProposeTaxonomy(ctx context.Context, r ProposeRequest) (TaxonomyProposal, error)
}

// ContentFetcher fetches page text. Retryable failures wrap domain.ErrTemporary.
type ContentFetcher interface {
	Fetch(ctx context.Context, url string) (string, error)
}

type ClassifierCapabilities struct {
	FixedChoice, ProposeCollections, ProposeTags, CalibratedConfidence bool
	MaxChoices                                                         int
}

// Subject is a bookmark with its highlights, or one focused highlight.
type Subject struct {
	Bookmark       domain.Bookmark
	Highlights     []domain.Highlight
	FocusHighlight *domain.Highlight
}

type ClassifyRequest struct {
	Subject        Subject
	Content        string                  // fetched text, untrusted, may be empty
	Taxonomy       domain.TaxonomySnapshot // allowed collections and rules
	TagVocab       []string                // most-used tags, capped
	TagsWanted     int                     // 0 means don't suggest tags
	WantCollection bool                    // false for highlights and pinned bookmarks
}

type ProposeRequest struct {
	Samples             []Subject
	ExistingCollections []string
	MaxCollections      int
	Extra               string
}

type TaxonomyProposal struct {
	Collections []domain.CollectionProposal
	Model       domain.ModelInfo
}
