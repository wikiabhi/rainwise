package pipeline

import (
	"time"

	"github.com/wikiabhi/rainwise/internal/domain"
)

// isNew reports whether the pipeline has never finished with this bookmark. A
// state row from a dry run or a failed attempt alone doesn't count.
func isNew(state domain.BookmarkState, known bool) bool {
	return !known || state.InputHash == ""
}

// pinBinds reports whether a backfill must respect the bookmark's pin (§9.1a).
// Baseline pins never bind a backfill. A first backfill ignores user pins made
// before its cutoff. Lacking the times to prove that, the pin stands.
func pinBinds(state domain.BookmarkState, opts Options) bool {
	if state.PinSource != domain.PinUser {
		return false
	}
	if !opts.IsFirstBackfill || opts.PinCutoff == nil || state.PinnedAt == nil {
		return true
	}
	return !state.PinnedAt.Before(*opts.PinCutoff)
}

// EvaluateMove decides whether a bookmark may change collection (§9.1, §9.1a).
// It says nothing about tags, or about whether to classify at all.
func EvaluateMove(state domain.BookmarkState, known bool, b domain.Bookmark, inbox *domain.ID, opts Options) (wantCollection bool) {
	if opts.TagsOnly {
		return false
	}
	switch opts.Trigger {
	case TriggerBackfill:
		return !pinBinds(state, opts)
	case TriggerReclassify:
		return opts.IncludePinned || state.PinSource != domain.PinUser
	}
	if state.PinSource != domain.PinNone {
		return false
	}
	if isNew(state, known) {
		if inbox == nil {
			return b.Collection.IsZero()
		}
		return b.Collection == *inbox
	}
	review, ok := reviewCollection(opts.Taxonomy, opts.Resolved)
	return ok && b.Collection == review
}

// DetectManualMove compares the bookmark's collection with the known one
// (§10.3) and reports whether the user moved it, pinning it if so. A collection
// matching the pending write is our own crashed write: the pending write is
// cleared and nothing is pinned. A pending write is left in place when the
// collection hasn't changed, because that alone doesn't show whether it landed.
func DetectManualMove(state *domain.BookmarkState, b domain.Bookmark, now time.Time) bool {
	if b.Collection == state.KnownCollection {
		return false
	}
	matchesPending := state.PendingCollection != nil && *state.PendingCollection == b.Collection
	state.KnownCollection = b.Collection
	state.KnownTags = b.Tags
	state.PendingCollection = nil
	state.PendingTags = nil
	if matchesPending {
		return false
	}
	state.PinSource = domain.PinUser
	state.PinnedAt = &now
	return true
}

func reviewCollection(tax domain.Taxonomy, resolved map[string]domain.ID) (domain.ID, bool) {
	for _, c := range tax.Collections {
		if c.Role == roleReview {
			id, ok := resolved[c.Key]
			return id, ok
		}
	}
	return domain.ID{}, false
}
