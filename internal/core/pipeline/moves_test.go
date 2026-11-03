package pipeline

import (
	"testing"
	"time"

	"github.com/wikiabhi/rainwise/internal/domain"
)

var (
	mvInbox  = domain.ID{Provider: "p", Local: "inbox"}
	mvCol    = domain.ID{Provider: "p", Local: "col"}
	mvOther  = domain.ID{Provider: "p", Local: "other"}
	mvReview = domain.ID{Provider: "p", Local: "review"}
	mvNow    = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	mvBefore = mvNow.Add(-time.Hour)
	mvAfter  = mvNow.Add(time.Hour)
)

func mvTaxonomy() domain.Taxonomy {
	return domain.Taxonomy{Collections: []domain.TaxonomyCollection{
		{Key: "col", Name: "Col"},
		{Key: "needs_review", Name: "Needs Review", Role: "review"},
	}}
}

func TestEvaluateMove(t *testing.T) {
	type tc struct {
		name     string
		trigger  Trigger
		loc      domain.ID
		pin      domain.PinSource
		pinnedAt *time.Time
		known    bool
		hash     string
		nilInbox bool
		opts     Options
		want     bool
	}
	first := Options{IsFirstBackfill: true, PinCutoff: &mvNow}
	cases := []tc{
		{name: "first backfill, unpinned", trigger: TriggerBackfill, loc: mvCol, known: true, hash: "h", opts: first, want: true},
		{name: "first backfill, pinned before cutoff", trigger: TriggerBackfill, loc: mvCol, pin: domain.PinUser, pinnedAt: &mvBefore, known: true, hash: "h", opts: first, want: true},
		{name: "first backfill, pinned after cutoff", trigger: TriggerBackfill, loc: mvCol, pin: domain.PinUser, pinnedAt: &mvAfter, known: true, hash: "h", opts: first, want: false},
		{name: "first backfill, pinned at cutoff", trigger: TriggerBackfill, loc: mvCol, pin: domain.PinUser, pinnedAt: &mvNow, known: true, hash: "h", opts: first, want: false},
		{name: "first backfill, no cutoff keeps the pin", trigger: TriggerBackfill, loc: mvCol, pin: domain.PinUser, pinnedAt: &mvBefore, known: true, hash: "h", opts: Options{IsFirstBackfill: true}, want: false},
		{name: "later backfill, user pin", trigger: TriggerBackfill, loc: mvCol, pin: domain.PinUser, pinnedAt: &mvBefore, known: true, hash: "h", want: false},
		{name: "later backfill, baseline pin", trigger: TriggerBackfill, loc: mvCol, pin: domain.PinBaseline, pinnedAt: &mvBefore, known: true, hash: "h", want: true},
		{name: "sync, new in inbox", trigger: TriggerSync, loc: mvInbox, want: true},
		{name: "sync, new in a collection", trigger: TriggerSync, loc: mvCol, want: false},
		{name: "webhook, new in inbox", trigger: TriggerWebhook, loc: mvInbox, want: true},
		{name: "webhook, new in a collection", trigger: TriggerWebhook, loc: mvCol, want: false},
		{name: "sync, seen by dry run only, in inbox", trigger: TriggerSync, loc: mvInbox, known: true, want: true},
		{name: "sync, pinned in inbox", trigger: TriggerSync, loc: mvInbox, pin: domain.PinUser, pinnedAt: &mvBefore, known: true, want: false},
		{name: "reclassify, user pin", trigger: TriggerReclassify, loc: mvCol, pin: domain.PinUser, pinnedAt: &mvBefore, known: true, hash: "h", want: false},
		{name: "reclassify, user pin, include pinned", trigger: TriggerReclassify, loc: mvCol, pin: domain.PinUser, pinnedAt: &mvBefore, known: true, hash: "h", opts: Options{IncludePinned: true}, want: true},
		{name: "reclassify, baseline pin", trigger: TriggerReclassify, loc: mvCol, pin: domain.PinBaseline, pinnedAt: &mvBefore, known: true, hash: "h", want: true},
		{name: "sync, changed input in review", trigger: TriggerSync, loc: mvReview, known: true, hash: "old", want: true},
		{name: "sync, changed input in review, pinned", trigger: TriggerSync, loc: mvReview, pin: domain.PinUser, pinnedAt: &mvBefore, known: true, hash: "old", want: false},
		{name: "sync, changed input elsewhere", trigger: TriggerSync, loc: mvCol, known: true, hash: "old", want: false},
		{name: "sync, changed input in inbox", trigger: TriggerSync, loc: mvInbox, known: true, hash: "old", want: false},
		{name: "backfill, tags only", trigger: TriggerBackfill, loc: mvCol, known: true, hash: "h", opts: Options{TagsOnly: true}, want: false},
		{name: "sync, tags only in inbox", trigger: TriggerSync, loc: mvInbox, opts: Options{TagsOnly: true}, want: false},
		{name: "reclassify, tags only, include pinned", trigger: TriggerReclassify, loc: mvCol, opts: Options{TagsOnly: true, IncludePinned: true}, want: false},
		{name: "no inbox provider, no collection", trigger: TriggerSync, nilInbox: true, want: true},
		{name: "no inbox provider, in a collection", trigger: TriggerSync, loc: mvCol, nilInbox: true, want: false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			opts := c.opts
			opts.Trigger = c.trigger
			opts.Taxonomy = mvTaxonomy()
			opts.Resolved = map[string]domain.ID{"col": mvCol, "needs_review": mvReview}
			inbox := &mvInbox
			if c.nilInbox {
				inbox = nil
			}
			st := domain.BookmarkState{PinSource: c.pin, PinnedAt: c.pinnedAt, InputHash: c.hash}
			got := EvaluateMove(st, c.known, domain.Bookmark{Collection: c.loc}, inbox, opts)
			if got != c.want {
				t.Errorf("EvaluateMove = %v, want %v", got, c.want)
			}
		})
	}
}

func TestDetectManualMove(t *testing.T) {
	cases := []struct {
		name        string
		known       domain.ID
		pending     *domain.ID
		current     domain.ID
		wantMoved   bool
		wantPin     domain.PinSource
		wantPending bool
	}{
		{name: "same collection", known: mvCol, current: mvCol},
		{name: "same collection keeps pending for the caller", known: mvCol, pending: &mvOther, current: mvCol, wantPending: true},
		{name: "different, no pending write", known: mvCol, current: mvOther, wantMoved: true, wantPin: domain.PinUser},
		{name: "matches pending write", known: mvCol, pending: &mvOther, current: mvOther},
		{name: "different from pending write", known: mvCol, pending: &mvReview, current: mvOther, wantMoved: true, wantPin: domain.PinUser},
		{name: "out of needs review", known: mvReview, current: mvCol, wantMoved: true, wantPin: domain.PinUser},
		{name: "to the inbox", known: mvCol, current: mvInbox, wantMoved: true, wantPin: domain.PinUser},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st := domain.BookmarkState{KnownCollection: c.known, PendingCollection: c.pending}
			b := domain.Bookmark{Collection: c.current, Tags: []string{"t"}}
			if got := DetectManualMove(&st, b, mvNow); got != c.wantMoved {
				t.Errorf("moved = %v, want %v", got, c.wantMoved)
			}
			if st.PinSource != c.wantPin {
				t.Errorf("pin = %q, want %q", st.PinSource, c.wantPin)
			}
			if c.wantMoved && (st.PinnedAt == nil || !st.PinnedAt.Equal(mvNow)) {
				t.Errorf("PinnedAt = %v", st.PinnedAt)
			}
			if (st.PendingCollection != nil) != c.wantPending {
				t.Errorf("pending = %v, want present=%v", st.PendingCollection, c.wantPending)
			}
			if st.KnownCollection != c.current {
				t.Errorf("known = %v, want %v", st.KnownCollection, c.current)
			}
		})
	}
}

func TestDetectManualMoveUpgradesBaselinePin(t *testing.T) {
	st := domain.BookmarkState{KnownCollection: mvCol, PinSource: domain.PinBaseline, PinnedAt: &mvBefore}
	if !DetectManualMove(&st, domain.Bookmark{Collection: mvOther}, mvNow) {
		t.Fatal("expected a move")
	}
	if st.PinSource != domain.PinUser || !st.PinnedAt.Equal(mvNow) {
		t.Fatalf("state = %+v", st)
	}
}
