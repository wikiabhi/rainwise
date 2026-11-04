package raindrop

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wikiabhi/rainwise/internal/domain"
)

func TestNormalize(t *testing.T) {
	p := New(Config{})
	norm := func(body string) ([]domain.Event, error) { return p.Normalize(context.Background(), nil, []byte(body)) }

	t.Run("valid", func(t *testing.T) {
		evs, err := norm(`{"type":"bookmark.created","resource":{"id":"12345","url":"https://example.com/post"},
			"bookmark":{"id":"12345","url":"https://example.com/post"},"occurred_at":"2026-11-03T10:00:00Z",
			"event_id":"src-1","metadata":{"source":"ifttt"}}`)
		if err != nil || len(evs) != 1 {
			t.Fatalf("%v %v", evs, err)
		}
		e := evs[0]
		if e.Provider != "raindrop" || e.Type != domain.EventBookmarkCreated || e.Resource.ID != "12345" ||
			e.Resource.URL != "https://example.com/post" || e.DedupKey != "src-1" || e.Metadata["source"] != "ifttt" ||
			!e.OccurredAt.Equal(time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)) {
			t.Fatalf("event = %+v", e)
		}
	})
	t.Run("url only", func(t *testing.T) {
		evs, err := norm(`{"type":"bookmark.moved","resource":{"url":"https://example.com/a"}}`)
		if err != nil || evs[0].Resource.URL == "" || evs[0].Resource.ID != "" {
			t.Fatalf("%v %v", evs, err)
		}
	})
	t.Run("highlight with bookmark", func(t *testing.T) {
		evs, err := norm(`{"type":"highlight.created","resource":{"id":"h1"},"bookmark":{"id":"5"}}`)
		if err != nil || evs[0].Bookmark == nil || evs[0].Bookmark.ID != "5" {
			t.Fatalf("%v %v", evs, err)
		}
	})
	for name, body := range map[string]string{
		"no resource":         `{"type":"bookmark.created","resource":{}}`,
		"missing resource":    `{"type":"bookmark.created"}`,
		"unknown type":        `{"type":"bookmark.deleted","resource":{"id":"1"}}`,
		"highlight no parent": `{"type":"highlight.created","resource":{"id":"h1"}}`,
		"highlight empty ref": `{"type":"highlight.created","resource":{"id":"h1"},"bookmark":{}}`,
		"not json":            `nope`,
		"bad time":            `{"type":"bookmark.created","resource":{"id":"1"},"occurred_at":"yesterday"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := norm(body); !errors.Is(err, domain.ErrValidation) {
				t.Fatalf("err = %v", err)
			}
		})
	}
	t.Run("dedup hash", func(t *testing.T) {
		const a = `{"type":"bookmark.created","resource":{"id":"1"},"occurred_at":"2026-11-03T10:00:00Z"}`
		const b = `{ "occurred_at":"2026-11-03T10:00:00Z", "resource":{"id":"1"}, "type":"bookmark.created" }`
		const c = `{"type":"bookmark.created","resource":{"id":"2"},"occurred_at":"2026-11-03T10:00:00Z"}`
		ea, _ := norm(a)
		eb, _ := norm(b)
		ec, _ := norm(c)
		if ea[0].DedupKey == "" || ea[0].DedupKey != eb[0].DedupKey || ea[0].DedupKey == ec[0].DedupKey {
			t.Fatalf("keys %q %q %q", ea[0].DedupKey, eb[0].DedupKey, ec[0].DedupKey)
		}
	})
}
