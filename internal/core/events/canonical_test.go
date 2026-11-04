package events

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/wikiabhi/rainwise/internal/domain"
)

func TestNormalizeCanonicalBookmark(t *testing.T) {
	body := `{"type":"bookmark.created","resource":{"id":"1","url":"https://e.test"},"occurred_at":"2026-11-03T10:00:00Z","event_id":"src-1","metadata":{"source":"ifttt"}}`
	evs, err := NormalizeCanonical("fake", []byte(body))
	if err != nil || len(evs) != 1 {
		t.Fatalf("got %v, %v", evs, err)
	}
	e := evs[0]
	if e.Provider != "fake" || e.Type != domain.EventBookmarkCreated || e.Resource.ID != "1" || e.DedupKey != "src-1" || e.Bookmark != nil {
		t.Errorf("event = %+v", e)
	}
	if !e.OccurredAt.Equal(time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)) {
		t.Errorf("occurred_at = %v", e.OccurredAt)
	}
}

func TestNormalizeCanonicalDedupHash(t *testing.T) {
	mk := func(provider, id string) string {
		body := `{"type":"bookmark.created","resource":{"id":"` + id + `"},"occurred_at":"2026-11-03T10:00:00Z"}`
		evs, err := NormalizeCanonical(provider, []byte(body))
		if err != nil {
			t.Fatal(err)
		}
		return evs[0].DedupKey
	}
	a := mk("fake", "1")
	if !strings.HasPrefix(a, "sha256:") || a != mk("fake", "1") {
		t.Errorf("key %q is not stable", a)
	}
	if a == mk("fake", "2") || a == mk("other", "1") {
		t.Error("keys must differ by resource and provider")
	}
}

func TestNormalizeCanonicalHighlight(t *testing.T) {
	ok := `{"type":"highlight.created","resource":{"id":"h1"},"bookmark":{"url":"https://e.test"}}`
	evs, err := NormalizeCanonical("fake", []byte(ok))
	if err != nil || evs[0].Bookmark == nil || evs[0].Bookmark.URL != "https://e.test" {
		t.Fatalf("got %+v, %v", evs, err)
	}
	if _, err := NormalizeCanonical("fake", []byte(`{"type":"highlight.created","resource":{"id":"h1"}}`)); !errors.Is(err, domain.ErrValidation) {
		t.Errorf("missing parent: %v", err)
	}
}

func TestNormalizeCanonicalInvalid(t *testing.T) {
	for name, body := range map[string]string{
		"not json":     `{`,
		"unknown type": `{"type":"bookmark.deleted","resource":{"id":"1"}}`,
		"no resource":  `{"type":"bookmark.created","resource":{}}`,
	} {
		if _, err := NormalizeCanonical("fake", []byte(body)); !errors.Is(err, domain.ErrValidation) {
			t.Errorf("%s: err = %v, want ErrValidation", name, err)
		}
	}
}
