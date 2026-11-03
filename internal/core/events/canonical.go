// Package events normalizes inbound webhook payloads into domain events.
package events

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/wikiabhi/rainwise/internal/domain"
)

type canonicalRef struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

type canonicalPayload struct {
	Type       string            `json:"type"`
	Resource   canonicalRef      `json:"resource"`
	Bookmark   *canonicalRef     `json:"bookmark"`
	OccurredAt time.Time         `json:"occurred_at"`
	EventID    string            `json:"event_id"`
	Metadata   map[string]string `json:"metadata"`
}

// NormalizeCanonical parses the canonical webhook payload. Events are hints:
// only references are kept, never content. Invalid payloads wrap
// domain.ErrValidation.
func NormalizeCanonical(provider string, body []byte) ([]domain.Event, error) {
	var p canonicalPayload
	dec := json.NewDecoder(bytes.NewReader(body))
	if err := dec.Decode(&p); err != nil {
		return nil, fmt.Errorf("webhook payload: %v: %w", err, domain.ErrValidation)
	}
	typ := domain.EventType(p.Type)
	switch typ {
	case domain.EventBookmarkCreated, domain.EventHighlightCreated, domain.EventBookmarkMoved:
	default:
		return nil, fmt.Errorf("unknown event type %q: %w", p.Type, domain.ErrValidation)
	}
	if p.Resource.ID == "" && p.Resource.URL == "" {
		return nil, fmt.Errorf("resource needs an id or url: %w", domain.ErrValidation)
	}
	ev := domain.Event{
		Provider:   provider,
		Type:       typ,
		Resource:   domain.ResourceRef{ID: p.Resource.ID, URL: p.Resource.URL},
		OccurredAt: p.OccurredAt,
		Metadata:   p.Metadata,
	}
	if typ == domain.EventHighlightCreated {
		if p.Bookmark == nil || (p.Bookmark.ID == "" && p.Bookmark.URL == "") {
			return nil, fmt.Errorf("highlight.created needs a bookmark id or url: %w", domain.ErrValidation)
		}
	}
	if p.Bookmark != nil && (p.Bookmark.ID != "" || p.Bookmark.URL != "") {
		ev.Bookmark = &domain.ResourceRef{ID: p.Bookmark.ID, URL: p.Bookmark.URL}
	}
	ev.DedupKey = p.EventID
	if ev.DedupKey == "" {
		h := sha256.New()
		for _, s := range []string{provider, p.Type, p.Resource.ID, p.Resource.URL, p.OccurredAt.UTC().Format(time.RFC3339Nano)} {
			fmt.Fprintf(h, "%d:%s|", len(s), s)
		}
		ev.DedupKey = "sha256:" + hex.EncodeToString(h.Sum(nil))
	}
	return []domain.Event{ev}, nil
}
