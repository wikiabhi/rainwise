package domain

import "time"

type Bookmark struct {
	ID          ID                `json:"id"`
	URL         string            `json:"url"`
	Title       string            `json:"title"`
	Description string            `json:"description"`
	Excerpt     string            `json:"excerpt"`
	Tags        []string          `json:"tags"`
	Collection  ID                `json:"collection"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
	Metadata    map[string]string `json:"metadata,omitempty"` // opaque to core
	// Highlights is set when the read that returned this bookmark included them.
	// Empty means it has none; nil means the read didn't say.
	Highlights []Highlight `json:"highlights,omitempty"`
}

type Highlight struct {
	ID        ID        `json:"id"`
	Bookmark  ID        `json:"bookmark"`
	Text      string    `json:"text"`
	Note      string    `json:"note"`
	Tags      []string  `json:"tags"`
	CreatedAt time.Time `json:"created_at"`
}

type Collection struct {
	ID          ID     `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Parent      *ID    `json:"parent,omitempty"`
}

type EventType string

const (
	EventBookmarkCreated  EventType = "bookmark.created"
	EventHighlightCreated EventType = "highlight.created"
	EventBookmarkMoved    EventType = "bookmark.moved"
)

// ResourceRef is an ID, or a URL to resolve.
type ResourceRef struct {
	ID  string `json:"id,omitempty"`
	URL string `json:"url,omitempty"`
}

type Event struct {
	Provider   string            `json:"provider"`
	Type       EventType         `json:"type"`
	Resource   ResourceRef       `json:"resource"`
	Bookmark   *ResourceRef      `json:"bookmark,omitempty"` // parent, for highlight events
	OccurredAt time.Time         `json:"occurred_at"`
	DedupKey   string            `json:"dedup_key"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

type Page[T any] struct {
	Items     []T    `json:"items"`
	NextToken string `json:"next_token,omitempty"`
}

type TagCount struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

// Event statuses.
const (
	EventPending = "pending"
	EventHeld    = "held"
	EventDone    = "done"
	EventFailed  = "failed"
)

type StoredEvent struct {
	Event
	ID          string    `json:"id"`
	DedupKey    string    `json:"dedup_key"`
	Status      string    `json:"status"`
	Attempts    int       `json:"attempts"`
	NextAttempt time.Time `json:"next_attempt"`
	ReceivedAt  time.Time `json:"received_at"`
}

type Snapshot struct {
	ID                 string       `json:"id"`
	Provider           string       `json:"provider"`
	JobID              string       `json:"job_id"`
	CreatedAt          time.Time    `json:"created_at"`
	Collections        []Collection `json:"collections"`
	CreatedCollections []ID         `json:"created_collections"`
	ScopeAll           bool         `json:"scope_all"`
	// FirstBackfillDone is the first_backfill_done setting when the snapshot was
	// taken, "" if unset. Only a whole-library snapshot records it.
	FirstBackfillDone string `json:"first_backfill_done,omitempty"`
}

type SnapshotItem struct {
	Bookmark   ID         `json:"bookmark"`
	Collection ID         `json:"collection"`
	Tags       []string   `json:"tags"`
	PinSource  PinSource  `json:"pin_source,omitempty"`
	PinnedAt   *time.Time `json:"pinned_at,omitempty"`
}
