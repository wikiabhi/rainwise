package domain

import "time"

type JobType string

const (
	JobSync       JobType = "sync"
	JobBackfill   JobType = "backfill"
	JobReclassify JobType = "reclassify"
	JobSnapshot   JobType = "snapshot"
	JobRestore    JobType = "restore"
)

type JobStatus string

const (
	JobQueued              JobStatus = "queued"
	JobRunning             JobStatus = "running"
	JobSucceeded           JobStatus = "succeeded"
	JobSucceededWithErrors JobStatus = "succeeded_with_errors"
	JobFailed              JobStatus = "failed"
	JobCancelling          JobStatus = "cancelling"
	JobCanceled            JobStatus = "canceled"
)

type JobParams struct {
	BatchSize       int               `json:"batch_size,omitempty"`
	ProposeOnly     bool              `json:"propose_only,omitempty"`
	Force           bool              `json:"force,omitempty"`
	IncludePinned   bool              `json:"include_pinned,omitempty"`
	TagsOnly        bool              `json:"tags_only,omitempty"`
	SnapshotID      string            `json:"snapshot_id,omitempty"`
	ApprovedNewTags map[string]string `json:"approved_new_tags,omitempty"`
}

type Scope struct {
	Collections []string `json:"collections,omitempty"`
	Tags        []string `json:"tags,omitempty"`
	IDs         []string `json:"ids,omitempty"`
}

// IsEmpty reports a scope covering the whole library.
func (s Scope) IsEmpty() bool {
	return len(s.Collections)+len(s.Tags)+len(s.IDs) == 0
}

type JobProgress struct {
	Total       int `json:"total"`
	Processed   int `json:"processed"`
	Changed     int `json:"changed"`
	Skipped     int `json:"skipped"`
	NeedsReview int `json:"needs_review"`
	Failed      int `json:"failed"`
}

type Checkpoint struct {
	// Phase is how far a multi-pass job has got. PageToken belongs to the phase's
	// listing, so an empty token is only "the start" of that phase.
	Phase     string     `json:"phase,omitempty"`
	PageToken string     `json:"page_token,omitempty"`
	PinCutoff *time.Time `json:"pin_cutoff,omitempty"`
	Pauses    int        `json:"pauses,omitempty"`
	// HighlightFeed is set by a sync that listed a bookmark without saying what
	// highlights it has, so the listing can't stand in for the highlight feed.
	HighlightFeed bool `json:"highlight_feed,omitempty"`
}

type Job struct {
	ID             string      `json:"id"`
	Type           JobType     `json:"type"`
	Status         JobStatus   `json:"status"`
	Provider       string      `json:"provider"`
	TaxonomyID     string      `json:"taxonomy_id"`
	DryRun         bool        `json:"dry_run"`
	Params         JobParams   `json:"params"`
	Scope          Scope       `json:"scope"`
	FromDryRun     string      `json:"from_dry_run,omitempty"`
	SnapshotID     string      `json:"snapshot_id,omitempty"`
	IdempotencyKey string      `json:"idempotency_key,omitempty"`
	Error          string      `json:"error,omitempty"`
	Progress       JobProgress `json:"progress"`
	Checkpoint     Checkpoint  `json:"checkpoint"`
	CreatedAt      time.Time   `json:"created_at"`
	StartedAt      *time.Time  `json:"started_at,omitempty"`
	FinishedAt     *time.Time  `json:"finished_at,omitempty"`
}
