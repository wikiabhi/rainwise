package ports

import (
	"context"
	"time"

	"github.com/wikiabhi/rainwise/internal/domain"
)

type Store interface {
	Jobs() JobStore
	Events() EventStore
	BookmarkStates() StateStore
	Snapshots() SnapshotStore
	Taxonomies() TaxonomyStore
}

type JobStore interface {
	CreateJob(ctx context.Context, j domain.Job) error
	GetJob(ctx context.Context, id string) (domain.Job, error)
	FindJobByIdempotencyKey(ctx context.Context, key string) (domain.Job, bool, error)
	FindActiveJob(ctx context.Context, provider string, t domain.JobType) (domain.Job, bool, error)
	ListJobs(ctx context.Context, cursor string, limit int) (domain.Page[domain.Job], error)
	UpdateJob(ctx context.Context, j domain.Job) error
	// UpdateJobIf writes j only if the stored status is among from, and reports
	// whether it did. A job's status has more than one writer, and this is how they
	// avoid overwriting each other. ErrNotFound if the job doesn't exist.
	UpdateJobIf(ctx context.Context, j domain.Job, from ...domain.JobStatus) (bool, error)
	ClaimNextQueued(ctx context.Context, provider string, dryRun bool) (domain.Job, bool, error)
	ListRunning(ctx context.Context) ([]domain.Job, error)
	SaveDecision(ctx context.Context, d domain.DecisionRecord) error
	GetDecision(ctx context.Context, jobID string, b domain.ID) (domain.DecisionRecord, bool, error)
	ListDecisions(ctx context.Context, jobID, cursor string, limit int) (domain.Page[domain.DecisionRecord], error)
}

type EventStore interface {
	InsertEvent(ctx context.Context, e domain.StoredEvent) (inserted bool, err error)
	ClaimEvents(ctx context.Context, now time.Time, limit int) ([]domain.StoredEvent, error)
	UpdateEvent(ctx context.Context, e domain.StoredEvent) error
	DropHeldBefore(ctx context.Context, t time.Time) (int, error)
}

type StateStore interface {
	GetState(ctx context.Context, id domain.ID) (domain.BookmarkState, bool, error)
	PutState(ctx context.Context, s domain.BookmarkState) error
	GetSetting(ctx context.Context, key string) (string, bool, error)
	PutSetting(ctx context.Context, key, value string) error
	// DeleteSetting removes a setting, if there is one.
	DeleteSetting(ctx context.Context, key string) error
}

type SnapshotStore interface {
	CreateSnapshot(ctx context.Context, s domain.Snapshot) error
	AddSnapshotItems(ctx context.Context, id string, items []domain.SnapshotItem) error
	AddCreatedCollection(ctx context.Context, id string, c domain.ID) error
	GetSnapshot(ctx context.Context, id string) (domain.Snapshot, error)
	ListSnapshots(ctx context.Context, cursor string, limit int) (domain.Page[domain.Snapshot], error)
	ListSnapshotItems(ctx context.Context, id, cursor string, limit int) (domain.Page[domain.SnapshotItem], error)
	PruneSnapshots(ctx context.Context, keep int, protect []string) error
}

type TaxonomyStore interface {
	GetTaxonomy(ctx context.Context, id string) (domain.Taxonomy, error)
	ListTaxonomies(ctx context.Context) ([]domain.Taxonomy, error)
	// PutTaxonomy bumps Version; returns ErrConflict if ifVersion mismatches.
	PutTaxonomy(ctx context.Context, t domain.Taxonomy, ifVersion *int) (domain.Taxonomy, error)
	SeedTaxonomy(ctx context.Context, t domain.Taxonomy) (seeded bool, err error)
	// RecordProposal notes a collection the classifier suggested. It doesn't bump
	// Version, and a same-named proposal increments Count.
	RecordProposal(ctx context.Context, id string, p domain.CollectionProposal) error
}
