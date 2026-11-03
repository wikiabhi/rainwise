package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/wikiabhi/rainwise/internal/core/pipeline"
	"github.com/wikiabhi/rainwise/internal/core/taxonomy"
	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

// Checkpoint phases. A phase is saved as soon as the one before it is done, so a
// resumed job neither repeats finished passes nor reads an empty page token as
// "start over" (see RunItems).
const (
	phaseProcess    = "process"    // Setup is done; items are being processed.
	phaseHighlights = "highlights" // Sync only: bookmarks are done.
	phaseDone       = "done"       // Every item is done; only the closing write is left.
)

const defaultTagVocabSize = 200

// plannedPrefix marks, in a dry run's decisions, a collection the real run would
// provision. Applying the plan swaps it for the collection's real ID.
const plannedPrefix = "planned:"

// SnapshotTaker records the state of a scope before a job changes it.
type SnapshotTaker interface {
	Take(ctx context.Context, provider string, scope domain.Scope, jobID string) (string, error)
}

// Deps are what the job handlers work with. Proposer is needed only for dynamic
// taxonomies, and Snapshots only for snapshots, restores, and real backfills and
// reclassifies.
type Deps struct {
	Provider     ports.Provider
	Pipeline     *pipeline.Pipeline
	Taxonomies   *taxonomy.Service
	Proposer     ports.TaxonomyProposer
	Store        ports.Store
	Now          func() time.Time
	Concurrency  int
	TagVocabSize int
	DurableWait  time.Duration
	Sleep        func(context.Context, time.Duration) error
	Snapshots    SnapshotTaker
}

// Handlers returns the handler for each job type.
func Handlers(d Deps) map[domain.JobType]Handler {
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.Sleep == nil {
		d.Sleep = realSleep
	}
	if d.TagVocabSize <= 0 {
		d.TagVocabSize = defaultTagVocabSize
	}
	return map[domain.JobType]Handler{
		domain.JobSync:       syncJob{d},
		domain.JobBackfill:   backfillJob{d},
		domain.JobReclassify: reclassifyJob{d},
		domain.JobSnapshot:   snapshotJob{d},
		domain.JobRestore:    restoreJob{d},
	}
}

func (d Deps) loopConfig() LoopConfig {
	return LoopConfig{Concurrency: d.Concurrency, Sleep: d.Sleep}
}

// startTime returns when the job started, which the runner normally has set.
func (d Deps) startTime(job *domain.Job) time.Time {
	if job.StartedAt == nil {
		now := d.Now().UTC()
		job.StartedAt = &now
	}
	return *job.StartedAt
}

// loadTaxonomy gets the job's taxonomy. The service refuses a real job on a draft
// when it's created, but the draft may have been put back since.
func (d Deps) loadTaxonomy(ctx context.Context, job *domain.Job) (domain.Taxonomy, error) {
	t, err := d.Taxonomies.Get(ctx, job.TaxonomyID)
	if err != nil {
		return domain.Taxonomy{}, fmt.Errorf("load taxonomy %q: %w", job.TaxonomyID, err)
	}
	if !job.DryRun && t.Status == domain.TaxonomyDraft {
		return domain.Taxonomy{}, fmt.Errorf("taxonomy %q is a draft, so only dry runs can use it: %w", t.ID, domain.ErrConflict)
	}
	return t, nil
}

// manager returns the provider's collection manager, or nil.
func (d Deps) manager() ports.CollectionManager {
	m, _ := d.Provider.(ports.CollectionManager)
	return m
}

func (d Deps) canProvision(t domain.Taxonomy) bool {
	return t.ProvisionMissing && d.manager() != nil && d.Provider.Capabilities().CreateCollections
}

// resolve wraps taxonomy.Resolve for a provider that may not list collections.
func (d Deps) resolve(ctx context.Context, t domain.Taxonomy, provision bool, onCreate func(domain.ID)) (map[string]domain.ID, []string, error) {
	resolved, missing, err := taxonomy.Resolve(ctx, d.manager(), t, provision, onCreate)
	if errors.Is(err, domain.ErrCapabilityUnsupported) {
		return taxonomy.Resolve(ctx, nil, t, false, nil)
	}
	return resolved, missing, err
}

// checkCollections fails a real job whose collections are missing and can't be
// made (§8.2), before it does anything else.
func (d Deps) checkCollections(ctx context.Context, t domain.Taxonomy) error {
	_, missing, err := d.resolve(ctx, t, false, nil)
	if err != nil {
		return err
	}
	return missingErr(t, missing, d.canProvision(t))
}

func missingErr(t domain.Taxonomy, missing []string, canProvision bool) error {
	if len(missing) == 0 || canProvision {
		return nil
	}
	return fmt.Errorf("taxonomy %q: collections %s are missing and can't be provisioned: %w",
		t.ID, strings.Join(missing, ", "), domain.ErrValidation)
}

// resolveForRun gives a real job the IDs it will write, provisioning as allowed.
// A dry run creates nothing, so what's missing gets a planned ID: otherwise every
// decision on a fresh library would read as Needs Review, and the plan would be
// useless to apply.
func (d Deps) resolveForRun(ctx context.Context, job *domain.Job, t domain.Taxonomy) (map[string]domain.ID, error) {
	if job.DryRun {
		resolved, _, err := d.resolve(ctx, t, false, nil)
		if err != nil {
			return nil, err
		}
		for _, c := range t.Collections {
			if _, ok := resolved[c.Key]; !ok {
				resolved[c.Key] = domain.ID{Provider: d.Provider.Name(), Local: plannedPrefix + c.Key}
			}
		}
		return resolved, nil
	}
	var recordErr error
	var onCreate func(domain.ID)
	if job.SnapshotID != "" {
		onCreate = func(id domain.ID) {
			recordErr = errors.Join(recordErr, d.Store.Snapshots().AddCreatedCollection(ctx, job.SnapshotID, id))
		}
	}
	resolved, missing, err := d.resolve(ctx, t, d.canProvision(t), onCreate)
	if err != nil {
		return nil, errors.Join(err, recordErr)
	}
	if recordErr != nil {
		return nil, fmt.Errorf("record created collection: %w", recordErr)
	}
	if err := missingErr(t, missing, false); err != nil {
		return nil, err
	}
	return resolved, nil
}

// libraryScan is what one pass over all bookmarks found.
type libraryScan struct {
	tags     map[string]int
	subjects []ports.Subject // In-scope bookmarks, when asked for.
	matched  int
}

// scanLibrary lists every bookmark before any item is processed.
func (d Deps) scanLibrary(ctx context.Context, job *domain.Job, rc RunContext, match scopeMatcher, keepSubjects bool) (*libraryScan, error) {
	scan := &libraryScan{tags: map[string]int{}}
	for token := ""; ; {
		if err := stopErr(ctx, rc); err != nil {
			return nil, err
		}
		page, err := d.Provider.ListBookmarks(ctx, ports.ListQuery{PageToken: token, Limit: job.Params.BatchSize})
		if err != nil {
			return nil, fmt.Errorf("scan bookmarks: %w", err)
		}
		for _, b := range page.Items {
			for _, t := range b.Tags {
				scan.tags[t]++
			}
			if match.matches(b) {
				scan.matched++
				if keepSubjects {
					scan.subjects = append(scan.subjects, ports.Subject{Bookmark: domain.Bookmark{
						ID: b.ID, Title: b.Title, Description: b.Description, Tags: b.Tags,
					}})
				}
			}
		}
		if page.NextToken == "" {
			return scan, nil
		}
		token = page.NextToken
	}
}

// listedTags asks the provider for tag counts, reporting whether it could.
func (d Deps) listedTags(ctx context.Context) (map[string]int, bool, error) {
	return taxonomy.ListedTags(ctx, d.Provider)
}

// vocabularyTerms returns the most-used tags, from the tag lister if there is one.
// Otherwise they come from scan, and if that's nil from a new pass over the
// library, which is returned for its counts too.
func (d Deps) vocabularyTerms(ctx context.Context, job *domain.Job, rc RunContext, match scopeMatcher, scan *libraryScan) ([]string, *libraryScan, error) {
	counts, ok, err := d.listedTags(ctx)
	if err != nil {
		return nil, scan, err
	}
	if !ok {
		if scan == nil {
			if scan, err = d.scanLibrary(ctx, job, rc, match, false); err != nil {
				return nil, nil, err
			}
		}
		counts = scan.tags
	}
	terms := taxonomy.TopTerms(counts, d.TagVocabSize)
	return terms, scan, nil
}

// bookmarkLister lists the bookmarks in scope as work items.
func (d Deps) bookmarkLister(job *domain.Job, since *time.Time, match scopeMatcher) ListFunc {
	return func(ctx context.Context, token string) ([]pipeline.WorkItem, string, error) {
		page, err := d.Provider.ListBookmarks(ctx, ports.ListQuery{Since: since, PageToken: token, Limit: job.Params.BatchSize})
		if err != nil {
			return nil, "", err
		}
		items := make([]pipeline.WorkItem, 0, len(page.Items))
		for _, b := range page.Items {
			if match.matches(b) {
				items = append(items, pipeline.WorkItem{Bookmark: b.ID})
			}
		}
		return items, page.NextToken, nil
	}
}

// logFailure makes an item's error visible, since RunItems only counts it.
func logFailure(ctx context.Context, job *domain.Job, item pipeline.WorkItem, err error) {
	if err != nil && ctx.Err() == nil {
		slog.Warn("item failed", "job", job.ID, "bookmark", item.Bookmark.String(), "error", err)
	}
}
