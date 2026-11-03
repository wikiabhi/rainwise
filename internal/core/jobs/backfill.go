package jobs

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/wikiabhi/rainwise/internal/core/pipeline"
	"github.com/wikiabhi/rainwise/internal/core/taxonomy"
	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

const (
	firstBackfillPrefix = "first_backfill_done:"
	decisionPageSize    = 500
)

// backfillJob is §9.4, and with a dynamic draft taxonomy the proposal of §8.3.
type backfillJob struct{ Deps }

func (h backfillJob) Run(ctx context.Context, job *domain.Job, rc RunContext) error {
	return h.runBatch(ctx, job, rc, pipeline.TriggerBackfill)
}

// runBatch is the shared body of backfill and reclassify: set up, then page
// through the library in scope.
func (d Deps) runBatch(ctx context.Context, job *domain.Job, rc RunContext, trigger pipeline.Trigger) error {
	d.startTime(job)
	if job.Checkpoint.Phase == phaseDone {
		return d.finishBatch(ctx, job, trigger)
	}
	t, err := d.loadTaxonomy(ctx, job)
	if err != nil {
		return err
	}
	match, err := d.newScopeMatcher(ctx, job.Scope, t)
	if err != nil {
		return err
	}
	var scan *libraryScan
	if job.Checkpoint.Phase == "" {
		if trigger == pipeline.TriggerBackfill {
			if t, scan, err = d.proposeDraft(ctx, job, rc, t, match); err != nil {
				return err
			}
			if job.Params.ProposeOnly {
				return nil
			}
		}
		if !job.DryRun {
			if err := d.prepareWrites(ctx, job, rc, t); err != nil {
				return err
			}
		}
	}
	resolved, err := d.resolveForRun(ctx, job, t)
	if err != nil {
		return err
	}
	terms, scan, err := d.vocabularyTerms(ctx, job, rc, match, scan)
	if err != nil {
		return err
	}
	opts := pipeline.Options{
		Trigger:         trigger,
		JobID:           job.ID,
		SnapshotID:      job.SnapshotID,
		DryRun:          job.DryRun,
		Force:           job.Params.Force || trigger == pipeline.TriggerReclassify,
		IncludePinned:   job.Params.IncludePinned,
		Taxonomy:        t,
		Resolved:        resolved,
		Vocab:           taxonomy.NewRunVocab(terms, t.Rules.TagAliases, job.DryRun && t.Rules.AllowNewTags),
		ApprovedNewTags: job.Params.ApprovedNewTags,
	}
	if trigger == pipeline.TriggerBackfill {
		if err := d.applyPinRules(ctx, job, &opts); err != nil {
			return err
		}
	}
	switch {
	case scan != nil:
		job.Progress.Total = scan.matched
	case len(job.Scope.IDs) > 0 && len(job.Scope.Collections)+len(job.Scope.Tags) == 0:
		job.Progress.Total = len(job.Scope.IDs)
	}
	job.Checkpoint.Phase = phaseProcess
	if err := rc.Checkpoint(ctx); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}

	process := func(ctx context.Context, item pipeline.WorkItem) (pipeline.Outcome, error) {
		itemOpts := opts
		if job.FromDryRun != "" {
			plan, err := d.planFor(ctx, job.FromDryRun, item.Bookmark, resolved)
			if err != nil {
				return pipeline.Outcome{}, err
			}
			if plan == nil {
				return pipeline.Outcome{Status: pipeline.Skipped, Record: domain.DecisionRecord{
					JobID: job.ID, Bookmark: item.Bookmark, Reason: "not in the plan",
				}}, nil
			}
			itemOpts.Plan = plan
		}
		out, err := d.Pipeline.Process(ctx, item, itemOpts)
		logFailure(ctx, job, item, err)
		return out, err
	}
	if err := RunItems(ctx, job, rc, d.loopConfig(), d.bookmarkLister(job, nil, match), process); err != nil {
		return err
	}
	job.Progress.Total = job.Progress.Processed
	job.Checkpoint.Phase, job.Checkpoint.PageToken = phaseDone, ""
	if err := rc.Checkpoint(ctx); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}
	return d.finishBatch(ctx, job, trigger)
}

// proposeDraft stores a proposal on a dynamic draft that has no collections yet,
// or on any draft with propose_only. A draft that has them may hold the user's
// edits, which a later dry run must not replace.
func (d Deps) proposeDraft(ctx context.Context, job *domain.Job, rc RunContext, t domain.Taxonomy, match scopeMatcher) (domain.Taxonomy, *libraryScan, error) {
	draft := job.DryRun && t.Mode == domain.ModeDynamic && t.Status == domain.TaxonomyDraft
	if job.Params.ProposeOnly && !draft {
		return t, nil, fmt.Errorf("propose_only needs a dry run on a dynamic draft taxonomy: %w", domain.ErrValidation)
	}
	if !draft || (len(t.Collections) > 0 && !job.Params.ProposeOnly) {
		return t, nil, nil
	}
	if d.Proposer == nil {
		return t, nil, fmt.Errorf("taxonomy %q needs a proposal, and no proposer is configured: %w", t.ID, domain.ErrValidation)
	}
	scan, err := d.scanLibrary(ctx, job, rc, match, true)
	if err != nil {
		return t, nil, err
	}
	existing, err := d.collectionNames(ctx)
	if err != nil {
		return t, nil, err
	}
	proposed, err := taxonomy.Propose(ctx, d.Proposer, sampleEvenly(scan.subjects, t.Dynamic.SampleSize), existing, t)
	if err != nil {
		return t, nil, err
	}
	version := t.Version
	saved, err := d.Taxonomies.Put(ctx, t.ID, proposed, &version)
	if err != nil {
		return t, nil, fmt.Errorf("store proposal: %w", err)
	}
	return saved, scan, nil
}

func (d Deps) collectionNames(ctx context.Context) ([]string, error) {
	m := d.manager()
	if m == nil {
		return nil, nil
	}
	cols, err := m.ListCollections(ctx)
	if errors.Is(err, domain.ErrCapabilityUnsupported) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list collections: %w", err)
	}
	names := make([]string, 0, len(cols))
	for _, c := range cols {
		names = append(names, c.Name)
	}
	return names, nil
}

// sampleEvenly picks up to size items, evenly spaced through all. Without a
// positive size it keeps all of them.
func sampleEvenly[T any](all []T, size int) []T {
	if size <= 0 || len(all) <= size {
		return all
	}
	out := make([]T, 0, size)
	for i := range size {
		out = append(out, all[i*len(all)/size])
	}
	return out
}

// prepareWrites does what a real job must before it changes anything: refuse what
// can't work, then take a snapshot and wait until it's durable (§9.4). A resumed
// job keeps its snapshot, and waits only if it never got past the wait.
func (d Deps) prepareWrites(ctx context.Context, job *domain.Job, rc RunContext, t domain.Taxonomy) error {
	if err := d.checkCollections(ctx, t); err != nil {
		return err
	}
	if job.SnapshotID == "" {
		if d.Snapshots == nil {
			return fmt.Errorf("%s needs a snapshot, and no snapshot taker is configured: %w", job.Type, domain.ErrValidation)
		}
		id, err := d.Snapshots.Take(ctx, job.Provider, job.Scope, job.ID)
		if err != nil {
			return fmt.Errorf("take snapshot: %w", err)
		}
		job.SnapshotID = id
		if err := rc.Checkpoint(ctx); err != nil {
			return fmt.Errorf("save checkpoint: %w", err)
		}
	}
	if d.DurableWait <= 0 {
		return stopErr(ctx, rc)
	}
	return sleepUnlessStopped(ctx, rc, d.Sleep, d.DurableWait)
}

// applyPinRules fills in §9.1a: whether this is the first backfill, and its pin
// cutoff, which is kept in the checkpoint so a resume uses the same one.
func (d Deps) applyPinRules(ctx context.Context, job *domain.Job, opts *pipeline.Options) error {
	_, done, err := d.Store.BookmarkStates().GetSetting(ctx, firstBackfillPrefix+d.Provider.Name())
	if err != nil {
		return err
	}
	opts.IsFirstBackfill = !done
	if job.Checkpoint.PinCutoff == nil {
		cutoff := d.startTime(job)
		if job.FromDryRun != "" {
			plan, err := d.Store.Jobs().GetJob(ctx, job.FromDryRun)
			if err != nil {
				return fmt.Errorf("load dry run %s: %w", job.FromDryRun, err)
			}
			cutoff = plan.CreatedAt
			if plan.StartedAt != nil {
				cutoff = *plan.StartedAt
			}
		}
		job.Checkpoint.PinCutoff = &cutoff
	}
	opts.PinCutoff = job.Checkpoint.PinCutoff
	return nil
}

// planFor returns the dry run's decision on a bookmark, or nil if it made none, in
// which case an apply job leaves the bookmark alone: applying a plan writes only
// what was reviewed (§7.3), and sync or a later job covers the rest. A target that
// was only planned becomes the collection now provisioned. If the
// taxonomy has since dropped it, the bookmark is classified again, as one whose
// hash changed would be.
func (d Deps) planFor(ctx context.Context, dryRunID string, id domain.ID, resolved map[string]domain.ID) (*domain.DecisionRecord, error) {
	rec, ok, err := d.Store.Jobs().GetDecision(ctx, dryRunID, id)
	if err != nil {
		return nil, fmt.Errorf("load plan for %s: %w", id, err)
	}
	if !ok {
		return nil, nil
	}
	if rec.TargetCollection != nil {
		if key, planned := strings.CutPrefix(rec.TargetCollection.Local, plannedPrefix); planned {
			if real, found := resolved[key]; found {
				rec.TargetCollection = &real
			} else {
				rec.InputHash = ""
			}
		}
	}
	return &rec, nil
}

// finishBatch closes a job whose items are all done. A whole-library backfill
// that finished, even with some item failures, is the first backfill: failures
// mustn't keep later manual moves from becoming final (§9.1a).
func (d Deps) finishBatch(ctx context.Context, job *domain.Job, trigger pipeline.Trigger) error {
	if trigger != pipeline.TriggerBackfill || job.DryRun || !job.Scope.IsEmpty() {
		return nil
	}
	states := d.Store.BookmarkStates()
	key := firstBackfillPrefix + d.Provider.Name()
	_, done, err := states.GetSetting(ctx, key)
	if err != nil || done {
		return err
	}
	if err := states.PutSetting(ctx, key, d.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return fmt.Errorf("save first backfill: %w", err)
	}
	return nil
}

// ProposedTag is a new tag a dry run would add, and on how many bookmarks.
type ProposedTag struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

// ProposedTags reports a job's new tags, most used first. It reads the stored
// decisions, so it holds up when a dry run was resumed partway.
func ProposedTags(ctx context.Context, store ports.Store, jobID string) ([]ProposedTag, error) {
	js := store.Jobs()
	if _, err := js.GetJob(ctx, jobID); err != nil {
		return nil, err
	}
	counts := map[string]int{}
	for cursor := ""; ; {
		page, err := js.ListDecisions(ctx, jobID, cursor, decisionPageSize)
		if err != nil {
			return nil, err
		}
		for _, rec := range page.Items {
			for _, t := range rec.NewTags {
				counts[t]++
			}
		}
		if page.NextToken == "" {
			break
		}
		cursor = page.NextToken
	}
	out := make([]ProposedTag, 0, len(counts))
	for name, n := range counts {
		out = append(out, ProposedTag{Name: name, Count: n})
	}
	slices.SortFunc(out, func(a, b ProposedTag) int {
		return cmp.Or(cmp.Compare(b.Count, a.Count), strings.Compare(a.Name, b.Name))
	})
	return out, nil
}
