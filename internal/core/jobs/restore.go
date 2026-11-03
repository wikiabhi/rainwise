package jobs

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/wikiabhi/rainwise/internal/core/pipeline"
	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

// phaseCleanup comes after the bookmarks of a real restore: only removing created
// collections and closing up are left.
const phaseCleanup = "cleanup"

// Results of a restore's decision records.
const (
	restoreProposed = "proposed"
	restorePending  = "pending"
	restoreApplied  = "applied"
	restoreFailed   = "failed"
	restoreSkipped  = "skipped"
)

const statusRestored = "restored"

// RestoreDiff is one change a restore makes or, in a dry run, would make. It's
// stored as a decision record: ObservedCollection, TargetCollection, ObservedTags,
// and SetTags.
type RestoreDiff struct {
	Bookmark       domain.ID `json:"bookmark"`
	FromCollection domain.ID `json:"from_collection"`
	ToCollection   domain.ID `json:"to_collection"`
	FromTags       []string  `json:"from_tags"`
	ToTags         []string  `json:"to_tags"`
}

func (d RestoreDiff) record(jobID, reason string) domain.DecisionRecord {
	to := d.ToCollection
	return domain.DecisionRecord{
		JobID:              jobID,
		Bookmark:           d.Bookmark,
		ObservedCollection: d.FromCollection,
		ObservedTags:       d.FromTags,
		TargetCollection:   &to,
		SetTags:            d.ToTags,
		Reason:             reason,
	}
}

// RestoreDiffs reads a restore job's changes, leaving out what it skipped.
func RestoreDiffs(ctx context.Context, store ports.Store, jobID string) ([]RestoreDiff, error) {
	var out []RestoreDiff
	for cursor := ""; ; {
		page, err := store.Jobs().ListDecisions(ctx, jobID, cursor, decisionPageSize)
		if err != nil {
			return nil, err
		}
		for _, rec := range page.Items {
			if rec.TargetCollection == nil {
				continue
			}
			out = append(out, RestoreDiff{
				Bookmark:       rec.Bookmark,
				FromCollection: rec.ObservedCollection,
				ToCollection:   *rec.TargetCollection,
				FromTags:       rec.ObservedTags,
				ToTags:         rec.SetTags,
			})
		}
		if page.NextToken == "" {
			return out, nil
		}
		cursor = page.NextToken
	}
}

// restoreJob is §11. Job.Params.SnapshotID is the snapshot to go back to, and
// Job.SnapshotID the one it takes of itself first, which undoes it.
//
// Every step can run again: bookmarks already matching need no write, collections
// are found by name before being made, and the snapshot it took stays on the job.
type restoreJob struct{ Deps }

func (h restoreJob) Run(ctx context.Context, job *domain.Job, rc RunContext) error {
	d := h.Deps
	d.startTime(job)
	if job.Checkpoint.Phase == phaseDone {
		return nil
	}
	snap, err := d.Store.Snapshots().GetSnapshot(ctx, job.Params.SnapshotID)
	if err != nil {
		return fmt.Errorf("load snapshot %q: %w", job.Params.SnapshotID, err)
	}
	if snap.Provider != d.Provider.Name() {
		return fmt.Errorf("snapshot %s is of provider %q: %w", snap.ID, snap.Provider, domain.ErrValidation)
	}
	if job.Checkpoint.Phase == "" {
		if err := d.prepareRestore(ctx, job, rc, snap); err != nil {
			return err
		}
	}
	cols, err := d.newRestoreCollections(ctx, job, snap)
	if err != nil {
		return err
	}
	run := &restoreRun{Deps: d, job: job, snap: snap, cols: cols}
	if job.Checkpoint.Phase == phaseProcess {
		if err := run.restoreItems(ctx, rc); err != nil {
			return err
		}
		job.Checkpoint.Phase, job.Checkpoint.PageToken = phaseCleanup, ""
		if job.DryRun {
			job.Checkpoint.Phase = phaseDone
		}
		if err := rc.Checkpoint(ctx); err != nil {
			return fmt.Errorf("save checkpoint: %w", err)
		}
	}
	if job.DryRun {
		return nil
	}
	if err := stopErr(ctx, rc); err != nil {
		return err
	}
	if err := run.removeCreatedCollections(ctx); err != nil {
		return err
	}
	if err := d.clearFirstBackfill(ctx, snap); err != nil {
		return err
	}
	job.Checkpoint.Phase = phaseDone
	if err := rc.Checkpoint(ctx); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}
	return nil
}

// prepareRestore refuses what can't work, counts the work, and for a real restore
// takes the snapshot that undoes it, before anything changes. That snapshot covers
// what this restore can touch: the bookmarks of the one it goes back to.
func (d Deps) prepareRestore(ctx context.Context, job *domain.Job, rc RunContext, snap domain.Snapshot) error {
	if _, err := d.firstBackfillAfter(ctx, snap); err != nil {
		return err
	}
	ids, err := d.snapshotBookmarks(ctx, snap.ID)
	if err != nil {
		return err
	}
	job.Progress.Total = len(ids)
	if !job.DryRun {
		// With no bookmarks there's nothing to name, and an empty scope is a
		// superset: the whole library, which is safe to record.
		scope := domain.Scope{IDs: ids}
		if snap.ScopeAll {
			scope = domain.Scope{}
		}
		if err := d.takeSnapshot(ctx, job, rc, scope); err != nil {
			return err
		}
	}
	job.Checkpoint.Phase = phaseProcess
	if err := rc.Checkpoint(ctx); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}
	return nil
}

func (d Deps) snapshotBookmarks(ctx context.Context, id string) ([]string, error) {
	var ids []string
	for cursor := ""; ; {
		page, err := d.Store.Snapshots().ListSnapshotItems(ctx, id, cursor, decisionPageSize)
		if err != nil {
			return nil, fmt.Errorf("read snapshot %s: %w", id, err)
		}
		for _, it := range page.Items {
			ids = append(ids, it.Bookmark.String())
		}
		if page.NextToken == "" {
			return ids, nil
		}
		cursor = page.NextToken
	}
}

// firstBackfillAfter reports whether a first backfill finished after this snapshot
// was taken, in which case restoring it makes the next backfill a first one again
// (§9.1a). A time that can't be read is an error, not a guess in either direction.
func (d Deps) firstBackfillAfter(ctx context.Context, snap domain.Snapshot) (bool, error) {
	raw, ok, err := d.Store.BookmarkStates().GetSetting(ctx, firstBackfillPrefix+snap.Provider)
	if err != nil || !ok {
		return false, err
	}
	at, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return false, fmt.Errorf("parse first backfill time %q: %w", raw, err)
	}
	return at.After(snap.CreatedAt), nil
}

func (d Deps) clearFirstBackfill(ctx context.Context, snap domain.Snapshot) error {
	after, err := d.firstBackfillAfter(ctx, snap)
	if err != nil || !after {
		return err
	}
	if err := d.Store.BookmarkStates().DeleteSetting(ctx, firstBackfillPrefix+snap.Provider); err != nil {
		return fmt.Errorf("clear first backfill: %w", err)
	}
	return nil
}

// restoreRun is the working state of one run of a restore job.
type restoreRun struct {
	Deps
	job  *domain.Job
	snap domain.Snapshot
	cols *restoreCollections
}

func (r *restoreRun) restoreItems(ctx context.Context, rc RunContext) error {
	// Written by the listing and read by the items of that page, which RunItems
	// starts afterwards and waits for before it lists again.
	var page map[domain.ID]domain.SnapshotItem
	list := func(ctx context.Context, token string) ([]pipeline.WorkItem, string, error) {
		p, err := r.Store.Snapshots().ListSnapshotItems(ctx, r.snap.ID, token, cmp.Or(r.job.Params.BatchSize, decisionPageSize))
		if err != nil {
			return nil, "", err
		}
		page = make(map[domain.ID]domain.SnapshotItem, len(p.Items))
		items := make([]pipeline.WorkItem, 0, len(p.Items))
		for _, it := range p.Items {
			page[it.Bookmark] = it
			items = append(items, pipeline.WorkItem{Bookmark: it.Bookmark})
		}
		return items, p.NextToken, nil
	}
	process := func(ctx context.Context, item pipeline.WorkItem) (pipeline.Outcome, error) {
		unlock, err := r.Pipeline.LockBookmark(ctx, item.Bookmark)
		if err != nil {
			return pipeline.Outcome{}, err
		}
		defer unlock()
		out, err := r.restoreItem(ctx, page[item.Bookmark])
		logFailure(ctx, r.job, item, err)
		return out, err
	}
	return RunItems(ctx, r.job, rc, r.loopConfig(), list, process)
}

func (r *restoreRun) restoreItem(ctx context.Context, item domain.SnapshotItem) (pipeline.Outcome, error) {
	b, err := r.Provider.GetBookmark(ctx, item.Bookmark)
	if errors.Is(err, domain.ErrNotFound) {
		return r.skip(ctx, item.Bookmark, "bookmark no longer exists")
	}
	if err != nil {
		return pipeline.Outcome{}, fmt.Errorf("read bookmark %s: %w", item.Bookmark, err)
	}
	trashed, err := r.cols.isUnlisted(ctx, b.Collection)
	if err != nil {
		return pipeline.Outcome{}, err
	}
	if trashed {
		return r.skip(ctx, item.Bookmark, "bookmark is in a collection the provider doesn't list, such as its trash")
	}

	target, note, err := r.cols.target(ctx, item.Collection)
	if err != nil {
		return pipeline.Outcome{}, fmt.Errorf("collection for %s: %w", item.Bookmark, err)
	}
	if target.IsZero() {
		target = b.Collection
	}
	// Never nil, which in a patch would mean "leave the tags as they are".
	tags := append([]string{}, item.Tags...)
	moved := target != b.Collection
	if !moved && sameTags(b.Tags, tags) {
		return r.alreadyMatches(ctx, b, target)
	}
	rec := RestoreDiff{Bookmark: b.ID, FromCollection: b.Collection, ToCollection: target, FromTags: b.Tags, ToTags: tags}.record(r.job.ID, note)
	if r.job.DryRun {
		rec.Result = restoreProposed
		if err := r.saveDecision(ctx, rec); err != nil {
			return pipeline.Outcome{}, err
		}
		return pipeline.Outcome{Status: pipeline.Changed, Record: rec}, nil
	}
	return r.write(ctx, b, target, tags, moved, rec)
}

// write sets one bookmark, recording the move first as the pipeline does: if this
// process dies with the write landed, whoever reads the bookmark next can tell our
// move from the user's and sets no pin.
func (r *restoreRun) write(ctx context.Context, b domain.Bookmark, target domain.ID, tags []string, moved bool, rec domain.DecisionRecord) (pipeline.Outcome, error) {
	states := r.Store.BookmarkStates()
	patch := ports.BookmarkPatch{SetTags: tags}
	if moved {
		patch.Collection = &target
		st, err := r.loadState(ctx, b.ID)
		if err != nil {
			return pipeline.Outcome{}, err
		}
		// A known collection stays: if the bookmark holds a manual move not yet seen
		// and this write fails, the next read must still see that move.
		if st.KnownCollection.IsZero() {
			st.KnownCollection, st.KnownTags = b.Collection, b.Tags
		}
		st.PendingCollection, st.PendingTags = &target, nil
		if err := states.PutState(ctx, st); err != nil {
			return pipeline.Outcome{}, fmt.Errorf("save state of %s: %w", b.ID, err)
		}
	}
	rec.Result = restorePending
	if err := r.saveDecision(ctx, rec); err != nil {
		return pipeline.Outcome{}, err
	}
	updated, err := r.Provider.UpdateBookmark(ctx, b.ID, patch)
	if errors.Is(err, domain.ErrNotFound) {
		return r.writeNotFound(ctx, b, rec, err)
	}
	if err != nil {
		return pipeline.Outcome{}, fmt.Errorf("write bookmark %s: %w", b.ID, err)
	}
	if err := r.settle(ctx, updated, moved); err != nil {
		return pipeline.Outcome{}, err
	}
	return r.applied(ctx, rec)
}

// writeNotFound settles a write refused as not found, which may mean a stale target
// collection and not a deleted bookmark. Only a read can tell, and either way
// nothing landed.
func (r *restoreRun) writeNotFound(ctx context.Context, b domain.Bookmark, rec domain.DecisionRecord, writeErr error) (pipeline.Outcome, error) {
	st, err := r.loadState(ctx, b.ID)
	if err != nil {
		return pipeline.Outcome{}, err
	}
	st.PendingCollection, st.PendingTags = nil, nil
	if err := r.Store.BookmarkStates().PutState(ctx, st); err != nil {
		return pipeline.Outcome{}, fmt.Errorf("save state of %s: %w", b.ID, err)
	}
	_, readErr := r.Provider.GetBookmark(ctx, b.ID)
	if errors.Is(readErr, domain.ErrNotFound) {
		return r.skip(ctx, b.ID, "bookmark no longer exists")
	}
	rec.Result = restoreFailed
	rec.Reason = "provider reported not found on write"
	if err := r.saveDecision(ctx, rec); err != nil {
		return pipeline.Outcome{}, err
	}
	if readErr != nil {
		return pipeline.Outcome{}, fmt.Errorf("write bookmark %s: %w; re-read: %w", b.ID, writeErr, readErr)
	}
	return pipeline.Outcome{}, fmt.Errorf("write bookmark %s: %w", b.ID, writeErr)
}

// alreadyMatches handles a bookmark that needs no write. It may be one this job
// wrote before being interrupted, which still counts as this job's change.
func (r *restoreRun) alreadyMatches(ctx context.Context, b domain.Bookmark, target domain.ID) (pipeline.Outcome, error) {
	if r.job.DryRun {
		return pipeline.Outcome{Status: pipeline.Unchanged}, nil
	}
	rec, ours, err := r.Store.Jobs().GetDecision(ctx, r.job.ID, b.ID)
	if err != nil {
		return pipeline.Outcome{}, fmt.Errorf("load decision: %w", err)
	}
	ours = ours && (rec.Result == restorePending || rec.Result == restoreApplied)
	if err := r.settle(ctx, b, ours && rec.ObservedCollection != target); err != nil {
		return pipeline.Outcome{}, err
	}
	if !ours {
		return pipeline.Outcome{Status: pipeline.Unchanged}, nil
	}
	if rec.Result == restoreApplied {
		return pipeline.Outcome{Status: pipeline.Changed, Record: rec}, nil
	}
	return r.applied(ctx, rec)
}

func (r *restoreRun) applied(ctx context.Context, rec domain.DecisionRecord) (pipeline.Outcome, error) {
	now := r.Now()
	rec.Result, rec.AppliedAt = restoreApplied, &now
	if err := r.saveDecision(ctx, rec); err != nil {
		return pipeline.Outcome{}, err
	}
	return pipeline.Outcome{Status: pipeline.Changed, Record: rec}, nil
}

// settle records a restored bookmark as it now is, so nothing reads it as a manual
// move, and so sync leaves it alone until a backfill or reclassify (§11). A user
// pin newer than the snapshot marks a manual change, so if the restore has just
// moved the bookmark back, the pin no longer says anything true and it goes.
func (r *restoreRun) settle(ctx context.Context, b domain.Bookmark, movedBack bool) error {
	st, err := r.loadState(ctx, b.ID)
	if err != nil {
		return err
	}
	now := r.Now()
	st.KnownCollection, st.KnownTags = b.Collection, b.Tags
	st.PendingCollection, st.PendingTags = nil, nil
	st.Status = statusRestored
	st.ProcessedAt, st.RestoredAt = &now, &now
	if movedBack && st.PinSource == domain.PinUser && st.PinnedAt != nil && st.PinnedAt.After(r.snap.CreatedAt) {
		st.PinSource, st.PinnedAt = domain.PinNone, nil
	}
	if err := r.Store.BookmarkStates().PutState(ctx, st); err != nil {
		return fmt.Errorf("save state of %s: %w", b.ID, err)
	}
	return nil
}

func (r *restoreRun) loadState(ctx context.Context, id domain.ID) (domain.BookmarkState, error) {
	st, known, err := r.Store.BookmarkStates().GetState(ctx, id)
	if err != nil {
		return domain.BookmarkState{}, fmt.Errorf("load state of %s: %w", id, err)
	}
	if !known {
		st = domain.BookmarkState{ID: id}
	}
	return st, nil
}

func (r *restoreRun) skip(ctx context.Context, id domain.ID, reason string) (pipeline.Outcome, error) {
	rec := domain.DecisionRecord{JobID: r.job.ID, Bookmark: id, Reason: reason, Result: restoreSkipped}
	if err := r.saveDecision(ctx, rec); err != nil {
		return pipeline.Outcome{}, err
	}
	return pipeline.Outcome{Status: pipeline.Skipped, Record: rec}, nil
}

func (r *restoreRun) saveDecision(ctx context.Context, rec domain.DecisionRecord) error {
	if err := r.Store.Jobs().SaveDecision(ctx, rec); err != nil {
		return fmt.Errorf("save decision for %s: %w", rec.Bookmark, err)
	}
	return nil
}

// sameTags compares exactly but for order, which tags don't have: "Go" is not "go".
func sameTags(a, b []string) bool {
	a, b = slices.Clone(a), slices.Clone(b)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(a, b)
}

// removeCreatedCollections deletes, if empty, what Rainwise created since the
// snapshot: restoring an older snapshot undoes the later jobs too, whose records
// are on their own snapshots. A collection this restore goes back to stays, whether
// it's the one the snapshot had or its replacement by name. A delete that fails
// counts against the job but leaves the restored bookmarks standing.
func (r *restoreRun) removeCreatedCollections(ctx context.Context) error {
	m := r.manager()
	if m == nil {
		return nil
	}
	if err := r.cols.refresh(ctx); err != nil {
		return err
	}
	listed, ok := r.cols.current()
	if !ok {
		return nil
	}
	created, err := r.createdSinceSnapshot(ctx)
	if err != nil {
		return err
	}
	gone := map[domain.ID]bool{}
	had := map[domain.ID]bool{}
	replacedNames := map[string]bool{}
	for _, c := range r.snap.Collections {
		had[c.ID] = true
		if _, ok := listed[c.ID]; !ok {
			gone[c.ID] = true
			replacedNames[strings.ToLower(c.Name)] = true
		}
	}
	for _, id := range created {
		c, ok := listed[id]
		switch {
		case had[id]:
			continue
		case !ok:
			gone[id] = true // Already deleted, perhaps by a run that stopped short.
			continue
		case replacedNames[strings.ToLower(c.Name)]:
			continue
		}
		deleted, err := m.DeleteCollectionIfEmpty(ctx, id)
		switch {
		case errors.Is(err, domain.ErrNotFound):
			gone[id] = true
		case err != nil:
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Warn("deleting created collection", "job", r.job.ID, "collection", id.String(), "error", err)
			r.job.Progress.Failed++
		case deleted:
			gone[id] = true
		}
	}
	return r.forgetCollections(ctx, gone)
}

// createdSinceSnapshot lists the collections created for the snapshot's job and
// for later ones, newest first within each, so a child goes before its parent.
func (r *restoreRun) createdSinceSnapshot(ctx context.Context) ([]domain.ID, error) {
	var out []domain.ID
	seen := map[domain.ID]bool{}
	add := func(s domain.Snapshot) {
		for _, id := range slices.Backward(s.CreatedCollections) {
			if !seen[id] {
				seen[id] = true
				out = append(out, id)
			}
		}
	}
	for cursor := ""; ; {
		page, err := r.Store.Snapshots().ListSnapshots(ctx, cursor, decisionPageSize)
		if err != nil {
			return nil, fmt.Errorf("list snapshots: %w", err)
		}
		for _, s := range page.Items {
			if s.ID != r.snap.ID && s.Provider == r.snap.Provider && !s.CreatedAt.Before(r.snap.CreatedAt) {
				add(s)
			}
		}
		if page.NextToken == "" {
			break
		}
		cursor = page.NextToken
	}
	add(r.snap)
	return out, nil
}

// forgetCollections takes dead IDs off every taxonomy's auto-created list. A
// restore job needn't have a taxonomy, and it undoes the work of all of them.
func (r *restoreRun) forgetCollections(ctx context.Context, gone map[domain.ID]bool) error {
	if len(gone) == 0 {
		return nil
	}
	taxonomies, err := r.Store.Taxonomies().ListTaxonomies(ctx)
	if err != nil {
		return fmt.Errorf("list taxonomies: %w", err)
	}
	for _, t := range taxonomies {
		if err := r.Pipeline.ForgetCreated(ctx, t.ID, gone); err != nil {
			return err
		}
	}
	return nil
}

// restoreCollections knows which collections exist, and finds or makes what a
// snapshot's bookmarks go back to. Items run concurrently and may want the same
// missing collection, so it's all under one lock.
type restoreCollections struct {
	d      Deps
	job    *domain.Job
	snap   map[domain.ID]domain.Collection
	inbox  *domain.ID
	create bool

	mu       sync.Mutex
	listable bool // Without a listing, nothing can be said about what exists.
	listed   map[domain.ID]domain.Collection
	replaced map[domain.ID]domain.ID
}

func (d Deps) newRestoreCollections(ctx context.Context, job *domain.Job, snap domain.Snapshot) (*restoreCollections, error) {
	caps := d.Provider.Capabilities()
	c := &restoreCollections{
		d:        d,
		job:      job,
		snap:     make(map[domain.ID]domain.Collection, len(snap.Collections)),
		inbox:    caps.InboxCollection,
		create:   d.manager() != nil && caps.CreateCollections,
		replaced: map[domain.ID]domain.ID{},
	}
	for _, sc := range snap.Collections {
		c.snap[sc.ID] = sc
	}
	if err := c.refresh(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *restoreCollections) refresh(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.refreshLocked(ctx)
}

func (c *restoreCollections) refreshLocked(ctx context.Context) error {
	m := c.d.manager()
	if m == nil {
		return nil
	}
	cols, err := m.ListCollections(ctx)
	if errors.Is(err, domain.ErrCapabilityUnsupported) {
		c.listable = false
		return nil
	}
	if err != nil {
		return fmt.Errorf("list collections: %w", err)
	}
	c.listable = true
	c.listed = make(map[domain.ID]domain.Collection, len(cols))
	for _, col := range cols {
		c.listed[col.ID] = col
	}
	return nil
}

func (c *restoreCollections) current() (map[domain.ID]domain.Collection, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.listed, c.listable
}

func (c *restoreCollections) isInbox(id domain.ID) bool {
	return c.inbox != nil && *c.inbox == id
}

// isUnlisted reports a bookmark sitting in a collection the provider doesn't list.
// That isn't where a user files things: an inbox is declared and everything else
// is listed, unless it's newer than the listing, so that's checked first. It's
// where a provider's trash keeps a deleted bookmark readable, and "deleted" is what
// §11 skips.
func (c *restoreCollections) isUnlisted(ctx context.Context, id domain.ID) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	known := func() bool {
		_, ok := c.listed[id]
		return !c.listable || ok || id.IsZero() || c.isInbox(id)
	}
	if known() {
		return false, nil
	}
	if err := c.refreshLocked(ctx); err != nil {
		return false, err
	}
	return !known(), nil
}

// target gives the collection a snapshot's bookmark goes back to. One that neither
// the provider nor the snapshot lists is left for the provider to accept or refuse.
// In a dry run, a collection that would be made keeps its old ID, and note says so.
func (c *restoreCollections) target(ctx context.Context, id domain.ID) (domain.ID, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.targetLocked(ctx, id)
}

func (c *restoreCollections) targetLocked(ctx context.Context, id domain.ID) (domain.ID, string, error) {
	if _, ok := c.listed[id]; ok || !c.listable || id.IsZero() || c.isInbox(id) {
		return id, "", nil
	}
	if made, ok := c.replaced[id]; ok {
		return made, "", nil
	}
	col, ok := c.snap[id]
	if !ok {
		return id, "", nil
	}
	var parent *domain.ID
	if col.Parent != nil {
		p, _, err := c.targetLocked(ctx, *col.Parent)
		if err != nil {
			return domain.ID{}, "", err
		}
		parent = &p
	}
	if found, ok := c.findByName(col.Name, parent); ok {
		c.replaced[id] = found
		return found, "", nil
	}
	if c.job.DryRun {
		return id, fmt.Sprintf("would recreate collection %q", col.Name), nil
	}
	if !c.create {
		return domain.ID{}, "", fmt.Errorf("collection %q is gone and can't be recreated: %w", col.Name, domain.ErrCapabilityUnsupported)
	}
	made, err := c.d.manager().CreateCollection(ctx, domain.Collection{Name: col.Name, Description: col.Description, Parent: parent})
	if err != nil {
		return domain.ID{}, "", fmt.Errorf("recreate collection %q: %w", col.Name, err)
	}
	c.listed[made.ID] = made
	c.replaced[id] = made.ID
	// On the restore's own snapshot, so that undoing the restore removes it.
	if err := c.d.Store.Snapshots().AddCreatedCollection(ctx, c.job.SnapshotID, made.ID); err != nil {
		return domain.ID{}, "", fmt.Errorf("record created collection: %w", err)
	}
	return made.ID, "", nil
}

// findByName matches among siblings, ignoring case. Of several, the lowest ID wins,
// as in taxonomy.Resolve.
func (c *restoreCollections) findByName(name string, parent *domain.ID) (domain.ID, bool) {
	var best domain.ID
	found := false
	for _, col := range c.listed {
		sameParent := (col.Parent == nil) == (parent == nil) && (parent == nil || *col.Parent == *parent)
		if sameParent && strings.EqualFold(col.Name, name) && (!found || col.ID.String() < best.String()) {
			best, found = col.ID, true
		}
	}
	return best, found
}
