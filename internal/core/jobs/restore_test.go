package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	cfake "github.com/wikiabhi/rainwise/internal/adapters/classifier/fake"
	pfake "github.com/wikiabhi/rainwise/internal/adapters/provider/fake"
	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

const autoCreatedKey = "auto_created_collections:fake:default"

// tick moves the clock on, so that what happens next is later than what came before.
func (e *jobEnv) tick() {
	e.setNow(e.nowFn().Add(time.Minute))
}

func (e *jobEnv) restoreJob(snapshotID string, mods ...func(*domain.Job)) *domain.Job {
	e.t.Helper()
	e.tick()
	return e.newJob(domain.JobRestore, func(j *domain.Job) {
		j.Params.SnapshotID = snapshotID
		j.TaxonomyID = "" // §11 posts restores without one.
		for _, m := range mods {
			m(j)
		}
	})
}

func (e *jobEnv) restore(snapshotID string, mods ...func(*domain.Job)) *domain.Job {
	e.t.Helper()
	job := e.restoreJob(snapshotID, mods...)
	e.mustRun(job)
	return job
}

// diffs reads the job's decision records as diffs, leaving out what it skipped.
func (e *jobEnv) diffs(jobID string) map[domain.ID]RestoreDiff {
	e.t.Helper()
	diffs, err := RestoreDiffs(context.Background(), e.store, jobID)
	if err != nil {
		e.t.Fatal(err)
	}
	out := map[domain.ID]RestoreDiff{}
	for _, d := range diffs {
		out[d.Bookmark] = d
	}
	return out
}

func (e *jobEnv) decision(jobID string, id domain.ID) domain.DecisionRecord {
	e.t.Helper()
	rec, ok, err := e.store.Jobs().GetDecision(context.Background(), jobID, id)
	if err != nil || !ok {
		e.t.Fatalf("decision for %s: ok=%v err=%v", id, ok, err)
	}
	return rec
}

// libraryState is what a restore must get right.
type libraryState map[domain.ID]domain.SnapshotItem

func (e *jobEnv) library(n int) libraryState {
	e.t.Helper()
	out := libraryState{}
	for i := 1; i <= n; i++ {
		b, ok := e.provider.Bookmark(bmID(i))
		if !ok {
			e.t.Fatalf("no bookmark %d", i)
		}
		out[b.ID] = domain.SnapshotItem{Bookmark: b.ID, Collection: b.Collection, Tags: b.Tags}
	}
	return out
}

func (e *jobEnv) checkLibrary(want libraryState) {
	e.t.Helper()
	for id, w := range want {
		b, ok := e.provider.Bookmark(id)
		if !ok {
			e.t.Errorf("%s is gone", id)
			continue
		}
		if b.Collection != w.Collection {
			e.t.Errorf("%s in %s, want %s", id, b.Collection, w.Collection)
		}
		if !slices.Equal(b.Tags, w.Tags) && (len(b.Tags) != 0 || len(w.Tags) != 0) {
			e.t.Errorf("%s tags %q, want %q", id, b.Tags, w.Tags)
		}
	}
}

func (e *jobEnv) hasCollection(name string) bool {
	return slices.Contains(e.collectionNames(), name)
}

// sortedEnv has four bookmarks and a real snapshot service, and a taxonomy with a
// collection that a backfill will have to create.
func sortedEnv(t *testing.T) *jobEnv {
	t.Helper()
	e := newJobEnv(t)
	useRealSnapshots(e, 10)
	tx := fixedTaxonomy()
	tx.Collections = append(tx.Collections, domain.TaxonomyCollection{Key: "c", Name: "Gamma"})
	e.seedTaxonomy(tx)
	e.addBookmark(1, pfake.Inbox, "shared", "Keep")
	e.addBookmark(2, colAlpha)
	e.addBookmark(3, colBeta, "shared")
	e.addBookmark(4, colBeta, "zeta", "alpha")
	e.cls.Script(cfake.Result{Decision: classified("c", 0.9, "shared", "zeta")})
	return e
}

func TestRoundTrip(t *testing.T) {
	e := sortedEnv(t)
	before := e.library(4)

	job := e.backfill(nil)
	if job.Progress.Failed != 0 || job.SnapshotID == "" {
		t.Fatalf("backfill = %+v, snapshot %q", job.Progress, job.SnapshotID)
	}
	if !e.hasCollection("Gamma") || e.collectionOf(bmID(2)).Local == colAlpha.Local {
		t.Fatalf("the backfill changed nothing: %v", e.collectionNames())
	}
	if slices.Equal(e.tagsOf(bmID(2)), before[bmID(2)].Tags) {
		t.Fatal("the backfill added no tags")
	}

	restore := e.restore(job.SnapshotID)

	e.checkLibrary(before)
	if e.hasCollection("Gamma") {
		t.Errorf("created collection survived: %v", e.collectionNames())
	}
	if !e.hasCollection("Alpha") || !e.hasCollection("Beta") {
		t.Errorf("the user's collections went: %v", e.collectionNames())
	}
	p := restore.Progress
	if p.Total != 4 || p.Processed != 4 || p.Changed != 4 || p.Failed != 0 {
		t.Errorf("progress = %+v", p)
	}
	for _, w := range e.provider.Writes() {
		if w.Patch.SetTags != nil && len(w.Patch.AddTags) != 0 {
			t.Errorf("write mixes AddTags and SetTags: %+v", w)
		}
	}

	t.Run("restoring again changes nothing", func(t *testing.T) {
		writes := len(e.provider.Writes())
		again := e.restore(job.SnapshotID)
		if len(e.provider.Writes()) != writes {
			t.Errorf("wrote %+v", e.provider.Writes()[writes:])
		}
		if again.Progress.Changed != 0 || again.Progress.Skipped != 4 {
			t.Errorf("progress = %+v", again.Progress)
		}
		e.checkLibrary(before)
	})
}

// A restored bookmark that had no tags must get none: nil would mean "leave them".
func TestRestoreClearsTagsToNone(t *testing.T) {
	e := sortedEnv(t)
	snap := e.takeSnapshot(domain.Scope{}).SnapshotID
	e.provider.Add(domain.Bookmark{ID: bmID(2), URL: "https://example.test/2", Title: "Post 2", Tags: []string{"added"}, Collection: colAlpha})

	job := e.restore(snap)

	if got := e.tagsOf(bmID(2)); len(got) != 0 {
		t.Errorf("tags = %q", got)
	}
	writes := e.provider.Writes()
	if len(writes) != 1 || writes[0].Patch.SetTags == nil || writes[0].Patch.Collection != nil {
		t.Errorf("writes = %+v", writes)
	}
	if st := e.state(bmID(2)); len(st.KnownTags) != 0 || st.PendingCollection != nil {
		t.Errorf("state = %+v", st)
	}
	if d := e.diffs(job.ID)[bmID(2)]; !slices.Equal(d.FromTags, []string{"added"}) || len(d.ToTags) != 0 {
		t.Errorf("diff = %+v", d)
	}
}

func TestRestoreDryRun(t *testing.T) {
	e := sortedEnv(t)
	snap := e.takeSnapshot(domain.Scope{}).SnapshotID
	// A manual move, a tag change, both, a deletion, and one left as it was.
	e.provider.SetCollection(bmID(1), colAlpha)
	e.provider.Add(domain.Bookmark{ID: bmID(2), URL: "https://example.test/2", Title: "Post 2", Tags: []string{"new"}, Collection: colAlpha})
	e.provider.SetCollection(bmID(3), colReview)
	e.provider.Remove(bmID(4))
	e.addBookmark(5, colBeta, "after")
	collections := e.collectionNames()
	before := e.library(3)
	setting := "2020-01-01T00:00:00Z"
	e.putSetting(firstBackfillKey, setting)

	job := e.restore(snap, dryRun)

	if w := e.provider.Writes(); len(w) != 0 {
		t.Errorf("writes = %+v", w)
	}
	if got := e.collectionNames(); !slices.Equal(got, collections) {
		t.Errorf("collections = %v", got)
	}
	e.checkLibrary(before)
	if job.SnapshotID != "" || len(e.sleeps) != 1 { // The one wait is the snapshot job's.
		t.Errorf("own snapshot %q, sleeps %v", job.SnapshotID, e.sleeps)
	}
	if _, known, _ := e.store.BookmarkStates().GetState(context.Background(), bmID(1)); known {
		t.Error("a dry run saved bookmark state")
	}
	if v, _ := e.setting(firstBackfillKey); v != setting {
		t.Errorf("first backfill setting = %q", v)
	}

	diffs := e.diffs(job.ID)
	if len(diffs) != 3 {
		t.Fatalf("diffs = %+v", diffs)
	}
	d := diffs[bmID(1)]
	if d.FromCollection != colAlpha || d.ToCollection != pfake.Inbox || !slices.Equal(d.FromTags, d.ToTags) {
		t.Errorf("moved: %+v", d)
	}
	d = diffs[bmID(2)]
	if d.FromCollection != colAlpha || d.ToCollection != colAlpha || !slices.Equal(d.FromTags, []string{"new"}) || len(d.ToTags) != 0 {
		t.Errorf("retagged: %+v", d)
	}
	d = diffs[bmID(3)]
	if d.FromCollection != colReview || d.ToCollection != colBeta {
		t.Errorf("moved: %+v", d)
	}
	if rec := e.decision(job.ID, bmID(4)); rec.Result != restoreSkipped {
		t.Errorf("deleted: %+v", rec)
	}
	if rec := e.decision(job.ID, bmID(1)); rec.Result != "proposed" {
		t.Errorf("result = %q", rec.Result)
	}
	if job.Progress.Changed != 3 || job.Progress.Skipped != 1 || job.Progress.Failed != 0 {
		t.Errorf("progress = %+v", job.Progress)
	}
}

func TestRestoreDryRunNamesCollectionItWouldRecreate(t *testing.T) {
	e := sortedEnv(t)
	gamma := e.addGamma()
	e.addBookmark(5, gamma)
	snap := e.takeSnapshot(domain.Scope{}).SnapshotID
	e.provider.SetCollection(bmID(5), colBeta)
	e.deleteCollection(gamma)

	job := e.restore(snap, dryRun)

	if e.hasCollection("Gamma") || len(e.provider.Writes()) != 0 {
		t.Errorf("collections %v, writes %+v", e.collectionNames(), e.provider.Writes())
	}
	rec := e.decision(job.ID, bmID(5))
	if rec.TargetCollection == nil || *rec.TargetCollection != gamma || !strings.Contains(rec.Reason, "Gamma") {
		t.Errorf("decision = %+v", rec)
	}
}

func (e *jobEnv) addGamma() domain.ID {
	e.t.Helper()
	made, err := e.provider.CreateCollection(context.Background(), domain.Collection{Name: "Gamma", Description: "G"})
	if err != nil {
		e.t.Fatal(err)
	}
	return made.ID
}

func (e *jobEnv) deleteCollection(id domain.ID) {
	e.t.Helper()
	gone, err := e.provider.DeleteCollectionIfEmpty(context.Background(), id)
	if err != nil || !gone {
		e.t.Fatalf("delete %s: gone=%v err=%v", id, gone, err)
	}
}

func TestRestoreOverwritesManualChanges(t *testing.T) {
	e := sortedEnv(t)
	before := e.library(4)
	snap := e.takeSnapshot(domain.Scope{}).SnapshotID
	e.syncNow()

	// One the pipeline has noticed and pinned, one it hasn't yet.
	e.provider.SetCollection(bmID(2), colReview)
	e.tick()
	e.syncNow()
	if st := e.state(bmID(2)); st.PinSource != domain.PinUser {
		t.Fatalf("not pinned: %+v", st)
	}
	e.provider.SetCollection(bmID(3), colReview)

	e.restore(snap)

	e.checkLibrary(before)
	if st := e.state(bmID(2)); st.PinSource != domain.PinNone || st.PinnedAt != nil {
		t.Errorf("pin on a move the restore undid: %+v", st)
	}
}

func TestRestoreBringsBackFromInbox(t *testing.T) {
	e := sortedEnv(t)
	snap := e.takeSnapshot(domain.Scope{}).SnapshotID
	e.provider.SetCollection(bmID(2), pfake.Inbox)
	job := e.restore(snap)
	if e.collectionOf(bmID(2)) != colAlpha || job.Progress.Changed != 1 {
		t.Errorf("in %s, progress %+v", e.collectionOf(bmID(2)), job.Progress)
	}
}

// With no collection listing, nothing can be said about what exists or is trash, so
// a bookmark goes back to the ID it had.
func TestRestoreWithoutCollectionSupport(t *testing.T) {
	e := newJobEnv(t)
	useRealSnapshots(e, 10)
	inbox := pfake.Inbox
	e.provider.SetCapabilities(ports.ProviderCapabilities{InboxCollection: &inbox})
	e.addBookmark(1, colAlpha, "t")
	snap := e.takeSnapshot(domain.Scope{}).SnapshotID
	e.provider.SetCollection(bmID(1), colOther)

	job := e.restore(snap)

	if e.collectionOf(bmID(1)) != colAlpha || job.Progress.Failed != 0 {
		t.Errorf("in %s, progress %+v", e.collectionOf(bmID(1)), job.Progress)
	}
}

// A bookmark's pin goes back to what the snapshot recorded, whichever way it has
// changed since.
func TestRestoreReturnsPins(t *testing.T) {
	ctx := context.Background()
	e := sortedEnv(t)
	pinnedAt := t0.Add(-time.Hour)
	put := func(n int, known domain.ID, source domain.PinSource, at *time.Time) {
		t.Helper()
		if err := e.store.BookmarkStates().PutState(ctx, domain.BookmarkState{ID: bmID(n), KnownCollection: known, PinSource: source, PinnedAt: at}); err != nil {
			t.Fatal(err)
		}
	}
	put(1, pfake.Inbox, domain.PinUser, &pinnedAt)
	put(2, colAlpha, domain.PinBaseline, &pinnedAt)
	snap := e.takeSnapshot(domain.Scope{}).SnapshotID
	e.provider.SetCollection(bmID(1), colBeta)
	e.provider.SetCollection(bmID(2), colBeta)

	// A pin the snapshot didn't have goes, moved or not; one it had comes back.
	e.tick()
	movedBack := e.nowFn()
	put(3, colBeta, domain.PinUser, &movedBack)
	put(1, colBeta, domain.PinNone, nil)

	e.restore(snap)

	if st := e.state(bmID(1)); st.PinSource != domain.PinUser || st.PinnedAt == nil || !st.PinnedAt.Equal(pinnedAt) {
		t.Errorf("user pin: %+v", st)
	}
	if st := e.state(bmID(2)); st.PinSource != domain.PinBaseline || st.PinnedAt == nil || !st.PinnedAt.Equal(pinnedAt) {
		t.Errorf("baseline pin: %+v", st)
	}
	if st := e.state(bmID(3)); st.PinSource != domain.PinNone || st.PinnedAt != nil {
		t.Errorf("newer pin: %+v", st)
	}
}

func TestRestoreSkipsDeleted(t *testing.T) {
	t.Run("gone before the restore", func(t *testing.T) {
		e := sortedEnv(t)
		snap := e.takeSnapshot(domain.Scope{}).SnapshotID
		e.provider.Remove(bmID(2))
		e.provider.SetCollection(bmID(3), colAlpha)

		job := e.restore(snap)

		if job.Progress.Failed != 0 || job.Progress.Skipped != 3 || job.Progress.Changed != 1 {
			t.Errorf("progress = %+v", job.Progress)
		}
		if _, ok := e.provider.Bookmark(bmID(2)); ok {
			t.Error("deleted bookmark came back")
		}
		if rec := e.decision(job.ID, bmID(2)); rec.Result != restoreSkipped || rec.Reason == "" {
			t.Errorf("decision = %+v", rec)
		}
		if _, ok := e.diffs(job.ID)[bmID(2)]; ok {
			t.Error("a skipped bookmark shows as a diff")
		}
		if _, known, _ := e.store.BookmarkStates().GetState(context.Background(), bmID(2)); known {
			t.Error("restore wrote state for a deleted bookmark")
		}
	})

	t.Run("gone just before the write", func(t *testing.T) {
		e := sortedEnv(t)
		snap := e.takeSnapshot(domain.Scope{}).SnapshotID
		e.provider.SetCollection(bmID(2), colBeta)
		e.deps.Provider = &updateHook{listLog: e.logged, before: func(id domain.ID, _ ports.BookmarkPatch) error {
			if id == bmID(2) {
				e.provider.Remove(id)
			}
			return nil
		}}
		job := e.restore(snap)
		if job.Progress.Failed != 0 {
			t.Errorf("progress = %+v", job.Progress)
		}
		if rec := e.decision(job.ID, bmID(2)); rec.Result != restoreSkipped {
			t.Errorf("decision = %+v", rec)
		}
		if st := e.state(bmID(2)); st.PendingCollection != nil {
			t.Errorf("state = %+v", st)
		}
	})

	// A provider's trash keeps a deleted bookmark readable, under a collection it
	// doesn't list. Moving it out would undo the user's deletion.
	t.Run("sent to the trash", func(t *testing.T) {
		e := sortedEnv(t)
		snap := e.takeSnapshot(domain.Scope{}).SnapshotID
		e.provider.SetCollection(bmID(2), colTrash)

		job := e.restore(snap)

		if e.collectionOf(bmID(2)) != colTrash || job.Progress.Failed != 0 {
			t.Errorf("in %s, progress %+v", e.collectionOf(bmID(2)), job.Progress)
		}
		if rec := e.decision(job.ID, bmID(2)); rec.Result != restoreSkipped {
			t.Errorf("decision = %+v", rec)
		}
	})

	t.Run("sent to the trash, from a place that still exists", func(t *testing.T) {
		e := sortedEnv(t)
		gamma := e.addGamma()
		e.addBookmark(5, gamma)
		snap := e.takeSnapshot(domain.Scope{}).SnapshotID
		// The inbox never appears in a listing, and still exists.
		e.provider.SetCollection(bmID(1), colTrash)
		e.provider.SetCollection(bmID(5), colTrash)

		job := e.restore(snap)

		for _, n := range []int{1, 5} {
			if e.collectionOf(bmID(n)) != colTrash {
				t.Errorf("bookmark %d came out of the trash, to %s", n, e.collectionOf(bmID(n)))
			}
			if rec := e.decision(job.ID, bmID(n)); rec.Result != restoreSkipped {
				t.Errorf("decision = %+v", rec)
			}
		}
	})

	t.Run("in a collection made after the job began", func(t *testing.T) {
		e := sortedEnv(t)
		snap := e.takeSnapshot(domain.Scope{}).SnapshotID
		e.provider.OnGet(func(id domain.ID) {
			if id == bmID(2) {
				e.provider.OnGet(nil)
				e.provider.SetCollection(id, e.addGamma())
			}
		})
		e.restore(snap)
		if e.collectionOf(bmID(2)) != colAlpha {
			t.Errorf("in %s", e.collectionOf(bmID(2)))
		}
	})
}

var colTrash = domain.ID{Provider: "fake", Local: "trash"}

// deleteCollectionToTrash deletes a collection the way some providers do: what's in
// it goes to their trash, which they don't list.
func (e *jobEnv) deleteCollectionToTrash(id domain.ID) {
	e.t.Helper()
	for token := ""; ; {
		page, err := e.provider.ListBookmarks(context.Background(), ports.ListQuery{Collection: &id, PageToken: token})
		if err != nil {
			e.t.Fatal(err)
		}
		for _, b := range page.Items {
			e.provider.SetCollection(b.ID, colTrash)
		}
		if page.NextToken == "" {
			break
		}
		token = page.NextToken
	}
	e.deleteCollection(id)
}

// updateHook runs a function before each write, whose error replaces the write.
type updateHook struct {
	*listLog
	before func(domain.ID, ports.BookmarkPatch) error
}

func (u *updateHook) UpdateBookmark(ctx context.Context, id domain.ID, p ports.BookmarkPatch) (domain.Bookmark, error) {
	if err := u.before(id, p); err != nil {
		return domain.Bookmark{}, err
	}
	return u.listLog.UpdateBookmark(ctx, id, p)
}

func TestRestoreLeavesNewerBookmarks(t *testing.T) {
	e := sortedEnv(t)
	snap := e.takeSnapshot(domain.Scope{}).SnapshotID
	newer := e.addBookmark(9, colBeta, "mine")
	e.provider.SetCollection(bmID(2), colBeta)

	job := e.restore(snap)

	if e.collectionOf(newer) != colBeta || !slices.Equal(e.tagsOf(newer), []string{"mine"}) {
		t.Errorf("newer bookmark changed: %s %q", e.collectionOf(newer), e.tagsOf(newer))
	}
	for _, w := range e.provider.Writes() {
		if w.ID == newer {
			t.Errorf("wrote to it: %+v", w)
		}
	}
	if _, known, _ := e.store.BookmarkStates().GetState(context.Background(), newer); known {
		t.Error("restore touched its state")
	}
	if job.Progress.Total != 4 {
		t.Errorf("progress = %+v", job.Progress)
	}
}

func TestRestoreScoped(t *testing.T) {
	e := sortedEnv(t)
	snap := e.takeSnapshot(domain.Scope{Collections: []string{"Beta"}}).SnapshotID
	e.provider.SetCollection(bmID(2), colReview) // Not in the snapshot's scope.
	e.provider.SetCollection(bmID(3), colReview)

	job := e.restore(snap)

	if e.collectionOf(bmID(2)) != colReview {
		t.Errorf("out-of-scope bookmark moved to %s", e.collectionOf(bmID(2)))
	}
	if e.collectionOf(bmID(3)) != colBeta {
		t.Errorf("in %s", e.collectionOf(bmID(3)))
	}
	// Its own snapshot covers what it changes, and only that.
	own := e.snapshotItems(job.SnapshotID)
	if len(own) != 2 || own[bmID(3)].Collection != colReview {
		t.Errorf("own snapshot = %+v", own)
	}
}

func TestRestoreRefusals(t *testing.T) {
	t.Run("an unknown snapshot", func(t *testing.T) {
		e := sortedEnv(t)
		job := e.restoreJob("nope")
		if err := e.runJob(job); !errors.Is(err, domain.ErrNotFound) {
			t.Errorf("err = %v", err)
		}
		if len(e.provider.Writes()) != 0 || len(e.snapshotIDs()) != 0 {
			t.Errorf("writes %+v, snapshots %v", e.provider.Writes(), e.snapshotIDs())
		}
	})
	t.Run("another provider's snapshot", func(t *testing.T) {
		e := sortedEnv(t)
		if err := e.store.Snapshots().CreateSnapshot(context.Background(), domain.Snapshot{ID: "theirs", Provider: "other", CreatedAt: t0}); err != nil {
			t.Fatal(err)
		}
		job := e.restoreJob("theirs")
		if err := e.runJob(job); !errors.Is(err, domain.ErrValidation) {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("no snapshot of its own means no writes", func(t *testing.T) {
		e := sortedEnv(t)
		snap := e.takeSnapshot(domain.Scope{}).SnapshotID
		e.provider.SetCollection(bmID(2), colBeta)
		e.deps.Snapshots = &snapshotStub{store: e.store, err: errors.New("disk full")}
		job := e.restoreJob(snap)
		if err := e.runJob(job); err == nil {
			t.Fatal("no error")
		}
		if len(e.provider.Writes()) != 0 || e.collectionOf(bmID(2)) != colBeta {
			t.Errorf("writes = %+v", e.provider.Writes())
		}
	})
}

func TestRestoreRecreatesDeletedCollection(t *testing.T) {
	t.Run("by name, and the bookmark returns to it", func(t *testing.T) {
		e := sortedEnv(t)
		gamma := e.addGamma()
		e.addBookmark(5, gamma, "g")
		snap := e.takeSnapshot(domain.Scope{}).SnapshotID
		e.provider.SetCollection(bmID(5), colBeta)
		e.deleteCollection(gamma)

		job := e.restore(snap)

		if !e.hasCollection("Gamma") {
			t.Fatalf("collections = %v", e.collectionNames())
		}
		now := e.collectionOf(bmID(5))
		if now == gamma || now == colBeta {
			t.Fatalf("bookmark in %s", now)
		}
		cols, err := e.provider.ListCollections(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range cols {
			if c.ID == now && (c.Name != "Gamma" || c.Description != "G") {
				t.Errorf("recreated as %+v", c)
			}
		}
		if st := e.state(bmID(5)); st.KnownCollection != now {
			t.Errorf("state = %+v", st)
		}
		// So undoing this restore removes it again.
		own, err := e.store.Snapshots().GetSnapshot(context.Background(), job.SnapshotID)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Contains(own.CreatedCollections, now) {
			t.Errorf("own snapshot created = %+v", own.CreatedCollections)
		}
		e.restore(job.SnapshotID)
		if e.hasCollection("Gamma") || e.collectionOf(bmID(5)) != colBeta {
			t.Errorf("after undo: %v, bookmark in %s", e.collectionNames(), e.collectionOf(bmID(5)))
		}
	})

	t.Run("for bookmarks its deletion sent to the trash", func(t *testing.T) {
		e := sortedEnv(t)
		gamma := e.addGamma()
		e.addBookmark(5, gamma, "g")
		e.addBookmark(6, gamma)
		snap := e.takeSnapshot(domain.Scope{}).SnapshotID
		e.deleteCollectionToTrash(gamma)
		// Trashed on purpose, from a collection that's still there.
		e.provider.SetCollection(bmID(2), colTrash)

		dry := e.restore(snap, dryRun)
		if e.hasCollection("Gamma") || len(e.provider.Writes()) != 0 {
			t.Errorf("dry run: collections %v, writes %+v", e.collectionNames(), e.provider.Writes())
		}
		if rec := e.decision(dry.ID, bmID(5)); rec.Result != "proposed" || rec.TargetCollection == nil || !strings.Contains(rec.Reason, "Gamma") {
			t.Errorf("dry run decision = %+v", rec)
		}

		job := e.restore(snap)

		now := e.collectionOf(bmID(5))
		if !e.hasCollection("Gamma") || now == gamma || now == colTrash {
			t.Fatalf("collections %v, bookmark in %s", e.collectionNames(), now)
		}
		if e.collectionOf(bmID(6)) != now || !slices.Equal(e.tagsOf(bmID(5)), []string{"g"}) {
			t.Errorf("bookmark 6 in %s, bookmark 5 tags %q", e.collectionOf(bmID(6)), e.tagsOf(bmID(5)))
		}
		if e.collectionOf(bmID(2)) != colTrash {
			t.Errorf("bookmark 2 came out of the trash, to %s", e.collectionOf(bmID(2)))
		}
		if job.Progress.Failed != 0 || job.Progress.Changed != 2 {
			t.Errorf("progress = %+v", job.Progress)
		}
		if st := e.state(bmID(5)); st.KnownCollection != now || st.PinSource != domain.PinNone {
			t.Errorf("state = %+v", st)
		}
	})

	t.Run("and a resume finds the one it already made", func(t *testing.T) {
		e := sortedEnv(t)
		gamma := e.addGamma()
		e.addBookmark(5, gamma)
		e.addBookmark(6, gamma)
		snap := e.takeSnapshot(domain.Scope{}).SnapshotID
		e.deleteCollectionToTrash(gamma)

		job := e.restoreJob(snap, func(j *domain.Job) { j.Params.BatchSize = 5 })
		// Saves in order: own snapshot, start of processing, page 1 (bookmarks 1 to 5).
		err := Handlers(e.deps)[domain.JobRestore].Run(context.Background(), job, e.stopAfter(job, 3))
		if !errors.Is(err, ErrInterrupted) {
			t.Fatalf("err = %v", err)
		}
		if e.collectionOf(bmID(6)) != colTrash || !e.hasCollection("Gamma") {
			t.Fatalf("bookmark 6 in %s, collections %v", e.collectionOf(bmID(6)), e.collectionNames())
		}
		e.mustRun(e.reload(job.ID))

		count := 0
		for _, n := range e.collectionNames() {
			if n == "Gamma" {
				count++
			}
		}
		if count != 1 || e.collectionOf(bmID(5)) != e.collectionOf(bmID(6)) {
			t.Errorf("collections %v, bookmarks in %s and %s", e.collectionNames(), e.collectionOf(bmID(5)), e.collectionOf(bmID(6)))
		}
	})

	t.Run("once, with several bookmarks at a time", func(t *testing.T) {
		e := sortedEnv(t)
		e.deps.Concurrency = 8
		gamma := e.addGamma()
		for n := 10; n < 40; n++ {
			e.addBookmark(n, gamma)
		}
		snap := e.takeSnapshot(domain.Scope{}).SnapshotID
		for n := 10; n < 40; n++ {
			e.provider.SetCollection(bmID(n), colBeta)
		}
		e.deleteCollection(gamma)

		e.restore(snap)

		var made []string
		for _, n := range e.collectionNames() {
			if n == "Gamma" {
				made = append(made, n)
			}
		}
		if len(made) != 1 {
			t.Errorf("collections = %v", e.collectionNames())
		}
		first := e.collectionOf(bmID(10))
		for n := 10; n < 40; n++ {
			if e.collectionOf(bmID(n)) != first {
				t.Errorf("bookmark %d in %s", n, e.collectionOf(bmID(n)))
			}
		}
	})

	t.Run("not when one by that name already stands", func(t *testing.T) {
		e := sortedEnv(t)
		gamma := e.addGamma()
		e.addBookmark(5, gamma)
		snap := e.takeSnapshot(domain.Scope{}).SnapshotID
		e.provider.SetCollection(bmID(5), colBeta)
		e.deleteCollection(gamma)
		again := e.addGamma()

		e.restore(snap)

		if e.collectionOf(bmID(5)) != again {
			t.Errorf("in %s, want %s", e.collectionOf(bmID(5)), again)
		}
	})

	t.Run("not for a bookmark that is gone", func(t *testing.T) {
		e := sortedEnv(t)
		gamma := e.addGamma()
		e.addBookmark(5, gamma)
		snap := e.takeSnapshot(domain.Scope{}).SnapshotID
		e.provider.Remove(bmID(5))
		e.deleteCollection(gamma)

		e.restore(snap)

		if e.hasCollection("Gamma") {
			t.Errorf("collections = %v", e.collectionNames())
		}
	})

	t.Run("a nested one under its parent", func(t *testing.T) {
		e := sortedEnv(t)
		ctx := context.Background()
		parent, err := e.provider.CreateCollection(ctx, domain.Collection{Name: "Parent"})
		if err != nil {
			t.Fatal(err)
		}
		child, err := e.provider.CreateCollection(ctx, domain.Collection{Name: "Child", Parent: &parent.ID})
		if err != nil {
			t.Fatal(err)
		}
		e.addBookmark(5, child.ID)
		snap := e.takeSnapshot(domain.Scope{}).SnapshotID
		e.provider.SetCollection(bmID(5), colBeta)
		e.deleteCollection(child.ID)
		e.deleteCollection(parent.ID)

		e.restore(snap)

		cols, err := e.provider.ListCollections(ctx)
		if err != nil {
			t.Fatal(err)
		}
		byName := map[string]domain.Collection{}
		for _, c := range cols {
			byName[c.Name] = c
		}
		madeChild, madeParent := byName["Child"], byName["Parent"]
		if madeChild.Parent == nil || *madeChild.Parent != madeParent.ID || madeParent.Parent != nil {
			t.Errorf("collections = %+v", cols)
		}
		if e.collectionOf(bmID(5)) != madeChild.ID {
			t.Errorf("in %s", e.collectionOf(bmID(5)))
		}
	})

	t.Run("a failed creation fails the item only", func(t *testing.T) {
		e := sortedEnv(t)
		gamma := e.addGamma()
		e.addBookmark(5, gamma)
		snap := e.takeSnapshot(domain.Scope{}).SnapshotID
		e.provider.SetCollection(bmID(5), colBeta)
		e.provider.SetCollection(bmID(2), colBeta)
		e.deleteCollection(gamma)
		caps := e.provider.Capabilities()
		e.deps.Provider = &noCreate{listLog: e.logged, caps: caps}

		job := e.restore(snap)

		if job.Progress.Failed != 1 || e.collectionOf(bmID(5)) != colBeta {
			t.Errorf("progress %+v, in %s", job.Progress, e.collectionOf(bmID(5)))
		}
		if e.collectionOf(bmID(2)) != colAlpha {
			t.Errorf("the rest stopped: %s", e.collectionOf(bmID(2)))
		}
	})
}

// noCreate lists collections but can't make them.
type noCreate struct {
	*listLog
	caps ports.ProviderCapabilities
}

func (n *noCreate) Capabilities() ports.ProviderCapabilities {
	c := n.caps
	c.CreateCollections = false
	return c
}

func TestRestoreKeepsNonEmptyCollection(t *testing.T) {
	e := sortedEnv(t)
	job := e.backfill(nil)
	cols := e.provider.Writes()
	gamma := *cols[0].Patch.Collection
	mine := e.addBookmark(9, gamma, "mine")

	e.restore(job.SnapshotID)

	if !e.hasCollection("Gamma") || e.collectionOf(mine) != gamma {
		t.Errorf("collections %v, bookmark in %s", e.collectionNames(), e.collectionOf(mine))
	}
	// It wasn't deleted, so the pipeline may go on offering it.
	e.putSetting(autoCreatedKey, createdSetting(t, gamma))
	e.restore(job.SnapshotID)
	if got := settingIDs(t, e); !slices.Equal(got, []domain.ID{gamma}) {
		t.Errorf("setting = %v", got)
	}
}

func createdSetting(t *testing.T, ids ...domain.ID) string {
	t.Helper()
	type entry struct {
		Key  string    `json:"key"`
		Name string    `json:"name"`
		ID   domain.ID `json:"id"`
	}
	var list []entry
	for _, id := range ids {
		list = append(list, entry{Key: strings.ToLower(id.Local), Name: id.Local, ID: id})
	}
	raw, err := json.Marshal(list)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func settingIDs(t *testing.T, e *jobEnv) []domain.ID {
	t.Helper()
	raw, ok := e.setting(autoCreatedKey)
	if !ok {
		return nil
	}
	var list []struct {
		ID domain.ID `json:"id"`
	}
	if err := json.Unmarshal([]byte(raw), &list); err != nil {
		t.Fatal(err)
	}
	var ids []domain.ID
	for _, c := range list {
		ids = append(ids, c.ID)
	}
	return ids
}

func TestRestoreCleansAutoCreatedSetting(t *testing.T) {
	t.Run("a deleted collection leaves it, with no taxonomy on the job", func(t *testing.T) {
		e := sortedEnv(t)
		snap := e.takeSnapshot(domain.Scope{}).SnapshotID
		made := e.addGamma()
		kept := domain.ID{Provider: "fake", Local: "kept"}
		e.provider.AddCollection(domain.Collection{ID: kept, Name: "Kept"})
		if err := e.store.Snapshots().AddCreatedCollection(context.Background(), snap, made); err != nil {
			t.Fatal(err)
		}
		e.putSetting(autoCreatedKey, createdSetting(t, made, kept))

		e.restore(snap)

		if e.hasCollection("Gamma") {
			t.Fatal("collection not deleted")
		}
		if got := settingIDs(t, e); !slices.Equal(got, []domain.ID{kept}) {
			t.Errorf("setting = %v", got)
		}
	})

	t.Run("a resume after the delete still cleans it", func(t *testing.T) {
		e := sortedEnv(t)
		snap := e.takeSnapshot(domain.Scope{}).SnapshotID
		made := e.addGamma()
		if err := e.store.Snapshots().AddCreatedCollection(context.Background(), snap, made); err != nil {
			t.Fatal(err)
		}
		e.putSetting(autoCreatedKey, createdSetting(t, made))
		e.deleteCollection(made) // As if the last run got this far and died.

		e.restore(snap)

		if got := settingIDs(t, e); len(got) != 0 {
			t.Errorf("setting = %v", got)
		}
	})

	t.Run("a replaced collection's old id leaves it", func(t *testing.T) {
		e := sortedEnv(t)
		gamma := e.addGamma()
		e.addBookmark(5, gamma)
		snap := e.takeSnapshot(domain.Scope{}).SnapshotID
		e.provider.SetCollection(bmID(5), colBeta)
		e.deleteCollection(gamma)
		e.putSetting(autoCreatedKey, createdSetting(t, gamma))

		e.restore(snap)

		if got := settingIDs(t, e); len(got) != 0 {
			t.Errorf("setting = %v", got)
		}
	})

	t.Run("a dry run leaves it", func(t *testing.T) {
		e := sortedEnv(t)
		snap := e.takeSnapshot(domain.Scope{}).SnapshotID
		made := e.addGamma()
		if err := e.store.Snapshots().AddCreatedCollection(context.Background(), snap, made); err != nil {
			t.Fatal(err)
		}
		e.putSetting(autoCreatedKey, createdSetting(t, made))
		e.restore(snap, dryRun)
		if !e.hasCollection("Gamma") || len(settingIDs(t, e)) != 1 {
			t.Errorf("collections %v, setting %v", e.collectionNames(), settingIDs(t, e))
		}
	})
}

func TestRestoreRemovesCollectionsOfLaterJobs(t *testing.T) {
	e := sortedEnv(t)
	first := e.takeSnapshot(domain.Scope{}).SnapshotID
	e.tick()
	later := e.takeSnapshot(domain.Scope{}).SnapshotID
	made := e.addGamma()
	if err := e.store.Snapshots().AddCreatedCollection(context.Background(), later, made); err != nil {
		t.Fatal(err)
	}

	e.restore(first)

	if e.hasCollection("Gamma") {
		t.Errorf("collections = %v", e.collectionNames())
	}
}

func TestRestoreNeverDeletesACollectionTheSnapshotHad(t *testing.T) {
	e := sortedEnv(t)
	gamma := e.addGamma()
	snap := e.takeSnapshot(domain.Scope{}).SnapshotID
	e.tick()
	later := e.takeSnapshot(domain.Scope{}).SnapshotID
	// Oddly recorded as created by a later job, and empty.
	if err := e.store.Snapshots().AddCreatedCollection(context.Background(), later, gamma); err != nil {
		t.Fatal(err)
	}
	e.restore(snap)
	if !e.hasCollection("Gamma") {
		t.Error("deleted a collection that existed at the snapshot")
	}
}

// A collection deleted since the snapshot and then made again by a later job is
// the one the snapshot had, as far as anyone can tell.
func TestRestoreKeepsTheReplacementOfACollectionTheSnapshotHad(t *testing.T) {
	e := sortedEnv(t)
	gamma := e.addGamma()
	snap := e.takeSnapshot(domain.Scope{}).SnapshotID
	e.deleteCollection(gamma)
	e.tick()
	later := e.takeSnapshot(domain.Scope{}).SnapshotID
	again := e.addGamma()
	if err := e.store.Snapshots().AddCreatedCollection(context.Background(), later, again); err != nil {
		t.Fatal(err)
	}
	e.putSetting(autoCreatedKey, createdSetting(t, gamma, again))

	e.restore(snap)

	if !e.hasCollection("Gamma") {
		t.Error("deleted it")
	}
	if got := settingIDs(t, e); !slices.Equal(got, []domain.ID{again}) {
		t.Errorf("setting = %v", got)
	}
}

func TestRestoreTakesOwnSnapshot(t *testing.T) {
	t.Run("before any write", func(t *testing.T) {
		e := sortedEnv(t)
		snap := e.takeSnapshot(domain.Scope{}).SnapshotID
		e.provider.SetCollection(bmID(2), colBeta)
		sleeps := len(e.sleeps)
		var own string
		e.deps.Snapshots = takerFunc(func(ctx context.Context, p string, s domain.Scope, j string) (string, error) {
			if len(e.provider.Writes()) != 0 {
				t.Error("wrote before its own snapshot")
			}
			var err error
			own, err = NewSnapshotService(SnapshotDeps{Provider: e.logged, Store: e.store, Now: e.nowFn, Retention: 10}).Take(ctx, p, s, j)
			return own, err
		})

		job := e.restore(snap)

		if job.SnapshotID != own || own == "" || own == snap {
			t.Errorf("own = %q, job has %q", own, job.SnapshotID)
		}
		if saved := e.reload(job.ID); saved.SnapshotID != own {
			t.Errorf("not saved: %q", saved.SnapshotID)
		}
		if len(e.sleeps) != sleeps+1 {
			t.Errorf("sleeps = %v", e.sleeps)
		}
		if got := e.snapshotItems(own)[bmID(2)].Collection; got != colBeta {
			t.Errorf("own snapshot has it in %s", got)
		}
	})

	t.Run("and undoing the restore with it works", func(t *testing.T) {
		undoSetup := func(t *testing.T) (e *jobEnv, pre, sorted libraryState, gamma domain.ID, restore *domain.Job, states map[domain.ID]domain.BookmarkState, setting string) {
			t.Helper()
			e = sortedEnv(t)
			pre = e.library(4)
			backfill := e.backfill(nil)
			gamma = e.collectionOf(bmID(1))
			var ok bool
			if setting, ok = e.setting(firstBackfillKey); !ok {
				t.Fatal("first backfill not recorded")
			}
			// Manual changes on top of the backfill: one a sync has pinned, and one
			// nothing has read yet.
			e.provider.SetCollection(bmID(4), colReview)
			e.tick()
			e.syncNow()
			if st := e.state(bmID(4)); st.PinSource != domain.PinUser {
				t.Fatalf("not pinned: %+v", st)
			}
			e.provider.SetCollection(bmID(3), colReview)
			sorted = e.library(4)
			states = map[domain.ID]domain.BookmarkState{}
			for n := 1; n <= 4; n++ {
				states[bmID(n)] = e.state(bmID(n))
			}
			e.tick()

			restore = e.restore(backfill.SnapshotID)
			e.checkLibrary(pre)
			if e.hasCollection("Gamma") {
				t.Fatal("restore left Gamma")
			}
			if st := e.state(bmID(4)); st.PinSource != domain.PinNone {
				t.Fatalf("restore kept a pin the snapshot didn't have: %+v", st)
			}
			if _, ok := e.setting(firstBackfillKey); ok {
				t.Fatal("restore kept the setting the snapshot didn't have")
			}
			return e, pre, sorted, gamma, restore, states, setting
		}

		t.Run("collections, tags, pins, and the setting come back", func(t *testing.T) {
			e, pre, sorted, gamma, restore, states, setting := undoSetup(t)

			undo := e.restore(restore.SnapshotID)

			// Everything is back, with Gamma made again under a new ID.
			remade := e.collectionOf(bmID(1))
			if remade == gamma || !e.hasCollection("Gamma") {
				t.Fatalf("bookmark 1 in %s, collections %v", remade, e.collectionNames())
			}
			for id, w := range sorted {
				if w.Collection == gamma {
					w.Collection = remade
					sorted[id] = w
				}
			}
			e.checkLibrary(sorted)
			for id, before := range states {
				st := e.state(id)
				if st.PinSource != before.PinSource && id != bmID(3) {
					t.Errorf("%s pin %q, was %q", id, st.PinSource, before.PinSource)
				}
				if before.PinnedAt != nil && (st.PinnedAt == nil || !st.PinnedAt.Equal(*before.PinnedAt)) {
					t.Errorf("%s pinned at %v, was %v", id, st.PinnedAt, before.PinnedAt)
				}
				if st.KnownCollection != sorted[id].Collection {
					t.Errorf("%s known in %s", id, st.KnownCollection)
				}
			}
			// Bookmark 3's move hadn't been read, and would have been pinned next.
			if st := e.state(bmID(3)); st.PinSource != domain.PinUser {
				t.Errorf("unread manual move: %+v", st)
			}
			if v, ok := e.setting(firstBackfillKey); !ok || v != setting {
				t.Errorf("setting = %q %v, want %q", v, ok, setting)
			}

			// And the undo can itself be undone, leaving no extra Gamma behind.
			e.restore(undo.SnapshotID)
			e.checkLibrary(pre)
			if e.hasCollection("Gamma") {
				t.Errorf("collections = %v", e.collectionNames())
			}
			if _, ok := e.setting(firstBackfillKey); ok {
				t.Error("undoing the undo kept the setting")
			}
			if st := e.state(bmID(4)); st.PinSource != domain.PinNone {
				t.Errorf("undoing the undo kept the pin: %+v", st)
			}
		})

		t.Run("a later backfill respects the user's pins", func(t *testing.T) {
			e, _, _, _, restore, _, _ := undoSetup(t)
			e.restore(restore.SnapshotID)
			e.tick()

			e.backfill(nil)

			for _, n := range []int{3, 4} {
				if got := e.collectionOf(bmID(n)); got != colReview {
					t.Errorf("bookmark %d moved to %s", n, got)
				}
				if st := e.state(bmID(n)); st.PinSource != domain.PinUser {
					t.Errorf("bookmark %d: %+v", n, st)
				}
			}
		})
	})
}

type takerFunc func(context.Context, string, domain.Scope, string) (string, error)

func (f takerFunc) Take(ctx context.Context, p string, s domain.Scope, j string) (string, error) {
	return f(ctx, p, s, j)
}

func TestRestoreUpdatesState(t *testing.T) {
	e := sortedEnv(t)
	backfill := e.backfill(nil)
	e.tick()
	e.syncNow()
	e.tick()
	e.restore(backfill.SnapshotID)
	restoredAt := e.nowFn()

	for n := 1; n <= 4; n++ {
		id := bmID(n)
		st := e.state(id)
		b, _ := e.provider.Bookmark(id)
		if st.KnownCollection != b.Collection || !slices.Equal(st.KnownTags, b.Tags) && len(b.Tags) != 0 {
			t.Errorf("%s: state %+v, bookmark %+v", id, st, b)
		}
		if st.PendingCollection != nil || st.PendingTags != nil {
			t.Errorf("%s left pending: %+v", id, st)
		}
		if st.RestoredAt == nil || !st.RestoredAt.Equal(restoredAt) {
			t.Errorf("%s restored at %v", id, st.RestoredAt)
		}
		if st.PinSource != domain.PinNone {
			t.Errorf("%s pinned: %+v", id, st)
		}
	}

	calls, writes := e.cls.Calls(), len(e.provider.Writes())
	e.tick()
	after := e.syncNow()
	if e.cls.Calls() != calls || len(e.provider.Writes()) != writes {
		t.Errorf("sync touched restored bookmarks: calls %d, writes %+v", e.cls.Calls()-calls, e.provider.Writes()[writes:])
	}
	for n := 1; n <= 4; n++ {
		if st := e.state(bmID(n)); st.PinSource != domain.PinNone || st.RestoredAt == nil {
			t.Errorf("after sync, %s: %+v", bmID(n), st)
		}
	}
	if after.Progress.Failed != 0 {
		t.Errorf("sync = %+v", after.Progress)
	}

	t.Run("a move after the restore is the user's", func(t *testing.T) {
		e.provider.SetCollection(bmID(2), colReview)
		e.tick()
		e.syncNow()
		if st := e.state(bmID(2)); st.PinSource != domain.PinUser {
			t.Errorf("state = %+v", st)
		}
	})

	t.Run("a backfill picks them up again", func(t *testing.T) {
		e.tick()
		e.backfill(nil)
		if st := e.state(bmID(1)); st.RestoredAt != nil {
			t.Errorf("state = %+v", st)
		}
		if e.collectionOf(bmID(1)) == pfake.Inbox {
			t.Error("not sorted again")
		}
	})
}

func TestRestoreStateForBookmarksNeverSeen(t *testing.T) {
	e := sortedEnv(t)
	snap := e.takeSnapshot(domain.Scope{}).SnapshotID

	e.restore(snap) // Nothing to change, and no state row yet.

	if len(e.provider.Writes()) != 0 {
		t.Errorf("writes = %+v", e.provider.Writes())
	}
	st := e.state(bmID(1))
	if st.KnownCollection != pfake.Inbox || st.RestoredAt == nil || st.PinSource != domain.PinNone {
		t.Errorf("state = %+v", st)
	}
	e.syncNow()
	if e.cls.Calls() != 0 {
		t.Errorf("classified %d restored bookmarks", e.cls.Calls())
	}
}

// A write that lands just before a crash mustn't read as a manual move, whoever
// looks next.
func TestRestoreRecordsIntentBeforeWriting(t *testing.T) {
	e := sortedEnv(t)
	snap := e.takeSnapshot(domain.Scope{}).SnapshotID
	e.provider.SetCollection(bmID(2), colBeta)
	var during domain.BookmarkState
	e.deps.Provider = &updateHook{listLog: e.logged, before: func(id domain.ID, _ ports.BookmarkPatch) error {
		during = e.state(id)
		return nil
	}}
	job := e.restore(snap)
	if during.PendingCollection == nil || *during.PendingCollection != colAlpha || during.KnownCollection != colBeta {
		t.Errorf("state during the write = %+v", during)
	}
	if rec := e.decision(job.ID, bmID(2)); rec.Result != "applied" || rec.AppliedAt == nil {
		t.Errorf("decision = %+v", rec)
	}
}

func TestRestoreWriteFailures(t *testing.T) {
	t.Run("a failed write fails the item and the rest go on", func(t *testing.T) {
		e := sortedEnv(t)
		snap := e.takeSnapshot(domain.Scope{}).SnapshotID
		e.provider.SetCollection(bmID(2), colBeta)
		e.provider.SetCollection(bmID(3), colReview)
		e.deps.Provider = &updateHook{listLog: e.logged, before: func(id domain.ID, _ ports.BookmarkPatch) error {
			if id == bmID(2) {
				return fmt.Errorf("boom: %w", domain.ErrTemporary)
			}
			return nil
		}}
		job := e.restore(snap)
		if job.Progress.Failed != 1 || e.collectionOf(bmID(3)) != colBeta {
			t.Errorf("progress %+v, b3 in %s", job.Progress, e.collectionOf(bmID(3)))
		}
		if st := e.state(bmID(2)); st.RestoredAt != nil {
			t.Errorf("marked restored: %+v", st)
		}
	})

	t.Run("a failed write doesn't hide a manual move not yet seen", func(t *testing.T) {
		e := sortedEnv(t)
		snap := e.takeSnapshot(domain.Scope{}).SnapshotID
		e.syncNow()
		e.provider.SetCollection(bmID(3), colReview)
		e.deps.Provider = &updateHook{listLog: e.logged, before: func(id domain.ID, _ ports.BookmarkPatch) error {
			if id == bmID(3) {
				return fmt.Errorf("boom: %w", domain.ErrTemporary)
			}
			return nil
		}}
		e.restore(snap)
		e.deps.Provider = e.logged
		e.tick()
		e.syncNow()
		if st := e.state(bmID(3)); st.PinSource != domain.PinUser || st.KnownCollection != colReview {
			t.Errorf("state = %+v", st)
		}
	})

	t.Run("a not-found write on a live bookmark isn't a deletion", func(t *testing.T) {
		e := sortedEnv(t)
		snap := e.takeSnapshot(domain.Scope{}).SnapshotID
		e.provider.SetCollection(bmID(2), colBeta)
		e.deps.Provider = &updateHook{listLog: e.logged, before: func(id domain.ID, _ ports.BookmarkPatch) error {
			if id == bmID(2) {
				return fmt.Errorf("collection gone: %w", domain.ErrNotFound)
			}
			return nil
		}}
		job := e.restore(snap)
		if job.Progress.Failed != 1 {
			t.Errorf("progress = %+v", job.Progress)
		}
		if st := e.state(bmID(2)); st.PendingCollection != nil || st.RestoredAt != nil {
			t.Errorf("state = %+v", st)
		}
	})

	t.Run("a failed delete doesn't undo the restore", func(t *testing.T) {
		e := sortedEnv(t)
		snap := e.takeSnapshot(domain.Scope{}).SnapshotID
		made := e.addGamma()
		if err := e.store.Snapshots().AddCreatedCollection(context.Background(), snap, made); err != nil {
			t.Fatal(err)
		}
		e.provider.SetCollection(bmID(2), colBeta)
		e.deps.Provider = &deleteFails{listLog: e.logged}
		job := e.restore(snap)
		if job.Progress.Failed != 1 || e.collectionOf(bmID(2)) != colAlpha {
			t.Errorf("progress %+v, b2 in %s", job.Progress, e.collectionOf(bmID(2)))
		}
		if job.Checkpoint.Phase != phaseDone {
			t.Errorf("checkpoint = %+v", job.Checkpoint)
		}
	})
}

type deleteFails struct{ *listLog }

func (d *deleteFails) DeleteCollectionIfEmpty(context.Context, domain.ID) (bool, error) {
	return false, fmt.Errorf("boom: %w", domain.ErrTemporary)
}

func TestRestoreResetsFirstBackfill(t *testing.T) {
	t.Run("a snapshot from before the first backfill clears it", func(t *testing.T) {
		e := sortedEnv(t)
		backfill := e.backfill(nil)
		if _, ok := e.setting(firstBackfillKey); !ok {
			t.Fatal("first backfill not recorded")
		}
		// The first backfill's own snapshot was taken before the setting's time.
		e.setNow(e.nowFn().Add(time.Hour))
		e.putSetting(firstBackfillKey, e.nowFn().Format(time.RFC3339Nano))
		e.tick()
		e.restore(backfill.SnapshotID)
		if _, ok := e.setting(firstBackfillKey); ok {
			t.Error("setting still there")
		}

		// And the next backfill really is a first one: it ignores older user pins.
		e.provider.SetCollection(bmID(2), colReview)
		e.tick()
		e.syncNow()
		if st := e.state(bmID(2)); st.PinSource != domain.PinUser {
			t.Fatalf("state = %+v", st)
		}
		e.tick()
		e.backfill(nil)
		if got := e.collectionOf(bmID(2)); got == colReview {
			t.Errorf("first backfill respected an old pin")
		}
		if _, ok := e.setting(firstBackfillKey); !ok {
			t.Error("not recorded again")
		}
	})

	t.Run("a later snapshot doesn't", func(t *testing.T) {
		e := sortedEnv(t)
		done := e.nowFn().Format(time.RFC3339Nano)
		e.putSetting(firstBackfillKey, done)
		e.tick()
		snap := e.takeSnapshot(domain.Scope{}).SnapshotID
		e.restore(snap)
		if v, ok := e.setting(firstBackfillKey); !ok || v != done {
			t.Errorf("setting = %q %v", v, ok)
		}
	})

	t.Run("undoing with its own snapshot puts it back", func(t *testing.T) {
		e := sortedEnv(t)
		pre := e.takeSnapshot(domain.Scope{}).SnapshotID
		e.tick()
		done := e.nowFn().Format(time.RFC3339Nano)
		e.putSetting(firstBackfillKey, done)
		e.tick()
		restore := e.restore(pre)
		if _, ok := e.setting(firstBackfillKey); ok {
			t.Fatal("not cleared")
		}
		e.restore(restore.SnapshotID)
		if v, ok := e.setting(firstBackfillKey); !ok || v != done {
			t.Errorf("setting = %q %v", v, ok)
		}
	})

	t.Run("a scoped restore leaves it, whenever the snapshot was taken", func(t *testing.T) {
		e := sortedEnv(t)
		snap := e.takeSnapshot(domain.Scope{Collections: []string{"Beta"}}).SnapshotID
		e.provider.SetCollection(bmID(3), colReview)
		e.tick()
		done := e.nowFn().Format(time.RFC3339Nano)
		e.putSetting(firstBackfillKey, done)
		e.tick()

		restore := e.restore(snap)
		if e.collectionOf(bmID(3)) != colBeta {
			t.Fatalf("in %s", e.collectionOf(bmID(3)))
		}
		if v, ok := e.setting(firstBackfillKey); !ok || v != done {
			t.Errorf("setting = %q %v", v, ok)
		}

		// Nor does a scoped one set what the snapshot didn't record.
		if err := e.store.BookmarkStates().DeleteSetting(context.Background(), firstBackfillKey); err != nil {
			t.Fatal(err)
		}
		e.restore(restore.SnapshotID)
		if _, ok := e.setting(firstBackfillKey); ok {
			t.Error("scoped restore set it")
		}
	})

	t.Run("no setting is fine", func(t *testing.T) {
		e := sortedEnv(t)
		snap := e.takeSnapshot(domain.Scope{}).SnapshotID
		e.restore(snap)
		if _, ok := e.setting(firstBackfillKey); ok {
			t.Error("restore set it")
		}
	})

}

func TestRestoreResume(t *testing.T) {
	// Saves in order: own snapshot, start of processing, page 1, page 2, cleanup,
	// done. Stopping after any of them must end in the same place.
	for stopAt := 1; stopAt <= 5; stopAt++ {
		t.Run(fmt.Sprintf("stopped after save %d", stopAt), func(t *testing.T) {
			e := sortedEnv(t)
			for n := 5; n <= 6; n++ {
				e.addBookmark(n, colBeta, "shared")
			}
			want := e.library(6)
			backfill := e.backfill(nil)
			e.tick()

			job := e.restoreJob(backfill.SnapshotID, func(j *domain.Job) { j.Params.BatchSize = 2 })
			err := Handlers(e.deps)[domain.JobRestore].Run(context.Background(), job, e.stopAfter(job, stopAt))
			if !errors.Is(err, ErrInterrupted) {
				t.Fatalf("err = %v", err)
			}
			snapshots := e.snapshotIDs()
			own := job.SnapshotID
			if own == "" {
				t.Fatal("no own snapshot")
			}
			ownItems := e.snapshotItems(own)

			saved := e.reload(job.ID)
			e.mustRun(saved)

			e.checkLibrary(want)
			if e.hasCollection("Gamma") {
				t.Errorf("collections = %v", e.collectionNames())
			}
			if saved.SnapshotID != own || !slices.Equal(e.snapshotIDs(), snapshots) {
				t.Errorf("a second snapshot: %q, %v", saved.SnapshotID, e.snapshotIDs())
			}
			if len(e.snapshotItems(own)) != len(ownItems) {
				t.Error("own snapshot changed")
			}
			p := saved.Progress
			if p.Total != 6 || p.Processed != 6 || p.Changed != 6 || p.Failed != 0 {
				t.Errorf("progress = %+v", p)
			}
			for n := 1; n <= 6; n++ {
				st := e.state(bmID(n))
				if st.RestoredAt == nil || st.PendingCollection != nil || st.PinSource != domain.PinNone {
					t.Errorf("state of %d = %+v", n, st)
				}
			}
			// The diff shows where each bookmark came from, not what the resume saw.
			diffs := e.diffs(saved.ID)
			if d := diffs[bmID(2)]; d.FromCollection == colAlpha || d.ToCollection != colAlpha {
				t.Errorf("diff = %+v", d)
			}
			if saved.Checkpoint.Phase != phaseDone {
				t.Errorf("checkpoint = %+v", saved.Checkpoint)
			}
		})
	}

	t.Run("a write that landed before a crash settles without a pin", func(t *testing.T) {
		e := sortedEnv(t)
		snap := e.takeSnapshot(domain.Scope{}).SnapshotID
		e.provider.SetCollection(bmID(2), colBeta)
		e.tick()
		pinnedAt := e.nowFn()
		if err := e.store.BookmarkStates().PutState(context.Background(), domain.BookmarkState{ID: bmID(2), KnownCollection: colBeta, PinSource: domain.PinUser, PinnedAt: &pinnedAt}); err != nil {
			t.Fatal(err)
		}

		// The write lands, and the job dies before it can record that.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		e.deps.Provider = &landThenCancel{listLog: e.logged, cancel: cancel}
		job := e.restoreJob(snap)
		if err := Handlers(e.deps)[domain.JobRestore].Run(ctx, job, e.rc(job)); !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v", err)
		}
		if st := e.state(bmID(2)); st.PendingCollection == nil {
			t.Fatalf("state = %+v", st)
		}

		e.deps.Provider = e.logged
		saved := e.reload(job.ID)
		writes := len(e.provider.Writes())
		e.mustRun(saved)

		if len(e.provider.Writes()) != writes {
			t.Errorf("wrote again: %+v", e.provider.Writes()[writes:])
		}
		st := e.state(bmID(2))
		if st.PendingCollection != nil || st.KnownCollection != colAlpha || st.RestoredAt == nil || st.PinSource != domain.PinNone {
			t.Errorf("state = %+v", st)
		}
		rec := e.decision(job.ID, bmID(2))
		if rec.Result != "applied" || rec.ObservedCollection != colBeta {
			t.Errorf("decision = %+v", rec)
		}
		if saved.Progress.Changed != 1 {
			t.Errorf("progress = %+v", saved.Progress)
		}
	})
}

// landThenCancel lets the first write through and then loses the rest of the run.
type landThenCancel struct {
	*listLog
	cancel context.CancelFunc
}

func (l *landThenCancel) UpdateBookmark(ctx context.Context, id domain.ID, p ports.BookmarkPatch) (domain.Bookmark, error) {
	b, err := l.listLog.UpdateBookmark(ctx, id, p)
	if err != nil {
		return b, err
	}
	l.cancel()
	return domain.Bookmark{}, context.Canceled
}

// A webhook item mustn't interleave with a restore of the same bookmark.
func TestRestoreHoldsBookmarkLock(t *testing.T) {
	e := sortedEnv(t)
	snap := e.takeSnapshot(domain.Scope{}).SnapshotID
	e.provider.SetCollection(bmID(2), colBeta)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	checks := 0
	e.deps.Provider = &updateHook{listLog: e.logged, before: func(id domain.ID, _ ports.BookmarkPatch) error {
		checks++
		// With the lock free, a cancelled context would get it about half the time.
		for range 40 {
			if unlock, err := e.deps.Pipeline.LockBookmark(cancelled, id); err == nil {
				unlock()
				t.Error("bookmark not locked during the write")
				break
			}
		}
		return nil
	}}
	e.restore(snap)
	if checks != 1 {
		t.Errorf("writes = %d", checks)
	}
}

// blankWrites answers a write as a broken adapter would: success with an empty bookmark.
type blankWrites struct{ *listLog }

func (blankWrites) UpdateBookmark(context.Context, domain.ID, ports.BookmarkPatch) (domain.Bookmark, error) {
	return domain.Bookmark{}, nil
}

func TestRestoreWriteAnsweredForAnotherBookmarkStaysPending(t *testing.T) {
	e := sortedEnv(t)
	snap := e.takeSnapshot(domain.Scope{}).SnapshotID
	e.syncNow()
	moved := e.collectionOf(bmID(1))
	if moved == pfake.Inbox {
		t.Fatal("sync should have filed it")
	}
	e.deps.Provider = blankWrites{e.logged}

	job := e.restoreJob(snap)
	if err := e.runJob(job); err != nil {
		t.Fatal(err)
	}
	if job.Progress.Failed == 0 {
		t.Fatalf("progress = %+v", job.Progress)
	}
	st := e.state(bmID(1))
	if st.PendingCollection == nil || *st.PendingCollection != pfake.Inbox {
		t.Fatalf("pending = %v, want the restore's write still pending", st.PendingCollection)
	}
	if st.KnownCollection != moved || st.RestoredAt != nil {
		t.Fatalf("state recorded the write: %+v", st)
	}
}
