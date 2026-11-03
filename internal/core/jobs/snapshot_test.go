package jobs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	pfake "github.com/wikiabhi/rainwise/internal/adapters/provider/fake"
	"github.com/wikiabhi/rainwise/internal/domain"
)

// useRealSnapshots swaps the stub for the service these tests are about.
func useRealSnapshots(e *jobEnv, retention int) *SnapshotService {
	svc := NewSnapshotService(SnapshotDeps{Provider: e.logged, Store: e.store, Now: e.nowFn, Retention: retention})
	e.deps.Snapshots = svc
	return svc
}

func (e *jobEnv) takeSnapshot(scope domain.Scope) *domain.Job {
	e.t.Helper()
	job := e.newJob(domain.JobSnapshot, func(j *domain.Job) { j.Scope = scope })
	e.mustRun(job)
	return job
}

func (e *jobEnv) snapshotItems(id string) map[domain.ID]domain.SnapshotItem {
	e.t.Helper()
	out := map[domain.ID]domain.SnapshotItem{}
	for cursor := ""; ; {
		page, err := e.store.Snapshots().ListSnapshotItems(context.Background(), id, cursor, 100)
		if err != nil {
			e.t.Fatal(err)
		}
		for _, it := range page.Items {
			out[it.Bookmark] = it
		}
		if page.NextToken == "" {
			return out
		}
		cursor = page.NextToken
	}
}

func (e *jobEnv) snapshotIDs() []string {
	e.t.Helper()
	page, err := e.store.Snapshots().ListSnapshots(context.Background(), "", 100)
	if err != nil {
		e.t.Fatal(err)
	}
	var ids []string
	for _, s := range page.Items {
		ids = append(ids, s.ID)
	}
	slices.Sort(ids)
	return ids
}

func TestSnapshotRecordsAll(t *testing.T) {
	t.Run("whole library", func(t *testing.T) {
		e := newJobEnv(t)
		useRealSnapshots(e, 10)
		e.addBookmark(1, colAlpha, "one", "Two")
		e.addBookmark(2, colBeta)
		e.addBookmark(3, pfake.Inbox, "three")

		job := e.takeSnapshot(domain.Scope{})

		if job.SnapshotID == "" {
			t.Fatal("no snapshot id on the job")
		}
		if saved := e.reload(job.ID); saved.SnapshotID != job.SnapshotID {
			t.Errorf("snapshot id not saved: %q", saved.SnapshotID)
		}
		snap, err := e.store.Snapshots().GetSnapshot(context.Background(), job.SnapshotID)
		if err != nil {
			t.Fatal(err)
		}
		if !snap.ScopeAll || snap.Provider != "fake" || snap.JobID != job.ID || !snap.CreatedAt.Equal(t0) {
			t.Errorf("snapshot = %+v", snap)
		}
		if len(snap.Collections) != 3 || snap.Collections[0].ID != colAlpha || snap.Collections[0].Name != "Alpha" {
			t.Errorf("collections = %+v", snap.Collections)
		}
		items := e.snapshotItems(job.SnapshotID)
		if len(items) != 3 {
			t.Fatalf("items = %+v", items)
		}
		for n, want := range map[int]domain.SnapshotItem{
			1: {Collection: colAlpha, Tags: []string{"one", "Two"}},
			2: {Collection: colBeta},
			3: {Collection: pfake.Inbox, Tags: []string{"three"}},
		} {
			got := items[bmID(n)]
			if got.Collection != want.Collection || !slices.Equal(got.Tags, want.Tags) {
				t.Errorf("item %d = %+v, want %+v", n, got, want)
			}
		}
		if job.Progress.Total != 3 || job.Progress.Processed != 3 {
			t.Errorf("progress = %+v", job.Progress)
		}
	})

	t.Run("each bookmark's pin", func(t *testing.T) {
		e := newJobEnv(t)
		useRealSnapshots(e, 10)
		ctx := context.Background()
		e.addBookmark(1, colAlpha)
		e.addBookmark(2, colBeta)
		e.addBookmark(3, colAlpha)
		e.addBookmark(4, colBeta)
		e.addBookmark(5, colBeta)
		earlier := t0.Add(-time.Hour)
		for _, st := range []domain.BookmarkState{
			{ID: bmID(1), KnownCollection: colAlpha, PinSource: domain.PinUser, PinnedAt: &earlier},
			{ID: bmID(2), KnownCollection: colBeta, PinSource: domain.PinBaseline, PinnedAt: &earlier},
			{ID: bmID(3), KnownCollection: colAlpha},
			{ID: bmID(4), KnownCollection: colAlpha},                              // Moved by hand, unread.
			{ID: bmID(5), KnownCollection: colAlpha, PendingCollection: &colBeta}, // Our own landed write.
		} {
			if err := e.store.BookmarkStates().PutState(ctx, st); err != nil {
				t.Fatal(err)
			}
		}
		e.addBookmark(6, colAlpha) // No state at all.

		items := e.snapshotItems(e.takeSnapshot(domain.Scope{}).SnapshotID)

		for n, want := range map[int]struct {
			source domain.PinSource
			at     *time.Time
		}{
			1: {domain.PinUser, &earlier},
			2: {domain.PinBaseline, &earlier},
			3: {domain.PinNone, nil},
			4: {domain.PinUser, &t0},
			5: {domain.PinNone, nil},
			6: {domain.PinNone, nil},
		} {
			got := items[bmID(n)]
			if got.PinSource != want.source || (got.PinnedAt == nil) != (want.at == nil) || (want.at != nil && !got.PinnedAt.Equal(*want.at)) {
				t.Errorf("bookmark %d: %+v, want %+v", n, got, want)
			}
		}
		// Reading isn't noticing: the next job still does that.
		if st := e.state(bmID(4)); st.PinSource != domain.PinNone || st.KnownCollection != colAlpha {
			t.Errorf("state changed: %+v", st)
		}
	})

	t.Run("the first-backfill setting, for the whole library only", func(t *testing.T) {
		e := newJobEnv(t)
		useRealSnapshots(e, 10)
		e.addBookmark(1, colBeta)
		get := func(id string) domain.Snapshot {
			t.Helper()
			snap, err := e.store.Snapshots().GetSnapshot(context.Background(), id)
			if err != nil {
				t.Fatal(err)
			}
			return snap
		}
		if v := get(e.takeSnapshot(domain.Scope{}).SnapshotID).FirstBackfillDone; v != "" {
			t.Errorf("unset: %q", v)
		}
		const done = "2026-01-02T03:04:05Z"
		e.putSetting(firstBackfillKey, done)
		if v := get(e.takeSnapshot(domain.Scope{}).SnapshotID).FirstBackfillDone; v != done {
			t.Errorf("set: %q", v)
		}
		if v := get(e.takeSnapshot(domain.Scope{Collections: []string{"Beta"}}).SnapshotID).FirstBackfillDone; v != "" {
			t.Errorf("scoped: %q", v)
		}
	})

	t.Run("more bookmarks than one page", func(t *testing.T) {
		e := newJobEnv(t)
		useRealSnapshots(e, 10)
		for n := 1; n <= 120; n++ {
			e.addBookmark(n, colAlpha)
		}
		job := e.takeSnapshot(domain.Scope{})
		if got := len(e.snapshotItems(job.SnapshotID)); got != 120 {
			t.Errorf("items = %d", got)
		}
	})

	t.Run("a scope leaves the rest out", func(t *testing.T) {
		e := newJobEnv(t)
		useRealSnapshots(e, 10)
		e.addBookmark(1, colAlpha)
		e.addBookmark(2, colBeta)
		job := e.takeSnapshot(domain.Scope{Collections: []string{"Beta"}})
		snap, err := e.store.Snapshots().GetSnapshot(context.Background(), job.SnapshotID)
		if err != nil {
			t.Fatal(err)
		}
		items := e.snapshotItems(job.SnapshotID)
		if snap.ScopeAll || len(items) != 1 || items[bmID(2)].Collection != colBeta {
			t.Errorf("scope all %v, items %+v", snap.ScopeAll, items)
		}
	})

	t.Run("prunes to retention and keeps those in use", func(t *testing.T) {
		e := newJobEnv(t)
		useRealSnapshots(e, 2)
		ctx := context.Background()
		for i, id := range []string{"old-running", "old-queued", "old-own", "old-free", "recent"} {
			if err := e.store.Snapshots().CreateSnapshot(ctx, domain.Snapshot{ID: id, Provider: "fake", CreatedAt: t0.Add(time.Duration(i-10) * time.Hour)}); err != nil {
				t.Fatal(err)
			}
		}
		// A running restore names its target in params; a queued one is waiting for
		// its target; a running backfill has its own snapshot on the job.
		e.newJob(domain.JobRestore, func(j *domain.Job) { j.Params.SnapshotID = "old-running" })
		e.newJob(domain.JobRestore, func(j *domain.Job) { j.Status = domain.JobQueued; j.Params.SnapshotID = "old-queued" })
		e.newJob(domain.JobBackfill, func(j *domain.Job) { j.SnapshotID = "old-own" })
		e.newJob(domain.JobRestore, func(j *domain.Job) { j.Status = domain.JobSucceeded; j.Params.SnapshotID = "old-free" })

		job := e.takeSnapshot(domain.Scope{})

		want := []string{job.SnapshotID, "old-own", "old-queued", "old-running", "recent"}
		slices.Sort(want)
		if got := e.snapshotIDs(); !slices.Equal(got, want) {
			t.Errorf("snapshots = %v, want %v", got, want)
		}
	})

	t.Run("retention of zero takes the default", func(t *testing.T) {
		e := newJobEnv(t)
		useRealSnapshots(e, 0)
		var last string
		for i := range 12 {
			e.setNow(t0.Add(time.Duration(i) * time.Minute))
			job := e.takeSnapshot(domain.Scope{})
			last = job.SnapshotID
			job.Status = domain.JobSucceeded // A finished job doesn't protect its snapshot.
			if err := e.store.Jobs().UpdateJob(context.Background(), *job); err != nil {
				t.Fatal(err)
			}
		}
		ids := e.snapshotIDs()
		if len(ids) != 10 || !slices.Contains(ids, last) {
			t.Errorf("snapshots = %v", ids)
		}
	})

	t.Run("a dry run stores nothing", func(t *testing.T) {
		e := newJobEnv(t)
		useRealSnapshots(e, 10)
		e.addBookmark(1, colAlpha)
		job := e.newJob(domain.JobSnapshot, dryRun)
		e.mustRun(job)
		if len(e.snapshotIDs()) != 0 || job.SnapshotID != "" || len(e.sleeps) != 0 {
			t.Errorf("snapshots %v, job %q, sleeps %v", e.snapshotIDs(), job.SnapshotID, e.sleeps)
		}
		if job.Progress.Total != 1 {
			t.Errorf("progress = %+v", job.Progress)
		}
	})
}

func TestSnapshotWaitsDurable(t *testing.T) {
	t.Run("waits after the commit, through the injected sleep", func(t *testing.T) {
		e := newJobEnv(t)
		useRealSnapshots(e, 10)
		e.addBookmark(1, colAlpha)
		sleep := e.deps.Sleep
		e.deps.Sleep = func(ctx context.Context, d time.Duration) error {
			if len(e.snapshotIDs()) != 1 {
				t.Error("waited before the snapshot was committed")
			}
			return sleep(ctx, d)
		}
		e.takeSnapshot(domain.Scope{})
		if !slices.Equal(e.sleeps, []time.Duration{30 * time.Second}) {
			t.Errorf("sleeps = %v", e.sleeps)
		}
	})

	t.Run("a stop during the wait resumes with the same snapshot", func(t *testing.T) {
		e := newJobEnv(t)
		useRealSnapshots(e, 10)
		e.addBookmark(1, colAlpha)
		job := e.newJob(domain.JobSnapshot, nil)
		err := Handlers(e.deps)[domain.JobSnapshot].Run(context.Background(), job, e.stopAfter(job, 1))
		if !errors.Is(err, ErrInterrupted) {
			t.Fatalf("err = %v", err)
		}
		saved := e.reload(job.ID)
		e.mustRun(saved)
		if saved.SnapshotID != job.SnapshotID || len(e.snapshotIDs()) != 1 {
			t.Errorf("snapshots = %v, id %q then %q", e.snapshotIDs(), job.SnapshotID, saved.SnapshotID)
		}
	})

	t.Run("no taker fails", func(t *testing.T) {
		e := newJobEnv(t)
		e.deps.Snapshots = nil
		job := e.newJob(domain.JobSnapshot, nil)
		if err := e.runJob(job); !errors.Is(err, domain.ErrValidation) {
			t.Errorf("err = %v", err)
		}
	})
}

// A taxonomy key means something only to a job that has the taxonomy, so the
// snapshot must cover what the job resolved it to, and nothing else.
func TestSnapshotCoversKeyScope(t *testing.T) {
	setup := func(t *testing.T, key string) *jobEnv {
		e := newJobEnv(t)
		useRealSnapshots(e, 10)
		tx := fixedTaxonomy()
		tx.Collections[1].Key = key // Names Beta.
		e.seedTaxonomy(tx)
		e.addBookmark(1, colBeta)
		e.addBookmark(2, colBeta)
		e.addBookmark(3, colAlpha)
		e.addBookmark(4, colReview)
		return e
	}
	check := func(t *testing.T, e *jobEnv, id string) {
		t.Helper()
		items := e.snapshotItems(id)
		var got []string
		for b := range items {
			got = append(got, b.Local)
		}
		slices.Sort(got)
		if want := []string{"b1", "b2"}; !slices.Equal(got, want) {
			t.Errorf("snapshot covers %v, want %v", got, want)
		}
	}

	for name, key := range map[string]string{
		"a key that names nothing else":     "reading",
		"a key spelled like another's name": "Alpha",
	} {
		t.Run("reclassify, "+name, func(t *testing.T) {
			e := setup(t, key)
			job := e.reclassify(domain.Scope{Collections: []string{key}}, nil)
			if job.Progress.Processed != 2 {
				t.Fatalf("progress = %+v", job.Progress)
			}
			check(t, e, job.SnapshotID)
			snap, err := e.store.Snapshots().GetSnapshot(context.Background(), job.SnapshotID)
			if err != nil {
				t.Fatal(err)
			}
			if snap.ScopeAll {
				t.Error("scoped snapshot marked as the whole library")
			}
		})
		t.Run("snapshot job, "+name, func(t *testing.T) {
			e := setup(t, key)
			check(t, e, e.takeSnapshot(domain.Scope{Collections: []string{key}}).SnapshotID)
		})
	}
}

func TestExportJSON(t *testing.T) {
	e := newJobEnv(t)
	svc := useRealSnapshots(e, 10)
	const total = 620 // More than one store page.
	for n := 1; n <= total; n++ {
		e.addBookmark(n, colAlpha, "tag"+strconv.Itoa(n))
	}
	id := e.takeSnapshot(domain.Scope{}).SnapshotID

	var buf bytes.Buffer
	if err := svc.Export(context.Background(), id, &buf); err != nil {
		t.Fatal(err)
	}
	var got struct {
		domain.Snapshot
		Items []domain.SnapshotItem `json:"items"`
	}
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("output doesn't parse: %v\n%s", err, buf.String())
	}
	if got.ID != id || got.Provider != "fake" || len(got.Collections) != 3 {
		t.Errorf("metadata = %+v", got.Snapshot)
	}
	want := e.snapshotItems(id)
	if len(got.Items) != total || len(want) != total {
		t.Fatalf("items = %d, want %d", len(got.Items), total)
	}
	for _, it := range got.Items {
		if !reflect.DeepEqual(it, want[it.Bookmark]) {
			t.Errorf("item %+v, want %+v", it, want[it.Bookmark])
		}
	}

	t.Run("an unknown snapshot writes nothing", func(t *testing.T) {
		var buf bytes.Buffer
		if err := svc.Export(context.Background(), "nope", &buf); !errors.Is(err, domain.ErrNotFound) {
			t.Errorf("err = %v", err)
		}
		if buf.Len() != 0 {
			t.Errorf("wrote %q", buf.String())
		}
	})

	t.Run("an empty snapshot is still valid", func(t *testing.T) {
		if err := e.store.Snapshots().CreateSnapshot(context.Background(), domain.Snapshot{ID: "empty", Provider: "fake", CreatedAt: t0}); err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		if err := svc.Export(context.Background(), "empty", &buf); err != nil {
			t.Fatal(err)
		}
		var got struct {
			Items []domain.SnapshotItem `json:"items"`
		}
		if err := json.Unmarshal(buf.Bytes(), &got); err != nil || got.Items == nil || len(got.Items) != 0 {
			t.Errorf("items %v, err %v, output %s", got.Items, err, buf.String())
		}
	})
}
