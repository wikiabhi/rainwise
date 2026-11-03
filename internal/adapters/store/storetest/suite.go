// Package storetest holds the conformance suite every ports.Store must pass.
package storetest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

var t0 = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func bid(n int) domain.ID { return domain.ID{Provider: "p", Local: fmt.Sprint(n)} }

func job(id string, at time.Time) domain.Job {
	return domain.Job{ID: id, Type: domain.JobBackfill, Status: domain.JobQueued, Provider: "p", TaxonomyID: "tx", CreatedAt: at}
}

func event(id, key string) domain.StoredEvent {
	return domain.StoredEvent{
		Event:       domain.Event{Provider: "p", Type: domain.EventBookmarkCreated, Resource: domain.ResourceRef{ID: id}, OccurredAt: t0},
		ID:          id,
		DedupKey:    key,
		Status:      domain.EventPending,
		NextAttempt: t0,
		ReceivedAt:  t0,
	}
}

// Run executes the conformance suite against stores built by newStore.
func Run(t *testing.T, newStore func(t *testing.T) ports.Store) {
	ctx := context.Background()
	must := func(t *testing.T, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	t.Run("InsertEventDedups", func(t *testing.T) {
		ev := newStore(t).Events()
		ok, err := ev.InsertEvent(ctx, event("e1", "k"))
		must(t, err)
		if !ok {
			t.Fatal("first insert should insert")
		}
		ok, err = ev.InsertEvent(ctx, event("e2", "k"))
		must(t, err)
		if ok {
			t.Fatal("second insert with same DedupKey should not insert")
		}
	})

	t.Run("ClaimEventsSkipsHeldAndFuture", func(t *testing.T) {
		ev := newStore(t).Events()
		held := event("held", "k1")
		held.Status = domain.EventHeld
		future := event("future", "k2")
		future.NextAttempt = t0.Add(time.Hour)
		for _, e := range []domain.StoredEvent{held, future, event("a", "k3"), event("b", "k4")} {
			_, err := ev.InsertEvent(ctx, e)
			must(t, err)
		}
		got, err := ev.ClaimEvents(ctx, t0, 10)
		must(t, err)
		if len(got) != 2 {
			t.Fatalf("claimed %d events, want 2", len(got))
		}
		for _, e := range got {
			if e.ID != "a" && e.ID != "b" {
				t.Errorf("unexpected claim %s", e.ID)
			}
		}
		again, err := ev.ClaimEvents(ctx, t0, 10)
		must(t, err)
		if len(again) != 0 {
			t.Fatalf("re-claimed %d events before update", len(again))
		}
		// Updating releases the claim (here: back to pending, retry now).
		must(t, ev.UpdateEvent(ctx, got[0]))
		again, err = ev.ClaimEvents(ctx, t0, 10)
		must(t, err)
		if len(again) != 1 || again[0].ID != got[0].ID {
			t.Fatalf("after update claimed %v, want only %s", again, got[0].ID)
		}
		later, err := ev.ClaimEvents(ctx, t0.Add(2*time.Hour), 10)
		must(t, err)
		if len(later) != 1 || later[0].ID != "future" {
			t.Fatalf("future event not claimed when due: %v", later)
		}
		must(t, ev.UpdateEvent(ctx, domain.StoredEvent{ID: "a", DedupKey: "k3", Status: domain.EventDone}))
		if err := ev.UpdateEvent(ctx, event("nope", "kx")); !errors.Is(err, domain.ErrNotFound) {
			t.Errorf("UpdateEvent missing = %v, want ErrNotFound", err)
		}
	})

	t.Run("ResetClaimed", func(t *testing.T) {
		ev := newStore(t).Events()
		_, err := ev.InsertEvent(ctx, event("a", "k1"))
		must(t, err)
		got, err := ev.ClaimEvents(ctx, t0, 10)
		must(t, err)
		again, err := ev.ClaimEvents(ctx, t0, 10)
		must(t, err)
		if len(got) != 1 || len(again) != 0 {
			t.Fatalf("claims = %d then %d, want 1 then 0", len(got), len(again))
		}
		must(t, ev.ResetClaimed(ctx))
		again, err = ev.ClaimEvents(ctx, t0, 10)
		must(t, err)
		if len(again) != 1 {
			t.Errorf("after reset claimed %d, want 1", len(again))
		}
	})

	t.Run("DropHeldBefore", func(t *testing.T) {
		ev := newStore(t).Events()
		old := event("old", "k1")
		old.Status, old.ReceivedAt = domain.EventHeld, t0
		fresh := event("fresh", "k2")
		fresh.Status, fresh.ReceivedAt = domain.EventHeld, t0.Add(48*time.Hour)
		pend := event("pend", "k3")
		for _, e := range []domain.StoredEvent{old, fresh, pend} {
			_, err := ev.InsertEvent(ctx, e)
			must(t, err)
		}
		n, err := ev.DropHeldBefore(ctx, t0.Add(24*time.Hour))
		must(t, err)
		if n != 1 {
			t.Fatalf("dropped %d, want 1", n)
		}
		// The dropped key is free again.
		ok, err := ev.InsertEvent(ctx, event("old2", "k1"))
		must(t, err)
		if !ok {
			t.Error("dedup key of dropped event should be reusable")
		}
	})

	t.Run("ClaimNextQueuedOldestFirst", func(t *testing.T) {
		js := newStore(t).Jobs()
		newer := job("newer", t0.Add(time.Minute))
		older := job("older", t0)
		dry := job("dry", t0.Add(-time.Hour))
		dry.DryRun = true
		other := job("other", t0.Add(-2*time.Hour))
		other.Provider = "q"
		for _, j := range []domain.Job{newer, older, dry, other} {
			must(t, js.CreateJob(ctx, j))
		}
		got, ok, err := js.ClaimNextQueued(ctx, "p", false)
		must(t, err)
		if !ok || got.ID != "older" || got.Status != domain.JobRunning {
			t.Fatalf("claim = %v %v", got.ID, got.Status)
		}
		stored, err := js.GetJob(ctx, "older")
		must(t, err)
		if stored.Status != domain.JobRunning {
			t.Errorf("stored status = %s", stored.Status)
		}
		got, ok, err = js.ClaimNextQueued(ctx, "p", true)
		must(t, err)
		if !ok || got.ID != "dry" {
			t.Fatalf("dry claim = %v", got.ID)
		}
		got, ok, err = js.ClaimNextQueued(ctx, "p", false)
		must(t, err)
		if !ok || got.ID != "newer" {
			t.Fatalf("claim = %v", got.ID)
		}
		_, ok, err = js.ClaimNextQueued(ctx, "p", false)
		must(t, err)
		if ok {
			t.Error("nothing left for provider p")
		}
		running, err := js.ListRunning(ctx)
		must(t, err)
		if len(running) != 3 {
			t.Errorf("ListRunning = %d, want 3", len(running))
		}
	})

	t.Run("FindActiveJob", func(t *testing.T) {
		js := newStore(t).Jobs()
		if _, ok, err := js.FindActiveJob(ctx, "p", domain.JobBackfill); err != nil || ok {
			t.Fatalf("empty store: ok=%v err=%v", ok, err)
		}
		j := job("j1", t0)
		must(t, js.CreateJob(ctx, j))
		got, ok, err := js.FindActiveJob(ctx, "p", domain.JobBackfill)
		must(t, err)
		if !ok || got.ID != "j1" {
			t.Fatal("queued job should be active")
		}
		if _, ok, _ := js.FindActiveJob(ctx, "p", domain.JobSync); ok {
			t.Error("different type matched")
		}
		j.Status = domain.JobRunning
		must(t, js.UpdateJob(ctx, j))
		if _, ok, _ := js.FindActiveJob(ctx, "p", domain.JobBackfill); !ok {
			t.Error("running job should be active")
		}
		j.Status = domain.JobSucceeded
		must(t, js.UpdateJob(ctx, j))
		if _, ok, _ := js.FindActiveJob(ctx, "p", domain.JobBackfill); ok {
			t.Error("finished job should not be active")
		}
	})

	t.Run("IdempotencyKeyLookup", func(t *testing.T) {
		js := newStore(t).Jobs()
		j := job("j1", t0)
		j.IdempotencyKey = "idem"
		must(t, js.CreateJob(ctx, j))
		must(t, js.CreateJob(ctx, job("j2", t0)))
		got, ok, err := js.FindJobByIdempotencyKey(ctx, "idem")
		must(t, err)
		if !ok || got.ID != "j1" {
			t.Fatal("lookup failed")
		}
		if _, ok, _ := js.FindJobByIdempotencyKey(ctx, "other"); ok {
			t.Error("unexpected match")
		}
		if err := js.CreateJob(ctx, j); !errors.Is(err, domain.ErrConflict) {
			t.Errorf("duplicate create = %v, want ErrConflict", err)
		}
	})

	t.Run("JobRoundTrip", func(t *testing.T) {
		js := newStore(t).Jobs()
		cut := t0.Add(time.Hour)
		started := t0.Add(time.Minute)
		j := job("j1", t0)
		j.Params = domain.JobParams{BatchSize: 5, Force: true, ApprovedNewTags: map[string]string{"a": "b"}}
		j.Scope = domain.Scope{Collections: []string{"c"}, Tags: []string{"t"}, IDs: []string{"p:1"}}
		j.Progress = domain.JobProgress{Total: 9, Processed: 3, Changed: 2, Skipped: 1, NeedsReview: 1, Failed: 1}
		j.Checkpoint = domain.Checkpoint{PageToken: "tok", PinCutoff: &cut, Pauses: 2}
		j.StartedAt = &started
		j.FromDryRun, j.SnapshotID, j.Error, j.DryRun = "d1", "s1", "boom", true
		must(t, js.CreateJob(ctx, j))
		got, err := js.GetJob(ctx, "j1")
		must(t, err)
		if !reflect.DeepEqual(got, j) {
			t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, j)
		}
		j.Progress.Processed = 9
		j.Status = domain.JobSucceeded
		fin := t0.Add(2 * time.Hour)
		j.FinishedAt = &fin
		must(t, js.UpdateJob(ctx, j))
		got, err = js.GetJob(ctx, "j1")
		must(t, err)
		if !reflect.DeepEqual(got, j) {
			t.Fatalf("update mismatch:\n got %+v\nwant %+v", got, j)
		}
		page, err := js.ListJobs(ctx, "", 10)
		must(t, err)
		if len(page.Items) != 1 {
			t.Errorf("ListJobs = %d", len(page.Items))
		}
	})

	t.Run("UpdateJobIf", func(t *testing.T) {
		js := newStore(t).Jobs()
		j := job("j1", t0)
		j.Status = domain.JobRunning
		must(t, js.CreateJob(ctx, j))

		j.Progress.Processed = 5
		ok, err := js.UpdateJobIf(ctx, j, domain.JobRunning)
		must(t, err)
		if !ok {
			t.Fatal("UpdateJobIf from the current status = false")
		}
		if got, _ := js.GetJob(ctx, "j1"); got.Progress.Processed != 5 {
			t.Errorf("update not written: %+v", got.Progress)
		}

		stale := j
		stale.Status, stale.Progress.Processed = domain.JobSucceeded, 9
		ok, err = js.UpdateJobIf(ctx, stale, domain.JobQueued, domain.JobCancelling)
		must(t, err)
		if ok {
			t.Fatal("UpdateJobIf from other statuses = true")
		}
		if got, _ := js.GetJob(ctx, "j1"); got.Status != domain.JobRunning || got.Progress.Processed != 5 {
			t.Errorf("refused update changed the job: %+v", got)
		}

		ok, err = js.UpdateJobIf(ctx, stale, domain.JobQueued, domain.JobRunning)
		must(t, err)
		if !ok {
			t.Fatal("UpdateJobIf with the status among several = false")
		}
		if got, _ := js.GetJob(ctx, "j1"); got.Status != domain.JobSucceeded {
			t.Errorf("status = %s", got.Status)
		}

		if _, err := js.UpdateJobIf(ctx, job("nope", t0), domain.JobQueued); !errors.Is(err, domain.ErrNotFound) {
			t.Errorf("missing job: err = %v, want ErrNotFound", err)
		}
	})

	t.Run("DecisionRoundTripAndPaging", func(t *testing.T) {
		js := newStore(t).Jobs()
		must(t, js.CreateJob(ctx, job("j1", t0)))
		applied := t0.Add(time.Minute)
		score := 0.5
		target := bid(999)
		mk := func(i int) domain.DecisionRecord {
			return domain.DecisionRecord{
				JobID: "j1", Bookmark: bid(i), InputHash: "h",
				ObservedCollection: bid(1000), ObservedTags: []string{"x"},
				TargetCollection: &target, AddTags: []string{"y"},
				Decision: domain.Decision{
					Collection: domain.CollectionChoice{Key: "k"},
					Tags:       []domain.TagSuggestion{{Name: "y", Score: &score}},
					Confidence: 0.9,
					Model:      domain.ModelInfo{Model: "m", InputTokens: 3},
				},
				Reason: "r", Result: "applied", AppliedAt: &applied,
			}
		}
		for i := 0; i < 120; i++ {
			must(t, js.SaveDecision(ctx, mk(i)))
		}
		got, ok, err := js.GetDecision(ctx, "j1", bid(7))
		must(t, err)
		if !ok || !reflect.DeepEqual(got, mk(7)) {
			t.Fatalf("decision mismatch: ok=%v\n got %+v\nwant %+v", ok, got, mk(7))
		}
		if _, ok, _ := js.GetDecision(ctx, "j1", bid(5000)); ok {
			t.Error("unexpected decision")
		}
		// Saving again replaces without duplicating.
		upd := mk(7)
		upd.Result = "skipped"
		must(t, js.SaveDecision(ctx, upd))

		seen := map[string]bool{}
		cursor, pages := "", 0
		for {
			p, err := js.ListDecisions(ctx, "j1", cursor, 50)
			must(t, err)
			pages++
			for _, d := range p.Items {
				if seen[d.Bookmark.String()] {
					t.Fatalf("repeat %s", d.Bookmark)
				}
				seen[d.Bookmark.String()] = true
			}
			if p.NextToken == "" {
				break
			}
			cursor = p.NextToken
		}
		if pages != 3 || len(seen) != 120 {
			t.Fatalf("pages=%d seen=%d, want 3 and 120", pages, len(seen))
		}
		if err := js.SaveDecision(ctx, domain.DecisionRecord{JobID: "missing", Bookmark: bid(1)}); !errors.Is(err, domain.ErrNotFound) {
			t.Errorf("decision for missing job = %v, want ErrNotFound", err)
		}
	})

	t.Run("StateRoundTrip", func(t *testing.T) {
		st := newStore(t).BookmarkStates()
		if _, ok, err := st.GetState(ctx, bid(1)); err != nil || ok {
			t.Fatalf("missing state: ok=%v err=%v", ok, err)
		}
		bare := domain.BookmarkState{ID: bid(1), KnownCollection: bid(10), KnownTags: []string{"a"}, InputHash: "h", Status: "done"}
		must(t, st.PutState(ctx, bare))
		got, ok, err := st.GetState(ctx, bid(1))
		must(t, err)
		if !ok || !reflect.DeepEqual(got, bare) {
			t.Fatalf("bare mismatch:\n got %+v\nwant %+v", got, bare)
		}
		pinned, proc, restored := t0, t0.Add(time.Minute), t0.Add(time.Hour)
		pc := bid(11)
		full := domain.BookmarkState{
			ID: bid(2), KnownCollection: bid(10), KnownTags: []string{"a", "b"},
			PinSource: domain.PinUser, PinnedAt: &pinned, InputHash: "h", TaxonomyVersion: 4,
			Model: "m", Status: "pending", PendingCollection: &pc, PendingTags: []string{"c"},
			FetchAttempts: 2, ProcessedAt: &proc, RestoredAt: &restored,
		}
		must(t, st.PutState(ctx, full))
		got, _, err = st.GetState(ctx, bid(2))
		must(t, err)
		if !reflect.DeepEqual(got, full) {
			t.Fatalf("full mismatch:\n got %+v\nwant %+v", got, full)
		}
		full.PendingCollection, full.PinnedAt = nil, nil
		must(t, st.PutState(ctx, full))
		got, _, _ = st.GetState(ctx, bid(2))
		if got.PendingCollection != nil || got.PinnedAt != nil {
			t.Error("pointers should clear to nil")
		}
	})

	t.Run("Settings", func(t *testing.T) {
		st := newStore(t).BookmarkStates()
		if _, ok, err := st.GetSetting(ctx, "w"); err != nil || ok {
			t.Fatalf("missing: ok=%v err=%v", ok, err)
		}
		must(t, st.PutSetting(ctx, "w", "1"))
		must(t, st.PutSetting(ctx, "w", "2"))
		v, ok, err := st.GetSetting(ctx, "w")
		must(t, err)
		if !ok || v != "2" {
			t.Errorf("got %q %v", v, ok)
		}
		must(t, st.DeleteSetting(ctx, "w"))
		if _, ok, err := st.GetSetting(ctx, "w"); err != nil || ok {
			t.Errorf("after delete: ok=%v err=%v", ok, err)
		}
		must(t, st.DeleteSetting(ctx, "w")) // Deleting what isn't there is fine.
	})

	t.Run("SnapshotItemsPaging", func(t *testing.T) {
		ss := newStore(t).Snapshots()
		snap := domain.Snapshot{
			ID: "s1", Provider: "p", JobID: "j1", CreatedAt: t0, ScopeAll: true, FirstBackfillDone: "2026-01-02T03:04:05Z",
			Collections:        []domain.Collection{{ID: bid(10), Name: "C", Parent: &domain.ID{Provider: "p", Local: "9"}}},
			CreatedCollections: []domain.ID{},
		}
		must(t, ss.CreateSnapshot(ctx, snap))
		got, err := ss.GetSnapshot(ctx, "s1")
		must(t, err)
		if !reflect.DeepEqual(got, snap) {
			t.Fatalf("snapshot mismatch:\n got %+v\nwant %+v", got, snap)
		}
		var items []domain.SnapshotItem
		for i := 0; i < 25; i++ {
			items = append(items, domain.SnapshotItem{Bookmark: bid(i), Collection: bid(10), Tags: []string{"t"}})
		}
		pinnedAt := t0.Add(time.Hour)
		items[3].PinSource, items[3].PinnedAt = domain.PinUser, &pinnedAt
		items[4].PinSource, items[4].PinnedAt = domain.PinBaseline, &pinnedAt
		must(t, ss.AddSnapshotItems(ctx, "s1", items[:10]))
		must(t, ss.AddSnapshotItems(ctx, "s1", items[10:]))
		var all []domain.SnapshotItem
		cursor := ""
		for {
			p, err := ss.ListSnapshotItems(ctx, "s1", cursor, 10)
			must(t, err)
			all = append(all, p.Items...)
			if p.NextToken == "" {
				break
			}
			cursor = p.NextToken
		}
		if !reflect.DeepEqual(all, items) {
			t.Fatalf("paged items differ: got %d want %d", len(all), len(items))
		}
	})

	t.Run("AddCreatedCollection", func(t *testing.T) {
		ss := newStore(t).Snapshots()
		must(t, ss.CreateSnapshot(ctx, domain.Snapshot{ID: "s1", CreatedAt: t0}))
		must(t, ss.AddCreatedCollection(ctx, "s1", bid(50)))
		must(t, ss.AddCreatedCollection(ctx, "s1", bid(51)))
		got, err := ss.GetSnapshot(ctx, "s1")
		must(t, err)
		if !reflect.DeepEqual(got.CreatedCollections, []domain.ID{bid(50), bid(51)}) {
			t.Errorf("created = %v", got.CreatedCollections)
		}
	})

	t.Run("PruneKeepsNewestAndProtected", func(t *testing.T) {
		ss := newStore(t).Snapshots()
		for i := 1; i <= 5; i++ {
			id := fmt.Sprintf("s%d", i)
			must(t, ss.CreateSnapshot(ctx, domain.Snapshot{ID: id, CreatedAt: t0.Add(time.Duration(i) * time.Hour)}))
			must(t, ss.AddSnapshotItems(ctx, id, []domain.SnapshotItem{{Bookmark: bid(i), Collection: bid(1)}}))
		}
		must(t, ss.PruneSnapshots(ctx, 2, []string{"s1"}))
		page, err := ss.ListSnapshots(ctx, "", 10)
		must(t, err)
		var ids []string
		for _, s := range page.Items {
			ids = append(ids, s.ID)
		}
		want := []string{"s5", "s4", "s1"} // newest first
		if !reflect.DeepEqual(ids, want) {
			t.Fatalf("remaining = %v, want %v", ids, want)
		}
		if _, err := ss.GetSnapshot(ctx, "s2"); !errors.Is(err, domain.ErrNotFound) {
			t.Errorf("pruned snapshot get = %v", err)
		}
	})

	t.Run("PutTaxonomyVersioning", func(t *testing.T) {
		ts := newStore(t).Taxonomies()
		tx := domain.Taxonomy{ID: "tx", Mode: domain.ModeFixed, Status: domain.TaxonomyActive,
			Collections: []domain.TaxonomyCollection{{Key: "k", Name: "K"}},
			Rules:       domain.TaxonomyRules{MinTags: 1, MaxTags: 3, TagAliases: map[string]string{"a": "b"}},
			Proposals:   []domain.CollectionProposal{{Name: "n", Count: 2}}}
		seeded, err := ts.SeedTaxonomy(ctx, tx)
		must(t, err)
		if !seeded {
			t.Fatal("first seed should seed")
		}
		first, err := ts.GetTaxonomy(ctx, "tx")
		must(t, err)
		changed := tx
		changed.Mode = domain.ModeDynamic
		seeded, err = ts.SeedTaxonomy(ctx, changed)
		must(t, err)
		if seeded {
			t.Error("second seed must not overwrite")
		}
		again, _ := ts.GetTaxonomy(ctx, "tx")
		if !reflect.DeepEqual(again, first) {
			t.Error("seed overwrote")
		}
		cur := first.Version
		put, err := ts.PutTaxonomy(ctx, changed, &cur)
		must(t, err)
		if put.Version != cur+1 {
			t.Errorf("version = %d, want %d", put.Version, cur+1)
		}
		if _, err := ts.PutTaxonomy(ctx, changed, &cur); !errors.Is(err, domain.ErrConflict) {
			t.Errorf("stale put = %v, want ErrConflict", err)
		}
		next, err := ts.PutTaxonomy(ctx, changed, nil)
		must(t, err)
		if next.Version != cur+2 {
			t.Errorf("unconditional version = %d", next.Version)
		}
		got, _ := ts.GetTaxonomy(ctx, "tx")
		if !reflect.DeepEqual(got, next) {
			t.Errorf("stored != returned:\n%+v\n%+v", got, next)
		}
		list, err := ts.ListTaxonomies(ctx)
		must(t, err)
		if len(list) != 1 {
			t.Errorf("list = %d", len(list))
		}
	})

	t.Run("RecordProposal", func(t *testing.T) {
		ts := newStore(t).Taxonomies()
		if err := ts.RecordProposal(ctx, "tx", domain.CollectionProposal{Name: "New"}); !errors.Is(err, domain.ErrNotFound) {
			t.Errorf("unknown taxonomy = %v, want ErrNotFound", err)
		}
		_, err := ts.SeedTaxonomy(ctx, domain.Taxonomy{ID: "tx", Status: domain.TaxonomyActive, Proposals: []domain.CollectionProposal{{Name: "Old", Count: 2}}})
		must(t, err)
		before, _ := ts.GetTaxonomy(ctx, "tx")
		must(t, ts.RecordProposal(ctx, "tx", domain.CollectionProposal{Name: "New", Description: "d", Count: 1}))
		must(t, ts.RecordProposal(ctx, "tx", domain.CollectionProposal{Name: "new", Count: 1}))
		must(t, ts.RecordProposal(ctx, "tx", domain.CollectionProposal{Name: "Old"}))
		got, err := ts.GetTaxonomy(ctx, "tx")
		must(t, err)
		if got.Version != before.Version {
			t.Errorf("version moved from %d to %d", before.Version, got.Version)
		}
		want := []domain.CollectionProposal{{Name: "Old", Count: 3}, {Name: "New", Description: "d", Count: 2}}
		if !reflect.DeepEqual(got.Proposals, want) {
			t.Errorf("proposals = %+v, want %+v", got.Proposals, want)
		}
		put, err := ts.PutTaxonomy(ctx, got, nil)
		must(t, err)
		if put.Version != before.Version+1 {
			t.Errorf("version after put = %d", put.Version)
		}
		again, _ := ts.GetTaxonomy(ctx, "tx")
		if !reflect.DeepEqual(again.Proposals, want) {
			t.Errorf("proposals after put = %+v", again.Proposals)
		}
	})

	t.Run("NotFoundErrors", func(t *testing.T) {
		s := newStore(t)
		checks := map[string]error{}
		_, checks["GetJob"] = s.Jobs().GetJob(ctx, "x")
		checks["UpdateJob"] = s.Jobs().UpdateJob(ctx, job("x", t0))
		_, checks["GetSnapshot"] = s.Snapshots().GetSnapshot(ctx, "x")
		checks["AddSnapshotItems"] = s.Snapshots().AddSnapshotItems(ctx, "x", nil)
		checks["AddCreatedCollection"] = s.Snapshots().AddCreatedCollection(ctx, "x", bid(1))
		_, checks["ListSnapshotItems"] = s.Snapshots().ListSnapshotItems(ctx, "x", "", 10)
		_, checks["GetTaxonomy"] = s.Taxonomies().GetTaxonomy(ctx, "x")
		for name, err := range checks {
			if !errors.Is(err, domain.ErrNotFound) {
				t.Errorf("%s = %v, want ErrNotFound", name, err)
			}
		}
	})
}
