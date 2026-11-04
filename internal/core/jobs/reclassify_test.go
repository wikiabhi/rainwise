package jobs

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	cfake "github.com/wikiabhi/rainwise/internal/adapters/classifier/fake"
	pfake "github.com/wikiabhi/rainwise/internal/adapters/provider/fake"
	"github.com/wikiabhi/rainwise/internal/domain"
)

func (e *jobEnv) reclassify(scope domain.Scope, mod func(*domain.Job)) *domain.Job {
	e.t.Helper()
	job := e.newJob(domain.JobReclassify, func(j *domain.Job) {
		j.Scope = scope
		if mod != nil {
			mod(j)
		}
	})
	e.mustRun(job)
	return job
}

func TestReclassifyScope(t *testing.T) {
	// b1 and b2 in Beta, b3 elsewhere; b2 and b3 tagged.
	setup := func(t *testing.T) *jobEnv {
		e := newJobEnv(t)
		e.addBookmark(1, colBeta)
		e.addBookmark(2, colBeta, "keep")
		e.addBookmark(3, colReview, "Keep")
		return e
	}
	moved := func(e *jobEnv) []int {
		var out []int
		for n := 1; n <= 3; n++ {
			if e.collectionOf(bmID(n)) == colAlpha {
				out = append(out, n)
			}
		}
		return out
	}
	cases := map[string]struct {
		scope domain.Scope
		want  []int
	}{
		"collection by name":      {domain.Scope{Collections: []string{"beta"}}, []int{1, 2}},
		"collection by key":       {domain.Scope{Collections: []string{"b"}}, []int{1, 2}},
		"collection by id":        {domain.Scope{Collections: []string{colBeta.String()}}, []int{1, 2}},
		"collection by local id":  {domain.Scope{Collections: []string{colBeta.Local}}, []int{1, 2}},
		"tag, ignoring case":      {domain.Scope{Tags: []string{"keep"}}, []int{2, 3}},
		"id":                      {domain.Scope{IDs: []string{bmID(3).String()}}, []int{3}},
		"local id":                {domain.Scope{IDs: []string{bmID(3).Local}}, []int{3}},
		"kinds narrow each other": {domain.Scope{Collections: []string{"Beta"}, Tags: []string{"keep"}}, []int{2}},
		"one kind widens":         {domain.Scope{IDs: []string{bmID(1).String(), bmID(3).String()}}, []int{1, 3}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			e := setup(t)
			job := e.reclassify(c.scope, nil)
			if got := moved(e); !slices.Equal(got, c.want) {
				t.Errorf("moved = %v, want %v", got, c.want)
			}
			if job.Progress.Processed != len(c.want) || job.Progress.Total != len(c.want) {
				t.Errorf("progress = %+v", job.Progress)
			}
			// However a collection was named, the snapshot gets the IDs it resolved to.
			wantScope := c.scope
			if len(wantScope.Collections) > 0 {
				wantScope.Collections = []string{colBeta.String()}
			}
			if len(e.snaps.calls) != 1 || !reflect.DeepEqual(e.snaps.calls[0], wantScope) || e.snaps.jobs[0] != job.ID {
				t.Errorf("snapshot calls = %+v", e.snaps.calls)
			}
			if job.SnapshotID == "" {
				t.Error("no snapshot id")
			}
			if _, ok := e.setting(firstBackfillKey); ok {
				t.Error("reclassify set the first-backfill setting")
			}
		})
	}

	t.Run("force bypasses the hash", func(t *testing.T) {
		e := setup(t)
		scope := domain.Scope{IDs: []string{bmID(1).String()}}
		e.reclassify(scope, nil)
		calls := e.cls.Calls()
		e.reclassify(scope, nil)
		if e.cls.Calls() != calls+1 {
			t.Errorf("calls = %d, want %d", e.cls.Calls(), calls+1)
		}
	})

	t.Run("user pins hold unless include_pinned", func(t *testing.T) {
		e := newJobEnv(t)
		pinned := e.userPinned(1, t0)
		scope := domain.Scope{IDs: []string{pinned.String()}}
		e.reclassify(scope, nil)
		if got := e.collectionOf(pinned); got != colBeta {
			t.Errorf("collection = %v", got)
		}
		e.reclassify(scope, func(j *domain.Job) { j.Params.IncludePinned = true })
		if got := e.collectionOf(pinned); got != colAlpha {
			t.Errorf("with include_pinned, collection = %v", got)
		}
	})

	t.Run("baseline pins don't hold", func(t *testing.T) {
		e := newJobEnv(t)
		id := e.addBookmark(1, colBeta)
		at := t0
		if err := e.store.BookmarkStates().PutState(context.Background(), domain.BookmarkState{ID: id, KnownCollection: colBeta, PinSource: domain.PinBaseline, PinnedAt: &at}); err != nil {
			t.Fatal(err)
		}
		e.reclassify(domain.Scope{IDs: []string{id.String()}}, nil)
		if got := e.collectionOf(id); got != colAlpha {
			t.Errorf("collection = %v", got)
		}
	})

	t.Run("no new tags without a plan", func(t *testing.T) {
		e := setup(t)
		e.cls.Script(cfake.Result{Decision: classified("a", 0.9, "keep", "brand-new")})
		e.reclassify(domain.Scope{IDs: []string{bmID(1).String()}}, nil)
		// The existing spelling wins, and "Keep" sorts first of the two in use.
		if got := e.tagsOf(bmID(1)); !slices.Equal(got, []string{"Keep"}) {
			t.Errorf("tags = %v", got)
		}
	})

	t.Run("dry run writes nothing and snapshots nothing", func(t *testing.T) {
		e := setup(t)
		e.reclassify(domain.Scope{IDs: []string{bmID(1).String()}}, dryRun)
		if len(e.provider.Writes()) != 0 || len(e.snaps.calls) != 0 {
			t.Errorf("writes %+v, snapshots %+v", e.provider.Writes(), e.snaps.calls)
		}
	})

	t.Run("empty scope is refused", func(t *testing.T) {
		e := setup(t)
		job := e.newJob(domain.JobReclassify, nil)
		if err := e.runJob(job); !errors.Is(err, domain.ErrValidation) {
			t.Errorf("err = %v", err)
		}
		if len(e.snaps.calls) != 0 {
			t.Error("took a snapshot")
		}
	})

	t.Run("unknown collection is refused before any work", func(t *testing.T) {
		e := setup(t)
		job := e.newJob(domain.JobReclassify, func(j *domain.Job) { j.Scope.Collections = []string{"typo"} })
		if err := e.runJob(job); !errors.Is(err, domain.ErrValidation) {
			t.Errorf("err = %v", err)
		}
		if len(e.snaps.calls) != 0 || e.cls.Calls() != 0 {
			t.Error("worked on a bad scope")
		}
	})

	t.Run("nested collection names don't match", func(t *testing.T) {
		e := setup(t)
		parent := colAlpha
		e.provider.AddCollection(domain.Collection{ID: colOther, Name: "Deep", Parent: &parent})
		job := e.newJob(domain.JobReclassify, func(j *domain.Job) { j.Scope.Collections = []string{"Deep"} })
		if err := e.runJob(job); !errors.Is(err, domain.ErrValidation) {
			t.Errorf("err = %v", err)
		}
	})

	t.Run("from a dry run", func(t *testing.T) {
		e := setup(t)
		scope := domain.Scope{IDs: []string{bmID(1).String()}}
		dry := e.reclassify(scope, dryRun)
		calls := e.cls.Calls()
		e.reclassify(scope, func(j *domain.Job) { j.FromDryRun = dry.ID })
		if e.cls.Calls() != calls {
			t.Errorf("classified again")
		}
		if got := e.collectionOf(bmID(1)); got != colAlpha {
			t.Errorf("collection = %v", got)
		}
	})
}

func TestReclassifyInboxIDScope(t *testing.T) {
	e := newJobEnv(t)
	id := e.addBookmark(1, pfake.Inbox)
	e.reclassify(domain.Scope{Collections: []string{pfake.Inbox.String()}}, nil)
	if got := e.collectionOf(id); got != colAlpha {
		t.Errorf("collection = %v", got)
	}
}

func TestReclassifyTagsOnly(t *testing.T) {
	tagsOnly := func(j *domain.Job) { j.Params.TagsOnly = true }
	setup := func(t *testing.T) *jobEnv {
		e := newJobEnv(t)
		e.addBookmark(1, colBeta)
		e.addBookmark(2, colReview, "postgres", "kafka")
		return e
	}
	scope := domain.Scope{IDs: []string{bmID(1).String()}}

	t.Run("adds tags, never asks for or makes a move", func(t *testing.T) {
		e := setup(t)
		e.cls.Script(cfake.Result{Decision: classified("a", 0.9, "postgres", "kafka")})
		job := e.reclassify(scope, tagsOnly)
		if got := e.collectionOf(bmID(1)); got != colBeta {
			t.Errorf("moved to %v", got)
		}
		if got := e.tagsOf(bmID(1)); !slices.Equal(got, []string{"postgres", "kafka"}) {
			t.Errorf("tags = %v", got)
		}
		if e.cls.Requests()[0].WantCollection {
			t.Error("asked the classifier for a collection")
		}
		if job.SnapshotID == "" {
			t.Error("no snapshot, so it couldn't be undone")
		}
	})

	t.Run("a dry run changes nothing, and its plan applies without a move", func(t *testing.T) {
		e := setup(t)
		e.cls.Script(cfake.Result{Decision: classified("a", 0.9, "postgres", "brand-new", "rejected")})
		dry := e.reclassify(scope, func(j *domain.Job) { tagsOnly(j); j.DryRun = true })
		if got := e.tagsOf(bmID(1)); len(got) != 0 {
			t.Fatalf("dry run wrote tags %v", got)
		}
		calls := e.cls.Calls()
		e.reclassify(scope, func(j *domain.Job) {
			tagsOnly(j)
			j.FromDryRun = dry.ID
			j.Params.ApprovedNewTags = map[string]string{"brand-new": "brand-new"}
		})
		if got := e.cls.Calls(); got != calls {
			t.Errorf("classified %d more times", got-calls)
		}
		if got := e.collectionOf(bmID(1)); got != colBeta {
			t.Errorf("moved to %v", got)
		}
		if got := e.tagsOf(bmID(1)); !slices.Equal(got, []string{"postgres", "brand-new"}) {
			t.Errorf("tags = %v", got)
		}
	})

	t.Run("a tags-only plan can't be applied as a full reclassify", func(t *testing.T) {
		e := setup(t)
		e.cls.Script(cfake.Result{Decision: classified("a", 0.9, "postgres")})
		dry := e.reclassify(scope, func(j *domain.Job) { tagsOnly(j); j.DryRun = true })
		svc := NewService(ServiceDeps{Store: e.store, ProviderNames: []string{"fake"}, Now: e.nowFn})
		_, _, err := svc.Create(context.Background(), CreateRequest{
			Type: domain.JobReclassify, Provider: dry.Provider, Taxonomy: dry.TaxonomyID, Scope: scope, FromDryRun: dry.ID,
		})
		if !errors.Is(err, domain.ErrValidation) {
			t.Errorf("err = %v, want a validation error", err)
		}
	})

	t.Run("low confidence adds nothing and doesn't send it to review", func(t *testing.T) {
		e := setup(t)
		e.cls.Script(cfake.Result{Decision: classified("a", 0.1, "postgres")})
		e.reclassify(scope, tagsOnly)
		if got := e.collectionOf(bmID(1)); got != colBeta {
			t.Errorf("moved to %v", got)
		}
		if got := e.tagsOf(bmID(1)); len(got) != 0 {
			t.Errorf("tags = %v", got)
		}
	})
}
