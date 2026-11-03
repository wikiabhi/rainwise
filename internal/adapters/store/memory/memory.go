// Package memory is an in-process ports.Store for tests and dry runs.
package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

const defaultLimit = 50

type store struct {
	mu sync.Mutex

	jobs      map[string]domain.Job
	jobOrder  []string
	decisions map[string][]domain.DecisionRecord // by job, insertion order

	events    map[string]domain.StoredEvent // by ID
	eventSeq  []string
	dedup     map[string]string // dedup key -> event ID
	claimed   map[string]bool
	states    map[domain.ID]domain.BookmarkState
	settings  map[string]string
	snapshots map[string]domain.Snapshot
	snapItems map[string][]domain.SnapshotItem
	taxos     map[string]domain.Taxonomy
}

// New returns an empty in-memory store.
func New() ports.Store {
	return &store{
		jobs:      map[string]domain.Job{},
		decisions: map[string][]domain.DecisionRecord{},
		events:    map[string]domain.StoredEvent{},
		dedup:     map[string]string{},
		claimed:   map[string]bool{},
		states:    map[domain.ID]domain.BookmarkState{},
		settings:  map[string]string{},
		snapshots: map[string]domain.Snapshot{},
		snapItems: map[string][]domain.SnapshotItem{},
		taxos:     map[string]domain.Taxonomy{},
	}
}

func (s *store) Jobs() ports.JobStore             { return (*jobStore)(s) }
func (s *store) Events() ports.EventStore         { return (*eventStore)(s) }
func (s *store) BookmarkStates() ports.StateStore { return (*stateStore)(s) }
func (s *store) Snapshots() ports.SnapshotStore   { return (*snapStore)(s) }
func (s *store) Taxonomies() ports.TaxonomyStore  { return (*taxoStore)(s) }

// clone deep-copies v so callers never alias stored data.
func clone[T any](v T) T {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	var out T
	if err := json.Unmarshal(b, &out); err != nil {
		panic(err)
	}
	return out
}

func notFound(what, id string) error { return fmt.Errorf("%s %q: %w", what, id, domain.ErrNotFound) }

// paginate returns the page of items starting at the offset cursor.
func paginate[T any](all []T, cursor string, limit int) (domain.Page[T], error) {
	if limit <= 0 {
		limit = defaultLimit
	}
	off := 0
	if cursor != "" {
		n, err := strconv.Atoi(cursor)
		if err != nil || n < 0 {
			return domain.Page[T]{}, fmt.Errorf("bad cursor %q: %w", cursor, domain.ErrValidation)
		}
		off = n
	}
	if off > len(all) {
		off = len(all)
	}
	end := off + limit
	page := domain.Page[T]{}
	if end < len(all) {
		page.NextToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}
	page.Items = clone(all[off:end])
	return page, nil
}

// ---- jobs ----

type jobStore store

func (j *jobStore) CreateJob(_ context.Context, job domain.Job) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, ok := j.jobs[job.ID]; ok {
		return fmt.Errorf("job %q exists: %w", job.ID, domain.ErrConflict)
	}
	if job.IdempotencyKey != "" {
		for _, o := range j.jobs {
			if o.IdempotencyKey == job.IdempotencyKey {
				return fmt.Errorf("idempotency key %q exists: %w", job.IdempotencyKey, domain.ErrConflict)
			}
		}
	}
	j.jobs[job.ID] = clone(job)
	j.jobOrder = append(j.jobOrder, job.ID)
	return nil
}

func (j *jobStore) GetJob(_ context.Context, id string) (domain.Job, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	job, ok := j.jobs[id]
	if !ok {
		return domain.Job{}, notFound("job", id)
	}
	return clone(job), nil
}

func (j *jobStore) FindJobByIdempotencyKey(_ context.Context, key string) (domain.Job, bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if key == "" {
		return domain.Job{}, false, nil
	}
	for _, id := range j.jobOrder {
		if job := j.jobs[id]; job.IdempotencyKey == key {
			return clone(job), true, nil
		}
	}
	return domain.Job{}, false, nil
}

func (j *jobStore) FindActiveJob(_ context.Context, provider string, t domain.JobType) (domain.Job, bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, id := range j.jobOrder {
		job := j.jobs[id]
		if job.Provider == provider && job.Type == t && (job.Status == domain.JobQueued || job.Status == domain.JobRunning) {
			return clone(job), true, nil
		}
	}
	return domain.Job{}, false, nil
}

func (j *jobStore) ListJobs(_ context.Context, cursor string, limit int) (domain.Page[domain.Job], error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	all := make([]domain.Job, 0, len(j.jobOrder))
	for _, id := range j.jobOrder {
		all = append(all, j.jobs[id])
	}
	// Newest first, ties by insertion order reversed.
	sort.SliceStable(all, func(a, b int) bool { return all[a].CreatedAt.After(all[b].CreatedAt) })
	return paginate(all, cursor, limit)
}

func (j *jobStore) UpdateJob(_ context.Context, job domain.Job) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, ok := j.jobs[job.ID]; !ok {
		return notFound("job", job.ID)
	}
	j.jobs[job.ID] = clone(job)
	return nil
}

func (j *jobStore) UpdateJobIf(_ context.Context, job domain.Job, from ...domain.JobStatus) (bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	cur, ok := j.jobs[job.ID]
	if !ok {
		return false, notFound("job", job.ID)
	}
	if !slices.Contains(from, cur.Status) {
		return false, nil
	}
	j.jobs[job.ID] = clone(job)
	return true, nil
}

func (j *jobStore) ClaimNextQueued(_ context.Context, provider string, dryRun bool) (domain.Job, bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	var best *domain.Job
	for _, id := range j.jobOrder {
		job := j.jobs[id]
		if job.Status != domain.JobQueued || job.Provider != provider || job.DryRun != dryRun {
			continue
		}
		if best == nil || job.CreatedAt.Before(best.CreatedAt) {
			c := job
			best = &c
		}
	}
	if best == nil {
		return domain.Job{}, false, nil
	}
	best.Status = domain.JobRunning
	j.jobs[best.ID] = *best
	return clone(*best), true, nil
}

func (j *jobStore) ListRunning(_ context.Context) ([]domain.Job, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	var out []domain.Job
	for _, id := range j.jobOrder {
		if job := j.jobs[id]; job.Status == domain.JobRunning {
			out = append(out, clone(job))
		}
	}
	return out, nil
}

func (j *jobStore) SaveDecision(_ context.Context, d domain.DecisionRecord) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, ok := j.jobs[d.JobID]; !ok {
		return notFound("job", d.JobID)
	}
	list := j.decisions[d.JobID]
	for i := range list {
		if list[i].Bookmark == d.Bookmark {
			list[i] = clone(d)
			return nil
		}
	}
	j.decisions[d.JobID] = append(list, clone(d))
	return nil
}

func (j *jobStore) GetDecision(_ context.Context, jobID string, b domain.ID) (domain.DecisionRecord, bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, d := range j.decisions[jobID] {
		if d.Bookmark == b {
			return clone(d), true, nil
		}
	}
	return domain.DecisionRecord{}, false, nil
}

func (j *jobStore) ListDecisions(_ context.Context, jobID, cursor string, limit int) (domain.Page[domain.DecisionRecord], error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return paginate(j.decisions[jobID], cursor, limit)
}

// ---- events ----

type eventStore store

func (e *eventStore) InsertEvent(_ context.Context, ev domain.StoredEvent) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.dedup[ev.DedupKey]; ok {
		return false, nil
	}
	if _, ok := e.events[ev.ID]; ok {
		return false, fmt.Errorf("event %q exists: %w", ev.ID, domain.ErrConflict)
	}
	e.events[ev.ID] = clone(ev)
	e.eventSeq = append(e.eventSeq, ev.ID)
	e.dedup[ev.DedupKey] = ev.ID
	return true, nil
}

func (e *eventStore) ClaimEvents(_ context.Context, now time.Time, limit int) ([]domain.StoredEvent, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	var due []domain.StoredEvent
	for _, id := range e.eventSeq {
		ev := e.events[id]
		if ev.Status == domain.EventPending && !e.claimed[id] && !ev.NextAttempt.After(now) {
			due = append(due, ev)
		}
	}
	sort.SliceStable(due, func(a, b int) bool {
		if !due[a].NextAttempt.Equal(due[b].NextAttempt) {
			return due[a].NextAttempt.Before(due[b].NextAttempt)
		}
		return due[a].ReceivedAt.Before(due[b].ReceivedAt)
	})
	if limit > 0 && len(due) > limit {
		due = due[:limit]
	}
	for _, ev := range due {
		e.claimed[ev.ID] = true
	}
	return clone(due), nil
}

func (e *eventStore) UpdateEvent(_ context.Context, ev domain.StoredEvent) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	old, ok := e.events[ev.ID]
	if !ok {
		return notFound("event", ev.ID)
	}
	if old.DedupKey != ev.DedupKey {
		delete(e.dedup, old.DedupKey)
		e.dedup[ev.DedupKey] = ev.ID
	}
	e.events[ev.ID] = clone(ev)
	delete(e.claimed, ev.ID)
	return nil
}

func (e *eventStore) DropHeldBefore(_ context.Context, t time.Time) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := 0
	kept := e.eventSeq[:0]
	for _, id := range e.eventSeq {
		ev := e.events[id]
		if ev.Status == domain.EventHeld && ev.ReceivedAt.Before(t) {
			delete(e.events, id)
			delete(e.dedup, ev.DedupKey)
			delete(e.claimed, id)
			n++
			continue
		}
		kept = append(kept, id)
	}
	e.eventSeq = kept
	return n, nil
}

// ---- bookmark state and settings ----

type stateStore store

func (s *stateStore) GetState(_ context.Context, id domain.ID) (domain.BookmarkState, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.states[id]
	return clone(st), ok, nil
}

func (s *stateStore) PutState(_ context.Context, st domain.BookmarkState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[st.ID] = clone(st)
	return nil
}

func (s *stateStore) GetSetting(_ context.Context, key string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.settings[key]
	return v, ok, nil
}

func (s *stateStore) PutSetting(_ context.Context, key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.settings[key] = value
	return nil
}

func (s *stateStore) DeleteSetting(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.settings, key)
	return nil
}

// ---- snapshots ----

type snapStore store

func (s *snapStore) CreateSnapshot(_ context.Context, snap domain.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.snapshots[snap.ID]; ok {
		return fmt.Errorf("snapshot %q exists: %w", snap.ID, domain.ErrConflict)
	}
	s.snapshots[snap.ID] = clone(snap)
	return nil
}

func (s *snapStore) AddSnapshotItems(_ context.Context, id string, items []domain.SnapshotItem) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.snapshots[id]; !ok {
		return notFound("snapshot", id)
	}
	s.snapItems[id] = append(s.snapItems[id], clone(items)...)
	return nil
}

func (s *snapStore) AddCreatedCollection(_ context.Context, id string, c domain.ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap, ok := s.snapshots[id]
	if !ok {
		return notFound("snapshot", id)
	}
	snap.CreatedCollections = append(snap.CreatedCollections, c)
	s.snapshots[id] = snap
	return nil
}

func (s *snapStore) GetSnapshot(_ context.Context, id string) (domain.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap, ok := s.snapshots[id]
	if !ok {
		return domain.Snapshot{}, notFound("snapshot", id)
	}
	return clone(snap), nil
}

func (s *snapStore) sortedLocked() []domain.Snapshot {
	all := make([]domain.Snapshot, 0, len(s.snapshots))
	for _, snap := range s.snapshots {
		all = append(all, snap)
	}
	sort.Slice(all, func(a, b int) bool {
		if !all[a].CreatedAt.Equal(all[b].CreatedAt) {
			return all[a].CreatedAt.After(all[b].CreatedAt)
		}
		return all[a].ID > all[b].ID
	})
	return all
}

func (s *snapStore) ListSnapshots(_ context.Context, cursor string, limit int) (domain.Page[domain.Snapshot], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return paginate(s.sortedLocked(), cursor, limit)
}

func (s *snapStore) ListSnapshotItems(_ context.Context, id, cursor string, limit int) (domain.Page[domain.SnapshotItem], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.snapshots[id]; !ok {
		return domain.Page[domain.SnapshotItem]{}, notFound("snapshot", id)
	}
	return paginate(s.snapItems[id], cursor, limit)
}

func (s *snapStore) PruneSnapshots(_ context.Context, keep int, protect []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	prot := map[string]bool{}
	for _, id := range protect {
		prot[id] = true
	}
	for i, snap := range s.sortedLocked() {
		if i < keep || prot[snap.ID] {
			continue
		}
		delete(s.snapshots, snap.ID)
		delete(s.snapItems, snap.ID)
	}
	return nil
}

// ---- taxonomies ----

type taxoStore store

func (s *taxoStore) GetTaxonomy(_ context.Context, id string) (domain.Taxonomy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.taxos[id]
	if !ok {
		return domain.Taxonomy{}, notFound("taxonomy", id)
	}
	return clone(t), nil
}

func (s *taxoStore) ListTaxonomies(_ context.Context) ([]domain.Taxonomy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.Taxonomy, 0, len(s.taxos))
	for _, t := range s.taxos {
		out = append(out, clone(t))
	}
	sort.Slice(out, func(a, b int) bool { return out[a].ID < out[b].ID })
	return out, nil
}

func (s *taxoStore) PutTaxonomy(_ context.Context, t domain.Taxonomy, ifVersion *int) (domain.Taxonomy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := 0
	if old, ok := s.taxos[t.ID]; ok {
		cur = old.Version
	}
	if ifVersion != nil && *ifVersion != cur {
		return domain.Taxonomy{}, fmt.Errorf("taxonomy %q is at version %d, not %d: %w", t.ID, cur, *ifVersion, domain.ErrConflict)
	}
	t.Version = cur + 1
	s.taxos[t.ID] = clone(t)
	return clone(t), nil
}

func (s *taxoStore) SeedTaxonomy(_ context.Context, t domain.Taxonomy) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.taxos[t.ID]; ok {
		return false, nil
	}
	if t.Version < 1 {
		t.Version = 1
	}
	s.taxos[t.ID] = clone(t)
	return true, nil
}

func (s *taxoStore) RecordProposal(_ context.Context, id string, p domain.CollectionProposal) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.taxos[id]
	if !ok {
		return notFound("taxonomy", id)
	}
	t = clone(t)
	t.Proposals = domain.MergeProposal(t.Proposals, p)
	s.taxos[id] = t
	return nil
}
