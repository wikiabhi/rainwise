package events

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/wikiabhi/rainwise/internal/core/pipeline"
	"github.com/wikiabhi/rainwise/internal/core/taxonomy"
	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

const (
	maxAttempts      = 5
	baseBackoff      = 30 * time.Second
	maxBackoff       = 30 * time.Minute
	heldTTL          = 7 * 24 * time.Hour
	dropHeldEvery    = time.Hour
	claimBatch       = 20
	defaultPollEvery = 2 * time.Second
	defaultVocabSize = 200
)

// errPermanent marks an event no retry can fix.
var errPermanent = errors.New("permanent event failure")

func newID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("random id: %v", err))
	}
	return "evt_" + hex.EncodeToString(b[:])
}

// Ingestor saves normalized events to the inbox (§9.2 step 2).
type Ingestor struct {
	store   ports.EventStore
	enabled bool
	now     func() time.Time
}

func NewIngestor(store ports.EventStore, automationEnabled bool, now func() time.Time) *Ingestor {
	if now == nil {
		now = time.Now
	}
	return &Ingestor{store: store, enabled: automationEnabled, now: now}
}

// Ingest stores each event as pending, or held while automation is off (§9.0).
// A repeated dedup key is ignored.
func (in *Ingestor) Ingest(ctx context.Context, evs []domain.Event) error {
	for _, e := range evs {
		now := in.now()
		status := domain.EventPending
		if !in.enabled {
			status = domain.EventHeld
		}
		_, err := in.store.InsertEvent(ctx, domain.StoredEvent{
			Event: e, ID: newID(), DedupKey: e.DedupKey, Status: status,
			NextAttempt: now, ReceivedAt: now,
		})
		if err != nil {
			return fmt.Errorf("store event: %w", err)
		}
	}
	return nil
}

// WorkerDeps are the worker's collaborators. Taxonomies and TaxonomyID say how
// the pipeline is configured; TagVocabSize defaults to 200.
type WorkerDeps struct {
	Store        ports.Store
	Pipeline     *pipeline.Pipeline
	Provider     ports.Provider
	Taxonomies   *taxonomy.Service
	TaxonomyID   string
	TagVocabSize int
	OnMoveRetag  bool
	// AutomationOff stops events being processed (§9.0), including any left pending
	// from when it was on. They wait for it to come back. Held events still age out.
	AutomationOff bool
	Now           func() time.Time
	Sleep         func(context.Context, time.Duration) error
	PollEvery     time.Duration
	Log           *slog.Logger
}

// Worker drains the event inbox (§9.2 step 3).
type Worker struct {
	d        WorkerDeps
	lastDrop time.Time
	vocabMu  sync.Mutex // serializes persisted-vocabulary updates
}

func NewWorker(d WorkerDeps) *Worker {
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.Sleep == nil {
		d.Sleep = realSleep
	}
	if d.PollEvery <= 0 {
		d.PollEvery = defaultPollEvery
	}
	if d.TagVocabSize <= 0 {
		d.TagVocabSize = defaultVocabSize
	}
	if d.Log == nil {
		d.Log = slog.New(slog.DiscardHandler)
	}
	return &Worker{d: d}
}

func realSleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Run processes events until ctx ends. Events a previous process left claimed
// become claimable again first.
func (w *Worker) Run(ctx context.Context) {
	if err := w.d.Store.Events().ResetClaimed(ctx); err != nil && ctx.Err() == nil {
		w.d.Log.Error("reset claimed events", "err", err)
	}
	for ctx.Err() == nil {
		n, err := w.tick(ctx)
		if err != nil && ctx.Err() == nil {
			w.d.Log.Error("event worker", "err", err)
		}
		if n > 0 && err == nil {
			continue
		}
		if w.d.Sleep(ctx, w.d.PollEvery) != nil {
			return
		}
	}
}

// tick drops stale held events if due, then handles one batch of due events.
func (w *Worker) tick(ctx context.Context) (int, error) {
	es := w.d.Store.Events()
	now := w.d.Now()
	if now.Sub(w.lastDrop) >= dropHeldEvery {
		n, err := es.DropHeldBefore(ctx, now.Add(-heldTTL))
		if err != nil {
			return 0, fmt.Errorf("drop held events: %w", err)
		}
		w.lastDrop = now
		if n > 0 {
			w.d.Log.Info("dropped stale held events", "count", n)
		}
	}
	if w.d.AutomationOff {
		return 0, nil
	}
	evs, err := es.ClaimEvents(ctx, now, claimBatch)
	if err != nil {
		return 0, fmt.Errorf("claim events: %w", err)
	}
	for _, ev := range evs {
		w.finish(ctx, ev, w.handle(ctx, ev))
	}
	return len(evs), nil
}

func (w *Worker) finish(ctx context.Context, ev domain.StoredEvent, err error) {
	// Saving must outlive a cancelled run, or the event stays claimed.
	sctx := context.WithoutCancel(ctx)
	log := w.d.Log.With("event", ev.ID, "type", string(ev.Type))
	switch {
	case err == nil:
		ev.Status = domain.EventDone
	case ctx.Err() != nil:
		ev.Status = domain.EventPending // interrupted, not failed
	case errors.Is(err, errPermanent):
		ev.Attempts++
		ev.Status = domain.EventFailed
		log.Warn("event failed", "err", err)
	default:
		ev.Attempts++
		if ev.Attempts >= maxAttempts {
			ev.Status = domain.EventFailed
			log.Warn("event failed after retries", "attempts", ev.Attempts, "err", err)
		} else {
			ev.NextAttempt = w.d.Now().Add(backoff(ev.Attempts))
			log.Warn("event will retry", "attempts", ev.Attempts, "next", ev.NextAttempt, "err", err)
		}
	}
	if uerr := w.d.Store.Events().UpdateEvent(sctx, ev); uerr != nil {
		log.Error("save event", "err", uerr)
	}
}

func backoff(attempts int) time.Duration {
	d := baseBackoff << (attempts - 1)
	if d > maxBackoff || d <= 0 {
		return maxBackoff
	}
	return d
}

func permanent(format string, a ...any) error {
	return fmt.Errorf("%s: %w", fmt.Sprintf(format, a...), errPermanent)
}

// resolve turns a reference into a bookmark ID, by URL if it has no ID. Only the
// reference is used, never anything else the payload carried.
func (w *Worker) resolve(ctx context.Context, ref domain.ResourceRef) (domain.ID, error) {
	if ref.ID != "" {
		return domain.ID{Provider: w.d.Provider.Name(), Local: ref.ID}, nil
	}
	r, ok := w.d.Provider.(ports.URLResolver)
	if !ok {
		return domain.ID{}, permanent("provider %s can't resolve URLs", w.d.Provider.Name())
	}
	id, err := r.ResolveURL(ctx, ref.URL)
	if errors.Is(err, domain.ErrNotFound) {
		return domain.ID{}, permanent("no bookmark for %s", ref.URL)
	}
	if err != nil {
		return domain.ID{}, fmt.Errorf("resolve url: %w", err)
	}
	return id, nil
}

func (w *Worker) handle(ctx context.Context, ev domain.StoredEvent) error {
	if ev.Provider != w.d.Provider.Name() {
		return permanent("event is for provider %q", ev.Provider)
	}
	switch ev.Type {
	case domain.EventBookmarkCreated:
		id, err := w.resolve(ctx, ev.Resource)
		if err != nil {
			return err
		}
		return w.process(ctx, pipeline.WorkItem{Bookmark: id}, false)
	case domain.EventHighlightCreated:
		if ev.Bookmark == nil || ev.Resource.ID == "" {
			return permanent("highlight event needs a highlight id and a bookmark")
		}
		id, err := w.resolve(ctx, *ev.Bookmark)
		if err != nil {
			return err
		}
		focus := domain.ID{Provider: id.Provider, Local: ev.Resource.ID}
		return w.process(ctx, pipeline.WorkItem{Bookmark: id, FocusHighlight: &focus}, true)
	case domain.EventBookmarkMoved:
		id, err := w.resolve(ctx, ev.Resource)
		if err != nil {
			return err
		}
		return w.moved(ctx, id)
	}
	return permanent("unknown event type %q", ev.Type)
}

// options builds what the pipeline needs for a webhook item (§9.2). The
// taxonomy is looked up and collections resolved without provisioning.
func (w *Worker) options(ctx context.Context, tagsOnly bool) (pipeline.Options, error) {
	t, err := w.d.Taxonomies.Get(ctx, w.d.TaxonomyID)
	if err != nil {
		return pipeline.Options{}, fmt.Errorf("load taxonomy %q: %w", w.d.TaxonomyID, err)
	}
	if t.Status == domain.TaxonomyDraft {
		return pipeline.Options{}, fmt.Errorf("taxonomy %q is a draft: %w", t.ID, domain.ErrConflict)
	}
	m, _ := w.d.Provider.(ports.CollectionManager)
	resolved, _, err := taxonomy.Resolve(ctx, m, t, false, nil)
	if errors.Is(err, domain.ErrCapabilityUnsupported) {
		resolved, _, err = taxonomy.Resolve(ctx, nil, t, false, nil)
	}
	if err != nil {
		return pipeline.Options{}, err
	}
	terms, err := w.vocabulary(ctx)
	if err != nil {
		return pipeline.Options{}, err
	}
	return pipeline.Options{
		Trigger:  pipeline.TriggerWebhook,
		TagsOnly: tagsOnly,
		// A highlight changes the input hash the bookmark pass may have recorded,
		// and that mustn't swallow its tags (as in sync).
		Force:    tagsOnly,
		Taxonomy: t,
		Resolved: resolved,
		Vocab:    taxonomy.NewRunVocab(terms, t.Rules.TagAliases, t.Rules.AllowNewTags),
	}, nil
}

// vocabulary is the provider's tag listing, else the persisted vocabulary (§8.4),
// which one library scan makes if there is none yet.
func (w *Worker) vocabulary(ctx context.Context) ([]string, error) {
	counts, ok, err := taxonomy.ListedTags(ctx, w.d.Provider)
	if err != nil {
		return nil, err
	}
	if ok {
		return taxonomy.TopTerms(counts, w.d.TagVocabSize), nil
	}
	w.vocabMu.Lock()
	defer w.vocabMu.Unlock()
	states := w.d.Store.BookmarkStates()
	name := w.d.Provider.Name()
	terms, ok, err := taxonomy.LoadVocab(ctx, states, name)
	if err != nil || ok {
		return terms, err
	}
	scanned := map[string]int{}
	err = taxonomy.ScanLibrary(ctx, w.d.Provider, 0, nil, func(b domain.Bookmark) {
		for _, t := range b.Tags {
			scanned[t]++
		}
	})
	if err != nil {
		return nil, err
	}
	terms = taxonomy.TopTerms(scanned, w.d.TagVocabSize)
	if err := taxonomy.SaveVocab(ctx, states, name, terms); err != nil {
		return nil, fmt.Errorf("save tag vocabulary: %w", err)
	}
	return terms, nil
}

// rememberTags adds the new tags a real run wrote to the persisted vocabulary, so
// later items reuse them. A provider that lists its own tags needs no help.
func (w *Worker) rememberTags(ctx context.Context, out pipeline.Outcome, vocab *taxonomy.RunVocab) error {
	if out.Status != pipeline.Changed {
		return nil
	}
	var added []string
	for _, t := range out.Record.AddTags {
		if _, known := vocab.Existing(t); !known {
			added = append(added, t)
		}
	}
	if len(added) == 0 {
		return nil
	}
	if _, listed, err := taxonomy.ListedTags(ctx, w.d.Provider); err != nil || listed {
		return err
	}
	w.vocabMu.Lock()
	defer w.vocabMu.Unlock()
	states := w.d.Store.BookmarkStates()
	name := w.d.Provider.Name()
	terms, _, err := taxonomy.LoadVocab(ctx, states, name)
	if err != nil {
		return err
	}
	if err := taxonomy.SaveVocab(ctx, states, name, taxonomy.AddVocab(terms, added, w.d.TagVocabSize)); err != nil {
		return fmt.Errorf("save tag vocabulary: %w", err)
	}
	return nil
}

func (w *Worker) process(ctx context.Context, item pipeline.WorkItem, tagsOnly bool) error {
	opts, err := w.options(ctx, tagsOnly)
	if err != nil {
		return err
	}
	out, err := w.d.Pipeline.Process(ctx, item, opts)
	if err != nil {
		return err
	}
	if err := w.rememberTags(ctx, out, opts.Vocab); err != nil {
		return err
	}
	// Webhook items have no job, so no decision is saved; the log is the record.
	w.d.Log.Info("webhook item processed",
		"bookmark", item.Bookmark.String(),
		"outcome", string(out.Status),
		"previous_collection", out.Record.ObservedCollection.String(),
		"previous_tags", out.Record.ObservedTags,
	)
	return nil
}

// moved handles bookmark.moved (§9.2, §10.3, §10.4): a move to the collection we
// last recorded is our own and is ignored; any other pins the bookmark.
func (w *Worker) moved(ctx context.Context, id domain.ID) error {
	unlock, err := w.d.Pipeline.LockBookmark(ctx, id)
	if err != nil {
		return err
	}
	pinned, err := w.pinIfMoved(ctx, id)
	unlock() // Process takes the same lock.
	if err != nil || !pinned || !w.d.OnMoveRetag {
		return err
	}
	return w.process(ctx, pipeline.WorkItem{Bookmark: id}, true)
}

// awaitsFiling reports a bookmark in the inbox, or with no collection on a provider
// that has no inbox.
func (w *Worker) awaitsFiling(b domain.Bookmark) bool {
	inbox := w.d.Provider.Capabilities().InboxCollection
	if inbox == nil {
		return b.Collection.IsZero()
	}
	return b.Collection == *inbox
}

func (w *Worker) pinIfMoved(ctx context.Context, id domain.ID) (bool, error) {
	b, err := w.d.Provider.GetBookmark(ctx, id)
	if errors.Is(err, domain.ErrNotFound) {
		w.d.Log.Info("moved bookmark is gone", "bookmark", id.String())
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read bookmark %s: %w", id, err)
	}
	states := w.d.Store.BookmarkStates()
	st, known, err := states.GetState(ctx, id)
	if err != nil {
		return false, fmt.Errorf("load state of %s: %w", id, err)
	}
	if !known {
		st = domain.BookmarkState{ID: id}
		// There's no earlier location to have moved from, and an unsorted bookmark
		// isn't a choice the user made. A pin would stop sync from ever filing it.
		if w.awaitsFiling(b) {
			st.KnownCollection, st.KnownTags = b.Collection, b.Tags
			if err := states.PutState(ctx, st); err != nil {
				return false, fmt.Errorf("save state of %s: %w", id, err)
			}
			return false, nil
		}
	}
	if !pipeline.DetectManualMove(&st, b, w.d.Now()) {
		return false, nil
	}
	if err := states.PutState(ctx, st); err != nil {
		return false, fmt.Errorf("save state of %s: %w", id, err)
	}
	w.d.Log.Info("manual move pinned", "bookmark", id.String(), "collection", b.Collection.String())
	return true, nil
}
