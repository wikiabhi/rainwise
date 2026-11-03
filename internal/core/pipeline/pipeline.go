package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/wikiabhi/rainwise/internal/core/taxonomy"
	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

const (
	defaultMinChars  = 80
	maxFetchAttempts = 3

	fetchOff    = "off"
	fetchAlways = "always"

	resultProposed = "proposed"
	resultPending  = "pending"
	resultApplied  = "applied"
	resultNoChange = "no_change"
	resultAborted  = "aborted"
	resultFailed   = "failed"
	resultDeleted  = "deleted"

	reasonBaseline = "baseline"

	createdSettingPrefix = "auto_created_collections:"
)

// Deps are the pipeline's collaborators. Fallback and Fetcher are optional.
type Deps struct {
	Provider             ports.Provider
	Classifier, Fallback ports.Classifier
	Fetcher              ports.ContentFetcher
	Store                ports.Store
	Now                  func() time.Time
	Log                  *slog.Logger
	ContentFetch         string
	MinChars             int
}

// WorkItem is one bookmark to process, or one of its highlights. Listed, if set,
// is the bookmark as a listing just reported it, which spares the read in step 1.
// It may be out of date by then, so Process reads afresh when it disagrees with
// the state, and step 8 always reads before a write.
type WorkItem struct {
	Bookmark       domain.ID
	FocusHighlight *domain.ID
	Listed         *domain.Bookmark
}

type Trigger string

const (
	TriggerBackfill   Trigger = "backfill"
	TriggerSync       Trigger = "sync"
	TriggerWebhook    Trigger = "webhook"
	TriggerReclassify Trigger = "reclassify"
)

// Options configure one Process call. Vocab is built by the caller, and decides
// whether new tags are allowed. With Plan set, only existing tags and those in
// ApprovedNewTags are written.
type Options struct {
	Trigger                                          Trigger
	JobID                                            string
	SnapshotID                                       string
	DryRun, Force, IncludePinned, Baseline, TagsOnly bool
	IsFirstBackfill                                  bool
	PinCutoff                                        *time.Time
	Taxonomy                                         domain.Taxonomy
	Resolved                                         map[string]domain.ID
	Vocab                                            *taxonomy.RunVocab
	Plan                                             *domain.DecisionRecord
	ApprovedNewTags                                  map[string]string
}

type OutcomeStatus string

const (
	Changed     OutcomeStatus = "changed"
	Unchanged   OutcomeStatus = "unchanged"
	Skipped     OutcomeStatus = "skipped"
	NeedsReview OutcomeStatus = "needs_review"
	Deleted     OutcomeStatus = "deleted"
)

// Outcome is what happened to an item. In a dry run, Status is what would have.
type Outcome struct {
	Status OutcomeStatus
	Record domain.DecisionRecord
}

// Pipeline is the one code path for every trigger (§9.1).
type Pipeline struct {
	d Deps

	locksMu sync.Mutex
	locks   map[domain.ID]*bookmarkLock

	createMu sync.Mutex
}

type bookmarkLock struct {
	held chan struct{}
	refs int
}

func New(d Deps) *Pipeline {
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.Log == nil {
		d.Log = slog.New(slog.DiscardHandler)
	}
	if d.MinChars <= 0 {
		d.MinChars = defaultMinChars
	}
	return &Pipeline{d: d, locks: map[domain.ID]*bookmarkLock{}}
}

// LockBookmark holds the per-bookmark lock for work done outside Process, until
// the returned function is called. It gives up when ctx ends.
func (p *Pipeline) LockBookmark(ctx context.Context, id domain.ID) (func(), error) {
	return p.lock(ctx, id)
}

// lock serializes work on one bookmark, and gives up when ctx ends.
func (p *Pipeline) lock(ctx context.Context, id domain.ID) (func(), error) {
	p.locksMu.Lock()
	l, ok := p.locks[id]
	if !ok {
		l = &bookmarkLock{held: make(chan struct{}, 1)}
		p.locks[id] = l
	}
	l.refs++
	p.locksMu.Unlock()

	drop := func() {
		p.locksMu.Lock()
		defer p.locksMu.Unlock()
		l.refs--
		if l.refs == 0 {
			delete(p.locks, id)
		}
	}
	select {
	case l.held <- struct{}{}:
		return func() { <-l.held; drop() }, nil
	case <-ctx.Done():
		drop()
		return nil, ctx.Err()
	}
}

// run is the working state of one Process call.
type run struct {
	opts       Options
	now        time.Time
	b          domain.Bookmark
	highlights []domain.Highlight
	focus      *domain.Highlight
	state      domain.BookmarkState
	known      bool
	isNew      bool
	hash       string
	tax        domain.Taxonomy // Options.Taxonomy plus auto-created collections
	resolved   map[string]domain.ID
	rec        domain.DecisionRecord
}

// Process runs one item through the pipeline. A returned error is an item
// failure: nothing was recorded as done, and the caller retries later.
func (p *Pipeline) Process(ctx context.Context, item WorkItem, opts Options) (Outcome, error) {
	unlock, err := p.lock(ctx, item.Bookmark)
	if err != nil {
		return Outcome{}, err
	}
	defer unlock()

	if item.FocusHighlight != nil {
		opts.TagsOnly = true
	}
	r := &run{opts: opts, now: p.d.Now()}

	// Step 1: read the bookmark and its highlights. The state comes first, since
	// it says whether a listed copy can stand in for the read.
	r.state, r.known, err = p.d.Store.BookmarkStates().GetState(ctx, item.Bookmark)
	if err != nil {
		return Outcome{}, fmt.Errorf("load state of %s: %w", item.Bookmark, err)
	}
	r.b, r.highlights, err = p.read(ctx, item, r.state, r.known)
	if errors.Is(err, domain.ErrNotFound) {
		return p.markDeleted(ctx, item.Bookmark, r.now)
	}
	if err != nil {
		return Outcome{}, err
	}
	r.hash = InputHash(r.b, r.highlights)
	r.rec = domain.DecisionRecord{
		JobID:              opts.JobID,
		Bookmark:           item.Bookmark,
		InputHash:          r.hash,
		ObservedCollection: r.b.Collection,
		ObservedTags:       r.b.Tags,
	}
	r.tax, r.resolved, err = p.effectiveTaxonomy(ctx, opts)
	if err != nil {
		return Outcome{}, err
	}

	// Step 2: detect manual moves.
	r.isNew = isNew(r.state, r.known)
	dirty := !r.known
	if !r.known {
		r.state = domain.BookmarkState{ID: item.Bookmark, KnownCollection: r.b.Collection, KnownTags: r.b.Tags}
	} else {
		landed, changed, ourMove := reconcile(r)
		dirty = changed
		if landed {
			return p.finishRecovered(ctx, r, ourMove)
		}
	}

	// Step 3: skip what's already handled.
	if reason := p.skipReason(r); reason != "" {
		r.rec.Reason = reason
		if reason == reasonBaseline && !opts.DryRun {
			recordBaseline(r)
			dirty = true
		}
		return p.leave(ctx, r, dirty)
	}
	if item.FocusHighlight != nil {
		for i := range r.highlights {
			if r.highlights[i].ID == *item.FocusHighlight {
				r.focus = &r.highlights[i]
			}
		}
		if r.focus == nil {
			r.rec.Reason = "highlight not found"
			return p.leave(ctx, r, dirty)
		}
	}

	// Step 4: decide whether a collection decision is wanted.
	want := EvaluateMove(r.state, r.known, r.b, p.d.Provider.Capabilities().InboxCollection, opts)
	automatic := opts.Trigger == TriggerSync || opts.Trigger == TriggerWebhook
	if automatic && !opts.TagsOnly && !want {
		if !r.isNew && !opts.Force {
			if !opts.DryRun {
				r.state.InputHash = r.hash
				r.state.KnownTags = r.b.Tags
				dirty = true
			}
			r.rec.Reason = "input changed; not re-sorted"
			return p.leave(ctx, r, dirty)
		}
		if r.isNew && r.state.PinSource == domain.PinNone && !opts.DryRun {
			pin(&r.state, domain.PinUser, r.now)
			dirty = true
		}
	}
	if opts.Plan != nil && want && r.b.Collection != opts.Plan.ObservedCollection {
		want = false // it moved since the plan was made, whoever moved it
	}
	if dirty {
		if err := p.putState(ctx, r.state); err != nil {
			return Outcome{}, err
		}
	}

	// Steps 5 and 6: classify and validate, or take both from the plan.
	v, d, err := p.decide(ctx, r, want)
	if err != nil {
		return Outcome{}, err
	}
	r.rec.Decision = d
	return p.apply(ctx, r, v)
}

func pin(s *domain.BookmarkState, source domain.PinSource, now time.Time) {
	s.PinSource = source
	s.PinnedAt = &now
}

// read is step 1. A listed copy is used only if it agrees with what's recorded. A
// listing can predate a write made since, by a webhook item or a restore, and read
// as a manual move or as a write that never landed. So a disagreement, or a write
// left pending, gets a fresh read to settle it.
func (p *Pipeline) read(ctx context.Context, item WorkItem, st domain.BookmarkState, known bool) (domain.Bookmark, []domain.Highlight, error) {
	var b domain.Bookmark
	if l := item.Listed; l != nil && (!known || (l.Collection == st.KnownCollection && st.PendingCollection == nil && len(st.PendingTags) == 0)) {
		b = *l
	} else {
		var err error
		if b, err = p.d.Provider.GetBookmark(ctx, item.Bookmark); err != nil {
			return domain.Bookmark{}, nil, fmt.Errorf("read bookmark %s: %w", item.Bookmark, err)
		}
	}
	if b.Highlights != nil {
		return b, b.Highlights, nil
	}
	highlights, err := p.listHighlights(ctx, item.Bookmark)
	if err != nil {
		return domain.Bookmark{}, nil, fmt.Errorf("read highlights of %s: %w", item.Bookmark, err)
	}
	return b, highlights, nil
}

func (p *Pipeline) listHighlights(ctx context.Context, id domain.ID) ([]domain.Highlight, error) {
	hr, ok := p.d.Provider.(ports.HighlightReader)
	if !ok || !p.d.Provider.Capabilities().Highlights {
		return nil, nil
	}
	return hr.ListHighlights(ctx, id)
}

// reconcile applies manual move detection and resolves any write left pending
// by a crash or failed call. landed means that write reached the provider.
func reconcile(r *run) (landed, changed, ourMove bool) {
	st := &r.state
	pendingCol, pendingTags := st.PendingCollection, st.PendingTags
	hadPending := pendingCol != nil || len(pendingTags) > 0
	if DetectManualMove(st, r.b, r.now) {
		return false, true, false
	}
	if !hadPending {
		return false, false, false
	}
	if pendingCol != nil {
		landed = *pendingCol == r.b.Collection
	} else {
		landed = hasAllTags(r.b.Tags, pendingTags)
	}
	st.PendingCollection, st.PendingTags = nil, nil
	return landed, true, pendingCol != nil
}

func hasAllTags(have, want []string) bool {
	keys := map[string]bool{}
	for _, t := range have {
		keys[taxonomy.ComparisonKey(t)] = true
	}
	for _, t := range want {
		if !keys[taxonomy.ComparisonKey(t)] {
			return false
		}
	}
	return true
}

// finishRecovered records the result of a write that landed before a crash. A
// dry run finalizes state too, since the write is a fact, but it applied nothing
// itself and reports nothing to do.
func (p *Pipeline) finishRecovered(ctx context.Context, r *run, ourMove bool) (Outcome, error) {
	const note = "write recovered after an interruption"
	if r.opts.DryRun {
		r.rec.Result = resultNoChange
		r.rec.Reason = note
		return p.complete(ctx, r, r.b, Unchanged, r.state.Model, ourMove, false)
	}
	if r.opts.JobID != "" {
		saved, ok, err := p.d.Store.Jobs().GetDecision(ctx, r.opts.JobID, r.rec.Bookmark)
		if err != nil {
			return Outcome{}, fmt.Errorf("load decision: %w", err)
		}
		if ok {
			r.rec = saved
		}
	}
	r.rec.Reason = joinReasons(r.rec.Reason, note)
	return p.complete(ctx, r, r.b, Changed, r.state.Model, ourMove, true)
}

// skipReason says why an item ends before any classification: it was restored,
// the baseline rule covers it, or its input hash matches. "" means it goes on.
func (p *Pipeline) skipReason(r *run) string {
	o := r.opts
	automatic := o.Trigger == TriggerSync || o.Trigger == TriggerWebhook
	restored := r.state.RestoredAt != nil
	switch {
	case restored && automatic:
		return "restored; left alone until a backfill or reclassify"
	case o.Baseline && !o.TagsOnly && r.isNew && !r.b.Collection.IsZero() && !p.inInbox(r.b):
		return reasonBaseline
	case !restored && !o.Force && !ignoresUserPin(r.state, o) && r.state.InputHash == r.hash &&
		(r.state.TaxonomyVersion == o.Taxonomy.Version || automatic):
		return "input unchanged"
	}
	return ""
}

// recordBaseline marks a bookmark known without classifying it (§9.3). A user
// pin already on it stays. The taxonomy version stays unset because no decision
// was made: by §10.2 sync then skips it, and a backfill still acts on it.
func recordBaseline(r *run) {
	st := &r.state
	if st.PinSource == domain.PinNone {
		pin(st, domain.PinBaseline, r.now)
	}
	st.InputHash = r.hash
	st.KnownTags = r.b.Tags
	st.Status = string(Skipped)
	st.ProcessedAt = &r.now
}

// ignoresUserPin reports a user pin this backfill must override (§9.1a), which
// takes precedence over the hash skip in §10.2.
func ignoresUserPin(st domain.BookmarkState, o Options) bool {
	return o.Trigger == TriggerBackfill && !o.TagsOnly && st.PinSource == domain.PinUser && !pinBinds(st, o)
}

func (p *Pipeline) inInbox(b domain.Bookmark) bool {
	in := p.d.Provider.Capabilities().InboxCollection
	return in != nil && b.Collection == *in
}

// leave ends an item as Skipped, saving state only if it changed.
func (p *Pipeline) leave(ctx context.Context, r *run, dirty bool) (Outcome, error) {
	if dirty {
		if err := p.putState(ctx, r.state); err != nil {
			return Outcome{}, err
		}
	}
	return Outcome{Status: Skipped, Record: r.rec}, nil
}

func tagRoom(existing []string, rules domain.TaxonomyRules) int {
	return max(0, rules.MaxTags-len(existing))
}

// capTags removes duplicates of existing tags and of each other, then trims to
// the room the bookmark has.
func capTags(existing, add []string, rules domain.TaxonomyRules) []string {
	room := tagRoom(existing, rules)
	seen := map[string]bool{}
	for _, e := range existing {
		seen[taxonomy.ComparisonKey(e)] = true
	}
	var out []string
	for _, t := range add {
		if len(out) >= room {
			break
		}
		k := taxonomy.ComparisonKey(t)
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, t)
	}
	return out
}

// planTags filters tags on the way to a real write under a plan (§8.4): an
// approved tag becomes its mapped value, an existing one stays, the rest go.
func planTags(add []string, o Options) []string {
	var out []string
	for _, t := range add {
		if mapped, ok := o.ApprovedNewTags[t]; ok {
			if strings.TrimSpace(mapped) != "" {
				out = append(out, mapped)
			}
			continue
		}
		if o.Vocab == nil {
			continue
		}
		if f, ok := o.Vocab.Existing(t); ok {
			out = append(out, f)
		}
	}
	return out
}

// decide produces the validated decision, by classifying or from the plan.
func (p *Pipeline) decide(ctx context.Context, r *run, want bool) (Validated, domain.Decision, error) {
	o := r.opts
	var (
		v   Validated
		d   domain.Decision
		err error
	)
	if o.Plan != nil && o.Plan.InputHash == r.hash {
		v, d = p.fromPlan(r, want)
	} else {
		v, d, err = p.classifyAndValidate(ctx, r, want)
		if err != nil {
			return Validated{}, domain.Decision{}, err
		}
	}
	if o.Plan != nil {
		v.AddTags = capTags(r.b.Tags, planTags(v.AddTags, o), r.tax.Rules)
	}
	return v, d, nil
}

func (p *Pipeline) fromPlan(r *run, want bool) (Validated, domain.Decision) {
	plan := r.opts.Plan
	v := Validated{AddTags: plan.AddTags}
	if plan.Reason != "" {
		v.Reasons = append(v.Reasons, plan.Reason)
	}
	if !want {
		return v, plan.Decision
	}
	switch {
	case plan.TargetCollection != nil:
		target := *plan.TargetCollection
		if review, ok := reviewCollection(r.tax, r.resolved); ok && target == review {
			v.ToReview = true
			// The dry run recorded nothing, so a declined proposal is recorded now.
			name := strings.TrimSpace(plan.Decision.Collection.ProposedName)
			if _, known := findCollection(r.tax, name); name != "" && !known {
				v.ProposedCollection = &domain.CollectionProposal{Name: name, Count: 1}
			}
		} else {
			v.Target = &target
		}
	case plan.Decision.Collection.ProposedName != "":
		d := withKnownProposal(plan.Decision, r.tax)
		d.Tags = nil // tags come from the plan; Validate must not re-resolve them
		fresh := Validate(p.validateInput(r, d, want))
		v.Target, v.ToReview, v.ProposedCollection = fresh.Target, fresh.ToReview, fresh.ProposedCollection
		v.Reasons = append(v.Reasons, fresh.Reasons...)
	}
	return v, plan.Decision
}

func (p *Pipeline) validateInput(r *run, d domain.Decision, want bool) ValidateInput {
	_, canCreate := p.d.Provider.(ports.CollectionManager)
	return ValidateInput{
		Decision:             d,
		Bookmark:             r.b,
		Taxonomy:             r.tax,
		Vocab:                r.opts.Vocab,
		WantCollection:       want,
		Resolved:             r.resolved,
		CanCreateCollections: canCreate && p.d.Provider.Capabilities().CreateCollections,
	}
}

// withKnownProposal turns a proposed name that now exists into a normal choice.
func withKnownProposal(d domain.Decision, tax domain.Taxonomy) domain.Decision {
	if d.Collection.ProposedName == "" {
		return d
	}
	if _, found := findCollection(tax, d.Collection.Key); found {
		return d
	}
	if c, found := findCollection(tax, d.Collection.ProposedName); found {
		d.Collection.Key = c.Key
	}
	return d
}

func (p *Pipeline) classifyAndValidate(ctx context.Context, r *run, want bool) (Validated, domain.Decision, error) {
	room := tagRoom(r.b.Tags, r.tax.Rules)
	if !want && room == 0 {
		return Validated{Reasons: []string{"nothing to decide"}}, domain.Decision{}, nil
	}
	content, err := p.fetchContent(ctx, r)
	if err != nil {
		return Validated{}, domain.Decision{}, err
	}
	if !hasUsableText(r, content) {
		v := Validated{Reasons: []string{"no usable text"}}
		if want && !inAllowed(p.validateInput(r, domain.Decision{}, want)) {
			v.ToReview = true
		}
		return v, domain.Decision{}, nil
	}
	d, note, err := p.classify(ctx, r, content, want, room)
	if err != nil {
		return Validated{}, domain.Decision{}, err
	}
	v := Validate(p.validateInput(r, withKnownProposal(d, r.tax), want))
	if note != "" {
		v.Reasons = append(v.Reasons, note)
	}
	if r.opts.TagsOnly && d.Confidence < r.tax.Rules.MinConfidence {
		v.AddTags, v.NewTags = nil, nil
		v.Reasons = append(v.Reasons, fmt.Sprintf("confidence %.2f below minimum %.2f: no tags from a highlight", d.Confidence, r.tax.Rules.MinConfidence))
	}
	return v, d, nil
}

// reviewableText returns the bookmark's own text, leaving out any field that
// only repeats its URL or domain. A URL alone is never grounds to classify.
func reviewableText(b domain.Bookmark) string {
	host := ""
	if u, err := url.Parse(strings.TrimSpace(b.URL)); err == nil {
		host = strings.TrimPrefix(strings.ToLower(u.Hostname()), "www.")
	}
	var parts []string
	for _, f := range []string{b.Title, b.Description, b.Excerpt} {
		f = strings.TrimSpace(f)
		if f == "" || strings.EqualFold(f, strings.TrimSpace(b.URL)) ||
			(host != "" && strings.TrimPrefix(strings.ToLower(f), "www.") == host) {
			continue
		}
		parts = append(parts, f)
	}
	return strings.Join(parts, " ")
}

func hasUsableText(r *run, content string) bool {
	if reviewableText(r.b) != "" || strings.TrimSpace(content) != "" {
		return true
	}
	for _, h := range r.highlights {
		if strings.TrimSpace(h.Text) != "" || strings.TrimSpace(h.Note) != "" {
			return true
		}
	}
	return false
}

// fetchContent applies §9.6. A temporary failure fails the item until the third
// attempt, which counts as permanent. A permanent failure yields no content.
func (p *Pipeline) fetchContent(ctx context.Context, r *run) (string, error) {
	if p.d.Fetcher == nil || p.d.ContentFetch == fetchOff {
		return "", nil
	}
	if p.d.ContentFetch != fetchAlways && utf8.RuneCountInString(reviewableText(r.b)) >= p.d.MinChars {
		return "", nil
	}
	content, err := p.d.Fetcher.Fetch(ctx, r.b.URL)
	if err == nil {
		return content, nil
	}
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	if !errors.Is(err, domain.ErrTemporary) {
		p.d.Log.Info("content fetch failed", "bookmark", r.b.ID.String(), "error", err)
		return "", nil
	}
	r.state.FetchAttempts++
	if r.state.FetchAttempts >= maxFetchAttempts {
		p.d.Log.Info("content fetch gave up", "bookmark", r.b.ID.String(), "error", err)
		return "", nil
	}
	if perr := p.putState(ctx, r.state); perr != nil {
		return "", perr
	}
	return "", fmt.Errorf("fetch content of %s: %w", r.b.ID, err)
}

// classify calls the primary classifier, then the fallback if it errors. The
// fallback never runs for low confidence (§6.4).
func (p *Pipeline) classify(ctx context.Context, r *run, content string, want bool, room int) (domain.Decision, string, error) {
	d, err := p.classifyWith(ctx, p.d.Classifier, r, content, want, room)
	if err == nil {
		return d, "", nil
	}
	if p.d.Fallback == nil || ctx.Err() != nil {
		return domain.Decision{}, "", fmt.Errorf("classify %s: %w", r.b.ID, err)
	}
	p.d.Log.Warn("classifier failed; using fallback", "classifier", p.d.Classifier.Name(), "fallback", p.d.Fallback.Name(), "bookmark", r.b.ID.String(), "error", err)
	d, ferr := p.classifyWith(ctx, p.d.Fallback, r, content, want, room)
	if ferr != nil {
		return domain.Decision{}, "", fmt.Errorf("classify %s: %w; fallback: %w", r.b.ID, err, ferr)
	}
	return d, fmt.Sprintf("fallback %s used after %s failed", p.d.Fallback.Name(), p.d.Classifier.Name()), nil
}

// classifyWith retries invalid output once; a second failure is an error.
func (p *Pipeline) classifyWith(ctx context.Context, c ports.Classifier, r *run, content string, want bool, room int) (domain.Decision, error) {
	var vocab []string
	if r.opts.Vocab != nil {
		vocab = r.opts.Vocab.Terms()
	}
	req := ports.ClassifyRequest{
		Subject: ports.Subject{Bookmark: r.b, Highlights: r.highlights, FocusHighlight: r.focus},
		Content: content,
		Taxonomy: domain.TaxonomySnapshot{
			ID:          r.tax.ID,
			Version:     r.tax.Version,
			Collections: r.tax.Collections,
			Rules:       r.tax.Rules,
		},
		TagVocab:       vocab,
		TagsWanted:     room, // ProposeTags is about inventing tags, not choosing from TagVocab.
		WantCollection: want,
	}
	d, err := c.Classify(ctx, req)
	if errors.Is(err, domain.ErrInvalidOutput) && ctx.Err() == nil {
		d, err = c.Classify(ctx, req)
	}
	if err != nil {
		return domain.Decision{}, err
	}
	if d.Model.Classifier == "" {
		d.Model.Classifier = c.Name()
	}
	return d, nil
}

func joinReasons(reasons ...string) string {
	var kept []string
	for _, s := range reasons {
		if s != "" {
			kept = append(kept, s)
		}
	}
	return strings.Join(kept, "; ")
}

// apply carries out steps 7 to 10 for a validated decision.
func (p *Pipeline) apply(ctx context.Context, r *run, v Validated) (Outcome, error) {
	r.rec.Reason = joinReasons(v.Reasons...)
	model := r.rec.Decision.Model.Model
	target := v.Target
	if v.ToReview {
		target = nil
		if review, ok := reviewCollection(r.tax, r.resolved); ok {
			target = &review
		} else {
			r.rec.Reason = joinReasons(r.rec.Reason, "no review collection resolved")
		}
	}
	if target != nil && *target == r.b.Collection {
		target = nil
	}
	proposal := v.ProposedCollection
	if v.ToReview {
		proposal = nil
	}
	tags := v.AddTags
	needsReview := v.ToReview
	r.setPlanned(target, tags)
	status := outcomeStatus(needsReview, target, tags, proposal)

	// Step 7: a dry run records the decision and stops.
	if r.opts.DryRun {
		if proposal != nil {
			r.rec.Reason = joinReasons(r.rec.Reason, fmt.Sprintf("would create collection %q", proposal.Name))
		}
		r.rec.Result = resultProposed
		r.rec.NewTags = newTags(tags, r.opts.Vocab)
		r.state.KnownCollection = r.b.Collection
		r.state.KnownTags = r.b.Tags
		if err := p.saveDecision(ctx, r.rec); err != nil {
			return Outcome{}, err
		}
		if err := p.putState(ctx, r.state); err != nil {
			return Outcome{}, err
		}
		return Outcome{Status: status, Record: r.rec}, nil
	}

	if v.ToReview && v.ProposedCollection != nil {
		if err := p.recordProposal(ctx, r, *v.ProposedCollection); err != nil {
			return Outcome{}, err
		}
	}

	if target == nil && len(tags) == 0 && proposal == nil {
		r.rec.Result = resultNoChange
		return p.complete(ctx, r, r.b, status, model, false, false)
	}

	// Step 8: re-read. A different collection means the user got there first,
	// which includes sending it to Trash, so nothing at all is written.
	cur, err := p.d.Provider.GetBookmark(ctx, r.b.ID)
	if errors.Is(err, domain.ErrNotFound) {
		return p.markDeleted(ctx, r.b.ID, r.now)
	}
	if err != nil {
		return Outcome{}, fmt.Errorf("re-read bookmark %s: %w", r.b.ID, err)
	}
	if cur.Collection != r.b.Collection {
		return p.abortForMove(ctx, r, cur)
	}
	tags = capTags(cur.Tags, tags, r.tax.Rules)

	if proposal != nil {
		id, reason, err := p.createCollection(ctx, r, proposal.Name)
		if err != nil {
			return Outcome{}, err
		}
		if id != nil {
			target = id
		} else {
			r.rec.Reason = joinReasons(r.rec.Reason, reason)
			needsReview = true
			if err := p.recordProposal(ctx, r, *proposal); err != nil {
				return Outcome{}, err
			}
			if review, ok := reviewCollection(r.tax, r.resolved); ok {
				target = &review
			}
		}
		if target != nil && *target == cur.Collection {
			target = nil
		}
	}
	r.setPlanned(target, tags)
	status = outcomeStatus(needsReview, target, tags, nil)
	if target == nil && len(tags) == 0 {
		r.rec.Result = resultNoChange
		return p.complete(ctx, r, cur, status, model, false, false)
	}

	// Step 9: record the intended write, then write it. If the write fails the
	// pending record stays: the call may have landed, and the next run checks.
	r.state.PendingCollection = target
	r.state.PendingTags = tags
	r.state.Model = model
	if err := p.putState(ctx, r.state); err != nil {
		return Outcome{}, err
	}
	r.rec.Result = resultPending
	if err := p.saveDecision(ctx, r.rec); err != nil {
		return Outcome{}, err
	}
	updated, err := p.d.Provider.UpdateBookmark(ctx, r.b.ID, ports.BookmarkPatch{Collection: target, AddTags: tags})
	if errors.Is(err, domain.ErrNotFound) {
		return p.handleWriteNotFound(ctx, r, err)
	}
	if err != nil {
		return Outcome{}, fmt.Errorf("write bookmark %s: %w", r.b.ID, err)
	}

	// Step 10: record the result.
	return p.complete(ctx, r, updated, status, model, target != nil, true)
}

// newTags lists the tags outside the vocabulary the run started with, on every
// use, unlike Validated.NewTags, which lists a tag only where it was first seen.
func newTags(tags []string, vocab *taxonomy.RunVocab) []string {
	if vocab == nil {
		return nil
	}
	var out []string
	for _, t := range tags {
		if _, ok := vocab.Existing(t); !ok {
			out = append(out, t)
		}
	}
	return out
}

// recordProposal keeps a collection name that wasn't created, for /proposals.
func (p *Pipeline) recordProposal(ctx context.Context, r *run, c domain.CollectionProposal) error {
	if err := p.d.Store.Taxonomies().RecordProposal(ctx, r.opts.Taxonomy.ID, c); err != nil {
		return fmt.Errorf("record proposal %q: %w", c.Name, err)
	}
	return nil
}

// handleWriteNotFound settles a write the provider refused as not found, which
// may mean a stale target collection and not a deleted bookmark. Only a read can
// tell. Either way the write didn't land, so nothing stays pending.
func (p *Pipeline) handleWriteNotFound(ctx context.Context, r *run, writeErr error) (Outcome, error) {
	_, readErr := p.d.Provider.GetBookmark(ctx, r.b.ID)
	if errors.Is(readErr, domain.ErrNotFound) {
		r.rec.Result = resultDeleted
		if err := p.saveDecision(ctx, r.rec); err != nil {
			return Outcome{}, err
		}
		return p.markDeleted(ctx, r.b.ID, r.now)
	}
	r.state.PendingCollection, r.state.PendingTags = nil, nil
	r.rec.Result = resultFailed
	r.rec.Reason = joinReasons(r.rec.Reason, "provider reported not found on write")
	if err := p.saveDecision(ctx, r.rec); err != nil {
		return Outcome{}, err
	}
	if err := p.putState(ctx, r.state); err != nil {
		return Outcome{}, err
	}
	if readErr != nil {
		return Outcome{}, fmt.Errorf("write bookmark %s: %w; re-read: %w", r.b.ID, writeErr, readErr)
	}
	return Outcome{}, fmt.Errorf("write bookmark %s: %w", r.b.ID, writeErr)
}

func (r *run) setPlanned(target *domain.ID, tags []string) {
	r.rec.TargetCollection = target
	r.rec.AddTags = tags
}

func outcomeStatus(needsReview bool, target *domain.ID, tags []string, proposal *domain.CollectionProposal) OutcomeStatus {
	switch {
	case needsReview:
		return NeedsReview
	case target != nil || len(tags) > 0 || proposal != nil:
		return Changed
	default:
		return Unchanged
	}
}

// abortForMove pins a bookmark the user moved during processing. The hash isn't
// recorded, so a run that still sees the bookmark can add its tags. Trash is
// never listed, so one sent there isn't seen again.
func (p *Pipeline) abortForMove(ctx context.Context, r *run, cur domain.Bookmark) (Outcome, error) {
	DetectManualMove(&r.state, cur, r.now)
	r.state.Status = string(Unchanged)
	r.setPlanned(nil, nil)
	r.rec.Result = resultAborted
	r.rec.Reason = joinReasons(r.rec.Reason, "moved by the user during processing")
	if err := p.saveDecision(ctx, r.rec); err != nil {
		return Outcome{}, err
	}
	if err := p.putState(ctx, r.state); err != nil {
		return Outcome{}, err
	}
	return Outcome{Status: Unchanged, Record: r.rec}, nil
}

// complete records a finished item. written means the provider was changed.
func (p *Pipeline) complete(ctx context.Context, r *run, b domain.Bookmark, status OutcomeStatus, model string, movedByUs, written bool) (Outcome, error) {
	// An answer about some other bookmark says nothing about this write. What's
	// pending stays, and the next read settles it.
	if b.ID != r.rec.Bookmark {
		return Outcome{}, fmt.Errorf("write bookmark %s: provider answered for %q", r.rec.Bookmark, b.ID)
	}
	if written {
		r.rec.Result = resultApplied
		applied := r.now
		r.rec.AppliedAt = &applied
	}
	if err := p.saveDecision(ctx, r.rec); err != nil {
		return Outcome{}, err
	}
	st := &r.state
	st.KnownCollection = b.Collection
	st.KnownTags = b.Tags
	st.PendingCollection, st.PendingTags = nil, nil
	if !p.keepsCollectionDecisionOpen(r, b) {
		st.InputHash = r.hash
	}
	// A tags-only run made no collection decision under this taxonomy, so a
	// later backfill must still see the version as different.
	if !r.opts.TagsOnly {
		st.TaxonomyVersion = r.opts.Taxonomy.Version
	}
	if model != "" {
		st.Model = model
	}
	st.Status = string(status)
	st.FetchAttempts = 0
	st.ProcessedAt = &r.now
	// A first backfill's pass settles an overridden pin, so resuming it doesn't
	// classify the bookmark again.
	if movedByUs || (!r.opts.DryRun && ignoresUserPin(*st, r.opts)) {
		st.PinSource = domain.PinNone
		st.PinnedAt = nil
	}
	if !r.opts.DryRun && (r.opts.Trigger == TriggerBackfill || r.opts.Trigger == TriggerReclassify) {
		st.RestoredAt = nil
	}
	if err := p.putState(ctx, *st); err != nil {
		return Outcome{}, err
	}
	return Outcome{Status: status, Record: r.rec}, nil
}

// keepsCollectionDecisionOpen reports a tags-only run on a bookmark still owed a
// collection decision: a new one, or one in Needs Review. Recording the hash
// would make the next sync skip it, so the hash waits for that decision.
func (p *Pipeline) keepsCollectionDecisionOpen(r *run, b domain.Bookmark) bool {
	if !r.opts.TagsOnly {
		return false
	}
	full := r.opts
	full.TagsOnly = false
	return r.isNew || EvaluateMove(r.state, r.known, b, p.d.Provider.Capabilities().InboxCollection, full)
}

func (p *Pipeline) markDeleted(ctx context.Context, id domain.ID, now time.Time) (Outcome, error) {
	st, known, err := p.d.Store.BookmarkStates().GetState(ctx, id)
	if err != nil {
		return Outcome{}, fmt.Errorf("load state of %s: %w", id, err)
	}
	if !known {
		st = domain.BookmarkState{ID: id}
	}
	st.Status = string(Deleted)
	st.PendingCollection, st.PendingTags = nil, nil
	st.ProcessedAt = &now
	if err := p.putState(ctx, st); err != nil {
		return Outcome{}, err
	}
	return Outcome{Status: Deleted, Record: domain.DecisionRecord{Bookmark: id, Reason: "bookmark no longer exists"}}, nil
}

func (p *Pipeline) putState(ctx context.Context, s domain.BookmarkState) error {
	if err := p.d.Store.BookmarkStates().PutState(ctx, s); err != nil {
		return fmt.Errorf("save state of %s: %w", s.ID, err)
	}
	return nil
}

// saveDecision stores the per-job record. Decisions belong to a job, so an item
// run outside one has nowhere to keep it.
func (p *Pipeline) saveDecision(ctx context.Context, rec domain.DecisionRecord) error {
	if rec.JobID == "" {
		return nil
	}
	if err := p.d.Store.Jobs().SaveDecision(ctx, rec); err != nil {
		return fmt.Errorf("save decision for %s: %w", rec.Bookmark, err)
	}
	return nil
}

type createdCollection struct {
	Key  string    `json:"key"`
	Name string    `json:"name"`
	ID   domain.ID `json:"id"`
}

func (p *Pipeline) createdSetting(t domain.Taxonomy) string {
	return createdSettingPrefix + p.d.Provider.Name() + ":" + t.ID
}

// loadCreated reads the collections auto-created for this taxonomy. They live
// in settings, not on the taxonomy, so creating one doesn't bump its version.
func (p *Pipeline) loadCreated(ctx context.Context, t domain.Taxonomy) ([]createdCollection, error) {
	raw, ok, err := p.d.Store.BookmarkStates().GetSetting(ctx, p.createdSetting(t))
	if err != nil {
		return nil, fmt.Errorf("load created collections: %w", err)
	}
	if !ok {
		return nil, nil
	}
	var out []createdCollection
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, fmt.Errorf("parse created collections: %w", err)
	}
	return out, nil
}

// ForgetCreated drops collections that no longer exist from those auto-created for
// a taxonomy, so that they aren't offered to classifiers or counted against the cap.
func (p *Pipeline) ForgetCreated(ctx context.Context, taxonomyID string, gone map[domain.ID]bool) error {
	p.createMu.Lock()
	defer p.createMu.Unlock()

	t := domain.Taxonomy{ID: taxonomyID}
	created, err := p.loadCreated(ctx, t)
	if err != nil {
		return err
	}
	kept := make([]createdCollection, 0, len(created))
	for _, c := range created {
		if !gone[c.ID] {
			kept = append(kept, c)
		}
	}
	if len(kept) == len(created) {
		return nil
	}
	raw, err := json.Marshal(kept)
	if err != nil {
		return fmt.Errorf("encode created collections: %w", err)
	}
	if err := p.d.Store.BookmarkStates().PutSetting(ctx, p.createdSetting(t), string(raw)); err != nil {
		return fmt.Errorf("save created collections: %w", err)
	}
	return nil
}

// effectiveTaxonomy adds auto-created collections to the caller's taxonomy, so
// classifiers can choose them, the validator counts them against the cap, and
// neither the cap nor their IDs are lost on restart.
func (p *Pipeline) effectiveTaxonomy(ctx context.Context, o Options) (domain.Taxonomy, map[string]domain.ID, error) {
	if !o.Taxonomy.Dynamic.AutoCreate {
		return o.Taxonomy, o.Resolved, nil
	}
	created, err := p.loadCreated(ctx, o.Taxonomy)
	if err != nil {
		return domain.Taxonomy{}, nil, err
	}
	tax, resolved := mergeCreated(o.Taxonomy, o.Resolved, created)
	return tax, resolved, nil
}

func mergeCreated(t domain.Taxonomy, resolved map[string]domain.ID, created []createdCollection) (domain.Taxonomy, map[string]domain.ID) {
	cols := append([]domain.TaxonomyCollection(nil), t.Collections...)
	ids := make(map[string]domain.ID, len(resolved)+len(created))
	for k, v := range resolved {
		ids[k] = v
	}
	t.Collections = cols
	for _, c := range created {
		if _, found := findCollection(t, c.Name); found {
			continue
		}
		t.Collections = append(t.Collections, domain.TaxonomyCollection{Key: c.Key, Name: c.Name})
		ids[c.Key] = c.ID
	}
	return t, ids
}

// createCollection creates or finds the named collection, with the cap
// re-checked under the lock so concurrent items can't both take the last slot.
// A nil ID with a reason means the cap was reached.
func (p *Pipeline) createCollection(ctx context.Context, r *run, name string) (*domain.ID, string, error) {
	p.createMu.Lock()
	defer p.createMu.Unlock()

	base := r.opts.Taxonomy
	created, err := p.loadCreated(ctx, base)
	if err != nil {
		return nil, "", err
	}
	tax, ids := mergeCreated(base, r.opts.Resolved, created)
	if c, found := findCollection(tax, name); found {
		if id, ok := ids[c.Key]; ok {
			return &id, "", nil
		}
	}
	if len(tax.Collections) >= tax.Dynamic.MaxCollections {
		return nil, fmt.Sprintf("proposed collection %q: collection cap %d reached", name, tax.Dynamic.MaxCollections), nil
	}
	manager, ok := p.d.Provider.(ports.CollectionManager)
	if !ok {
		return nil, fmt.Sprintf("proposed collection %q: provider cannot create collections", name), nil
	}
	existing, err := manager.ListCollections(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("list collections: %w", err)
	}
	id, reused := domain.ID{}, false
	for _, c := range existing {
		if c.Parent == nil && strings.EqualFold(c.Name, name) {
			id, reused = c.ID, true
			break
		}
	}
	if !reused {
		made, err := manager.CreateCollection(ctx, domain.Collection{Name: name})
		if err != nil {
			return nil, "", fmt.Errorf("create collection %q: %w", name, err)
		}
		id = made.ID
		if r.opts.SnapshotID != "" {
			if err := p.d.Store.Snapshots().AddCreatedCollection(ctx, r.opts.SnapshotID, id); err != nil {
				return nil, "", fmt.Errorf("record created collection: %w", err)
			}
		}
	}
	created = append(created, createdCollection{Key: strings.ToLower(name), Name: name, ID: id})
	raw, err := json.Marshal(created)
	if err != nil {
		return nil, "", fmt.Errorf("encode created collections: %w", err)
	}
	if err := p.d.Store.BookmarkStates().PutSetting(ctx, p.createdSetting(base), string(raw)); err != nil {
		return nil, "", fmt.Errorf("save created collections: %w", err)
	}
	return &id, "", nil
}
