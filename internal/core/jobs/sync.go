package jobs

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/wikiabhi/rainwise/internal/core/pipeline"
	"github.com/wikiabhi/rainwise/internal/core/taxonomy"
	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

const (
	watermarkPrefix = "sync_watermark:"
	baselinePrefix  = "sync_baseline_done:"
	sinceMargin     = 48 * time.Hour
)

// syncJob is §9.3: a pass over bookmarks and over new highlights.
type syncJob struct{ Deps }

func (h syncJob) Run(ctx context.Context, job *domain.Job, rc RunContext) error {
	d := h.Deps
	if d.AutomationOff && !job.DryRun {
		return fmt.Errorf("real sync: %w", domain.ErrAutomationDisabled)
	}
	start := d.startTime(job)
	key := watermarkPrefix + d.Provider.Name()
	watermark, hasWatermark, err := d.loadWatermark(ctx, key)
	if err != nil {
		return err
	}
	if job.Checkpoint.Phase != phaseDone {
		if err := h.runPasses(ctx, job, rc, start, watermark, hasWatermark); err != nil {
			return err
		}
		job.Progress.Total = job.Progress.Processed
		job.Checkpoint.Phase, job.Checkpoint.PageToken = phaseDone, ""
		if err := rc.Checkpoint(ctx); err != nil {
			return fmt.Errorf("save checkpoint: %w", err)
		}
	}
	// A full listing comes back to a failed item by itself. An incremental one
	// wouldn't, so it holds its place until nothing failed.
	if job.DryRun || (d.Provider.Capabilities().IncrementalSince && job.Progress.Failed > 0) {
		return nil
	}
	if err := d.Store.BookmarkStates().PutSetting(ctx, key, start.UTC().Format(time.RFC3339Nano)); err != nil {
		return fmt.Errorf("save watermark: %w", err)
	}
	return nil
}

func (d Deps) loadWatermark(ctx context.Context, key string) (time.Time, bool, error) {
	raw, ok, err := d.Store.BookmarkStates().GetSetting(ctx, key)
	if err != nil || !ok {
		return time.Time{}, false, err
	}
	at, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("parse %s: %w", key, err)
	}
	return at, true, nil
}

// syncVocabulary is the provider's tag listing, which costs one request and sees tags
// added by hand. Without one it's the persisted vocabulary, which only a backfill
// refreshes, because building one can take a pass over the whole library. If there is
// none, this makes it.
func (d Deps) syncVocabulary(ctx context.Context, job *domain.Job, rc RunContext) ([]string, error) {
	counts, listed, err := d.listedTags(ctx)
	if err != nil {
		return nil, err
	}
	if listed {
		terms := taxonomy.TopTerms(counts, d.TagVocabSize)
		if err := taxonomy.SaveVocab(ctx, d.Store.BookmarkStates(), d.Provider.Name(), terms); err != nil {
			return nil, fmt.Errorf("save tag vocabulary: %w", err)
		}
		return terms, nil
	}
	terms, ok, err := taxonomy.LoadVocab(ctx, d.Store.BookmarkStates(), d.Provider.Name())
	if err != nil || ok {
		return terms, err
	}
	terms, _, err = d.vocabularyTerms(ctx, job, rc, scopeMatcher{}, nil)
	return terms, err
}

func (h syncJob) runPasses(ctx context.Context, job *domain.Job, rc RunContext, start, watermark time.Time, hasWatermark bool) error {
	d := h.Deps
	baselineKey := baselinePrefix + d.Provider.Name()
	baselineAt, baselineDone, err := d.loadWatermark(ctx, baselineKey)
	if err != nil {
		return err
	}
	// A watermark also shows that a first listing finished.
	firstSync := !hasWatermark && !baselineDone
	highlightsFrom := watermark
	if !hasWatermark {
		highlightsFrom = baselineAt
	}
	t, err := d.loadTaxonomy(ctx, job)
	if err != nil {
		return err
	}
	resolved, err := d.resolveForRun(ctx, job, t)
	if err != nil {
		return err
	}
	terms, err := d.syncVocabulary(ctx, job, rc)
	if err != nil {
		return err
	}
	opts := pipeline.Options{
		Trigger:  pipeline.TriggerSync,
		JobID:    job.ID,
		DryRun:   job.DryRun,
		Baseline: firstSync,
		Taxonomy: t,
		Resolved: resolved,
		Vocab:    taxonomy.NewRunVocab(terms, t.Rules.TagAliases, t.Rules.AllowNewTags),
	}
	process := func(ctx context.Context, item pipeline.WorkItem) (pipeline.Outcome, error) {
		itemOpts := opts
		// Force: the bookmark's own item may already have recorded the hash these
		// highlights changed, and that mustn't swallow their tags.
		if item.FocusHighlight != nil {
			itemOpts.Force, itemOpts.TagsOnly = true, true
		}
		out, err := d.Pipeline.Process(ctx, item, itemOpts)
		if err == nil {
			err = d.rememberTags(ctx, job, out, opts.Vocab)
		}
		logFailure(ctx, job, item, err)
		return out, err
	}
	caps := d.Provider.Capabilities()
	hr, readsHighlights := d.Provider.(ports.HighlightReader)
	readsHighlights = readsHighlights && caps.Highlights
	// An incremental listing leaves out any bookmark whose lastUpdate didn't change,
	// which a new highlight may not touch, so it can't stand in for the feed.
	fullListing := !hasWatermark || !caps.IncrementalSince

	if job.Checkpoint.Phase == "" {
		var since *time.Time
		if !fullListing {
			at := watermark.Add(-sinceMargin)
			since = &at
		}
		list := d.bookmarkLister(job, since, scopeMatcher{})
		if readsHighlights {
			list = listedHighlights(job, list, fullListing && !firstSync, highlightsFrom)
		}
		if err := RunItems(ctx, job, rc, d.loopConfig(), list, process); err != nil {
			return err
		}
		job.Checkpoint.Phase, job.Checkpoint.PageToken = phaseHighlights, ""
		if err := rc.Checkpoint(ctx); err != nil {
			return fmt.Errorf("save checkpoint: %w", err)
		}
	}

	// A first sync follows the baseline rule, so it spends nothing on old highlights.
	// However its items went, it's done, or one bad bookmark would keep every later
	// sync in baseline mode. A resume after a crash here lands back on this write.
	if firstSync {
		if job.DryRun {
			return nil
		}
		if err := d.Store.BookmarkStates().PutSetting(ctx, baselineKey, start.UTC().Format(time.RFC3339Nano)); err != nil {
			return fmt.Errorf("save sync baseline: %w", err)
		}
		return nil
	}
	if !readsHighlights || (fullListing && !job.Checkpoint.HighlightFeed) {
		return nil
	}
	return RunItems(ctx, job, rc, d.loopConfig(), highlightLister(job, hr, highlightsFrom), process)
}

// listedHighlights adds to a bookmark listing a tags-only item for each highlight
// the listing shows made since from, if focus is set. A bookmark listed without
// saying what highlights it has sets job.Checkpoint.HighlightFeed, and the feed then
// covers everything, which can repeat a highlight seen here at the cost of one more
// tags-only run. Like the feed, this takes no margin (see highlightLister).
func listedHighlights(job *domain.Job, list ListFunc, focus bool, from time.Time) ListFunc {
	return func(ctx context.Context, token string) ([]pipeline.WorkItem, string, error) {
		items, next, err := list(ctx, token)
		if err != nil {
			return nil, "", err
		}
		out := items
		for _, it := range items {
			if it.Listed.Highlights == nil {
				job.Checkpoint.HighlightFeed = true
				continue
			}
			if !focus {
				continue
			}
			for _, h := range it.Listed.Highlights {
				if !h.CreatedAt.Before(from) {
					id := h.ID
					out = append(out, pipeline.WorkItem{Bookmark: it.Bookmark, FocusHighlight: &id, Listed: it.Listed})
				}
			}
		}
		return out, next, nil
	}
}

// highlightLister lists highlights made since the last successful sync. Unlike the
// bookmark pass this takes no margin: a highlight run is forced past the input
// hash, so one seen twice would cost a second classifier call on every sync in
// the margin. The feed may ignore Since, so it's checked here too.
func highlightLister(job *domain.Job, hr ports.HighlightReader, since time.Time) ListFunc {
	return func(ctx context.Context, token string) ([]pipeline.WorkItem, string, error) {
		page, err := hr.ListAllHighlights(ctx, ports.ListQuery{Since: &since, PageToken: token, Limit: job.Params.BatchSize})
		if err != nil {
			return nil, "", err
		}
		var items []pipeline.WorkItem
		for _, h := range page.Items {
			if h.CreatedAt.Before(since) {
				continue
			}
			if h.Bookmark.IsZero() {
				slog.Warn("highlight has no parent bookmark; skipping", "job", job.ID, "highlight", h.ID.String())
				continue
			}
			focus := h.ID
			items = append(items, pipeline.WorkItem{Bookmark: h.Bookmark, FocusHighlight: &focus})
		}
		return items, page.NextToken, nil
	}
}
