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
	sinceMargin     = 48 * time.Hour
)

// syncJob is §9.3: a pass over bookmarks, then one over new highlights.
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
		if err := h.runPasses(ctx, job, rc, watermark, hasWatermark); err != nil {
			return err
		}
		job.Progress.Total = job.Progress.Processed
		job.Checkpoint.Phase, job.Checkpoint.PageToken = phaseDone, ""
		if err := rc.Checkpoint(ctx); err != nil {
			return fmt.Errorf("save checkpoint: %w", err)
		}
	}
	if job.DryRun || job.Progress.Failed > 0 {
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

func (h syncJob) runPasses(ctx context.Context, job *domain.Job, rc RunContext, watermark time.Time, hasWatermark bool) error {
	d := h.Deps
	t, err := d.loadTaxonomy(ctx, job)
	if err != nil {
		return err
	}
	resolved, err := d.resolveForRun(ctx, job, t)
	if err != nil {
		return err
	}
	terms, _, err := d.vocabularyTerms(ctx, job, rc, scopeMatcher{}, nil)
	if err != nil {
		return err
	}
	opts := pipeline.Options{
		Trigger:  pipeline.TriggerSync,
		JobID:    job.ID,
		DryRun:   job.DryRun,
		Baseline: !hasWatermark,
		Taxonomy: t,
		Resolved: resolved,
		Vocab:    taxonomy.NewRunVocab(terms, t.Rules.TagAliases, t.Rules.AllowNewTags),
	}
	process := func(opts pipeline.Options) ProcessFunc {
		return func(ctx context.Context, item pipeline.WorkItem) (pipeline.Outcome, error) {
			out, err := d.Pipeline.Process(ctx, item, opts)
			logFailure(ctx, job, item, err)
			return out, err
		}
	}

	if job.Checkpoint.Phase == "" {
		var since *time.Time
		if hasWatermark && d.Provider.Capabilities().IncrementalSince {
			at := watermark.Add(-sinceMargin)
			since = &at
		}
		if err := RunItems(ctx, job, rc, d.loopConfig(), d.bookmarkLister(job, since, scopeMatcher{}), process(opts)); err != nil {
			return err
		}
		job.Checkpoint.Phase, job.Checkpoint.PageToken = phaseHighlights, ""
		if err := rc.Checkpoint(ctx); err != nil {
			return fmt.Errorf("save checkpoint: %w", err)
		}
	}

	// A first sync follows the baseline rule, so it spends nothing on old highlights.
	hr, ok := d.Provider.(ports.HighlightReader)
	if !hasWatermark || !ok || !d.Provider.Capabilities().Highlights {
		return nil
	}
	// Force: the bookmark pass may already have recorded the hash these highlights
	// changed, and that mustn't swallow their tags.
	opts.Force, opts.TagsOnly = true, true
	return RunItems(ctx, job, rc, d.loopConfig(), highlightLister(job, hr, watermark), process(opts))
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
