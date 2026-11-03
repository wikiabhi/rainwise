package jobs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/wikiabhi/rainwise/internal/core/pipeline"
	"github.com/wikiabhi/rainwise/internal/domain"
)

// ErrTooManyFailures is returned by RunItems when failures outlast every pause.
var ErrTooManyFailures = errors.New("too many consecutive item failures")

const (
	defaultConcurrency            = 4
	defaultMaxConsecutiveFailures = 10
	defaultMaxPauses              = 3
	defaultPauseFor               = time.Minute
)

// LoopConfig tunes RunItems. Zero values take the §9.4 defaults.
type LoopConfig struct {
	Concurrency            int
	MaxConsecutiveFailures int
	MaxPauses              int
	PauseFor               time.Duration
	Sleep                  func(context.Context, time.Duration) error
}

func (c LoopConfig) withDefaults() LoopConfig {
	if c.Concurrency <= 0 {
		c.Concurrency = defaultConcurrency
	}
	if c.MaxConsecutiveFailures <= 0 {
		c.MaxConsecutiveFailures = defaultMaxConsecutiveFailures
	}
	if c.MaxPauses <= 0 {
		c.MaxPauses = defaultMaxPauses
	}
	if c.PauseFor <= 0 {
		c.PauseFor = defaultPauseFor
	}
	if c.Sleep == nil {
		c.Sleep = realSleep
	}
	return c
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

// ListFunc returns the page at token, and the token of the next ("" at the end).
type ListFunc func(ctx context.Context, token string) (items []pipeline.WorkItem, next string, err error)

// ProcessFunc handles one item. An error is recorded as an item failure and the
// loop goes on, so log it here if it should be seen.
type ProcessFunc func(ctx context.Context, item pipeline.WorkItem) (pipeline.Outcome, error)

// RunItems pages through a listing from job.Checkpoint.PageToken, running up to
// cfg.Concurrency items at a time.
//
// A saved checkpoint is always a page boundary: a token with the counters of the
// pages before it. So a page's counters join job.Progress only once all of it has
// run. An interrupted page runs again in full on resume, without double counting;
// the input hash makes that cheap.
//
// The last page isn't checkpointed, because an empty saved token would restart a
// crashed job from page one with full counters. Its counters reach the store in
// the runner's final write, and a crash before that repeats only the last page.
// A handler that works on after RunItems should keep this in mind before it saves.
//
// When rc.Stopping closes, in-flight items finish and it returns ErrInterrupted.
//
// After MaxConsecutiveFailures failures in a row it pauses for PauseFor. Pauses
// add up over the job and survive a resume. Once MaxPauses pauses are spent, the
// next such run of failures returns ErrTooManyFailures, with the allowance reset
// so that re-queuing the job resumes it from its checkpoint.
func RunItems(ctx context.Context, job *domain.Job, rc RunContext, cfg LoopConfig, list ListFunc, process ProcessFunc) error {
	l := &itemLoop{job: job, rc: rc, cfg: cfg.withDefaults(), process: process}
	for {
		if err := l.stopErr(ctx); err != nil {
			return l.abort(ctx, err)
		}
		items, next, err := list(ctx, job.Checkpoint.PageToken)
		if err != nil {
			if ctx.Err() != nil {
				return l.abort(ctx, ctx.Err())
			}
			return fmt.Errorf("list page: %w", err)
		}
		page, err := l.runPage(ctx, items)
		if errors.Is(err, ErrTooManyFailures) {
			return err
		}
		if err != nil {
			return l.abort(ctx, err)
		}
		addProgress(&job.Progress, page)
		job.Checkpoint.PageToken = next
		if next == "" {
			return nil
		}
		if err := rc.Checkpoint(ctx); err != nil {
			return fmt.Errorf("save checkpoint: %w", err)
		}
	}
}

type itemLoop struct {
	job     *domain.Job
	rc      RunContext
	cfg     LoopConfig
	process ProcessFunc

	mu          sync.Mutex
	page        domain.JobProgress
	consecutive int // Carries across pages.
	lastErr     error
}

// abort saves what the last page boundary established and returns why it stopped.
func (l *itemLoop) abort(ctx context.Context, cause error) error {
	return errors.Join(cause, l.rc.Checkpoint(ctx))
}

func (l *itemLoop) stopErr(ctx context.Context) error {
	select {
	case <-l.rc.Stopping:
		return ErrInterrupted
	default:
		return ctx.Err()
	}
}

// runPage returns the page's counters, or an error if it didn't run in full.
func (l *itemLoop) runPage(ctx context.Context, items []pipeline.WorkItem) (domain.JobProgress, error) {
	l.page = domain.JobProgress{}
	sem := make(chan struct{}, l.cfg.Concurrency)
	var wg sync.WaitGroup
	var stop error
launch:
	for _, it := range items {
		if stop = l.acquire(ctx, sem); stop != nil {
			break
		}
		// Waiting for a slot may have let failures pile up, so check only now.
		if l.failing() {
			<-sem
			wg.Wait()
			if stop = l.pauseIfFailing(ctx); stop != nil {
				break launch
			}
			if stop = l.acquire(ctx, sem); stop != nil {
				break
			}
		}
		wg.Go(func() {
			defer func() { <-sem }()
			out, err := l.process(ctx, it)
			l.record(out, err)
		})
	}
	wg.Wait()
	if stop == nil {
		// Items cut short by the context aren't real outcomes.
		stop = ctx.Err()
	}
	return l.page, stop
}

func (l *itemLoop) acquire(ctx context.Context, sem chan struct{}) error {
	if err := l.stopErr(ctx); err != nil {
		return err
	}
	select {
	case sem <- struct{}{}:
		return nil
	case <-l.rc.Stopping:
		return ErrInterrupted
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *itemLoop) failing() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.consecutive >= l.cfg.MaxConsecutiveFailures
}

// pauseIfFailing runs with no items in flight. One of those that just finished
// may have succeeded, in which case there's nothing to pause for.
func (l *itemLoop) pauseIfFailing(ctx context.Context) error {
	if !l.failing() {
		return nil
	}
	cp := &l.job.Checkpoint
	if cp.Pauses >= l.cfg.MaxPauses {
		cp.Pauses = 0
		return fmt.Errorf("%w: %d in a row after %d pauses, last error: %v",
			ErrTooManyFailures, l.consecutive, l.cfg.MaxPauses, l.lastErr)
	}
	cp.Pauses++
	if err := l.rc.Checkpoint(ctx); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}
	if err := l.sleep(ctx); err != nil {
		return err
	}
	l.consecutive = 0
	return nil
}

// sleep waits out a pause, ending early when the job is told to stop.
func (l *itemLoop) sleep(ctx context.Context) error {
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-l.rc.Stopping:
			cancel()
		case <-sctx.Done():
		}
	}()
	err := l.cfg.Sleep(sctx, l.cfg.PauseFor)
	if stop := l.stopErr(ctx); stop != nil {
		return stop
	}
	return err
}

func (l *itemLoop) record(out pipeline.Outcome, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.page.Processed++
	if err != nil {
		l.page.Failed++
		l.consecutive++
		l.lastErr = err
		return
	}
	l.consecutive = 0
	switch out.Status {
	case pipeline.Changed:
		l.page.Changed++
	case pipeline.NeedsReview:
		l.page.NeedsReview++
	default: // Skipped, Unchanged, Deleted: handled, with nothing to do.
		l.page.Skipped++
	}
}

func addProgress(p *domain.JobProgress, d domain.JobProgress) {
	p.Processed += d.Processed
	p.Changed += d.Changed
	p.Skipped += d.Skipped
	p.NeedsReview += d.NeedsReview
	p.Failed += d.Failed
}
