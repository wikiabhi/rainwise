package jobs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wikiabhi/rainwise/internal/core/pipeline"
	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

const testWait = 10 * time.Second

type handlerFunc func(ctx context.Context, job *domain.Job, rc RunContext) error

func (f handlerFunc) Run(ctx context.Context, job *domain.Job, rc RunContext) error {
	return f(ctx, job, rc)
}

// newTestRunner polls hourly, so anything that happens in a test came from Wake.
func newTestRunner(s ports.Store, handlers map[domain.JobType]Handler) *Runner {
	return NewRunner(RunnerDeps{Store: s, Handlers: handlers, Provider: "p", Now: func() time.Time { return t0.Add(time.Hour) },
		PollEvery: time.Hour, Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
}

func backfillHandler(f handlerFunc) map[domain.JobType]Handler {
	return map[domain.JobType]Handler{domain.JobBackfill: f}
}

func shutdown(t *testing.T, r *Runner) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	if err := r.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func recv[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(testWait):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

// waitStatus spins rather than sleeps; tests never really sleep.
func waitStatus(t *testing.T, s ports.Store, id string, want domain.JobStatus) domain.Job {
	t.Helper()
	deadline := time.Now().Add(testWait)
	for {
		j, err := s.Jobs().GetJob(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if j.Status == want {
			return j
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %s: status %s, want %s", id, j.Status, want)
		}
		runtime.Gosched()
	}
}

func getJob(t *testing.T, s ports.Store, id string) domain.Job {
	t.Helper()
	j, err := s.Jobs().GetJob(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func TestRunnerOneWriteJobAtATime(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	entered := make(chan string, 3)
	release := map[string]chan struct{}{"w1": make(chan struct{}), "w2": make(chan struct{}), "dry": make(chan struct{})}
	var writers, peak atomic.Int32
	h := func(ctx context.Context, job *domain.Job, _ RunContext) error {
		if !job.DryRun {
			n := writers.Add(1)
			defer writers.Add(-1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
		}
		entered <- job.ID
		<-release[job.ID]
		return nil
	}
	seedJob(t, s, domain.Job{ID: "w1", Status: domain.JobQueued, CreatedAt: t0})
	seedJob(t, s, domain.Job{ID: "w2", Status: domain.JobQueued, CreatedAt: t0.Add(time.Second)})
	seedJob(t, s, domain.Job{ID: "dry", Status: domain.JobQueued, DryRun: true, CreatedAt: t0.Add(2 * time.Second)})

	r := newTestRunner(s, backfillHandler(h))
	r.Start(ctx)
	defer shutdown(t, r)

	got := []string{recv(t, entered, "first job"), recv(t, entered, "second job")}
	slices.Sort(got)
	if !slices.Equal(got, []string{"dry", "w1"}) {
		t.Fatalf("running = %v, want the older real job alongside the dry run", got)
	}
	if st := getJob(t, s, "w2").Status; st != domain.JobQueued {
		t.Fatalf("w2 = %s, want queued while w1 runs", st)
	}
	close(release["dry"])
	waitStatus(t, s, "dry", domain.JobSucceeded)
	r.Wake()
	if st := getJob(t, s, "w2").Status; st != domain.JobQueued {
		t.Fatalf("w2 = %s, a finished dry run must not free the write slot", st)
	}
	close(release["w1"])
	if id := recv(t, entered, "w2"); id != "w2" {
		t.Fatalf("entered %s, want w2", id)
	}
	close(release["w2"])
	waitStatus(t, s, "w2", domain.JobSucceeded)
	if peak.Load() != 1 {
		t.Errorf("peak real jobs = %d, want 1", peak.Load())
	}
}

func TestRunnerSetsStartedAt(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	seen := make(chan domain.Job, 1)
	seedJob(t, s, domain.Job{ID: "j", Status: domain.JobQueued})
	r := newTestRunner(s, backfillHandler(func(_ context.Context, job *domain.Job, _ RunContext) error {
		seen <- getJob(t, s, job.ID)
		return nil
	}))
	r.Start(ctx)
	defer shutdown(t, r)
	stored := recv(t, seen, "handler")
	if stored.StartedAt == nil || !stored.StartedAt.Equal(t0.Add(time.Hour)) {
		t.Errorf("StartedAt while running = %v, want the runner's now", stored.StartedAt)
	}
	j := waitStatus(t, s, "j", domain.JobSucceeded)
	if j.FinishedAt == nil || !j.FinishedAt.Equal(t0.Add(time.Hour)) {
		t.Errorf("FinishedAt = %v", j.FinishedAt)
	}
}

func TestRunnerResumesOnStart(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	started := t0.Add(-time.Hour)
	seedJob(t, s, domain.Job{ID: "j", Status: domain.JobRunning, StartedAt: &started,
		Checkpoint: domain.Checkpoint{PageToken: "p2"}, Progress: domain.JobProgress{Processed: 5, Changed: 5}})
	seedJob(t, s, domain.Job{ID: "elsewhere", Provider: "q", Status: domain.JobRunning})
	seen := make(chan domain.Job, 2)
	r := newTestRunner(s, backfillHandler(func(_ context.Context, job *domain.Job, _ RunContext) error {
		seen <- *job
		return nil
	}))
	r.Start(ctx)
	got := recv(t, seen, "resumed job")
	if got.ID != "j" || got.Checkpoint.PageToken != "p2" || got.Progress.Processed != 5 {
		t.Errorf("resumed = %+v", got)
	}
	j := waitStatus(t, s, "j", domain.JobSucceeded)
	if j.StartedAt == nil || !j.StartedAt.Equal(started) {
		t.Errorf("StartedAt = %v, want the original %v", j.StartedAt, started)
	}
	shutdown(t, r)
	if st := getJob(t, s, "elsewhere").Status; st != domain.JobRunning {
		t.Errorf("another provider's job = %s, want untouched", st)
	}
	if len(seen) != 0 {
		t.Errorf("handler ran for another provider's job")
	}
}

func TestRunnerResumesWriteJobsOneAtATime(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	seedJob(t, s, domain.Job{ID: "a", Status: domain.JobRunning, CreatedAt: t0})
	seedJob(t, s, domain.Job{ID: "b", Status: domain.JobRunning, CreatedAt: t0.Add(time.Second)})
	seedJob(t, s, domain.Job{ID: "q", Status: domain.JobQueued, CreatedAt: t0.Add(-time.Hour)})
	entered := make(chan string, 3)
	release := make(chan struct{})
	r := newTestRunner(s, backfillHandler(func(_ context.Context, job *domain.Job, _ RunContext) error {
		entered <- job.ID
		<-release
		return nil
	}))
	r.Start(ctx)
	defer shutdown(t, r)
	var order []string
	for range 3 {
		order = append(order, recv(t, entered, "next job"))
		if len(entered) != 0 {
			t.Fatalf("two write jobs in flight: %v and more", order)
		}
		release <- struct{}{}
	}
	if !slices.Equal(order, []string{"a", "b", "q"}) {
		t.Errorf("order = %v, want resumed jobs before queued ones", order)
	}
	close(release)
}

func TestRunnerFinishesCancellingOnStart(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	seedJob(t, s, domain.Job{ID: "j", Status: domain.JobCancelling})
	seedJob(t, s, domain.Job{ID: "elsewhere", Provider: "q", Status: domain.JobCancelling})
	r := newTestRunner(s, backfillHandler(func(context.Context, *domain.Job, RunContext) error {
		t.Error("handler ran for a cancelling job")
		return nil
	}))
	r.Start(ctx)
	j := waitStatus(t, s, "j", domain.JobCanceled)
	if j.FinishedAt == nil {
		t.Error("FinishedAt not set")
	}
	shutdown(t, r)
	if st := getJob(t, s, "elsewhere").Status; st != domain.JobCancelling {
		t.Errorf("another provider's job = %s", st)
	}
}

func TestRunnerCancel(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	svc := newTestService(s, true)
	entered := make(chan struct{})
	r := newTestRunner(s, backfillHandler(func(_ context.Context, _ *domain.Job, rc RunContext) error {
		close(entered)
		<-rc.Stopping
		return ErrInterrupted
	}))
	svc.Notifier = r.Wake
	r.Start(ctx)
	defer shutdown(t, r)
	j, _, err := svc.Create(ctx, backfillReq())
	if err != nil {
		t.Fatal(err)
	}
	recv(t, entered, "handler")
	got, err := svc.Cancel(ctx, j.ID)
	if err != nil || got.Status != domain.JobCancelling {
		t.Fatalf("Cancel = %s, %v", got.Status, err)
	}
	fin := waitStatus(t, s, j.ID, domain.JobCanceled)
	if fin.FinishedAt == nil || fin.Error != "" {
		t.Errorf("canceled job = %+v", fin)
	}
}

func TestCheckpointKeepsCancel(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	svc := newTestService(s, true) // No Notifier: only the checkpoint can notice.
	entered, cancelled := make(chan struct{}), make(chan struct{})
	r := newTestRunner(s, backfillHandler(func(ctx context.Context, job *domain.Job, rc RunContext) error {
		close(entered)
		<-cancelled
		job.Progress.Processed = 7
		job.Checkpoint.PageToken = "p9"
		if err := rc.Checkpoint(ctx); err != nil {
			t.Errorf("Checkpoint: %v", err)
		}
		stored := getJob(t, s, job.ID)
		if stored.Status != domain.JobCancelling {
			t.Errorf("status after checkpoint = %s, a checkpoint must not undo a cancel", stored.Status)
		}
		if stored.Progress.Processed != 7 || stored.Checkpoint.PageToken != "p9" {
			t.Errorf("checkpoint not saved: %+v", stored)
		}
		select {
		case <-rc.Stopping:
		default:
			t.Error("Stopping not closed after a checkpoint saw the cancel")
		}
		return ErrInterrupted
	}))
	r.Start(ctx)
	defer shutdown(t, r)
	seedJob(t, s, domain.Job{ID: "j", Status: domain.JobQueued})
	r.Wake()
	recv(t, entered, "handler")
	if _, err := svc.Cancel(ctx, "j"); err != nil {
		t.Fatal(err)
	}
	close(cancelled)
	waitStatus(t, s, "j", domain.JobCanceled)
}

func TestCancelBeatsSuccess(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	svc := newTestService(s, true)
	entered, cancelled := make(chan struct{}), make(chan struct{})
	r := newTestRunner(s, backfillHandler(func(context.Context, *domain.Job, RunContext) error {
		close(entered)
		<-cancelled
		return nil
	}))
	r.Start(ctx)
	defer shutdown(t, r)
	seedJob(t, s, domain.Job{ID: "j", Status: domain.JobQueued})
	r.Wake()
	recv(t, entered, "handler")
	if _, err := svc.Cancel(ctx, "j"); err != nil {
		t.Fatal(err)
	}
	close(cancelled)
	waitStatus(t, s, "j", domain.JobCanceled)
}

func TestShutdownCheckpoints(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	started := t0.Add(-time.Minute)
	seedJob(t, s, domain.Job{ID: "j", Status: domain.JobRunning, StartedAt: &started})

	pages := map[string]struct {
		items []pipeline.WorkItem
		next  string
	}{
		"":   {[]pipeline.WorkItem{item("1"), item("2")}, "p2"},
		"p2": {[]pipeline.WorkItem{item("3"), item("4")}, ""},
	}
	list := func(_ context.Context, tok string) ([]pipeline.WorkItem, string, error) {
		return pages[tok].items, pages[tok].next, nil
	}

	var mu sync.Mutex
	finished := map[string]int{}
	inFlight := make(chan struct{})
	gate := make(chan struct{})
	stopping := make(chan (<-chan struct{}), 1)
	r1 := newTestRunner(s, backfillHandler(func(ctx context.Context, job *domain.Job, rc RunContext) error {
		stopping <- rc.Stopping
		return RunItems(ctx, job, rc, LoopConfig{Concurrency: 1}, list, func(_ context.Context, it pipeline.WorkItem) (pipeline.Outcome, error) {
			if it.Bookmark.Local == "3" {
				close(inFlight)
				<-gate
			}
			mu.Lock()
			finished[it.Bookmark.Local]++
			mu.Unlock()
			return pipeline.Outcome{Status: pipeline.Changed}, nil
		})
	}))
	r1.Start(ctx)
	stop := recv(t, stopping, "handler start")
	recv(t, inFlight, "item 3 to be in flight")

	done := make(chan error, 1)
	go func() {
		c, cancel := context.WithTimeout(ctx, testWait)
		defer cancel()
		done <- r1.Shutdown(c)
	}()
	recv(t, stop, "Stopping to close")
	select {
	case err := <-done:
		t.Fatalf("Shutdown returned %v before the in-flight item finished", err)
	default:
	}
	close(gate)
	if err := recv(t, done, "Shutdown"); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	if finished["3"] != 1 {
		t.Errorf("in-flight item finished %d times, want 1", finished["3"])
	}
	if finished["4"] != 0 {
		t.Errorf("item 4 started after Stopping closed")
	}
	mu.Unlock()
	j := getJob(t, s, "j")
	if j.Status != domain.JobRunning || j.FinishedAt != nil || j.Error != "" {
		t.Fatalf("after shutdown: %+v, want still running", j)
	}
	if j.Checkpoint.PageToken != "p2" || j.Progress.Processed != 2 || j.Progress.Changed != 2 {
		t.Fatalf("checkpoint = %+v progress = %+v, want page 2 and page 1's counters", j.Checkpoint, j.Progress)
	}

	var seen []string
	r2 := newTestRunner(s, backfillHandler(func(ctx context.Context, job *domain.Job, rc RunContext) error {
		return RunItems(ctx, job, rc, LoopConfig{Concurrency: 1}, list, func(_ context.Context, it pipeline.WorkItem) (pipeline.Outcome, error) {
			seen = append(seen, it.Bookmark.Local)
			return pipeline.Outcome{Status: pipeline.Changed}, nil
		})
	}))
	r2.Start(ctx)
	defer shutdown(t, r2)
	fin := waitStatus(t, s, "j", domain.JobSucceeded)
	if !slices.Equal(seen, []string{"3", "4"}) {
		t.Errorf("resume processed %v, want page 2 only", seen)
	}
	if fin.Progress.Processed != 4 || fin.Progress.Changed != 4 {
		t.Errorf("progress = %+v, want 4 without double counting", fin.Progress)
	}
	if !fin.StartedAt.Equal(started) {
		t.Errorf("StartedAt = %v, want the original", fin.StartedAt)
	}
}

func TestShutdownDeadlineStaysResumable(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	seedJob(t, s, domain.Job{ID: "j", Status: domain.JobQueued})
	entered := make(chan struct{})
	r := newTestRunner(s, backfillHandler(func(ctx context.Context, _ *domain.Job, _ RunContext) error {
		close(entered)
		<-ctx.Done() // Ignores Stopping, so only the deadline can end it.
		return fmt.Errorf("provider call: %w", ctx.Err())
	}))
	r.Start(ctx)
	recv(t, entered, "handler")
	expired, cancel := context.WithCancel(ctx)
	cancel()
	if err := r.Shutdown(expired); !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown = %v, want the context's error", err)
	}
	shutdown(t, r) // Now the handler has been cancelled, this waits for it.
	if j := getJob(t, s, "j"); j.Status != domain.JobRunning || j.Error != "" {
		t.Fatalf("job = %s %q, want running, resumable", j.Status, j.Error)
	}
}

// Cancelling Start's context ends handlers before the loop can react, and that
// must not read as a failure.
func TestStartContextCancelStaysResumable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := newTestStore(t)
	seedJob(t, s, domain.Job{ID: "j", Status: domain.JobQueued})
	r := newTestRunner(s, backfillHandler(func(ctx context.Context, _ *domain.Job, _ RunContext) error {
		cancel()
		return ctx.Err()
	}))
	r.Start(ctx)
	recv(t, ctx.Done(), "handler to run")
	shutdown(t, r)
	if j := getJob(t, s, "j"); j.Status != domain.JobRunning || j.Error != "" {
		t.Fatalf("job = %s %q, want running, resumable", j.Status, j.Error)
	}
}

func TestShutdownBeforeStart(t *testing.T) {
	s := newTestStore(t)
	seedJob(t, s, domain.Job{ID: "j", Status: domain.JobQueued})
	r := newTestRunner(s, nil)
	shutdown(t, r)
	r.Start(context.Background())
	shutdown(t, r)
	if st := getJob(t, s, "j").Status; st != domain.JobQueued {
		t.Errorf("status = %s, a stopped runner must claim nothing", st)
	}
}

func TestStatusFromProgress(t *testing.T) {
	tests := []struct {
		name  string
		p     domain.JobProgress
		err   error
		want  domain.JobStatus
		wantE string
	}{
		{name: "clean", p: domain.JobProgress{Processed: 3}, want: domain.JobSucceeded},
		{name: "item failures", p: domain.JobProgress{Processed: 3, Failed: 1}, want: domain.JobSucceededWithErrors},
		{name: "handler error", err: errors.New("boom"), want: domain.JobFailed, wantE: "boom"},
		{name: "unrequested interrupt", err: ErrInterrupted, want: domain.JobFailed, wantE: ErrInterrupted.Error()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestStore(t)
			seedJob(t, s, domain.Job{ID: "j", Status: domain.JobQueued})
			r := newTestRunner(s, backfillHandler(func(_ context.Context, job *domain.Job, _ RunContext) error {
				job.Progress = tt.p
				return tt.err
			}))
			r.Start(context.Background())
			defer shutdown(t, r)
			j := waitStatus(t, s, "j", tt.want)
			if j.Error != tt.wantE || j.FinishedAt == nil || j.Progress != tt.p {
				t.Errorf("job = %+v", j)
			}
		})
	}
}

func TestRunnerNoHandler(t *testing.T) {
	s := newTestStore(t)
	seedJob(t, s, domain.Job{ID: "j", Status: domain.JobQueued})
	r := newTestRunner(s, nil)
	r.Start(context.Background())
	defer shutdown(t, r)
	if j := waitStatus(t, s, "j", domain.JobFailed); j.Error == "" {
		t.Error("no error recorded")
	}
}

// ---- RunItems ----

func item(id string) pipeline.WorkItem {
	return pipeline.WorkItem{Bookmark: domain.ID{Provider: "p", Local: id}}
}

func items(from, n int) []pipeline.WorkItem {
	out := make([]pipeline.WorkItem, n)
	for i := range out {
		out[i] = item(strconv.Itoa(from + i))
	}
	return out
}

func onePage(its []pipeline.WorkItem) ListFunc {
	return func(context.Context, string) ([]pipeline.WorkItem, string, error) { return its, "", nil }
}

type cpLog struct {
	saved []domain.Job
	job   *domain.Job
}

func (c *cpLog) rc(job *domain.Job) RunContext {
	c.job = job
	return RunContext{Checkpoint: func(context.Context) error {
		c.saved = append(c.saved, *job)
		return nil
	}}
}

func TestRunItemsResume(t *testing.T) {
	ctx := context.Background()
	var tokens []string
	list := func(_ context.Context, tok string) ([]pipeline.WorkItem, string, error) {
		tokens = append(tokens, tok)
		switch tok {
		case "p2":
			return items(3, 2), "p3", nil
		case "p3":
			return items(5, 2), "", nil
		}
		t.Fatalf("listed page %q", tok)
		return nil, "", nil
	}
	job := &domain.Job{Checkpoint: domain.Checkpoint{PageToken: "p2"}, Progress: domain.JobProgress{Processed: 2, Changed: 2}}
	var cp cpLog
	var seen []string
	err := RunItems(ctx, job, cp.rc(job), LoopConfig{Concurrency: 1}, list, func(_ context.Context, it pipeline.WorkItem) (pipeline.Outcome, error) {
		seen = append(seen, it.Bookmark.Local)
		return pipeline.Outcome{Status: pipeline.Changed}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(tokens, []string{"p2", "p3"}) || !slices.Equal(seen, []string{"3", "4", "5", "6"}) {
		t.Errorf("tokens = %v, seen = %v", tokens, seen)
	}
	if job.Progress.Processed != 6 || job.Progress.Changed != 6 {
		t.Errorf("progress = %+v", job.Progress)
	}
	if len(cp.saved) == 0 || cp.saved[0].Checkpoint.PageToken != "p3" || cp.saved[0].Progress.Processed != 4 {
		t.Fatalf("first checkpoint = %+v, want next token with that page's counters", cp.saved)
	}
}

// The last page must not leave a saved empty token next to the full counters,
// which a crash before the final status write would restart from page one with.
func TestRunItemsLastPageLeavesResumableCheckpoint(t *testing.T) {
	ctx := context.Background()
	list := func(_ context.Context, tok string) ([]pipeline.WorkItem, string, error) {
		if tok == "" {
			return items(1, 1), "p2", nil
		}
		return items(2, 1), "", nil
	}
	job := &domain.Job{}
	var cp cpLog
	err := RunItems(ctx, job, cp.rc(job), LoopConfig{Concurrency: 1}, list, func(context.Context, pipeline.WorkItem) (pipeline.Outcome, error) {
		return pipeline.Outcome{Status: pipeline.Changed}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range cp.saved {
		if s.Checkpoint.PageToken == "" {
			t.Errorf("saved an empty page token with progress %+v", s.Progress)
		}
	}
	if job.Checkpoint.PageToken != "" || job.Progress.Processed != 2 {
		t.Errorf("in memory: %+v %+v", job.Checkpoint, job.Progress)
	}
}

func TestRunItemsCounters(t *testing.T) {
	statuses := map[string]pipeline.OutcomeStatus{
		"1": pipeline.Changed, "2": pipeline.Changed, "3": pipeline.Skipped, "4": pipeline.Unchanged,
		"5": pipeline.Deleted, "6": pipeline.NeedsReview,
	}
	job := &domain.Job{Progress: domain.JobProgress{Total: 7}}
	var cp cpLog
	err := RunItems(context.Background(), job, cp.rc(job), LoopConfig{Concurrency: 3}, onePage(items(1, 7)),
		func(_ context.Context, it pipeline.WorkItem) (pipeline.Outcome, error) {
			st, ok := statuses[it.Bookmark.Local]
			if !ok {
				return pipeline.Outcome{}, errors.New("boom")
			}
			return pipeline.Outcome{Status: st}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	want := domain.JobProgress{Total: 7, Processed: 7, Changed: 2, Skipped: 3, NeedsReview: 1, Failed: 1}
	if job.Progress != want {
		t.Errorf("progress = %+v, want %+v", job.Progress, want)
	}
}

func TestRunItemsConcurrencyBound(t *testing.T) {
	var cur, peak atomic.Int32
	reached := make(chan struct{})
	var once sync.Once
	job := &domain.Job{}
	var cp cpLog
	err := RunItems(context.Background(), job, cp.rc(job), LoopConfig{Concurrency: 3}, onePage(items(1, 20)),
		func(context.Context, pipeline.WorkItem) (pipeline.Outcome, error) {
			n := cur.Add(1)
			defer cur.Add(-1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			if n == 3 {
				once.Do(func() { close(reached) })
			}
			recv(t, reached, "3 items in flight")
			return pipeline.Outcome{Status: pipeline.Changed}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if peak.Load() != 3 {
		t.Errorf("peak = %d, want exactly 3", peak.Load())
	}
}

type pauseRec struct {
	n       int
	onPause func(n int)
	durs    []time.Duration
}

func (p *pauseRec) sleep(_ context.Context, d time.Duration) error {
	p.n++
	p.durs = append(p.durs, d)
	if p.onPause != nil {
		p.onPause(p.n)
	}
	return nil
}

func failAll(context.Context, pipeline.WorkItem) (pipeline.Outcome, error) {
	return pipeline.Outcome{}, errors.New("classifier down")
}

func TestRunItemsPauseRule(t *testing.T) {
	ctx := context.Background()

	t.Run("10 consecutive errors pauses", func(t *testing.T) {
		var p pauseRec
		job := &domain.Job{}
		var cp cpLog
		fails := 0
		err := RunItems(ctx, job, cp.rc(job), LoopConfig{Concurrency: 1, Sleep: p.sleep}, onePage(items(1, 15)),
			func(context.Context, pipeline.WorkItem) (pipeline.Outcome, error) {
				if fails < 10 {
					fails++
					if p.n != 0 {
						t.Error("paused before the tenth failure")
					}
					return pipeline.Outcome{}, errors.New("down")
				}
				if p.n != 1 {
					t.Errorf("item after 10 failures ran with %d pauses, want 1", p.n)
				}
				return pipeline.Outcome{Status: pipeline.Changed}, nil
			})
		if err != nil {
			t.Fatal(err)
		}
		if p.n != 1 || p.durs[0] != time.Minute {
			t.Errorf("pauses = %d %v, want 1 of a minute", p.n, p.durs)
		}
		if job.Checkpoint.Pauses != 1 || len(cp.saved) == 0 || cp.saved[0].Checkpoint.Pauses != 1 {
			t.Errorf("pause not checkpointed: %+v, saved %d", job.Checkpoint, len(cp.saved))
		}
		if job.Progress.Failed != 10 || job.Progress.Changed != 5 {
			t.Errorf("progress = %+v", job.Progress)
		}
	})

	t.Run("a success resets", func(t *testing.T) {
		var p pauseRec
		job := &domain.Job{}
		var cp cpLog
		i := 0
		err := RunItems(ctx, job, cp.rc(job), LoopConfig{Concurrency: 1, Sleep: p.sleep}, onePage(items(1, 100)),
			func(context.Context, pipeline.WorkItem) (pipeline.Outcome, error) {
				i++
				if i%10 == 0 {
					return pipeline.Outcome{Status: pipeline.NeedsReview}, nil
				}
				return pipeline.Outcome{}, errors.New("down")
			})
		if err != nil {
			t.Fatal(err)
		}
		if p.n != 0 {
			t.Errorf("paused %d times, want 0", p.n)
		}
	})

	t.Run("consecutive failures span pages", func(t *testing.T) {
		var p pauseRec
		job := &domain.Job{}
		var cp cpLog
		list := func(_ context.Context, tok string) ([]pipeline.WorkItem, string, error) {
			n, _ := strconv.Atoi(tok)
			if n >= 3 {
				return items(n, 4), "", nil
			}
			return items(n, 4), strconv.Itoa(n + 1), nil
		}
		if err := RunItems(ctx, job, cp.rc(job), LoopConfig{Concurrency: 1, Sleep: p.sleep}, list, failAll); err != nil {
			t.Fatal(err)
		}
		if p.n != 1 {
			t.Errorf("paused %d times over 16 failures in pages of 4, want 1", p.n)
		}
	})

	t.Run("3 pauses then failed, resumable", func(t *testing.T) {
		var p pauseRec
		job := &domain.Job{}
		var cp cpLog
		err := RunItems(ctx, job, cp.rc(job), LoopConfig{Concurrency: 2, Sleep: p.sleep}, onePage(items(1, 1000)), failAll)
		if !errors.Is(err, ErrTooManyFailures) {
			t.Fatalf("err = %v, want ErrTooManyFailures", err)
		}
		if p.n != 3 {
			t.Errorf("pauses = %d, want 3", p.n)
		}
		if job.Checkpoint.Pauses != 0 {
			t.Errorf("Pauses = %d, a re-queued job gets a fresh allowance", job.Checkpoint.Pauses)
		}
	})

	t.Run("pauses survive a resume", func(t *testing.T) {
		var p pauseRec
		job := &domain.Job{Checkpoint: domain.Checkpoint{Pauses: 3}}
		var cp cpLog
		err := RunItems(ctx, job, cp.rc(job), LoopConfig{Concurrency: 1, Sleep: p.sleep}, onePage(items(1, 100)), failAll)
		if !errors.Is(err, ErrTooManyFailures) || p.n != 0 {
			t.Fatalf("err = %v, pauses = %d", err, p.n)
		}
	})

	t.Run("stopping interrupts a pause", func(t *testing.T) {
		stop := make(chan struct{})
		job := &domain.Job{}
		var cp cpLog
		rc := cp.rc(job)
		rc.Stopping = stop
		processed := 0
		cfg := LoopConfig{Concurrency: 1, Sleep: func(ctx context.Context, _ time.Duration) error {
			close(stop)
			<-ctx.Done()
			return ctx.Err()
		}}
		err := RunItems(ctx, job, rc, cfg, onePage(items(1, 100)), func(context.Context, pipeline.WorkItem) (pipeline.Outcome, error) {
			processed++
			return pipeline.Outcome{}, errors.New("down")
		})
		if !errors.Is(err, ErrInterrupted) {
			t.Fatalf("err = %v, want ErrInterrupted", err)
		}
		if processed != 10 {
			t.Errorf("processed = %d, want 10", processed)
		}
		if job.Checkpoint.Pauses != 1 {
			t.Errorf("Pauses = %d", job.Checkpoint.Pauses)
		}
	})
}

func TestRunnerRequeueAfterTooManyFailures(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	seedJob(t, s, domain.Job{ID: "j", Status: domain.JobQueued})
	list := func(_ context.Context, tok string) ([]pipeline.WorkItem, string, error) {
		if tok == "" {
			return items(1, 1), "p2", nil
		}
		return items(2, 100), "", nil
	}
	var healthy atomic.Bool
	var firstOfPage2 atomic.Int32
	cfg := LoopConfig{Concurrency: 1, Sleep: func(context.Context, time.Duration) error { return nil }}
	r := newTestRunner(s, backfillHandler(func(ctx context.Context, job *domain.Job, rc RunContext) error {
		return RunItems(ctx, job, rc, cfg, list, func(_ context.Context, it pipeline.WorkItem) (pipeline.Outcome, error) {
			if it.Bookmark.Local == "2" {
				firstOfPage2.Add(1)
			}
			if it.Bookmark.Local == "1" || healthy.Load() {
				return pipeline.Outcome{Status: pipeline.Changed}, nil
			}
			return pipeline.Outcome{}, errors.New("classifier down")
		})
	}))
	r.Start(ctx)
	defer shutdown(t, r)
	r.Wake()
	failed := waitStatus(t, s, "j", domain.JobFailed)
	if failed.Error == "" || failed.FinishedAt == nil || failed.Checkpoint.PageToken != "p2" || failed.Progress.Processed != 1 {
		t.Fatalf("failed job = %+v", failed)
	}

	healthy.Store(true)
	failed.Status = domain.JobQueued
	if err := s.Jobs().UpdateJob(ctx, failed); err != nil {
		t.Fatal(err)
	}
	r.Wake()
	done := waitStatus(t, s, "j", domain.JobSucceeded)
	if done.Error != "" || done.Progress.Processed != 101 || done.Progress.Failed != 0 {
		t.Errorf("after re-queue: %+v", done)
	}
	if firstOfPage2.Load() != 2 {
		t.Errorf("item 2 ran %d times, want again on resume", firstOfPage2.Load())
	}
	if !done.StartedAt.Equal(t0.Add(time.Hour)) {
		t.Errorf("StartedAt = %v", done.StartedAt)
	}
}
