package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

// ErrInterrupted is what a handler returns when RunContext.Stopping closes. The
// job stays resumable, unless it was the job's cancel that closed it.
var ErrInterrupted = errors.New("job interrupted")

const (
	defaultPollEvery = 5 * time.Second
	recoverPageSize  = 100
)

// Handler does the work of one job type. It may change job.Progress,
// job.Checkpoint, and job.SnapshotID, and must not hold on to job after returning.
type Handler interface {
	Run(ctx context.Context, job *domain.Job, rc RunContext) error
}

type RunContext struct {
	// Checkpoint saves job.Progress and job.Checkpoint. It works on a cancelled
	// context, so a job being stopped can still save.
	Checkpoint func(context.Context) error
	// Stopping closes when the handler should finish in-flight work and return
	// ErrInterrupted, on shutdown or when the job is cancelled.
	Stopping <-chan struct{}
}

type RunnerDeps struct {
	Store     ports.Store
	Handlers  map[domain.JobType]Handler
	Provider  string
	Now       func() time.Time
	PollEvery time.Duration
	Log       *slog.Logger
}

// Runner runs one provider's jobs: at most one real job at a time, with dry runs
// alongside (§7.3). A job's status is written only through guarded updates, so
// that the runner's saves and a concurrent Service.Cancel can't undo each other.
type Runner struct {
	d    RunnerDeps
	wake chan struct{}

	// jobCtx is what handlers run under. Cancelling it is the hard stop.
	jobCtx    context.Context
	hardStop  context.CancelFunc
	loopDone  chan struct{}
	recovered bool          // Touched only by the loop.
	resumes   []domain.Job  // Real jobs awaiting the write slot. Touched only by the loop.
	stop      chan struct{} // Closes when the loop should exit.
	wg        sync.WaitGroup

	mu       sync.Mutex
	started  bool
	stopping bool
	busy     bool // A real job holds the write slot.
	active   map[string]*activeJob
}

type activeJob struct {
	id       string
	job      *domain.Job // Owned by the job's goroutine.
	stopping chan struct{}
	stopOnce sync.Once
}

func (a *activeJob) requestStop() { a.stopOnce.Do(func() { close(a.stopping) }) }

func NewRunner(d RunnerDeps) *Runner {
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.PollEvery <= 0 {
		d.PollEvery = defaultPollEvery
	}
	if d.Log == nil {
		d.Log = slog.Default()
	}
	return &Runner{
		d:        d,
		wake:     make(chan struct{}, 1),
		loopDone: make(chan struct{}),
		stop:     make(chan struct{}),
		active:   map[string]*activeJob{},
	}
}

// Start resumes interrupted jobs and begins claiming queued ones. Cancelling ctx
// interrupts handlers at once, so for a graceful stop use Shutdown and leave ctx
// alive until it returns.
func (r *Runner) Start(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started || r.stopping {
		return
	}
	r.started = true
	r.jobCtx, r.hardStop = context.WithCancel(ctx)
	go r.loop()
}

// Wake makes the runner look for work now. It never blocks.
func (r *Runner) Wake() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Shutdown stops claiming jobs and closes Stopping, then waits for handlers. Jobs
// it interrupts stay running in the store, for the next Start to resume. If ctx
// ends first, handlers are cancelled and it returns without waiting for them; their
// last checkpoint still stands. Calling it again waits for them to return.
func (r *Runner) Shutdown(ctx context.Context) error {
	r.beginStop()
	r.mu.Lock()
	started := r.started
	r.mu.Unlock()
	if !started {
		return nil
	}
	done := make(chan struct{})
	go func() {
		<-r.loopDone
		r.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		r.hardStop()
		return nil
	case <-ctx.Done():
		r.hardStop()
		return ctx.Err()
	}
}

func (r *Runner) beginStop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopping {
		return
	}
	r.stopping = true
	close(r.stop)
	for _, a := range r.active {
		a.requestStop()
	}
}

func (r *Runner) isStopping() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stopping
}

func (r *Runner) loop() {
	defer close(r.loopDone)
	t := time.NewTicker(r.d.PollEvery)
	defer t.Stop()
	for {
		r.tick()
		select {
		case <-r.stop:
			return
		case <-r.jobCtx.Done():
			r.beginStop()
			return
		case <-r.wake:
		case <-t.C:
		}
	}
}

func (r *Runner) tick() {
	ctx := r.jobCtx
	if !r.recovered {
		if err := r.recoverJobs(ctx); err != nil {
			// Claiming now could overlap a job awaiting resume, so try again later.
			r.d.Log.Error("recovering jobs", "error", err)
			return
		}
		r.recovered = true
	}
	r.noticeCancels(ctx)
	for r.takeNextWrite(ctx) {
	}
	for {
		j, ok, err := r.d.Store.Jobs().ClaimNextQueued(ctx, r.d.Provider, true)
		if err != nil {
			r.d.Log.Error("claiming dry-run job", "error", err)
			return
		}
		if !ok {
			return
		}
		r.launch(j)
	}
}

// recoverJobs picks up what a previous process left behind. A cancelling job has
// nothing to resume, so it finishes as canceled.
func (r *Runner) recoverJobs(ctx context.Context) error {
	js := r.d.Store.Jobs()
	running, err := js.ListRunning(ctx)
	if err != nil {
		return err
	}
	cursor := ""
	for {
		page, err := js.ListJobs(ctx, cursor, recoverPageSize)
		if err != nil {
			return err
		}
		for _, j := range page.Items {
			if j.Provider != r.d.Provider || j.Status != domain.JobCancelling {
				continue
			}
			r.markFinished(&j, domain.JobCanceled)
			if _, err := js.UpdateJobIf(ctx, j, domain.JobCancelling); err != nil {
				return err
			}
		}
		if page.NextToken == "" {
			break
		}
		cursor = page.NextToken
	}
	for _, j := range running {
		switch {
		case j.Provider != r.d.Provider:
		case j.DryRun:
			r.launch(j)
		default:
			r.resumes = append(r.resumes, j)
		}
	}
	return nil
}

func (r *Runner) noticeCancels(ctx context.Context) {
	r.mu.Lock()
	active := make([]*activeJob, 0, len(r.active))
	for _, a := range r.active {
		active = append(active, a)
	}
	r.mu.Unlock()
	for _, a := range active {
		j, err := r.d.Store.Jobs().GetJob(ctx, a.id)
		if err != nil {
			r.d.Log.Error("checking job for cancel", "job", a.id, "error", err)
			continue
		}
		if j.Status == domain.JobCancelling {
			a.requestStop()
		}
	}
}

// takeNextWrite starts the next real job if the write slot is free, and reports
// whether it did. Resumed jobs go first, being the oldest work.
func (r *Runner) takeNextWrite(ctx context.Context) bool {
	r.mu.Lock()
	busy := r.busy
	r.mu.Unlock()
	if busy {
		return false
	}
	if len(r.resumes) > 0 {
		j := r.resumes[0]
		r.resumes = r.resumes[1:]
		return r.launch(j)
	}
	j, ok, err := r.d.Store.Jobs().ClaimNextQueued(ctx, r.d.Provider, false)
	if err != nil {
		r.d.Log.Error("claiming job", "error", err)
		return false
	}
	return ok && r.launch(j)
}

// launch runs a job the store already has as running. While stopping it does
// nothing, which leaves the job there for the next Start to resume.
func (r *Runner) launch(j domain.Job) bool {
	a := &activeJob{id: j.ID, job: &j, stopping: make(chan struct{})}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopping {
		return false
	}
	r.active[j.ID] = a
	if !j.DryRun {
		r.busy = true
	}
	r.wg.Go(func() {
		r.run(a)
		r.mu.Lock()
		delete(r.active, j.ID)
		if !j.DryRun {
			r.busy = false
		}
		r.mu.Unlock()
		r.Wake()
	})
	return true
}

func (r *Runner) run(a *activeJob) {
	j := a.job
	if j.StartedAt == nil {
		now := r.d.Now().UTC()
		j.StartedAt = &now
	}
	j.FinishedAt, j.Error = nil, "" // A re-queued failed job starts clean.
	if err := r.save(r.jobCtx, a); err != nil {
		// The job stays running in the store, and resumes on the next start.
		r.d.Log.Error("starting job", "job", j.ID, "error", err)
		return
	}
	select {
	case <-a.stopping: // Cancelled before it began.
		r.finish(a, ErrInterrupted)
		return
	default:
	}
	h, ok := r.d.Handlers[j.Type]
	if !ok {
		r.finish(a, fmt.Errorf("no handler for job type %q: %w", j.Type, domain.ErrValidation))
		return
	}
	r.finish(a, h.Run(r.jobCtx, j, RunContext{
		Checkpoint: func(ctx context.Context) error { return r.save(ctx, a) },
		Stopping:   a.stopping,
	}))
}

// save writes the job without touching its status: running stays running, and
// cancelling stays cancelling, which also tells this job to stop. A status only
// moves from running to cancelling, so if the first guard misses, the second holds.
func (r *Runner) save(ctx context.Context, a *activeJob) error {
	ctx = context.WithoutCancel(ctx)
	js := r.d.Store.Jobs()
	j := a.job
	j.Status = domain.JobRunning
	if ok, err := js.UpdateJobIf(ctx, *j, domain.JobRunning); err != nil || ok {
		return err
	}
	j.Status = domain.JobCancelling
	ok, err := js.UpdateJobIf(ctx, *j, domain.JobCancelling)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("job %s is no longer running: %w", j.ID, domain.ErrConflict)
	}
	a.requestStop()
	return nil
}

// finish records how the handler ended. If the job was cancelled meanwhile, that
// wins over any other result. If the write fails, the job stays running in the
// store and resumes on the next start; its writes are idempotent.
func (r *Runner) finish(a *activeJob, err error) {
	ctx := context.WithoutCancel(r.jobCtx)
	js := r.d.Store.Jobs()
	j := a.job

	interrupted := errors.Is(err, ErrInterrupted) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
	switch {
	case err == nil && j.Progress.Failed > 0:
		r.markFinished(j, domain.JobSucceededWithErrors)
	case err == nil:
		r.markFinished(j, domain.JobSucceeded)
	case interrupted && (r.isStopping() || r.jobCtx.Err() != nil):
		j.Status = domain.JobRunning
	default:
		r.markFinished(j, domain.JobFailed)
		j.Error = err.Error()
	}

	ok, uerr := js.UpdateJobIf(ctx, *j, domain.JobRunning)
	if ok && j.Status == domain.JobFailed {
		r.d.Log.Error("job failed", "job", j.ID, "type", j.Type, "error", err)
	}
	if uerr == nil && !ok {
		r.markFinished(j, domain.JobCanceled)
		j.Error = ""
		ok, uerr = js.UpdateJobIf(ctx, *j, domain.JobCancelling)
		if uerr == nil && !ok {
			uerr = fmt.Errorf("job %s is no longer running: %w", j.ID, domain.ErrConflict)
		}
	}
	if uerr != nil {
		r.d.Log.Error("saving finished job", "job", j.ID, "status", j.Status, "error", uerr)
	}
}

func (r *Runner) markFinished(j *domain.Job, status domain.JobStatus) {
	now := r.d.Now().UTC()
	j.Status, j.FinishedAt = status, &now
}
