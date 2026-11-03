// Package app is the composition root: it turns a config into a running service.
// It lives apart from cmd/rainwise because a main package can't be imported, and
// the end-to-end tests need to build what main builds.
package app

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/wikiabhi/rainwise/internal/adapters/contentfetch"
	"github.com/wikiabhi/rainwise/internal/adapters/store/sqlite"
	"github.com/wikiabhi/rainwise/internal/config"
	"github.com/wikiabhi/rainwise/internal/core/events"
	"github.com/wikiabhi/rainwise/internal/core/jobs"
	"github.com/wikiabhi/rainwise/internal/core/pipeline"
	"github.com/wikiabhi/rainwise/internal/core/taxonomy"
	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/httpapi"
	"github.com/wikiabhi/rainwise/internal/ports"
	"github.com/wikiabhi/rainwise/internal/scheduler"
)

const (
	readHeaderTimeout = 10 * time.Second
	// stopWait is how long Shutdown waits for work it has cancelled to return.
	stopWait = 5 * time.Second
)

// BuildDeps are optional test overrides. Provider stands in for processing.provider
// and Classifier for roles.classifier, whatever type their config entries name.
// Ticker replaces the scheduler's clock, and PollEvery how often the job runner and
// event worker look for work.
type BuildDeps struct {
	Provider   ports.Provider
	Classifier ports.Classifier
	Ticker     scheduler.TickerFunc
	PollEvery  time.Duration
}

// App is the wired service.
type App struct {
	// Handler is the REST API. It works without Serve, and until Shutdown.
	Handler http.Handler

	store     *sqlite.Store
	server    *http.Server
	runner    *jobs.Runner
	worker    *events.Worker
	scheduler *scheduler.Scheduler
	log       *slog.Logger

	stopScheduler, stopWorker context.CancelFunc
	schedulerDone, workerDone chan struct{}

	shutdownOnce sync.Once
	shutdownErr  error
}

// Check applies every startup rule that doesn't need the database, and touches
// nothing. Build also applies them to the taxonomies stored in the database.
func Check(cfg *config.Config, deps BuildDeps) error {
	_, err := prepare(cfg, deps)
	return err
}

func prepare(cfg *config.Config, deps BuildDeps) (*adapters, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	a, err := buildAdapters(cfg, deps)
	if err != nil {
		return nil, err
	}
	configured := make([]domain.Taxonomy, 0, len(cfg.Taxonomies))
	for _, t := range cfg.Taxonomies {
		configured = append(configured, t)
	}
	slices.SortFunc(configured, func(x, y domain.Taxonomy) int { return cmp.Compare(x.ID, y.ID) })
	if err := checkTaxonomies(cfg, a, configured); err != nil {
		return nil, err
	}
	a.configured = configured
	return a, nil
}

// Build checks cfg, opens the database, seeds taxonomies, and wires everything. It
// starts nothing; a config it rejects leaves no database behind.
func Build(cfg *config.Config, deps BuildDeps) (*App, error) {
	ad, err := prepare(cfg, deps)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	store, err := sqlite.Open(ctx, cfg.State.SQLitePath)
	if err != nil {
		return nil, fmt.Errorf("state.sqlite_path: %w", err)
	}
	a, err := wire(ctx, cfg, deps, ad, store)
	if err != nil {
		return nil, errors.Join(err, store.Close())
	}
	return a, nil
}

func wire(ctx context.Context, cfg *config.Config, deps BuildDeps, ad *adapters, store *sqlite.Store) (*App, error) {
	log := slog.Default()
	taxonomies := taxonomy.New(store.Taxonomies())
	if err := taxonomies.Seed(ctx, ad.configured, log); err != nil {
		return nil, err
	}
	// §8.1: the database wins over config, so it's the stored ones that must pass.
	stored, err := taxonomies.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("read taxonomies: %w", err)
	}
	if err := checkTaxonomies(cfg, ad, stored); err != nil {
		return nil, err
	}

	var fetcher ports.ContentFetcher
	if cfg.Processing.ContentFetch != contentFetchOff {
		fetcher = contentfetch.New(contentfetch.Options{})
	}
	pipe := pipeline.New(pipeline.Deps{
		Provider:     ad.provider,
		Classifier:   ad.classifier,
		Fallback:     ad.fallback,
		Fetcher:      fetcher,
		Store:        store,
		Log:          log,
		ContentFetch: cfg.Processing.ContentFetch,
		MinChars:     cfg.Processing.ContentFetchMinChars,
	})
	snapshots := jobs.NewSnapshotService(jobs.SnapshotDeps{
		Provider:  ad.provider,
		Store:     store,
		Retention: cfg.State.SnapshotRetention,
	})
	runner := jobs.NewRunner(jobs.RunnerDeps{
		Store: store,
		Handlers: jobs.Handlers(jobs.Deps{
			Provider:     ad.provider,
			Pipeline:     pipe,
			Taxonomies:   taxonomies,
			Proposer:     ad.proposer,
			Store:        store,
			Concurrency:  cfg.Processing.Concurrency,
			TagVocabSize: cfg.Processing.TagVocabSize,
			DurableWait:  cfg.State.DurableWait,
			Snapshots:    snapshots,

			AutomationOff: !cfg.Automation.Enabled,
		}),
		Provider:  ad.name,
		PollEvery: deps.PollEvery,
		Log:       log,
	})
	svc := jobs.NewService(jobs.ServiceDeps{
		Store:             store,
		AutomationEnabled: cfg.Automation.Enabled,
		ProviderNames:     []string{ad.name},
	})
	svc.Notifier = runner.Wake

	handler := httpapi.New(httpapi.Deps{
		APIToken:        cfg.Server.APIToken.Value(),
		WebhookSecrets:  ad.webhookSecrets(cfg),
		Normalizers:     map[string]ports.EventNormalizer{ad.name: ad.normalizer()},
		Jobs:            svc,
		Taxonomies:      taxonomies,
		Snapshots:       snapshots,
		Ingestor:        events.NewIngestor(store.Events(), cfg.Automation.Enabled, nil),
		Store:           store,
		Providers:       []httpapi.ProviderInfo{{Name: ad.name, Capabilities: ad.provider.Capabilities()}},
		Classifiers:     ad.classifierInfo(cfg),
		DefaultProvider: ad.name,
		DefaultTaxonomy: cfg.Processing.Taxonomy,
		Log:             log,
	})
	return &App{
		Handler: handler,
		store:   store,
		server:  &http.Server{Addr: cfg.Server.Addr, Handler: handler, ReadHeaderTimeout: readHeaderTimeout},
		runner:  runner,
		worker: events.NewWorker(events.WorkerDeps{
			Store:        store,
			Pipeline:     pipe,
			Provider:     ad.provider,
			Taxonomies:   taxonomies,
			TaxonomyID:   cfg.Processing.Taxonomy,
			TagVocabSize: cfg.Processing.TagVocabSize,
			OnMoveRetag:  cfg.Processing.OnMoveRetag,
			PollEvery:    deps.PollEvery,
			Log:          log,

			AutomationOff: !cfg.Automation.Enabled,
		}),
		scheduler: newScheduler(cfg, svc, ad.name, deps.Ticker, log),
		log:       log,
	}, nil
}

// newScheduler returns nil when no timer should run. Automation is the one switch
// for writes nobody requested (§9.0), so the timer never runs without it.
func newScheduler(cfg *config.Config, svc *jobs.Service, provider string, ticker scheduler.TickerFunc, log *slog.Logger) *scheduler.Scheduler {
	if !cfg.Scheduler.Enabled {
		return nil
	}
	if !cfg.Automation.Enabled {
		log.Warn("scheduler.enabled is on but automation.enabled is off, so no syncs will be scheduled")
		return nil
	}
	return scheduler.New(cfg.Scheduler.SyncEvery, func(ctx context.Context) error {
		_, _, err := svc.Create(ctx, jobs.CreateRequest{Type: domain.JobSync, Provider: provider, Taxonomy: cfg.Processing.Taxonomy})
		return err
	}, ticker, log)
}

// Start resumes interrupted jobs and begins background work. Cancelling ctx stops
// jobs at once, so give it a context that outlives any stop signal, and stop
// gracefully with Shutdown.
func (a *App) Start(ctx context.Context) {
	a.runner.Start(ctx)

	var workerCtx context.Context
	workerCtx, a.stopWorker = context.WithCancel(ctx)
	a.workerDone = make(chan struct{})
	go func() {
		defer close(a.workerDone)
		a.worker.Run(workerCtx)
	}()

	if a.scheduler != nil {
		var schedulerCtx context.Context
		schedulerCtx, a.stopScheduler = context.WithCancel(ctx)
		a.schedulerDone = make(chan struct{})
		go func() {
			defer close(a.schedulerDone)
			a.scheduler.Run(schedulerCtx)
		}()
	}
}

// Serve listens on server.addr until Shutdown, when it returns http.ErrServerClosed.
func (a *App) Serve() error { return a.server.ListenAndServe() }

// Shutdown stops the service in dependency order. The scheduler goes first, then
// jobs finish their in-flight items and save a checkpoint (§10.5) while the API can
// still report on them. Then the API and event worker stop, and the database closes
// last. Only the first call does anything; later ones return its result.
func (a *App) Shutdown(ctx context.Context) error {
	a.shutdownOnce.Do(func() { a.shutdownErr = a.shutdown(ctx) })
	return a.shutdownErr
}

func (a *App) shutdown(ctx context.Context) error {
	var errs []error

	// Work that has been cancelled returns soon, but the database mustn't close
	// under it, so this wait doesn't depend on ctx having any time left.
	settle, cancel := context.WithTimeout(context.WithoutCancel(ctx), stopWait)
	defer cancel()
	wait := func(what string, done <-chan struct{}) {
		if done == nil {
			return
		}
		select {
		case <-done:
		case <-settle.Done():
			errs = append(errs, fmt.Errorf("%s did not stop in time", what))
		}
	}

	if a.stopScheduler != nil {
		a.stopScheduler()
		wait("scheduler", a.schedulerDone)
	}
	if err := a.runner.Shutdown(ctx); err != nil {
		a.log.Warn("jobs did not stop in time; cancelling them", "error", err)
		errs = append(errs, fmt.Errorf("job runner: %w", err))
		if err := a.runner.Shutdown(settle); err != nil {
			errs = append(errs, fmt.Errorf("job runner: %w", err))
		}
	}
	if err := a.server.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("http server: %w", err), a.server.Close())
	}
	if a.stopWorker != nil {
		a.stopWorker()
		wait("event worker", a.workerDone)
	}
	if err := a.store.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close database: %w", err))
	}
	return errors.Join(errs...)
}
