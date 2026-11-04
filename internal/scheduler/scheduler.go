// Package scheduler is the built-in timer that asks for syncs (§12). It knows
// nothing of jobs: whoever builds it says what a tick does.
package scheduler

import (
	"context"
	"log/slog"
	"time"
)

// TickerFunc starts a ticker, returning its channel and how to stop it.
type TickerFunc func(time.Duration) (<-chan time.Time, func())

func realTicker(d time.Duration) (<-chan time.Time, func()) {
	t := time.NewTicker(d)
	return t.C, t.Stop
}

type Scheduler struct {
	every  time.Duration
	create func(context.Context) error
	ticker TickerFunc
	log    *slog.Logger
}

// New returns a scheduler that calls create on every tick. A nil ticker means a
// real one, and a nil log the default.
func New(every time.Duration, create func(context.Context) error, ticker TickerFunc, log *slog.Logger) *Scheduler {
	if ticker == nil {
		ticker = realTicker
	}
	if log == nil {
		log = slog.Default()
	}
	return &Scheduler{every: every, create: create, ticker: ticker, log: log}
}

// Run ticks until ctx ends. A failed create is logged, and the next tick tries again.
func (s *Scheduler) Run(ctx context.Context) {
	ticks, stop := s.ticker(s.every)
	defer stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticks:
			if err := s.create(ctx); err != nil && ctx.Err() == nil {
				s.log.Error("scheduled sync", "error", err)
			}
		}
	}
}
