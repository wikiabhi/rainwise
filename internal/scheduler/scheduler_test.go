package scheduler

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

const testWait = 10 * time.Second

type fakeTicker struct {
	ticks   chan time.Time
	every   chan time.Duration
	stopped chan struct{}
}

func newFakeTicker() *fakeTicker {
	return &fakeTicker{ticks: make(chan time.Time), every: make(chan time.Duration, 1), stopped: make(chan struct{})}
}

func (f *fakeTicker) ticker(d time.Duration) (<-chan time.Time, func()) {
	f.every <- d
	return f.ticks, func() { close(f.stopped) }
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

func TestSchedulerTick(t *testing.T) {
	ft := newFakeTicker()
	logs := &bytes.Buffer{}
	created := make(chan struct{})
	calls := 0
	s := New(30*time.Minute, func(context.Context) error {
		calls++
		created <- struct{}{}
		if calls == 1 {
			return errors.New("boom")
		}
		return nil
	}, ft.ticker, slog.New(slog.NewTextHandler(logs, nil)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()

	if got := recv(t, ft.every, "ticker"); got != 30*time.Minute {
		t.Fatalf("every = %v", got)
	}
	for range 3 {
		ft.ticks <- time.Now()
		recv(t, created, "a sync on the tick")
	}
	// The fourth tick is taken only once the third create has returned, so the
	// loop has survived the error by now.
	cancel()
	recv(t, done, "Run to return")
	recv(t, ft.stopped, "ticker to stop")
	if calls != 3 {
		t.Fatalf("calls = %d, want one per tick", calls)
	}
	if !strings.Contains(logs.String(), "boom") {
		t.Fatalf("the error should be logged: %q", logs.String())
	}
}

func TestSchedulerNeverCreatesBeforeATick(t *testing.T) {
	ft := newFakeTicker()
	s := New(time.Minute, func(context.Context) error {
		t.Error("created a job with no tick")
		return nil
	}, ft.ticker, slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	recv(t, ft.every, "ticker")
	cancel()
	recv(t, done, "Run to return")
}
