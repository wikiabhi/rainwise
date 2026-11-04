package raindrop

import (
	"net/http"
	"strconv"
	"sync"
	"time"
)

// limiter is a token bucket that also honors server-requested pauses. Callers
// reserve a slot under the lock and sleep outside it, so concurrent calls
// queue in order without holding the lock while sleeping.
type limiter struct {
	mu         sync.Mutex
	now        func() time.Time
	rate       float64 // tokens per second
	capacity   float64
	tokens     float64
	last       time.Time
	pauseUntil time.Time
}

func newLimiter(perMinute int, now func() time.Time) *limiter {
	return &limiter{now: now, rate: float64(perMinute) / 60, capacity: float64(perMinute), tokens: float64(perMinute)}
}

// reserve takes a token and returns how long the caller must wait before use.
func (l *limiter) reserve() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if !l.last.IsZero() {
		l.tokens = min(l.capacity, l.tokens+now.Sub(l.last).Seconds()*l.rate)
	}
	l.last = now
	var wait time.Duration
	if l.pauseUntil.After(now) {
		wait = l.pauseUntil.Sub(now)
	}
	l.tokens--
	if l.tokens < 0 {
		wait = max(wait, time.Duration(-l.tokens/l.rate*float64(time.Second)))
	}
	return wait
}

// pause delays every later reservation until t.
func (l *limiter) pause(t time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if t.After(l.pauseUntil) {
		l.pauseUntil = t
	}
}

// resetTime reads X-RateLimit-Reset, a UTC epoch in seconds.
func resetTime(h http.Header) (time.Time, bool) {
	n, err := strconv.ParseInt(h.Get("X-RateLimit-Reset"), 10, 64)
	if err != nil || n <= 0 {
		return time.Time{}, false
	}
	return time.Unix(n, 0), true
}

// exhausted reports whether either spelling of the remaining header is zero.
func exhausted(h http.Header) bool {
	for _, k := range []string{"RateLimit-Remaining", "X-RateLimit-Remaining"} {
		if v := h.Get(k); v != "" {
			n, err := strconv.Atoi(v)
			return err == nil && n <= 0
		}
	}
	return false
}
