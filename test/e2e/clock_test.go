//go:build e2e

package e2e

import (
	"sync"
	"testing"
	"time"
)

// testClock moves the service clock forward, for the tests whose AWS rule
// spans more time than a test can wait: retention (at least 60 seconds),
// the FIFO deduplication interval (5 minutes), and the 60-second rules of
// PurgeQueue and DeleteQueue. The design drives retention, delay,
// visibility, receive counts and dead-letter moves "all on the service
// clock".
type testClock interface {
	// Advance moves the service clock forward by d.
	Advance(t *testing.T, d time.Duration)
}

// offsetClock is the real time plus an offset the test grows, so the
// service's clock keeps moving between advances as a real one does.
type offsetClock struct {
	offset time.Duration
	mu     sync.Mutex
}

func (c *offsetClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Now().Add(c.offset)
}

func (c *offsetClock) Advance(t *testing.T, d time.Duration) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.offset += d
}

// startServerWithClock starts the app on a fresh store, as startServer does,
// with a clock the test controls, set through (*app.Config).SetClock between
// LoadConfigFrom and app.New. SigV4 keeps the real clock. A long poll that
// is already waiting does not see the clock move, so a test advances the
// clock first and receives after.
func startServerWithClock(t *testing.T) (*server, testClock) {
	t.Helper()
	clock := &offsetClock{}
	return startServerWith(t, nil, clock.Now), clock
}
