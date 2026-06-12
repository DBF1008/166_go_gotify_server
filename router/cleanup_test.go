package router

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// fakeCleanupDB records calls to CleanupExpiredMessages and signals each call so
// tests can wait for cleanups deterministically (no reliance on timing).
type fakeCleanupDB struct {
	mu      sync.Mutex
	calls   int
	lastNow time.Time
	signal  chan time.Time
}

func newFakeCleanupDB() *fakeCleanupDB {
	return &fakeCleanupDB{signal: make(chan time.Time, 1024)}
}

func (f *fakeCleanupDB) CleanupExpiredMessages(now time.Time) (int64, error) {
	f.mu.Lock()
	f.calls++
	f.lastNow = now
	f.mu.Unlock()
	select {
	case f.signal <- now:
	default:
	}
	return 0, nil
}

func (f *fakeCleanupDB) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// TestMessageCleaner_RunsOnStart verifies the cleaner performs an immediate
// cleanup when started — this is the restart-recovery behavior that purges
// messages which expired while the server was down.
func TestMessageCleaner_RunsOnStart(t *testing.T) {
	fake := newFakeCleanupDB()
	fixed := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	c := newMessageCleaner(fake, time.Hour, func() time.Time { return fixed })
	c.start()
	defer c.close()

	select {
	case got := <-fake.signal:
		assert.Equal(t, fixed, got, "initial cleanup should use the injected clock")
	case <-time.After(2 * time.Second):
		t.Fatal("cleaner did not run an initial cleanup on start")
	}
}

// TestMessageCleaner_PeriodicThenStop verifies the cleaner runs repeatedly on
// its interval and stops cleanly on close.
func TestMessageCleaner_PeriodicThenStop(t *testing.T) {
	fake := newFakeCleanupDB()
	c := newMessageCleaner(fake, 5*time.Millisecond, time.Now)
	c.start()

	// Expect the initial sweep plus several periodic sweeps.
	for i := 0; i < 3; i++ {
		select {
		case <-fake.signal:
		case <-time.After(2 * time.Second):
			t.Fatalf("expected cleanup call %d to fire", i)
		}
	}

	c.close() // blocks until the background goroutine has stopped
	countAfterClose := fake.callCount()

	// No further cleanups must happen after close returns.
	time.Sleep(30 * time.Millisecond)
	assert.Equal(t, countAfterClose, fake.callCount(), "no cleanups should run after close")
}

// TestMessageCleaner_CloseIdempotent verifies close can be called multiple times.
func TestMessageCleaner_CloseIdempotent(t *testing.T) {
	fake := newFakeCleanupDB()
	c := newMessageCleaner(fake, time.Hour, time.Now)
	c.start()
	c.close()
	assert.NotPanics(t, func() { c.close() })
}
