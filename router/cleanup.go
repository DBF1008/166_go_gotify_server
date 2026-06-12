package router

import (
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// messageCleanupDatabase is the subset of database access required to purge
// expired messages.
type messageCleanupDatabase interface {
	CleanupExpiredMessages(now time.Time) (int64, error)
}

// messageCleaner periodically removes expired messages from the database. It
// runs one cleanup immediately on start so that messages which expired while the
// server was down are purged on the next boot (restart recovery), and then
// repeats on a fixed interval until closed.
type messageCleaner struct {
	db       messageCleanupDatabase
	interval time.Duration
	now      func() time.Time
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

func newMessageCleaner(db messageCleanupDatabase, interval time.Duration, now func() time.Time) *messageCleaner {
	return &messageCleaner{
		db:       db,
		interval: interval,
		now:      now,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// start launches the cleanup loop in a background goroutine. It must be called
// exactly once and before close.
func (c *messageCleaner) start() {
	go c.run()
}

func (c *messageCleaner) run() {
	defer close(c.done)
	// Initial sweep: catch up on everything that expired while we were not running.
	c.clean()

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.clean()
		case <-c.stop:
			return
		}
	}
}

func (c *messageCleaner) clean() {
	deleted, err := c.db.CleanupExpiredMessages(c.now())
	if err != nil {
		log.Error().Err(err).Msg("Error cleaning up expired messages")
		return
	}
	if deleted > 0 {
		log.Info().Int64("count", deleted).Msg("Cleaned up expired messages")
	}
}

// close stops the cleanup loop and waits for the background goroutine to finish.
// It is safe to call multiple times.
func (c *messageCleaner) close() {
	c.stopOnce.Do(func() {
		close(c.stop)
	})
	<-c.done
}
