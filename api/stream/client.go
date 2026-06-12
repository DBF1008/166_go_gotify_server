package stream

import (
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/gotify/server/v2/model"
	"github.com/rs/zerolog/log"
)

const (
	writeWait = 2 * time.Second
)

var ping = func(conn *websocket.Conn) error {
	return conn.WriteMessage(websocket.PingMessage, nil)
}

var writeJSON = func(conn *websocket.Conn, v interface{}) error {
	return conn.WriteJSON(v)
}

type client struct {
	conn    *websocket.Conn
	onClose func(*client)
	write   chan *model.MessageExternal
	userID  uint
	token   string
	once    once

	// writeMu serializes WebSocket writes between the replay loop (Handle)
	// and the write handler goroutine.
	writeMu sync.Mutex

	// replaying is closed when the replay phase finishes (or immediately if
	// no replay is needed). While open, the write handler deduplicates
	// messages from the channel against the replay watermark.
	replaying chan struct{}

	// lastSentID tracks the highest message ID already sent during replay.
	lastSentID uint
	lastSentMu sync.Mutex
}

func newClient(conn *websocket.Conn, userID uint, token string, onClose func(*client)) *client {
	return &client{
		conn:      conn,
		write:     make(chan *model.MessageExternal, 16),
		replaying: make(chan struct{}),
		userID:    userID,
		token:     token,
		onClose:   onClose,
	}
}

// shouldSend returns true if the message has not been sent yet (ID > lastSentID)
// and atomically updates the watermark. Thread-safe.
func (c *client) shouldSend(msg *model.MessageExternal) bool {
	c.lastSentMu.Lock()
	defer c.lastSentMu.Unlock()
	if msg.ID <= c.lastSentID {
		return false
	}
	c.lastSentID = msg.ID
	return true
}

// setLastSentID sets the deduplication watermark after replay completes.
func (c *client) setLastSentID(id uint) {
	c.lastSentMu.Lock()
	c.lastSentID = id
	c.lastSentMu.Unlock()
}

// finishReplay signals that the replay phase is complete. After this the write
// handler forwards all messages without dedup. Safe to call multiple times.
func (c *client) finishReplay() {
	select {
	case <-c.replaying:
	default:
		close(c.replaying)
	}
}

// Close closes the connection.
func (c *client) Close() {
	c.once.Do(func() {
		c.conn.Close()
		close(c.write)
	})
}

// NotifyClose closes the connection and notifies that the connection was closed.
func (c *client) NotifyClose() {
	c.once.Do(func() {
		c.conn.Close()
		close(c.write)
		c.onClose(c)
	})
}

// startReading starts listening on the client connection. As we do not need anything from the client,
// we ignore incoming messages. Leaves the loop on errors.
func (c *client) startReading(pongWait time.Duration) {
	defer c.NotifyClose()
	c.conn.SetReadLimit(64)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(appData string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	for {
		if _, _, err := c.conn.NextReader(); err != nil {
			printWebSocketError("ReadError", err)
			return
		}
	}
}

// startWriteHandler starts the write loop. The method has the following tasks:
// * ping the client in the interval provided as parameter
// * write messages send by the channel to the client
// * on errors exit the loop.
//
// During the replay phase messages from the channel are deduplicated against
// the replay watermark; after replay all messages are forwarded directly.
func (c *client) startWriteHandler(pingPeriod time.Duration) {
	pingTicker := time.NewTicker(pingPeriod)
	defer func() {
		c.NotifyClose()
		pingTicker.Stop()
	}()

	for {
		select {
		case message, ok := <-c.write:
			if !ok {
				return
			}

			// During replay: deduplicate against the watermark.
			// After replay: always forward.
			select {
			case <-c.replaying:
				// replay done, no dedup
			default:
				if !c.shouldSend(message) {
					continue
				}
			}

			c.writeMu.Lock()
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			err := writeJSON(c.conn, message)
			c.writeMu.Unlock()
			if err != nil {
				printWebSocketError("WriteError", err)
				return
			}
		case <-pingTicker.C:
			c.writeMu.Lock()
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			err := ping(c.conn)
			c.writeMu.Unlock()
			if err != nil {
				printWebSocketError("PingError", err)
				return
			}
		}
	}
}

func printWebSocketError(prefix string, err error) {
	closeError, ok := err.(*websocket.CloseError)

	if ok && closeError != nil && (closeError.Code == 1000 || closeError.Code == 1001) {
		// normal closure
		return
	}

	log.Warn().Err(err).Msgf("WebSocket %s", prefix)
}
