package stream

import (
	"time"

	"github.com/gorilla/websocket"
	"github.com/gotify/server/v2/model"
	"github.com/rs/zerolog/log"
)

const (
	writeWait = 2 * time.Second

	// replayBatchSize bounds how many missed messages are loaded from the history per query, so a
	// large backlog is streamed in chunks instead of being held in memory all at once.
	replayBatchSize = 100
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
	// since is the resume cursor: the id of the last message the client already processed. Zero
	// means "first connection" (no replay).
	since uint
	// lastSentID tracks the highest message id delivered to the client so duplicates can be
	// skipped while transitioning from the replay to the live stream. Only used when resuming.
	lastSentID uint
	history    MessageHistory
	once       once
}

func newClient(conn *websocket.Conn, userID uint, token string, since uint, history MessageHistory, onClose func(*client)) *client {
	return &client{
		conn:       conn,
		write:      make(chan *model.MessageExternal, 1),
		userID:     userID,
		token:      token,
		since:      since,
		lastSentID: since,
		history:    history,
		onClose:    onClose,
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

// startWriteHandler starts listening on the client connection. As we do not need anything from the client,
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
// * replay messages the client missed while disconnected (when a resume cursor was provided)
// * ping the client in the interval provided as parameter
// * write messages send by the channel to the client
// * on errors exit the loop.
func (c *client) startWriteHandler(pingPeriod time.Duration) {
	pingTicker := time.NewTicker(pingPeriod)
	defer func() {
		c.NotifyClose()
		pingTicker.Stop()
	}()

	// Replay everything the client missed before switching to the live stream. The client was
	// already registered, so any message produced during the replay is buffered on the write
	// channel and de-duplicated below by its id.
	if !c.replayMissedMessages() {
		return
	}

	for {
		select {
		case message, ok := <-c.write:
			if !ok {
				return
			}

			if c.since > 0 && message.ID != 0 && message.ID <= c.lastSentID {
				// Already delivered during the replay; skip the duplicate.
				continue
			}

			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := writeJSON(c.conn, message); err != nil {
				printWebSocketError("WriteError", err)
				return
			}
			if c.since > 0 {
				c.lastSentID = message.ID
			}
		case <-pingTicker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := ping(c.conn); err != nil {
				printWebSocketError("PingError", err)
				return
			}
		}
	}
}

// replayMissedMessages streams every message created after the resume cursor, in chronological
// order, before the live stream begins. It returns false if writing to the connection failed and
// the write loop should stop. A nil history or a zero cursor (first connection) replays nothing.
func (c *client) replayMissedMessages() bool {
	if c.history == nil || c.since == 0 {
		return true
	}
	cursor := c.since
	for {
		msgs, err := c.history.GetMessagesAfter(c.userID, cursor, replayBatchSize)
		if err != nil {
			// Keep the live subscription instead of dropping the connection on a transient history
			// error; the client still receives newly created messages.
			log.Warn().Err(err).Msg("WebSocket ReplayError")
			return true
		}
		for _, msg := range msgs {
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := writeJSON(c.conn, msg); err != nil {
				printWebSocketError("WriteError", err)
				return false
			}
			c.lastSentID = msg.ID
			cursor = msg.ID
		}
		if len(msgs) < replayBatchSize {
			return true
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
