package stream

import (
	"time"

	"github.com/gorilla/websocket"
	"github.com/gotify/server/v2/model"
	"github.com/rs/zerolog/log"
)

const (
	writeWait      = 2 * time.Second
	writeChanBuffer = 128
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
}

func newClient(conn *websocket.Conn, userID uint, token string, onClose func(*client)) *client {
	return &client{
		conn:    conn,
		write:   make(chan *model.MessageExternal, writeChanBuffer),
		userID:  userID,
		token:   token,
		onClose: onClose,
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

// enqueueOrClose attempts a non-blocking send of msg into the write channel.
// If the channel is full (slow consumer), the underlying connection is closed.
// This causes the read goroutine to detect the error and call NotifyClose()
// from its own goroutine, avoiding a deadlock with Notify()'s RLock.
func (c *client) enqueueOrClose(msg *model.MessageExternal) {
	select {
	case c.write <- msg:
	default:
		// Slow consumer: close the raw connection. The read/write goroutines
		// will detect the error and trigger NotifyClose() asynchronously.
		log.Warn().Msgf("Closing slow WebSocket client for user %d (write buffer full)", c.userID)
		c.conn.Close()
	}
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
// * ping the client in the interval provided as parameter
// * write messages send by the channel to the client
// * on errors exit the loop.
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

			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := writeJSON(c.conn, message); err != nil {
				printWebSocketError("WriteError", err)
				return
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

func printWebSocketError(prefix string, err error) {
	closeError, ok := err.(*websocket.CloseError)

	if ok && closeError != nil && (closeError.Code == 1000 || closeError.Code == 1001) {
		// normal closure
		return
	}

	log.Warn().Err(err).Msgf("WebSocket %s", prefix)
}
