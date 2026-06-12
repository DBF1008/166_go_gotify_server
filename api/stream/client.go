package stream

import (
	"time"

	"github.com/gorilla/websocket"
	"github.com/gotify/server/v2/model"
	"github.com/rs/zerolog/log"
)

const (
	writeWait = 2 * time.Second

	// messageBufferSize is the number of messages buffered per client before the
	// client is considered too slow. The buffer absorbs short write bursts; once
	// it overflows the client is disconnected (see client.enqueue) so that a
	// single slow or suspended connection cannot block message dispatch for the
	// other clients. Disconnected clients reconnect and fetch any missed messages
	// through the REST API.
	messageBufferSize = 16
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
	closing chan struct{}
	userID  uint
	token   string
	once    once
}

func newClient(conn *websocket.Conn, userID uint, token string, onClose func(*client)) *client {
	return &client{
		conn:    conn,
		write:   make(chan *model.MessageExternal, messageBufferSize),
		closing: make(chan struct{}),
		userID:  userID,
		token:   token,
		onClose: onClose,
	}
}

// Close closes the connection.
func (c *client) Close() {
	c.once.Do(func() {
		c.conn.Close()
		close(c.closing)
	})
}

// NotifyClose closes the connection and notifies that the connection was closed.
func (c *client) NotifyClose() {
	c.once.Do(func() {
		c.conn.Close()
		close(c.closing)
		c.onClose(c)
	})
}

// enqueue hands a message to the client's write loop without ever blocking the
// caller. If the buffer is full the client is not keeping up (e.g. a slow network
// or a suspended browser tab), so it is disconnected instead of stalling dispatch
// for the other clients; it will reconnect and fetch missed messages via the REST
// API. Callers must not hold the API lock, as a disconnect acquires it.
func (c *client) enqueue(msg *model.MessageExternal) {
	select {
	case c.write <- msg:
	case <-c.closing:
		// Already shutting down; nothing to deliver.
	default:
		c.NotifyClose()
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
		case message := <-c.write:
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
		case <-c.closing:
			return
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
