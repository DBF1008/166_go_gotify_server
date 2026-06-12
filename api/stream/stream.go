package stream

import (
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/gotify/server/v2/auth"
	"github.com/gotify/server/v2/mode"
	"github.com/gotify/server/v2/model"
	"github.com/rs/zerolog/log"
)

// The API provides a handler for a WebSocket stream API.
type API struct {
	clients     map[uint][]*client
	lock        sync.RWMutex
	pingPeriod  time.Duration
	pongTimeout time.Duration
	upgrader    *websocket.Upgrader
	// fetcher retrieves messages for cursor-based replay. May be nil to disable replay.
	fetcher MessageFetcher
}

// New creates a new instance of API.
// pingPeriod: is the interval, in which is server sends the a ping to the client.
// pongTimeout: is the duration after the connection will be terminated, when the client does not respond with the
// pong command.
// fetcher: optional MessageFetcher for cursor-based replay. Pass nil to disable replay.
func New(pingPeriod, pongTimeout time.Duration, allowedWebSocketOrigins []string, fetcher MessageFetcher) *API {
	return &API{
		clients:     make(map[uint][]*client),
		pingPeriod:  pingPeriod,
		pongTimeout: pingPeriod + pongTimeout,
		upgrader:    newUpgrader(allowedWebSocketOrigins),
		fetcher:     fetcher,
	}
}

// CollectConnectedClientTokens returns all tokens of the connected clients.
func (a *API) CollectConnectedClientTokens() []string {
	a.lock.RLock()
	defer a.lock.RUnlock()
	var clients []string
	for _, cs := range a.clients {
		for _, c := range cs {
			clients = append(clients, c.token)
		}
	}
	return uniq(clients)
}

// NotifyDeletedUser closes existing connections for the given user.
func (a *API) NotifyDeletedUser(userID uint) error {
	a.lock.Lock()
	defer a.lock.Unlock()
	if clients, ok := a.clients[userID]; ok {
		for _, client := range clients {
			client.Close()
		}
		delete(a.clients, userID)
	}
	return nil
}

// NotifyDeletedClient closes existing connections with the given token.
func (a *API) NotifyDeletedClient(userID uint, token string) {
	a.lock.Lock()
	defer a.lock.Unlock()
	if clients, ok := a.clients[userID]; ok {
		for i := len(clients) - 1; i >= 0; i-- {
			client := clients[i]
			if client.token == token {
				client.Close()
				clients = append(clients[:i], clients[i+1:]...)
			}
		}
		a.clients[userID] = clients
	}
}

// Notify notifies the clients with the given userID that a new messages was created.
func (a *API) Notify(userID uint, msg *model.MessageExternal) {
	a.lock.RLock()
	defer a.lock.RUnlock()
	if clients, ok := a.clients[userID]; ok {
		for _, c := range clients {
			c.write <- msg
		}
	}
}

func (a *API) remove(remove *client) {
	a.lock.Lock()
	defer a.lock.Unlock()
	if userIDClients, ok := a.clients[remove.userID]; ok {
		for i, client := range userIDClients {
			if client == remove {
				a.clients[remove.userID] = append(userIDClients[:i], userIDClients[i+1:]...)
				break
			}
		}
	}
}

func (a *API) register(client *client) {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.clients[client.userID] = append(a.clients[client.userID], client)
}

// Handle handles incoming requests. First it upgrades the protocol to the WebSocket protocol and then starts listening
// for read and writes.
// swagger:operation GET /stream message streamMessages
//
// Websocket, return newly created messages.
//
//	---
//	schema: ws, wss
//	produces: [application/json]
//	security: [clientTokenAuthorizationHeader: [], clientTokenHeader: [], clientTokenQuery: [], basicAuth: []]
//	responses:
//	  200:
//	    description: Ok
//	    schema:
//	        $ref: "#/definitions/Message"
//	  400:
//	    description: Bad Request
//	    schema:
//	        $ref: "#/definitions/Error"
//	  401:
//	    description: Unauthorized
//	    schema:
//	        $ref: "#/definitions/Error"
//	  403:
//	    description: Forbidden
//	    schema:
//	        $ref: "#/definitions/Error"
//	  500:
//	    description: Server Error
//	    schema:
//	        $ref: "#/definitions/Error"
func (a *API) Handle(ctx *gin.Context) {
	userID := auth.GetUserID(ctx)

	// Parse and validate cursor BEFORE upgrading to WebSocket.
	var cursor uint
	if cursorParam := ctx.Query("lastMessageID"); cursorParam != "" {
		parsed, err := strconv.ParseUint(cursorParam, 10, 64)
		if err != nil || parsed == 0 {
			ctx.AbortWithError(http.StatusBadRequest,
				fmt.Errorf("invalid lastMessageID parameter: %s", cursorParam))
			return
		}
		cursor = uint(parsed)

		if a.fetcher == nil {
			ctx.AbortWithError(http.StatusServiceUnavailable,
				fmt.Errorf("replay not available"))
			return
		}

		exists, err := a.fetcher.MessageExistsForUser(userID, cursor)
		if err != nil {
			ctx.AbortWithError(http.StatusInternalServerError, err)
			return
		}
		if !exists {
			ctx.AbortWithError(http.StatusNotFound,
				fmt.Errorf("message %d not found or does not belong to user", cursor))
			return
		}
	}

	conn, err := a.upgrader.Upgrade(ctx.Writer, ctx.Request, nil)
	if err != nil {
		ctx.Error(err)
		return
	}

	var token string
	if c := auth.GetClient(ctx); c != nil {
		token = c.Token
	}
	cl := newClient(conn, userID, token, a.remove)

	// If no replay is needed, signal immediately so the write handler
	// forwards all messages without dedup.
	if cursor == 0 || a.fetcher == nil {
		cl.finishReplay()
	}

	// Register FIRST so that no Notify messages are missed during replay.
	a.register(cl)

	// Start read/write goroutines before replay so that Notify has a consumer.
	go cl.startReading(a.pongTimeout)
	go cl.startWriteHandler(a.pingPeriod)

	// Replay historical messages if a valid cursor was provided.
	if cursor > 0 && a.fetcher != nil {
		msgs, err := a.fetcher.GetMessagesByUserAfter(userID, cursor)
		if err != nil {
			log.Error().Err(err).Uint("userID", userID).Uint("cursor", cursor).
				Msg("replay fetch failed, client continues with live stream only")
		} else {
			var maxReplayedID uint
			for _, msg := range msgs {
				// Only send messages that pass the dedup check.
				if !cl.shouldSend(msg) {
					continue
				}
				cl.writeMu.Lock()
				cl.conn.SetWriteDeadline(time.Now().Add(writeWait))
				err := writeJSON(cl.conn, msg)
				cl.writeMu.Unlock()
				if err != nil {
					printWebSocketError("ReplayWriteError", err)
					cl.NotifyClose()
					return
				}
				if msg.ID > maxReplayedID {
					maxReplayedID = msg.ID
				}
			}
			// Update watermark so the write handler deduplicates subsequent messages.
			if maxReplayedID > 0 {
				cl.setLastSentID(maxReplayedID)
			}
		}
		// Replay complete — write handler stops dedup from now on.
		cl.finishReplay()
	}
}

// Close closes all client connections and stops answering new connections.
func (a *API) Close() {
	a.lock.Lock()
	defer a.lock.Unlock()

	for _, clients := range a.clients {
		for _, client := range clients {
			client.Close()
		}
	}
	for k := range a.clients {
		delete(a.clients, k)
	}
}

func uniq[T comparable](s []T) []T {
	m := make(map[T]struct{}, len(s))
	r := make([]T, 0, len(s))
	for _, v := range s {
		if _, ok := m[v]; !ok {
			m[v] = struct{}{}
			r = append(r, v)
		}
	}
	return r
}

func isAllowedOrigin(r *http.Request, allowedOrigins []*regexp.Regexp) bool {
	origin := r.Header.Get("origin")
	if origin == "" {
		return true
	}

	u, err := url.Parse(origin)
	if err != nil {
		return false
	}

	if strings.EqualFold(u.Host, r.Host) {
		return true
	}

	for _, allowedOrigin := range allowedOrigins {
		if allowedOrigin.MatchString(strings.ToLower(u.Hostname())) {
			return true
		}
	}

	return false
}

func newUpgrader(allowedWebSocketOrigins []string) *websocket.Upgrader {
	compiledAllowedOrigins := compileAllowedWebSocketOrigins(allowedWebSocketOrigins)
	return &websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			if mode.IsDev() {
				return true
			}
			return isAllowedOrigin(r, compiledAllowedOrigins)
		},
	}
}

func compileAllowedWebSocketOrigins(allowedOrigins []string) []*regexp.Regexp {
	var compiledAllowedOrigins []*regexp.Regexp
	for _, origin := range allowedOrigins {
		compiledAllowedOrigins = append(compiledAllowedOrigins, regexp.MustCompile(origin))
	}

	return compiledAllowedOrigins
}
