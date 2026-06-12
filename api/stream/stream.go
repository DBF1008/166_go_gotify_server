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
)

// MessageHistory provides the messages a client missed while it was disconnected, so they can be
// replayed when the client reconnects with a resume cursor.
type MessageHistory interface {
	// GetMessagesAfter returns up to limit messages for the user with an id greater than 'after',
	// ordered ascending (oldest first).
	GetMessagesAfter(userID, after uint, limit int) ([]*model.MessageExternal, error)
}

// The API provides a handler for a WebSocket stream API.
type API struct {
	clients     map[uint][]*client
	lock        sync.RWMutex
	pingPeriod  time.Duration
	pongTimeout time.Duration
	upgrader    *websocket.Upgrader
	history     MessageHistory
}

// New creates a new instance of API.
// pingPeriod: is the interval, in which is server sends the a ping to the client.
// pongTimeout: is the duration after the connection will be terminated, when the client does not respond with the
// pong command.
// history: provides missed messages for clients that reconnect with a resume cursor. It may be nil,
// in which case clients always start from the live stream without replay.
func New(pingPeriod, pongTimeout time.Duration, allowedWebSocketOrigins []string, history MessageHistory) *API {
	return &API{
		clients:     make(map[uint][]*client),
		pingPeriod:  pingPeriod,
		pongTimeout: pingPeriod + pongTimeout,
		upgrader:    newUpgrader(allowedWebSocketOrigins),
		history:     history,
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
//	parameters:
//	- name: since
//	  in: query
//	  description: replay all messages created after this message id, then continue with the live stream. Pass the id of the last message the client already processed. Omit it (or use 0) on first connection to start from the live stream without replay.
//	  minimum: 0
//	  required: false
//	  type: integer
//	  format: int64
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
	since, err := parseSince(ctx)
	if err != nil {
		ctx.AbortWithError(http.StatusBadRequest, err)
		return
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
	client := newClient(conn, auth.GetUserID(ctx), token, since, a.history, a.remove)
	a.register(client)
	go client.startReading(a.pongTimeout)
	go client.startWriteHandler(a.pingPeriod)
}

// parseSince reads the optional `since` resume cursor from the request query. It is the id of the
// last message the client already processed; the stream replays every message created after it.
// An empty value means "no cursor" (first connection). A malformed value yields an error so the
// caller can reject the request with a clear status before upgrading the connection.
func parseSince(ctx *gin.Context) (uint, error) {
	raw := strings.TrimSpace(ctx.Query("since"))
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid since cursor %q: must be a non-negative integer", raw)
	}
	return uint(value), nil
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
