package stream

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/gotify/server/v2/auth"
	"github.com/gotify/server/v2/mode"
	"github.com/gotify/server/v2/model"
	"github.com/stretchr/testify/assert"
)

// nonBlockingTestClient creates a test client with a buffered readMessage channel
// and a non-blocking send, preventing goroutine leaks when connections are closed
// while messages are in flight.
func nonBlockingTestClient(t *testing.T, url string) *testingClient {
	ws, _, err := websocket.DefaultDialer.Dial(url, nil)
	assert.Nil(t, err)
	readMessages := make(chan model.MessageExternal, writeChanBuffer+16)
	client := &testingClient{conn: ws, readMessage: readMessages, t: t}
	go func() {
		for {
			_, payload, err := client.conn.ReadMessage()
			if err != nil {
				return
			}
			actual := &model.MessageExternal{}
			json.NewDecoder(bytes.NewBuffer(payload)).Decode(actual)
			select {
			case client.readMessage <- *actual:
			default:
				// drop if buffer full — prevents goroutine leak
			}
		}
	}()
	return client
}

// blockWritesForConn returns a writeJSON replacement that blocks indefinitely
// for the specified connection while passing through all other connections.
// Call the returned cleanup function to unblock and restore the original.
func blockWritesForConn(blockConn *websocket.Conn) (cleanup func()) {
	old := writeJSON
	block := make(chan struct{})
	writeJSON = func(conn *websocket.Conn, v interface{}) error {
		if conn == blockConn {
			<-block
			return errors.New("blocked write")
		}
		return old(conn, v)
	}
	return func() {
		writeJSON = old
		close(block)
	}
}

// fillWriteBuffer sends enough messages to completely fill one client's write
// channel buffer plus the one message currently being processed by the write
// handler. After this, the next enqueueOrClose call will hit the default branch.
func fillWriteBuffer(api *API, userID uint) {
	for i := 0; i < writeChanBuffer+1; i++ {
		api.Notify(userID, &model.MessageExternal{ID: uint(i), Message: "fill"})
	}
}

// getServerConn returns the server-side websocket connection for the first
// client of the given user.
func getServerConn(api *API, userID uint) *websocket.Conn {
	api.lock.RLock()
	defer api.lock.RUnlock()
	if cs, ok := api.clients[userID]; ok && len(cs) > 0 {
		return cs[0].conn
	}
	return nil
}

// drainUntilMessage reads from the client's readMessage channel until the
// specified message is found or the deadline expires.
func drainUntilMessage(t *testing.T, client *testingClient, expected *model.MessageExternal, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case msg := <-client.readMessage:
			if msg.ID == expected.ID && msg.Message == expected.Message {
				return
			}
		case <-deadline:
			t.Fatalf("did not receive expected message (ID=%d, Message=%q) within %v",
				expected.ID, expected.Message, timeout)
		}
	}
}

// TestSlowConsumerDoesNotBlockOtherClients verifies that a single slow/blocked
// WebSocket client does not prevent other clients of the same user from
// receiving messages promptly.
func TestSlowConsumerDoesNotBlockOtherClients(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	userIDs := []uint{1, 1}
	tokens := []string{"slow", "fast"}
	i := 0
	server, api := bootTestServer(func(context *gin.Context) {
		auth.RegisterClient(context, &model.Client{UserID: userIDs[i], Token: tokens[i]})
		i++
	})
	defer server.Close()
	defer api.Close()

	wsURL := wsURL(server.URL)

	slowClient := nonBlockingTestClient(t, wsURL)
	defer slowClient.conn.Close()
	fastClient := nonBlockingTestClient(t, wsURL)
	defer fastClient.conn.Close()

	waitForConnectedClients(api, 2)

	// Block writes for the slow client's server-side connection.
	slowConn := getServerConn(api, 1)
	assert.NotNil(t, slowConn)
	cleanup := blockWritesForConn(slowConn)
	defer cleanup()

	// Give the write handler time to start and pick up the first message.
	time.Sleep(50 * time.Millisecond)

	// Fill the slow client's write buffer completely.
	fillWriteBuffer(api, 1)

	// Give the fast client's write handler time to drain its buffer.
	// fillWriteBuffer sends to all clients of user 1, so the fast client
	// also received messages. We need its buffer to drain before the
	// critical message.
	time.Sleep(200 * time.Millisecond)

	// Now send one more message. With the fix, Notify() must return quickly
	// (non-blocking send) and the fast client should receive it.
	done := make(chan struct{})
	go func() {
		api.Notify(1, &model.MessageExternal{ID: 999, Message: "critical"})
		close(done)
	}()

	select {
	case <-done:
		// Notify returned — good, it didn't block.
	case <-time.After(2 * time.Second):
		t.Fatal("Notify() blocked on slow consumer — regression detected")
	}

	// The fast client should receive the "critical" message (possibly after
	// some "fill" messages from fillWriteBuffer).
	drainUntilMessage(t, fastClient, &model.MessageExternal{ID: 999, Message: "critical"}, 2*time.Second)
}

// TestSlowConsumerDoesNotBlockOtherUsers verifies that a slow consumer for one
// user does not delay message delivery to clients of a different user.
func TestSlowConsumerDoesNotBlockOtherUsers(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	userIDs := []uint{1, 2}
	tokens := []string{"user1", "user2"}
	i := 0
	server, api := bootTestServer(func(context *gin.Context) {
		auth.RegisterClient(context, &model.Client{UserID: userIDs[i], Token: tokens[i]})
		i++
	})
	defer server.Close()
	defer api.Close()

	wsURL := wsURL(server.URL)

	user1Client := nonBlockingTestClient(t, wsURL)
	defer user1Client.conn.Close()
	user2Client := nonBlockingTestClient(t, wsURL)
	defer user2Client.conn.Close()

	waitForConnectedClients(api, 2)

	// Block writes for user 1's connection.
	slowConn := getServerConn(api, 1)
	assert.NotNil(t, slowConn)
	cleanup := blockWritesForConn(slowConn)
	defer cleanup()

	time.Sleep(50 * time.Millisecond)

	// Fill user 1's buffer.
	fillWriteBuffer(api, 1)

	// Notify user 2 — should not be blocked by user 1's slow consumer.
	done := make(chan struct{})
	go func() {
		api.Notify(2, &model.MessageExternal{ID: 42, Message: "for-user2"})
		close(done)
	}()

	select {
	case <-done:
		// Good.
	case <-time.After(2 * time.Second):
		t.Fatal("Notify() for user 2 was blocked by user 1's slow consumer")
	}

	user2Client.expectMessage(&model.MessageExternal{ID: 42, Message: "for-user2"})
}

// TestSlowConsumerGetsDisconnected verifies that a client whose write buffer
// overflows is disconnected and removed from the client registry.
func TestSlowConsumerGetsDisconnected(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	server, api := bootTestServer(staticUserID())
	defer server.Close()
	defer api.Close()

	wsURL := wsURL(server.URL)
	user := nonBlockingTestClient(t, wsURL)
	defer user.conn.Close()

	waitForConnectedClients(api, 1)
	assert.Equal(t, 1, countClients(api))

	// Block writes for this client.
	slowConn := getServerConn(api, 1)
	cleanup := blockWritesForConn(slowConn)
	defer cleanup()

	time.Sleep(50 * time.Millisecond)

	// Overflow the buffer: writeChanBuffer+1 to fill, then one more to trigger close.
	fillWriteBuffer(api, 1)
	// This one should trigger the default branch in enqueueOrClose → conn.Close().
	api.Notify(1, &model.MessageExternal{ID: 999, Message: "overflow"})

	// Wait for the slow client to be removed from the registry.
	deadline := time.After(5 * time.Second)
	for {
		if countClients(api) == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("Slow consumer was not disconnected; clients remaining: %d", countClients(api))
		case <-time.After(20 * time.Millisecond):
		}
	}

	assert.Equal(t, 0, countClients(api))
}

// TestClientDeletionNotBlockedBySlowConsumer verifies that NotifyDeletedClient
// can close a specific client's connection even when another client of the same
// user has a full write buffer (slow consumer).
func TestClientDeletionNotBlockedBySlowConsumer(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	userIDs := []uint{1, 1}
	tokens := []string{"slow-token", "fast-token"}
	i := 0
	server, api := bootTestServer(func(context *gin.Context) {
		auth.RegisterClient(context, &model.Client{UserID: userIDs[i], Token: tokens[i]})
		i++
	})
	defer server.Close()
	defer api.Close()

	wsURL := wsURL(server.URL)

	slowClient := nonBlockingTestClient(t, wsURL)
	defer slowClient.conn.Close()
	fastClient := nonBlockingTestClient(t, wsURL)
	defer fastClient.conn.Close()

	waitForConnectedClients(api, 2)

	// Block writes for the slow client.
	slowConn := getServerConn(api, 1)
	cleanup := blockWritesForConn(slowConn)
	defer cleanup()

	time.Sleep(50 * time.Millisecond)

	// Fill the slow client's buffer.
	fillWriteBuffer(api, 1)

	// Delete the fast client — this must not be blocked by the slow consumer.
	done := make(chan struct{})
	go func() {
		api.NotifyDeletedClient(1, "fast-token")
		close(done)
	}()

	select {
	case <-done:
		// NotifyDeletedClient returned promptly — good.
	case <-time.After(2 * time.Second):
		t.Fatal("NotifyDeletedClient was blocked by slow consumer")
	}

	// The fast client should have been removed.
	api.lock.RLock()
	remaining := len(api.clients[1])
	api.lock.RUnlock()

	assert.LessOrEqual(t, remaining, 1, "fast client should have been removed")

	// The fast client's connection should be closed.
	fastClient.conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	_, _, err := fastClient.conn.ReadMessage()
	assert.Error(t, err, "fast client connection should be closed")
}

// TestActiveDisconnectCleansUp verifies that a client that actively closes its
// connection is properly removed from the registry, even when other clients
// exist for the same user.
func TestActiveDisconnectCleansUp(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	userIDs := []uint{1, 1}
	tokens := []string{"a", "b"}
	i := 0
	server, api := bootTestServer(func(context *gin.Context) {
		auth.RegisterClient(context, &model.Client{UserID: userIDs[i], Token: tokens[i]})
		i++
	})
	defer server.Close()
	defer api.Close()

	wsURL := wsURL(server.URL)

	clientA := nonBlockingTestClient(t, wsURL)
	clientB := nonBlockingTestClient(t, wsURL)
	defer clientB.conn.Close()

	waitForConnectedClients(api, 2)
	assert.Equal(t, 2, countClients(api))

	// Client A actively closes its connection.
	clientA.conn.Close()

	// Wait for cleanup.
	deadline := time.After(5 * time.Second)
	for {
		if countClients(api) == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("Active disconnect was not cleaned up; clients remaining: %d", countClients(api))
		case <-time.After(20 * time.Millisecond):
		}
	}

	// Client B should still work.
	api.Notify(1, &model.MessageExternal{ID: 10, Message: "still-alive"})
	clientB.expectMessage(&model.MessageExternal{ID: 10, Message: "still-alive"})
}

// TestExpiryCleanup verifies that a client that does not respond to pings
// (simulating a hung tab or network partition) is eventually cleaned up via
// the pong timeout mechanism.
func TestExpiryCleanup(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.CheckTimeout(t, 15*time.Second)()

	server, api := bootTestServer(staticUserID())
	defer server.Close()
	defer api.Close()

	wsURL := wsURL(server.URL)

	// Dial but do NOT start reading — the client will not respond to pings.
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	assert.Nil(t, err)
	defer ws.Close()

	waitForConnectedClients(api, 1)
	assert.Equal(t, 1, countClients(api))

	// Wait for ping period + pong timeout to expire.
	time.Sleep(api.pingPeriod + api.pongTimeout + 500*time.Millisecond)

	assert.Equal(t, 0, countClients(api), "unresponsive client should have been cleaned up")
}

// TestNotifyDeletedUserNotBlockedBySlowConsumer verifies that NotifyDeletedUser
// can close all connections for a user even when one of them is a slow consumer.
func TestNotifyDeletedUserNotBlockedBySlowConsumer(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	userIDs := []uint{1, 1}
	tokens := []string{"slow-u", "fast-u"}
	i := 0
	server, api := bootTestServer(func(context *gin.Context) {
		auth.RegisterClient(context, &model.Client{UserID: userIDs[i], Token: tokens[i]})
		i++
	})
	defer server.Close()
	defer api.Close()

	wsURL := wsURL(server.URL)

	slowClient := nonBlockingTestClient(t, wsURL)
	defer slowClient.conn.Close()
	fastClient := nonBlockingTestClient(t, wsURL)
	defer fastClient.conn.Close()

	waitForConnectedClients(api, 2)

	// Block writes for the slow client.
	slowConn := getServerConn(api, 1)
	cleanup := blockWritesForConn(slowConn)
	defer cleanup()

	time.Sleep(50 * time.Millisecond)
	fillWriteBuffer(api, 1)

	// Delete the entire user — must not be blocked.
	done := make(chan struct{})
	go func() {
		api.NotifyDeletedUser(1)
		close(done)
	}()

	select {
	case <-done:
		// Good.
	case <-time.After(2 * time.Second):
		t.Fatal("NotifyDeletedUser was blocked by slow consumer")
	}

	// Both connections should be closed.
	api.lock.RLock()
	_, exists := api.clients[1]
	api.lock.RUnlock()
	assert.False(t, exists, "all clients for user 1 should be removed")
}
