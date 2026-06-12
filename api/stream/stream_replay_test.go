package stream

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"bytes"

	"github.com/fortytw2/leaktest"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/gotify/server/v2/mode"
	"github.com/gotify/server/v2/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Mock MessageFetcher ---

type mockFetcher struct {
	mu       sync.Mutex
	messages map[uint][]*model.MessageExternal // userID → messages
}

func newMockFetcher() *mockFetcher {
	return &mockFetcher{
		messages: make(map[uint][]*model.MessageExternal),
	}
}

func (m *mockFetcher) addMessage(userID uint, msg *model.MessageExternal) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages[userID] = append(m.messages[userID], msg)
}

func (m *mockFetcher) GetMessagesByUserAfter(userID uint, since uint) ([]*model.MessageExternal, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*model.MessageExternal
	for _, msg := range m.messages[userID] {
		if msg.ID > since {
			result = append(result, msg)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (m *mockFetcher) MessageExistsForUser(userID uint, messageID uint) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, msg := range m.messages[userID] {
		if msg.ID == messageID {
			return true, nil
		}
	}
	return false, nil
}

// --- Test infrastructure ---

func bootReplayTestServer(fetcher MessageFetcher, middleware gin.HandlerFunc) (*httptest.Server, *API) {
	r := gin.New()
	r.Use(middleware)
	api := New(500*time.Millisecond, 500*time.Millisecond, []string{}, fetcher)
	r.GET("/", api.Handle)
	server := httptest.NewServer(r)
	return server, api
}

func testClientWithCursor(t *testing.T, url string, cursor uint) *testingClient {
	wsURL := url
	if cursor > 0 {
		wsURL = url + "?lastMessageID=" + fmt.Sprint(cursor)
	}
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.Nil(t, err)

	readMessages := make(chan model.MessageExternal, 100)
	client := &testingClient{conn: ws, readMessage: readMessages, t: t}

	go func() {
		for {
			_, payload, err := client.conn.ReadMessage()
			if err != nil {
				return
			}
			actual := &model.MessageExternal{}
			json.NewDecoder(bytes.NewBuffer(payload)).Decode(actual)
			client.readMessage <- *actual
		}
	}()

	return client
}

func testClientCursorExpectingHTTPError(t *testing.T, url string, cursor uint, expectedStatus int) {
	wsURL := url
	if cursor > 0 {
		wsURL = url + "?lastMessageID=" + fmt.Sprint(cursor)
	}
	_, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err == nil {
		t.Fatal("expected WebSocket dial error but got nil")
	}
	if resp != nil {
		assert.Equal(t, expectedStatus, resp.StatusCode)
		resp.Body.Close()
	}
}

func testClientCursorExpectingError(t *testing.T, url string, cursorParam string, expectedStatus int) {
	wsURL := url + "?lastMessageID=" + cursorParam
	_, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err == nil {
		t.Fatal("expected WebSocket dial error but got nil")
	}
	if resp != nil {
		assert.Equal(t, expectedStatus, resp.StatusCode)
		resp.Body.Close()
	}
}

// expectMultipleMessages reads exactly count messages from the client and returns them.
func expectMultipleMessages(c *testingClient, count int) []model.MessageExternal {
	var msgs []model.MessageExternal
	for i := 0; i < count; i++ {
		select {
		case <-time.After(2 * time.Second):
			c.t.Fatalf("expected message %d/%d but timed out", i+1, count)
		case msg := <-c.readMessage:
			msgs = append(msgs, msg)
		}
	}
	return msgs
}

// --- Tests ---

// TestFirstConnection_NoCursor verifies that a client connecting without a cursor
// receives live messages as before (backward compatible behavior).
func TestFirstConnection_NoCursor(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.Check(t)()

	fetcher := newMockFetcher()
	server, api := bootReplayTestServer(fetcher, staticUserID())
	defer server.Close()
	defer api.Close()

	wsURL := wsURL(server.URL)
	user := testClient(t, wsURL)
	defer user.conn.Close()

	waitForConnectedClients(api, 1)

	// Should receive live messages normally.
	api.Notify(1, &model.MessageExternal{ID: 10, Message: "live-1"})
	user.expectMessage(&model.MessageExternal{ID: 10, Message: "live-1"})

	api.Notify(1, &model.MessageExternal{ID: 11, Message: "live-2"})
	user.expectMessage(&model.MessageExternal{ID: 11, Message: "live-2"})

	// No extra messages.
	user.expectNoMessage()
}

// TestReconnect_ReplaysMissedMessages verifies that a reconnecting client with a
// cursor receives all messages missed during disconnection, then continues with live.
func TestReconnect_ReplaysMissedMessages(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.Check(t)()

	fetcher := newMockFetcher()
	// Simulate: user 1 has messages 1, 2, 3 in the database.
	fetcher.addMessage(1, &model.MessageExternal{ID: 1, ApplicationID: 1, Message: "msg-1"})
	fetcher.addMessage(1, &model.MessageExternal{ID: 2, ApplicationID: 1, Message: "msg-2"})
	fetcher.addMessage(1, &model.MessageExternal{ID: 3, ApplicationID: 1, Message: "msg-3"})

	server, api := bootReplayTestServer(fetcher, staticUserID())
	defer server.Close()
	defer api.Close()

	wsURL := wsURL(server.URL)

	// Client reconnects with cursor=1 (already processed message 1).
	user := testClientWithCursor(t, wsURL, 1)
	defer user.conn.Close()

	waitForConnectedClients(api, 1)

	// Should receive replayed messages 2 and 3 (ID > 1, ascending order).
	msgs := expectMultipleMessages(user, 2)
	assert.Equal(t, uint(2), msgs[0].ID)
	assert.Equal(t, "msg-2", msgs[0].Message)
	assert.Equal(t, uint(3), msgs[1].ID)
	assert.Equal(t, "msg-3", msgs[1].Message)

	// New live message should arrive seamlessly.
	api.Notify(1, &model.MessageExternal{ID: 4, ApplicationID: 1, Message: "msg-4"})
	user.expectMessage(&model.MessageExternal{ID: 4, ApplicationID: 1, Message: "msg-4"})

	user.expectNoMessage()
}

// TestReconnect_NoDuplicatesOnRace verifies that when a Notify arrives during
// replay, the deduplication mechanism prevents duplicate delivery.
func TestReconnect_NoDuplicatesOnRace(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.Check(t)()

	fetcher := newMockFetcher()
	// User has messages 1, 2, 3.
	fetcher.addMessage(1, &model.MessageExternal{ID: 1, ApplicationID: 1, Message: "msg-1"})
	fetcher.addMessage(1, &model.MessageExternal{ID: 2, ApplicationID: 1, Message: "msg-2"})
	fetcher.addMessage(1, &model.MessageExternal{ID: 3, ApplicationID: 1, Message: "msg-3"})

	server, api := bootReplayTestServer(fetcher, staticUserID())
	defer server.Close()
	defer api.Close()

	wsURL := wsURL(server.URL)

	// Connect with cursor=1, expecting replay of messages 2 and 3.
	user := testClientWithCursor(t, wsURL, 1)
	defer user.conn.Close()

	waitForConnectedClients(api, 1)

	// Simulate race: Notify message 2 arrives via live path while replay is happening.
	// The shouldSend dedup should filter this out since replay already sends it.
	api.Notify(1, &model.MessageExternal{ID: 2, ApplicationID: 1, Message: "msg-2"})

	// Collect all messages received within a short window.
	msgs := expectMultipleMessages(user, 2)

	// We should get exactly messages 2 and 3, each appearing exactly once.
	ids := make(map[uint]int)
	for _, msg := range msgs {
		ids[msg.ID]++
	}
	assert.Equal(t, 1, ids[2], "message 2 should appear exactly once")
	assert.Equal(t, 1, ids[3], "message 3 should appear exactly once")

	// New message after replay should work.
	api.Notify(1, &model.MessageExternal{ID: 4, ApplicationID: 1, Message: "msg-4"})
	user.expectMessage(&model.MessageExternal{ID: 4, ApplicationID: 1, Message: "msg-4"})

	user.expectNoMessage()
}

// TestReconnect_MultiAppMessages verifies that messages from multiple applications
// are replayed in correct ID order (not grouped by application).
func TestReconnect_MultiAppMessages(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.Check(t)()

	fetcher := newMockFetcher()
	// Messages from two different apps interleaved by ID.
	fetcher.addMessage(1, &model.MessageExternal{ID: 1, ApplicationID: 1, Message: "app1-msg1"})
	fetcher.addMessage(1, &model.MessageExternal{ID: 2, ApplicationID: 2, Message: "app2-msg1"})
	fetcher.addMessage(1, &model.MessageExternal{ID: 3, ApplicationID: 1, Message: "app1-msg2"})
	fetcher.addMessage(1, &model.MessageExternal{ID: 4, ApplicationID: 2, Message: "app2-msg2"})
	fetcher.addMessage(1, &model.MessageExternal{ID: 5, ApplicationID: 1, Message: "app1-msg3"})

	server, api := bootReplayTestServer(fetcher, staticUserID())
	defer server.Close()
	defer api.Close()

	wsURL := wsURL(server.URL)

	// Reconnect with cursor=1, expecting messages 2, 3, 4, 5 in ID order.
	user := testClientWithCursor(t, wsURL, 1)
	defer user.conn.Close()

	waitForConnectedClients(api, 1)

	msgs := expectMultipleMessages(user, 4)
	assert.Equal(t, uint(2), msgs[0].ID)
	assert.Equal(t, uint(2), msgs[0].ApplicationID)
	assert.Equal(t, uint(3), msgs[1].ID)
	assert.Equal(t, uint(1), msgs[1].ApplicationID)
	assert.Equal(t, uint(4), msgs[2].ID)
	assert.Equal(t, uint(2), msgs[2].ApplicationID)
	assert.Equal(t, uint(5), msgs[3].ID)
	assert.Equal(t, uint(1), msgs[3].ApplicationID)

	// Live messages from both apps should work.
	api.Notify(1, &model.MessageExternal{ID: 6, ApplicationID: 2, Message: "app2-msg3"})
	user.expectMessage(&model.MessageExternal{ID: 6, ApplicationID: 2, Message: "app2-msg3"})

	api.Notify(1, &model.MessageExternal{ID: 7, ApplicationID: 1, Message: "app1-msg4"})
	user.expectMessage(&model.MessageExternal{ID: 7, ApplicationID: 1, Message: "app1-msg4"})

	user.expectNoMessage()
}

// TestReconnect_CursorIsLatest verifies that when the cursor points to the
// latest message, no replay occurs and only new messages are received.
func TestReconnect_CursorIsLatest(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.Check(t)()

	fetcher := newMockFetcher()
	fetcher.addMessage(1, &model.MessageExternal{ID: 1, ApplicationID: 1, Message: "msg-1"})
	fetcher.addMessage(1, &model.MessageExternal{ID: 2, ApplicationID: 1, Message: "msg-2"})
	fetcher.addMessage(1, &model.MessageExternal{ID: 3, ApplicationID: 1, Message: "msg-3"})

	server, api := bootReplayTestServer(fetcher, staticUserID())
	defer server.Close()
	defer api.Close()

	wsURL := wsURL(server.URL)

	// Cursor=3 is the latest message; nothing to replay.
	user := testClientWithCursor(t, wsURL, 3)
	defer user.conn.Close()

	waitForConnectedClients(api, 1)

	// No replay messages expected.
	user.expectNoMessage()

	// New message should arrive normally.
	api.Notify(1, &model.MessageExternal{ID: 4, ApplicationID: 1, Message: "msg-4"})
	user.expectMessage(&model.MessageExternal{ID: 4, ApplicationID: 1, Message: "msg-4"})
}

// TestInvalidCursor_NotFound verifies that a cursor pointing to a non-existent
// message returns HTTP 404 and does not upgrade to WebSocket.
func TestInvalidCursor_NotFound(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.Check(t)()

	fetcher := newMockFetcher()
	fetcher.addMessage(1, &model.MessageExternal{ID: 1, ApplicationID: 1, Message: "msg-1"})

	server, api := bootReplayTestServer(fetcher, staticUserID())
	defer server.Close()
	defer api.Close()

	wsURL := wsURL(server.URL)
	testClientCursorExpectingHTTPError(t, wsURL, 999, http.StatusNotFound)
}

// TestInvalidCursor_WrongUser verifies that a cursor pointing to a message
// belonging to a different user returns HTTP 404.
func TestInvalidCursor_WrongUser(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.Check(t)()

	fetcher := newMockFetcher()
	// Message 5 belongs to user 2, not user 1.
	fetcher.addMessage(2, &model.MessageExternal{ID: 5, ApplicationID: 2, Message: "user2-msg"})

	server, api := bootReplayTestServer(fetcher, staticUserID())
	defer server.Close()
	defer api.Close()

	wsURL := wsURL(server.URL)

	// User 1 tries to use cursor=5 which belongs to user 2.
	testClientCursorExpectingHTTPError(t, wsURL, 5, http.StatusNotFound)
}

// TestInvalidCursor_BadFormat verifies that invalid cursor values
// (non-numeric, zero, negative) return HTTP 400.
func TestInvalidCursor_BadFormat(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.Check(t)()

	fetcher := newMockFetcher()
	server, api := bootReplayTestServer(fetcher, staticUserID())
	defer server.Close()
	defer api.Close()

	wsURL := wsURL(server.URL)

	// Non-numeric value.
	testClientCursorExpectingError(t, wsURL, "abc", http.StatusBadRequest)

	// Zero value.
	testClientCursorExpectingError(t, wsURL, "0", http.StatusBadRequest)

	// Negative value.
	testClientCursorExpectingError(t, wsURL, "-1", http.StatusBadRequest)
}

// TestReconnect_NoDuplicatesWhenNotifyArrivesBeforeReplay verifies the scenario
// where a Notify message arrives after registration but before replay sends it.
func TestReconnect_NoDuplicatesWhenNotifyArrivesBeforeReplay(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.Check(t)()

	fetcher := newMockFetcher()
	fetcher.addMessage(1, &model.MessageExternal{ID: 1, ApplicationID: 1, Message: "msg-1"})
	fetcher.addMessage(1, &model.MessageExternal{ID: 2, ApplicationID: 1, Message: "msg-2"})

	server, api := bootReplayTestServer(fetcher, staticUserID())
	defer server.Close()
	defer api.Close()

	wsURL := wsURL(server.URL)

	// Connect with cursor=1.
	user := testClientWithCursor(t, wsURL, 1)
	defer user.conn.Close()

	waitForConnectedClients(api, 1)

	// Simulate Notify for message 2 arriving via the live path
	// (could happen between registration and replay completion).
	api.Notify(1, &model.MessageExternal{ID: 2, ApplicationID: 1, Message: "msg-2"})

	// Collect all messages — should be exactly one copy of msg-2.
	msgs := expectMultipleMessages(user, 1)
	assert.Equal(t, uint(2), msgs[0].ID)
	assert.Equal(t, "msg-2", msgs[0].Message)

	// Ensure no duplicate arrives.
	user.expectNoMessage()

	// New message after should work fine.
	api.Notify(1, &model.MessageExternal{ID: 3, ApplicationID: 1, Message: "msg-3"})
	user.expectMessage(&model.MessageExternal{ID: 3, ApplicationID: 1, Message: "msg-3"})
}

// TestReconnect_ReplayNotAvailableWhenFetcherNil verifies that when no fetcher
// is configured, a cursor parameter returns HTTP 503.
func TestReconnect_ReplayNotAvailableWhenFetcherNil(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.Check(t)()

	// Boot server with nil fetcher (replay disabled).
	server, api := bootReplayTestServer(nil, staticUserID())
	defer server.Close()
	defer api.Close()

	wsURL := wsURL(server.URL)
	testClientCursorExpectingHTTPError(t, wsURL, 5, http.StatusServiceUnavailable)
}

// TestReconnect_MultipleClientsIndependent verifies that two clients of the same
// user can reconnect independently with different cursors.
func TestReconnect_MultipleClientsIndependent(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.Check(t)()

	fetcher := newMockFetcher()
	fetcher.addMessage(1, &model.MessageExternal{ID: 1, ApplicationID: 1, Message: "msg-1"})
	fetcher.addMessage(1, &model.MessageExternal{ID: 2, ApplicationID: 1, Message: "msg-2"})
	fetcher.addMessage(1, &model.MessageExternal{ID: 3, ApplicationID: 1, Message: "msg-3"})
	fetcher.addMessage(1, &model.MessageExternal{ID: 4, ApplicationID: 1, Message: "msg-4"})

	server, api := bootReplayTestServer(fetcher, staticUserID())
	defer server.Close()
	defer api.Close()

	wsURL := wsURL(server.URL)

	// Client A reconnects with cursor=2, expecting messages 3 and 4.
	clientA := testClientWithCursor(t, wsURL, 2)
	defer clientA.conn.Close()

	// Client B reconnects with cursor=3, expecting message 4.
	clientB := testClientWithCursor(t, wsURL, 3)
	defer clientB.conn.Close()

	waitForConnectedClients(api, 2)

	// Client A should get messages 3 and 4.
	msgsA := expectMultipleMessages(clientA, 2)
	assert.Equal(t, uint(3), msgsA[0].ID)
	assert.Equal(t, uint(4), msgsA[1].ID)

	// Client B should get only message 4.
	msgsB := expectMultipleMessages(clientB, 1)
	assert.Equal(t, uint(4), msgsB[0].ID)

	// Both should receive new messages.
	api.Notify(1, &model.MessageExternal{ID: 5, ApplicationID: 1, Message: "msg-5"})
	clientA.expectMessage(&model.MessageExternal{ID: 5, ApplicationID: 1, Message: "msg-5"})
	clientB.expectMessage(&model.MessageExternal{ID: 5, ApplicationID: 1, Message: "msg-5"})

	clientA.expectNoMessage()
	clientB.expectNoMessage()
}

// TestReconnect_NoCrossUserLeak verifies that replay only returns messages
// belonging to the authenticated user, even when the fetcher has messages
// for other users.
func TestReconnect_NoCrossUserLeak(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.Check(t)()

	fetcher := newMockFetcher()
	// User 1 messages.
	fetcher.addMessage(1, &model.MessageExternal{ID: 1, ApplicationID: 1, Message: "user1-msg1"})
	fetcher.addMessage(1, &model.MessageExternal{ID: 3, ApplicationID: 1, Message: "user1-msg2"})
	// User 2 messages (should not leak to user 1).
	fetcher.addMessage(2, &model.MessageExternal{ID: 2, ApplicationID: 2, Message: "user2-msg1"})
	fetcher.addMessage(2, &model.MessageExternal{ID: 4, ApplicationID: 2, Message: "user2-msg2"})

	server, api := bootReplayTestServer(fetcher, staticUserID())
	defer server.Close()
	defer api.Close()

	wsURL := wsURL(server.URL)

	// User 1 reconnects with cursor=0 (all messages).
	// Note: cursor=0 is invalid, so use cursor for msg 1.
	user := testClientWithCursor(t, wsURL, 1)
	defer user.conn.Close()

	waitForConnectedClients(api, 1)

	// Should only get user 1's message 3 (ID > 1).
	msgs := expectMultipleMessages(user, 1)
	assert.Equal(t, uint(3), msgs[0].ID)
	assert.Equal(t, "user1-msg2", msgs[0].Message)

	// No user 2 messages should appear.
	user.expectNoMessage()
}

// TestReconnect_InvalidCursorWithQueryString verifies various invalid cursor
// query string formats are rejected.
func TestReconnect_InvalidCursorWithQueryString(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.Check(t)()

	fetcher := newMockFetcher()
	server, api := bootReplayTestServer(fetcher, staticUserID())
	defer server.Close()
	defer api.Close()

	baseURL := wsURL(server.URL)

	tests := []struct {
		name   string
		cursor string
		status int
	}{
		{"empty string", "", http.StatusBadRequest},
		{"float value", "1.5", http.StatusBadRequest},
		{"very large number", "999999999999999999999", http.StatusBadRequest},
		{"special chars", "!@#$", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wsURL := baseURL
			if tt.cursor != "" {
				wsURL = baseURL + "?lastMessageID=" + tt.cursor
			}
			_, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
			if err == nil {
				// Empty cursor string means no lastMessageID param, which is valid (first connection).
				if tt.cursor != "" {
					t.Fatal("expected WebSocket dial error but got nil")
				}
			}
			if resp != nil && tt.cursor != "" {
				assert.Equal(t, tt.status, resp.StatusCode, "cursor=%q", tt.cursor)
				resp.Body.Close()
			}
		})
	}
}

// TestReconnect_ReplayTransitionsToLiveSeamlessly is an end-to-end test:
// connect → replay → live → disconnect → reconnect with new cursor.
func TestReconnect_ReplayTransitionsToLiveSeamlessly(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.Check(t)()

	fetcher := newMockFetcher()
	fetcher.addMessage(1, &model.MessageExternal{ID: 1, ApplicationID: 1, Message: "msg-1"})

	server, api := bootReplayTestServer(fetcher, staticUserID())
	defer server.Close()
	defer api.Close()

	wsURL := wsURL(server.URL)

	// Phase 1: First connection, no cursor.
	user := testClient(t, wsURL)
	waitForConnectedClients(api, 1)

	// Receive a live message.
	api.Notify(1, &model.MessageExternal{ID: 2, ApplicationID: 1, Message: "msg-2"})
	user.expectMessage(&model.MessageExternal{ID: 2, ApplicationID: 1, Message: "msg-2"})

	// Disconnect.
	user.conn.Close()
	time.Sleep(100 * time.Millisecond)

	// Phase 2: Add more messages while "disconnected".
	fetcher.addMessage(1, &model.MessageExternal{ID: 2, ApplicationID: 1, Message: "msg-2"})
	fetcher.addMessage(1, &model.MessageExternal{ID: 3, ApplicationID: 1, Message: "msg-3"})
	fetcher.addMessage(1, &model.MessageExternal{ID: 4, ApplicationID: 1, Message: "msg-4"})

	// Reconnect with cursor=2 (last processed).
	user2 := testClientWithCursor(t, wsURL, 2)
	defer user2.conn.Close()

	waitForConnectedClients(api, 1)

	// Should replay messages 3 and 4.
	msgs := expectMultipleMessages(user2, 2)
	assert.Equal(t, uint(3), msgs[0].ID)
	assert.Equal(t, "msg-3", msgs[0].Message)
	assert.Equal(t, uint(4), msgs[1].ID)
	assert.Equal(t, "msg-4", msgs[1].Message)

	// New live message should work.
	api.Notify(1, &model.MessageExternal{ID: 5, ApplicationID: 1, Message: "msg-5"})
	user2.expectMessage(&model.MessageExternal{ID: 5, ApplicationID: 1, Message: "msg-5"})

	user2.expectNoMessage()
}

// TestReconnect_LargeReplayBatch verifies replay works correctly with many messages.
func TestReconnect_LargeReplayBatch(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.Check(t)()

	fetcher := newMockFetcher()
	// Create 50 messages.
	for i := uint(1); i <= 50; i++ {
		fetcher.addMessage(1, &model.MessageExternal{
			ID:            i,
			ApplicationID: 1,
			Message:       fmt.Sprintf("msg-%d", i),
		})
	}

	server, api := bootReplayTestServer(fetcher, staticUserID())
	defer server.Close()
	defer api.Close()

	wsURL := wsURL(server.URL)

	// Reconnect with cursor=10, expecting messages 11-50 (40 messages).
	user := testClientWithCursor(t, wsURL, 10)
	defer user.conn.Close()

	waitForConnectedClients(api, 1)

	msgs := expectMultipleMessages(user, 40)

	// Verify order and completeness.
	for i, msg := range msgs {
		expectedID := uint(11 + i)
		assert.Equal(t, expectedID, msg.ID, "message at index %d", i)
		assert.Equal(t, fmt.Sprintf("msg-%d", expectedID), msg.Message)
	}

	// Live message after large replay.
	api.Notify(1, &model.MessageExternal{ID: 51, ApplicationID: 1, Message: "msg-51"})
	user.expectMessage(&model.MessageExternal{ID: 51, ApplicationID: 1, Message: "msg-51"})

	user.expectNoMessage()
}
