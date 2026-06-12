package stream

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/gorilla/websocket"
	"github.com/gotify/server/v2/mode"
	"github.com/gotify/server/v2/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeHistory is an in-memory MessageHistory used to drive the replay logic deterministically.
// Its messages are kept sorted ascending by id, mirroring the real database query contract.
type fakeHistory struct {
	mu       sync.Mutex
	messages []*model.MessageExternal
	err      error
	calls    int
}

func (f *fakeHistory) GetMessagesAfter(userID, after uint, limit int) ([]*model.MessageExternal, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	var out []*model.MessageExternal
	for _, m := range f.messages {
		if m.ID > after {
			out = append(out, m)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (f *fakeHistory) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func extMsg(id, appID uint, message string) *model.MessageExternal {
	return &model.MessageExternal{ID: id, ApplicationID: appID, Message: message}
}

func resumeClient(t *testing.T, server *httptest.Server, since string) *testingClient {
	return testClient(t, wsURL(server.URL)+"/?since="+since)
}

// Scenario: first connection (no resume cursor). The history must not be queried and no backlog
// must be replayed; the client only receives messages produced from now on.
func TestResume_FirstConnection_NoReplay(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.Check(t)()

	history := &fakeHistory{messages: []*model.MessageExternal{
		extMsg(1, 10, "old-1"),
		extMsg(2, 10, "old-2"),
		extMsg(3, 10, "old-3"),
	}}
	server, api := bootTestServerWithHistory(staticUserID(), history)
	defer api.Close()
	defer server.Close()

	user := testClient(t, wsURL(server.URL)) // no `since` query => first connection
	defer user.conn.Close()

	waitForConnectedClients(api, 1)

	// No replay on a first connection.
	user.expectNoMessage()
	assert.Equal(t, 0, history.callCount(), "history must not be queried without a cursor")

	// Live messages are still delivered as usual.
	api.Notify(1, extMsg(4, 10, "live-4"))
	user.expectMessage(extMsg(4, 10, "live-4"))
}

// Scenario: disconnect and reconnect. The client comes back with the id of the last message it
// processed and receives exactly the messages created after it, in order, then resumes the live
// stream. Messages that overlap the replay (already delivered) are de-duplicated.
func TestResume_Reconnect_ReplaysMissedThenLive(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.Check(t)()

	history := &fakeHistory{messages: []*model.MessageExternal{
		extMsg(1, 10, "m1"),
		extMsg(2, 10, "m2"),
		extMsg(3, 10, "m3"),
		extMsg(4, 10, "m4"),
		extMsg(5, 10, "m5"),
	}}
	server, api := bootTestServerWithHistory(staticUserID(), history)
	defer api.Close()
	defer server.Close()

	// The client last processed message 2; everything after it was missed while disconnected.
	user := resumeClient(t, server, "2")
	defer user.conn.Close()

	waitForConnectedClients(api, 1)

	// Missed messages are replayed in chronological (ascending id) order.
	user.expectMessage(extMsg(3, 10, "m3"))
	user.expectMessage(extMsg(4, 10, "m4"))
	user.expectMessage(extMsg(5, 10, "m5"))
	user.expectNoMessage()

	// A live duplicate of an already-replayed message (id <= last replayed) is skipped.
	api.Notify(1, extMsg(3, 10, "m3"))
	user.expectNoMessage()

	// Also skip a duplicate of the boundary (the most recently replayed) message.
	time.Sleep(50 * time.Millisecond) // let the replay's lastSentID settle to 5
	api.Notify(1, extMsg(5, 10, "m5"))
	user.expectNoMessage()

	// Genuinely new live messages flow through and continue the stream.
	api.Notify(1, extMsg(6, 10, "live-6"))
	user.expectMessage(extMsg(6, 10, "live-6"))
	api.Notify(1, extMsg(7, 10, "live-7"))
	user.expectMessage(extMsg(7, 10, "live-7"))
}

// Scenario: mixed messages from multiple applications. The replay must return them interleaved by
// id (global chronological order), not grouped per application.
func TestResume_MultipleApplications_MixedOrder(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.Check(t)()

	const appA, appB uint = 10, 20
	history := &fakeHistory{messages: []*model.MessageExternal{
		extMsg(1, appA, "a-1"), // already seen (cursor = 1)
		extMsg(2, appB, "b-2"),
		extMsg(3, appA, "a-3"),
		extMsg(4, appB, "b-4"),
		extMsg(5, appA, "a-5"),
	}}
	server, api := bootTestServerWithHistory(staticUserID(), history)
	defer api.Close()
	defer server.Close()

	user := resumeClient(t, server, "1")
	defer user.conn.Close()

	waitForConnectedClients(api, 1)

	// Replayed strictly by id, with the two applications interleaved.
	user.expectMessage(extMsg(2, appB, "b-2"))
	user.expectMessage(extMsg(3, appA, "a-3"))
	user.expectMessage(extMsg(4, appB, "b-4"))
	user.expectMessage(extMsg(5, appA, "a-5"))
	user.expectNoMessage()

	// The live stream continues, regardless of which application produces the next message.
	api.Notify(1, extMsg(6, appB, "b-6"))
	user.expectMessage(extMsg(6, appB, "b-6"))
}

// Scenario: invalid cursor (malformed value). The request must be rejected with 400 before the
// connection is upgraded, so the client gets a clear error instead of a silently broken stream.
func TestResume_InvalidCursor_Malformed_Returns400(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.Check(t)()

	history := &fakeHistory{}
	server, api := bootTestServerWithHistory(staticUserID(), history)
	defer api.Close()
	defer server.Close()

	for _, bad := range []string{"notanumber", "-1", "1.5", "12abc"} {
		conn, resp, err := websocket.DefaultDialer.Dial(wsURL(server.URL)+"/?since="+bad, nil)
		require.Error(t, err, "cursor %q must be rejected", bad)
		assert.Equal(t, websocket.ErrBadHandshake, err)
		require.NotNil(t, resp)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "cursor %q", bad)
		resp.Body.Close()
		if conn != nil {
			conn.Close()
		}
	}

	// No connection was ever upgraded, so nothing should be registered and the history is untouched.
	assert.Equal(t, 0, countClients(api))
	assert.Equal(t, 0, history.callCount())
}

// Scenario: invalid cursor (numerically valid but ahead of everything we have). There is nothing
// to replay, so the stream comes up cleanly and simply continues with live messages.
func TestResume_InvalidCursor_AheadOfData_NoReplay(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.Check(t)()

	history := &fakeHistory{messages: []*model.MessageExternal{
		extMsg(1, 10, "m1"),
		extMsg(2, 10, "m2"),
		extMsg(3, 10, "m3"),
	}}
	server, api := bootTestServerWithHistory(staticUserID(), history)
	defer api.Close()
	defer server.Close()

	user := resumeClient(t, server, "100") // cursor past the newest message id
	defer user.conn.Close()

	waitForConnectedClients(api, 1)

	// The cursor was consulted, but there is nothing newer to replay.
	user.expectNoMessage()
	assert.GreaterOrEqual(t, history.callCount(), 1)

	// New live messages (necessarily with a higher id) are still delivered.
	api.Notify(1, extMsg(101, 10, "live-101"))
	user.expectMessage(extMsg(101, 10, "live-101"))
}

// Scenario: a history backend error during replay must not drop the connection; the client keeps
// its live subscription instead of being forced to retry.
func TestResume_HistoryError_KeepsLiveStream(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.Check(t)()

	history := &fakeHistory{err: errors.New("history unavailable")}
	server, api := bootTestServerWithHistory(staticUserID(), history)
	defer api.Close()
	defer server.Close()

	user := resumeClient(t, server, "2")
	defer user.conn.Close()

	waitForConnectedClients(api, 1)

	// Replay failed, but the connection stays open and live messages flow.
	user.expectNoMessage()
	assert.GreaterOrEqual(t, history.callCount(), 1)

	api.Notify(1, extMsg(9, 10, "live-9"))
	user.expectMessage(extMsg(9, 10, "live-9"))
}
