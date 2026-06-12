package stream

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
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

func TestFailureOnNormalHttpRequest(t *testing.T) {
	mode.Set(mode.TestDev)

	defer leaktest.Check(t)()

	server, api := bootTestServer(staticUserID())
	defer server.Close()
	defer api.Close()

	resp, err := http.Get(server.URL)
	assert.Nil(t, err)
	assert.Equal(t, 400, resp.StatusCode)
	resp.Body.Close()
}

func TestWriteMessageFails(t *testing.T) {
	mode.Set(mode.TestDev)
	oldWrite := writeJSON
	// try emulate an write error, mostly this should kill the ReadMessage goroutine first but you'll never know.
	writeJSON = func(conn *websocket.Conn, v interface{}) error {
		return errors.New("asd")
	}
	defer func() {
		writeJSON = oldWrite
	}()
	defer leaktest.Check(t)()

	server, api := bootTestServer(func(context *gin.Context) {
		auth.RegisterClient(context, &model.Client{UserID: 1})
	})
	defer server.Close()
	defer api.Close()

	wsURL := wsURL(server.URL)
	user := testClient(t, wsURL)

	waitForConnectedClients(api, 1)

	clients := clients(api, 1)
	assert.NotEmpty(t, clients)

	api.Notify(1, &model.MessageExternal{Message: "HI"})
	user.expectNoMessage()
}

func TestWritePingFails(t *testing.T) {
	mode.Set(mode.TestDev)
	oldPing := ping
	// try emulate an write error, mostly this should kill the ReadMessage gorouting first but you'll never know.
	ping = func(conn *websocket.Conn) error {
		return errors.New("asd")
	}
	defer func() {
		ping = oldPing
	}()

	defer leaktest.CheckTimeout(t, 10*time.Second)()

	server, api := bootTestServer(staticUserID())
	defer api.Close()
	defer server.Close()

	wsURL := wsURL(server.URL)
	user := testClient(t, wsURL)
	defer user.conn.Close()

	waitForConnectedClients(api, 1)

	clients := clients(api, 1)

	assert.NotEmpty(t, clients)

	time.Sleep(api.pingPeriod + (50 * time.Millisecond)) // waiting for ping

	api.Notify(1, &model.MessageExternal{Message: "HI"})
	user.expectNoMessage()
}

func TestPing(t *testing.T) {
	mode.Set(mode.TestDev)

	server, api := bootTestServer(staticUserID())
	defer server.Close()
	defer api.Close()

	wsURL := wsURL(server.URL)

	user := createClient(t, wsURL)
	defer user.conn.Close()

	ping := make(chan bool)
	oldPingHandler := user.conn.PingHandler()
	user.conn.SetPingHandler(func(appData string) error {
		err := oldPingHandler(appData)
		ping <- true
		return err
	})

	startReading(user)

	expectNoMessage(user)

	select {
	case <-time.After(2 * time.Second):
		assert.Fail(t, "Expected ping but there was one :(")
	case <-ping:
		// expected
	}

	expectNoMessage(user)
	api.Notify(1, &model.MessageExternal{Message: "HI"})
	user.expectMessage(&model.MessageExternal{Message: "HI"})
}

func TestCloseClientOnNotReading(t *testing.T) {
	mode.Set(mode.TestDev)

	server, api := bootTestServer(staticUserID())
	defer server.Close()
	defer api.Close()

	wsURL := wsURL(server.URL)

	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	assert.Nil(t, err)
	defer ws.Close()

	waitForConnectedClients(api, 1)

	assert.NotEmpty(t, clients(api, 1))

	time.Sleep(api.pingPeriod + api.pongTimeout)

	assert.Empty(t, clients(api, 1))
}

func TestMessageDirectlyAfterConnect(t *testing.T) {
	mode.Set(mode.Prod)
	defer leaktest.Check(t)()
	server, api := bootTestServer(staticUserID())
	defer server.Close()
	defer api.Close()

	wsURL := wsURL(server.URL)

	user := testClient(t, wsURL)
	defer user.conn.Close()

	waitForConnectedClients(api, 1)

	api.Notify(1, &model.MessageExternal{Message: "msg"})
	user.expectMessage(&model.MessageExternal{Message: "msg"})
}

func TestDeleteClientShouldCloseConnection(t *testing.T) {
	mode.Set(mode.Prod)
	defer leaktest.Check(t)()
	server, api := bootTestServer(staticUserID())
	defer server.Close()
	defer api.Close()

	wsURL := wsURL(server.URL)

	user := testClient(t, wsURL)
	defer user.conn.Close()

	waitForConnectedClients(api, 1)

	api.Notify(1, &model.MessageExternal{Message: "msg"})
	user.expectMessage(&model.MessageExternal{Message: "msg"})

	api.NotifyDeletedClient(1, "customtoken")

	api.Notify(1, &model.MessageExternal{Message: "msg"})
	user.expectNoMessage()
}

func TestDeleteMultipleClients(t *testing.T) {
	mode.Set(mode.TestDev)

	defer leaktest.Check(t)()
	userIDs := []uint{1, 1, 1, 1, 2, 2, 3}
	tokens := []string{"1-1", "1-2", "1-2", "1-3", "2-1", "2-2", "3"}
	i := 0
	server, api := bootTestServer(func(context *gin.Context) {
		auth.RegisterClient(context, &model.Client{UserID: userIDs[i], Token: tokens[i]})
		i++
	})
	defer server.Close()

	wsURL := wsURL(server.URL)

	userOneIPhone := testClient(t, wsURL)
	defer userOneIPhone.conn.Close()
	userOneAndroid := testClient(t, wsURL)
	defer userOneAndroid.conn.Close()
	userOneBrowser := testClient(t, wsURL)
	defer userOneBrowser.conn.Close()
	userOneOther := testClient(t, wsURL)
	defer userOneOther.conn.Close()
	userOne := []*testingClient{userOneAndroid, userOneBrowser, userOneIPhone, userOneOther}

	userTwoBrowser := testClient(t, wsURL)
	defer userTwoBrowser.conn.Close()
	userTwoAndroid := testClient(t, wsURL)
	defer userTwoAndroid.conn.Close()
	userTwo := []*testingClient{userTwoAndroid, userTwoBrowser}

	userThreeAndroid := testClient(t, wsURL)
	defer userThreeAndroid.conn.Close()
	userThree := []*testingClient{userThreeAndroid}

	waitForConnectedClients(api, len(userOne)+len(userTwo)+len(userThree))

	api.Notify(1, &model.MessageExternal{ID: 4, Message: "there"})
	expectMessage(&model.MessageExternal{ID: 4, Message: "there"}, userOne...)
	expectNoMessage(userTwo...)
	expectNoMessage(userThree...)

	api.NotifyDeletedClient(1, "1-2")

	api.Notify(1, &model.MessageExternal{ID: 2, Message: "there"})
	expectMessage(&model.MessageExternal{ID: 2, Message: "there"}, userOneIPhone, userOneOther)
	expectNoMessage(userOneBrowser, userOneAndroid)
	expectNoMessage(userThree...)
	expectNoMessage(userTwo...)

	api.Notify(2, &model.MessageExternal{ID: 2, Message: "there"})
	expectNoMessage(userOne...)
	expectMessage(&model.MessageExternal{ID: 2, Message: "there"}, userTwo...)
	expectNoMessage(userThree...)

	api.Notify(3, &model.MessageExternal{ID: 5, Message: "there"})
	expectNoMessage(userOne...)
	expectNoMessage(userTwo...)
	expectMessage(&model.MessageExternal{ID: 5, Message: "there"}, userThree...)

	api.Close()
}

func TestDeleteUser(t *testing.T) {
	mode.Set(mode.TestDev)

	defer leaktest.Check(t)()
	userIDs := []uint{1, 1, 1, 1, 2, 2, 3}
	tokens := []string{"1-1", "1-2", "1-2", "1-3", "2-1", "2-2", "3"}
	i := 0
	server, api := bootTestServer(func(context *gin.Context) {
		auth.RegisterClient(context, &model.Client{UserID: userIDs[i], Token: tokens[i]})
		i++
	})
	defer server.Close()

	wsURL := wsURL(server.URL)

	userOneIPhone := testClient(t, wsURL)
	defer userOneIPhone.conn.Close()
	userOneAndroid := testClient(t, wsURL)
	defer userOneAndroid.conn.Close()
	userOneBrowser := testClient(t, wsURL)
	defer userOneBrowser.conn.Close()
	userOneOther := testClient(t, wsURL)
	defer userOneOther.conn.Close()
	userOne := []*testingClient{userOneAndroid, userOneBrowser, userOneIPhone, userOneOther}

	userTwoBrowser := testClient(t, wsURL)
	defer userTwoBrowser.conn.Close()
	userTwoAndroid := testClient(t, wsURL)
	defer userTwoAndroid.conn.Close()
	userTwo := []*testingClient{userTwoAndroid, userTwoBrowser}

	userThreeAndroid := testClient(t, wsURL)
	defer userThreeAndroid.conn.Close()
	userThree := []*testingClient{userThreeAndroid}

	waitForConnectedClients(api, len(userOne)+len(userTwo)+len(userThree))

	api.Notify(1, &model.MessageExternal{ID: 4, Message: "there"})
	expectMessage(&model.MessageExternal{ID: 4, Message: "there"}, userOne...)
	expectNoMessage(userTwo...)
	expectNoMessage(userThree...)

	api.NotifyDeletedUser(1)

	api.Notify(1, &model.MessageExternal{ID: 2, Message: "there"})
	expectNoMessage(userOne...)
	expectNoMessage(userThree...)
	expectNoMessage(userTwo...)

	api.Notify(2, &model.MessageExternal{ID: 2, Message: "there"})
	expectNoMessage(userOne...)
	expectMessage(&model.MessageExternal{ID: 2, Message: "there"}, userTwo...)
	expectNoMessage(userThree...)

	api.Notify(3, &model.MessageExternal{ID: 5, Message: "there"})
	expectNoMessage(userOne...)
	expectNoMessage(userTwo...)
	expectMessage(&model.MessageExternal{ID: 5, Message: "there"}, userThree...)

	api.Close()
}

func TestCollectConnectedClientTokens(t *testing.T) {
	mode.Set(mode.TestDev)

	defer leaktest.Check(t)()
	userIDs := []uint{1, 1, 1, 2, 2}
	tokens := []string{"1-1", "1-2", "1-2", "2-1", "2-2"}
	i := 0
	server, api := bootTestServer(func(context *gin.Context) {
		auth.RegisterClient(context, &model.Client{UserID: userIDs[i], Token: tokens[i]})
		i++
	})
	defer server.Close()

	wsURL := wsURL(server.URL)
	userOneConnOne := testClient(t, wsURL)
	defer userOneConnOne.conn.Close()
	userOneConnTwo := testClient(t, wsURL)
	defer userOneConnTwo.conn.Close()
	userOneConnThree := testClient(t, wsURL)
	defer userOneConnThree.conn.Close()
	waitForConnectedClients(api, 3)

	ret := api.CollectConnectedClientTokens()
	sort.Strings(ret)
	assert.Equal(t, []string{"1-1", "1-2"}, ret)

	userTwoConnOne := testClient(t, wsURL)
	defer userTwoConnOne.conn.Close()
	userTwoConnTwo := testClient(t, wsURL)
	defer userTwoConnTwo.conn.Close()
	waitForConnectedClients(api, 5)

	ret = api.CollectConnectedClientTokens()
	sort.Strings(ret)
	assert.Equal(t, []string{"1-1", "1-2", "2-1", "2-2"}, ret)
}

func TestMultipleClients(t *testing.T) {
	mode.Set(mode.TestDev)

	defer leaktest.Check(t)()
	userIDs := []uint{1, 1, 1, 2, 2, 3}
	i := 0
	server, api := bootTestServer(func(context *gin.Context) {
		auth.RegisterClient(context, &model.Client{UserID: userIDs[i], Token: "t" + fmt.Sprint(userIDs[i])})
		i++
	})
	defer server.Close()

	wsURL := wsURL(server.URL)

	userOneIPhone := testClient(t, wsURL)
	defer userOneIPhone.conn.Close()
	userOneAndroid := testClient(t, wsURL)
	defer userOneAndroid.conn.Close()
	userOneBrowser := testClient(t, wsURL)
	defer userOneBrowser.conn.Close()
	userOne := []*testingClient{userOneAndroid, userOneBrowser, userOneIPhone}

	userTwoBrowser := testClient(t, wsURL)
	defer userTwoBrowser.conn.Close()
	userTwoAndroid := testClient(t, wsURL)
	defer userTwoAndroid.conn.Close()
	userTwo := []*testingClient{userTwoAndroid, userTwoBrowser}

	userThreeAndroid := testClient(t, wsURL)
	defer userThreeAndroid.conn.Close()
	userThree := []*testingClient{userThreeAndroid}

	waitForConnectedClients(api, len(userOne)+len(userTwo)+len(userThree))

	// there should not be messages at the beginning
	expectNoMessage(userOne...)
	expectNoMessage(userTwo...)
	expectNoMessage(userThree...)

	api.Notify(1, &model.MessageExternal{ID: 1, Message: "hello"})
	time.Sleep(500 * time.Millisecond)
	expectMessage(&model.MessageExternal{ID: 1, Message: "hello"}, userOne...)
	expectNoMessage(userTwo...)
	expectNoMessage(userThree...)

	api.Notify(2, &model.MessageExternal{ID: 2, Message: "there"})
	expectNoMessage(userOne...)
	expectMessage(&model.MessageExternal{ID: 2, Message: "there"}, userTwo...)
	expectNoMessage(userThree...)

	userOneIPhone.conn.Close()

	expectNoMessage(userOne...)
	expectNoMessage(userTwo...)
	expectNoMessage(userThree...)

	api.Notify(1, &model.MessageExternal{ID: 3, Message: "how"})
	expectMessage(&model.MessageExternal{ID: 3, Message: "how"}, userOneAndroid, userOneBrowser)
	expectNoMessage(userOneIPhone)
	expectNoMessage(userTwo...)
	expectNoMessage(userThree...)

	api.Notify(2, &model.MessageExternal{ID: 4, Message: "are"})

	expectNoMessage(userOne...)
	expectMessage(&model.MessageExternal{ID: 4, Message: "are"}, userTwo...)
	expectNoMessage(userThree...)

	api.Close()

	api.Notify(2, &model.MessageExternal{ID: 5, Message: "you"})

	expectNoMessage(userOne...)
	expectNoMessage(userTwo...)
	expectNoMessage(userThree...)
}

func Test_sameOrigin_returnsTrue(t *testing.T) {
	mode.Set(mode.Prod)
	req := httptest.NewRequest("GET", "http://example.com/stream", nil)
	req.Header.Set("Origin", "http://example.com")
	actual := isAllowedOrigin(req, nil)
	assert.True(t, actual)
}

func Test_sameOrigin_returnsTrue_withCustomPort(t *testing.T) {
	mode.Set(mode.Prod)
	req := httptest.NewRequest("GET", "http://example.com:8080/stream", nil)
	req.Header.Set("Origin", "http://example.com:8080")
	actual := isAllowedOrigin(req, nil)
	assert.True(t, actual)
}

func Test_isAllowedOrigin_withoutAllowedOrigins_failsWhenNotSameOrigin(t *testing.T) {
	mode.Set(mode.Prod)
	req := httptest.NewRequest("GET", "http://example.com/stream", nil)
	req.Header.Set("Origin", "http://gorify.example.com")
	actual := isAllowedOrigin(req, nil)
	assert.False(t, actual)
}

func Test_isAllowedOriginMatching(t *testing.T) {
	mode.Set(mode.Prod)
	compiledAllowedOrigins := compileAllowedWebSocketOrigins([]string{"go.{4}\\.example\\.com", "go\\.example\\.com"})

	req := httptest.NewRequest("GET", "http://example.me/stream", nil)
	req.Header.Set("Origin", "http://gorify.example.com")
	assert.True(t, isAllowedOrigin(req, compiledAllowedOrigins))

	req.Header.Set("Origin", "http://go.example.com")
	assert.True(t, isAllowedOrigin(req, compiledAllowedOrigins))

	req.Header.Set("Origin", "http://hello.example.com")
	assert.False(t, isAllowedOrigin(req, compiledAllowedOrigins))
}

func Test_emptyOrigin_returnsTrue(t *testing.T) {
	mode.Set(mode.Prod)
	req := httptest.NewRequest("GET", "http://example.com/stream", nil)
	actual := isAllowedOrigin(req, nil)
	assert.True(t, actual)
}

func Test_otherOrigin_returnsFalse(t *testing.T) {
	mode.Set(mode.Prod)
	req := httptest.NewRequest("GET", "http://example.com/stream", nil)
	req.Header.Set("Origin", "http://otherexample.de")
	actual := isAllowedOrigin(req, nil)
	assert.False(t, actual)
}

func Test_invalidOrigin_returnsFalse(t *testing.T) {
	mode.Set(mode.Prod)
	req := httptest.NewRequest("GET", "http://example.com/stream", nil)
	req.Header.Set("Origin", "http\\://otherexample.de")
	actual := isAllowedOrigin(req, nil)
	assert.False(t, actual)
}

func Test_compileAllowedWebSocketOrigins(t *testing.T) {
	assert.Equal(t, 0, len(compileAllowedWebSocketOrigins([]string{})))
	assert.Equal(t, 3, len(compileAllowedWebSocketOrigins([]string{"^.*$", "", "abc"})))
}

func clients(api *API, user uint) []*client {
	api.lock.RLock()
	defer api.lock.RUnlock()

	return api.clients[user]
}

func countClients(a *API) int {
	a.lock.RLock()
	defer a.lock.RUnlock()

	var i int
	for _, clients := range a.clients {
		i += len(clients)
	}
	return i
}

func testClient(t *testing.T, url string) *testingClient {
	client := createClient(t, url)
	startReading(client)
	return client
}

func startReading(client *testingClient) {
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
}

func createClient(t *testing.T, url string) *testingClient {
	ws, _, err := websocket.DefaultDialer.Dial(url, nil)
	assert.Nil(t, err)

	readMessages := make(chan model.MessageExternal)

	return &testingClient{conn: ws, readMessage: readMessages, t: t}
}

type testingClient struct {
	conn        *websocket.Conn
	readMessage chan model.MessageExternal
	t           *testing.T
}

func (c *testingClient) expectMessage(expected *model.MessageExternal) {
	select {
	case <-time.After(50 * time.Millisecond):
		assert.Fail(c.t, "Expected message but none was send :(")
	case actual := <-c.readMessage:
		assert.Equal(c.t, *expected, actual)
	}
}

func expectMessage(expected *model.MessageExternal, clients ...*testingClient) {
	for _, client := range clients {
		client.expectMessage(expected)
	}
}

func expectNoMessage(clients ...*testingClient) {
	for _, client := range clients {
		client.expectNoMessage()
	}
}

func (c *testingClient) expectNoMessage() {
	select {
	case <-time.After(50 * time.Millisecond):
		// no message == as expected
	case msg := <-c.readMessage:
		assert.Fail(c.t, "Expected NO message but there was one :(", fmt.Sprint(msg))
	}
}

func bootTestServer(handlerFunc gin.HandlerFunc) (*httptest.Server, *API) {
	r := gin.New()
	r.Use(handlerFunc)
	// ping every 500 ms, and the client has 500 ms to respond
	api := New(500*time.Millisecond, 500*time.Millisecond, []string{})

	r.GET("/", api.Handle)
	server := httptest.NewServer(r)
	return server, api
}

func wsURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
}

func staticUserID() gin.HandlerFunc {
	return func(context *gin.Context) {
		auth.RegisterClient(context, &model.Client{UserID: 1, Token: "customtoken"})
	}
}

func waitForConnectedClients(api *API, count int) {
	for i := 0; i < 10; i++ {
		if countClients(api) == count {
			// ok
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// --- regression coverage: a single slow / suspended connection must not block
// message dispatch, client deletion or expired-client cleanup ---

// slowWriter hijacks writeJSON so that the first client the server writes to
// behaves like a slow or suspended consumer: its writes block until release is
// called, while every other client is written normally. Connect the slow client
// first and prime it (a single Notify) before connecting any other client, so it
// is the connection that gets captured.
// slowWriter makes the first client the server writes to behave like a slow or
// suspended consumer: its writes block until release is called, while every other
// client is written normally. Connect the slow client first and prime it (a single
// Notify) before connecting any other client, so it is the connection captured.
//
// The writeJSON test seam is a package-global function, so it is installed exactly
// once (lazily, before any client exists and can read it) and per-test control is
// routed through the mutex-guarded activeSlow pointer. This keeps teardown free of
// a data race on writeJSON while client write loops are still running.
type slowWriter struct {
	mu          sync.Mutex
	conn        *websocket.Conn
	release     chan struct{}
	releaseOnce sync.Once
}

var (
	slowSeamOnce  sync.Once
	realWriteJSON func(*websocket.Conn, interface{}) error
	slowMu        sync.Mutex
	activeSlow    *slowWriter
)

func installSlowSeam() {
	slowSeamOnce.Do(func() {
		realWriteJSON = writeJSON
		writeJSON = func(conn *websocket.Conn, v interface{}) error {
			slowMu.Lock()
			sw := activeSlow
			slowMu.Unlock()
			if sw != nil {
				sw.mu.Lock()
				if sw.conn == nil {
					sw.conn = conn
				}
				blocked := conn == sw.conn
				sw.mu.Unlock()
				if blocked {
					<-sw.release
					return errors.New("slow client released")
				}
			}
			return realWriteJSON(conn, v)
		}
	})
}

func newSlowWriter() *slowWriter {
	installSlowSeam()
	sw := &slowWriter{release: make(chan struct{})}
	slowMu.Lock()
	activeSlow = sw
	slowMu.Unlock()
	return sw
}

func (sw *slowWriter) captured() bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return sw.conn != nil
}

// releaseAndRestore unblocks the slow client's writes and detaches this slowWriter.
// It is safe to call multiple times and should be deferred so blocked write loops
// can exit before leaktest runs.
func (sw *slowWriter) releaseAndRestore() {
	sw.releaseOnce.Do(func() { close(sw.release) })
	slowMu.Lock()
	if activeSlow == sw {
		activeSlow = nil
	}
	slowMu.Unlock()
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// assertReturnsWithin fails the test if fn does not return within d, which is the
// signal that fn is blocked (e.g. waiting on a lock held by a stuck dispatch).
func assertReturnsWithin(t *testing.T, d time.Duration, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		fn()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("%s did not return within %s; it is blocked", what, d)
	}
}

func TestSlowClientIsDisconnectedWithoutBlockingDispatch(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.Check(t)()

	sw := newSlowWriter()
	defer sw.releaseAndRestore()

	userIDs := []uint{1, 2}
	tokens := []string{"slow", "healthy"}
	i := 0
	server, api := bootTestServer(func(ctx *gin.Context) {
		auth.RegisterClient(ctx, &model.Client{UserID: userIDs[i], Token: tokens[i]})
		i++
	})
	defer server.Close()
	defer api.Close()
	url := wsURL(server.URL)

	// Connect the slow consumer first and prime it so its write loop parks.
	slow := testClient(t, url)
	defer slow.conn.Close()
	waitForConnectedClients(api, 1)
	api.Notify(1, &model.MessageExternal{ID: 0, Message: "prime"})
	waitFor(t, "slow client to be captured", sw.captured)

	// A healthy client of a different user.
	healthy := testClient(t, url)
	defer healthy.conn.Close()
	waitForConnectedClients(api, 2)

	// Flooding the slow user well past its buffer must not block dispatch.
	assertReturnsWithin(t, 2*time.Second, "Notify to a slow client", func() {
		for n := 0; n < messageBufferSize*4; n++ {
			api.Notify(1, &model.MessageExternal{ID: uint(n + 1), Message: "flood"})
		}
	})

	// The other user keeps receiving while the slow client is stuck.
	api.Notify(2, &model.MessageExternal{ID: 99, Message: "hi"})
	healthy.expectMessage(&model.MessageExternal{ID: 99, Message: "hi"})

	// The slow client is disconnected so it can no longer back up the pipeline.
	waitForConnectedClients(api, 1)
	assert.Empty(t, clients(api, 1))
	assert.NotEmpty(t, clients(api, 2))
}

func TestSlowClientDoesNotDelaySameUser(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.Check(t)()

	sw := newSlowWriter()
	defer sw.releaseAndRestore()

	tokens := []string{"slow", "healthy"}
	i := 0
	server, api := bootTestServer(func(ctx *gin.Context) {
		auth.RegisterClient(ctx, &model.Client{UserID: 1, Token: tokens[i]})
		i++
	})
	defer server.Close()
	defer api.Close()
	url := wsURL(server.URL)

	// Slow client of user 1, parked and first in the user's client list.
	slow := testClient(t, url)
	defer slow.conn.Close()
	waitForConnectedClients(api, 1)
	api.Notify(1, &model.MessageExternal{ID: 0, Message: "prime"})
	waitFor(t, "slow client to be captured", sw.captured)

	// A second, healthy client of the SAME user.
	healthy := testClient(t, url)
	defer healthy.conn.Close()
	waitForConnectedClients(api, 2)

	// Notifying the user must reach the healthy client immediately, even though
	// an earlier client of the same user is stuck.
	assertReturnsWithin(t, 2*time.Second, "Notify to a user with a slow client", func() {
		api.Notify(1, &model.MessageExternal{ID: 1, Message: "fast"})
	})
	healthy.expectMessage(&model.MessageExternal{ID: 1, Message: "fast"})
}

func TestClientActiveDisconnectDoesNotBlock(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.Check(t)()

	userIDs := []uint{1, 1, 2}
	tokens := []string{"1-a", "1-b", "2-a"}
	i := 0
	server, api := bootTestServer(func(ctx *gin.Context) {
		auth.RegisterClient(ctx, &model.Client{UserID: userIDs[i], Token: tokens[i]})
		i++
	})
	defer server.Close()
	defer api.Close()
	url := wsURL(server.URL)

	userOneA := testClient(t, url)
	defer userOneA.conn.Close()
	userOneB := testClient(t, url)
	defer userOneB.conn.Close()
	userTwo := testClient(t, url)
	defer userTwo.conn.Close()
	waitForConnectedClients(api, 3)

	// One client of user 1 actively disconnects (e.g. the browser tab is closed).
	userOneA.conn.Close()
	waitForConnectedClients(api, 2)

	// Dispatch to the remaining clients still works and does not block.
	assertReturnsWithin(t, 2*time.Second, "Notify after a client disconnected", func() {
		api.Notify(1, &model.MessageExternal{ID: 1, Message: "still here"})
		api.Notify(2, &model.MessageExternal{ID: 2, Message: "hello"})
	})
	userOneB.expectMessage(&model.MessageExternal{ID: 1, Message: "still here"})
	userTwo.expectMessage(&model.MessageExternal{ID: 2, Message: "hello"})
	userOneA.expectNoMessage()
}

func TestClientDeletionNotBlockedBySlowClient(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.Check(t)()

	sw := newSlowWriter()
	defer sw.releaseAndRestore()

	tokens := []string{"slow", "deleteme"}
	i := 0
	server, api := bootTestServer(func(ctx *gin.Context) {
		auth.RegisterClient(ctx, &model.Client{UserID: 1, Token: tokens[i]})
		i++
	})
	defer server.Close()
	defer api.Close()
	url := wsURL(server.URL)

	// Slow client of user 1, parked and first in the user's client list.
	slow := testClient(t, url)
	defer slow.conn.Close()
	waitForConnectedClients(api, 1)
	api.Notify(1, &model.MessageExternal{ID: 0, Message: "prime"})
	waitFor(t, "slow client to be captured", sw.captured)

	// Another client of the same user that we are going to delete. No read loop,
	// so messages sent to it during the test cannot leak a reader goroutine.
	deleteMe := createClient(t, url)
	defer deleteMe.conn.Close()
	waitForConnectedClients(api, 2)

	// In the old, broken code this dispatch would block forever holding the read
	// lock (the slow client's buffer is full), which in turn blocked deletion.
	go api.Notify(1, &model.MessageExternal{ID: 1, Message: "second"})

	assertReturnsWithin(t, 2*time.Second, "NotifyDeletedClient", func() {
		api.NotifyDeletedClient(1, "deleteme")
	})

	for _, c := range clients(api, 1) {
		assert.NotEqual(t, "deleteme", c.token, "deleted client must be gone")
	}
}

func TestExpiredCleanupNotBlockedBySlowClient(t *testing.T) {
	mode.Set(mode.TestDev)
	defer leaktest.Check(t)()

	sw := newSlowWriter()
	defer sw.releaseAndRestore()

	userIDs := []uint{1, 2}
	tokens := []string{"slow", "healthy"}
	i := 0
	server, api := bootTestServer(func(ctx *gin.Context) {
		auth.RegisterClient(ctx, &model.Client{UserID: userIDs[i], Token: tokens[i]})
		i++
	})
	defer server.Close()
	defer api.Close()
	url := wsURL(server.URL)

	// Slow ("expired") client of user 1.
	slow := testClient(t, url)
	defer slow.conn.Close()
	waitForConnectedClients(api, 1)
	api.Notify(1, &model.MessageExternal{ID: 0, Message: "prime"})
	waitFor(t, "slow client to be captured", sw.captured)

	// A healthy client of another user that must keep working.
	healthy := testClient(t, url)
	defer healthy.conn.Close()
	waitForConnectedClients(api, 2)

	// A dispatch is in flight to the slow client (old code: stuck holding the lock).
	go api.Notify(1, &model.MessageExternal{ID: 1, Message: "second"})

	// The cleanup loop disconnects expired clients via NotifyDeletedClient
	// (see router.go). That must not be blocked by the slow client.
	assertReturnsWithin(t, 2*time.Second, "expired-client cleanup", func() {
		api.NotifyDeletedClient(1, "slow")
	})
	assert.Empty(t, clients(api, 1))

	// Dispatch to the rest of the system is unaffected.
	assertReturnsWithin(t, 2*time.Second, "Notify after cleanup", func() {
		api.Notify(2, &model.MessageExternal{ID: 2, Message: "hello"})
	})
	healthy.expectMessage(&model.MessageExternal{ID: 2, Message: "hello"})
}
