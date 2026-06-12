package session

import (
	"testing"
	"time"

	"github.com/gotify/server/v2/model"
	"github.com/gotify/server/v2/test/testdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func TestServiceSuite(t *testing.T) {
	suite.Run(t, new(ServiceSuite))
}

type ServiceSuite struct {
	suite.Suite
	db  *testdb.Database
	svc *Service
}

func (s *ServiceSuite) BeforeTest(suiteName, testName string) {
	s.db = testdb.NewDB(s.T())
	s.svc = NewService(s.db, testTokens("Ctok12345678901234ab"))
	s.svc.timeNow = func() time.Time { return testdb.Now }
}

func (s *ServiceSuite) AfterTest(suiteName, testName string) {
	s.db.Close()
}

// --- Policy tests ---

func (s *ServiceSuite) TestBrowserSessionPolicy() {
	p := BrowserSessionPolicy{}
	assert.Equal(s.T(), 7*24*time.Hour, p.ExpiresAfterInactivity())
	assert.Equal(s.T(), time.Hour, p.InitialElevationDuration())
	assert.True(s.T(), p.HasCookie())
	assert.Equal(s.T(), 604800, p.CookieMaxAge())
}

func (s *ServiceSuite) TestNativeSessionPolicy() {
	p := NativeSessionPolicy{}
	assert.Equal(s.T(), 7*24*time.Hour, p.ExpiresAfterInactivity())
	assert.Equal(s.T(), time.Hour, p.InitialElevationDuration())
	assert.False(s.T(), p.HasCookie())
	assert.Equal(s.T(), 0, p.CookieMaxAge())
}

func (s *ServiceSuite) TestPersistentClientPolicyDefault() {
	p := PersistentClientPolicy{}
	assert.Equal(s.T(), time.Duration(0), p.ExpiresAfterInactivity())
	assert.Equal(s.T(), time.Duration(0), p.InitialElevationDuration())
	assert.False(s.T(), p.HasCookie())
	assert.Equal(s.T(), 0, p.CookieMaxAge())
}

func (s *ServiceSuite) TestPersistentClientPolicyCustomExpiry() {
	custom := uint(3600)
	p := PersistentClientPolicy{CustomExpiry: &custom}
	assert.Equal(s.T(), time.Hour, p.ExpiresAfterInactivity())
}

// --- Create tests ---

func (s *ServiceSuite) TestCreateBrowserSession() {
	s.db.User(1)
	client, err := s.svc.Create(1, "my-browser", BrowserSessionPolicy{}, s.tokenExists)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), client)

	assert.Equal(s.T(), "my-browser", client.Name)
	assert.Equal(s.T(), uint(1), client.UserID)
	assert.Equal(s.T(), "Ctok12345678901234ab", client.Token)
	assert.Equal(s.T(), uint(604800), client.ExpiresAfterInactivitySeconds)
	require.NotNil(s.T(), client.ElevatedUntil)
	assert.Equal(s.T(), testdb.Now.Add(time.Hour), *client.ElevatedUntil)
	assert.NotZero(s.T(), client.CreatedAt)
}

func (s *ServiceSuite) TestCreateNativeSession() {
	s.db.User(1)
	client, err := s.svc.Create(1, "my-native", NativeSessionPolicy{}, s.tokenExists)
	require.NoError(s.T(), err)

	assert.Equal(s.T(), uint(604800), client.ExpiresAfterInactivitySeconds)
	require.NotNil(s.T(), client.ElevatedUntil)
	assert.Equal(s.T(), testdb.Now.Add(time.Hour), *client.ElevatedUntil)
}

func (s *ServiceSuite) TestCreatePersistentClient() {
	s.db.User(1)
	client, err := s.svc.Create(1, "my-api", PersistentClientPolicy{}, s.tokenExists)
	require.NoError(s.T(), err)

	assert.Equal(s.T(), uint(0), client.ExpiresAfterInactivitySeconds)
	assert.Nil(s.T(), client.ElevatedUntil)
	assert.Nil(s.T(), client.ExpiresAt)
}

func (s *ServiceSuite) TestCreatePersistentClientWithCustomExpiry() {
	s.db.User(1)
	custom := uint(86400)
	client, err := s.svc.Create(1, "my-api-exp", PersistentClientPolicy{CustomExpiry: &custom}, s.tokenExists)
	require.NoError(s.T(), err)

	assert.Equal(s.T(), uint(86400), client.ExpiresAfterInactivitySeconds)
	assert.Nil(s.T(), client.ElevatedUntil)
}

func (s *ServiceSuite) TestCreateTokenUniqueness() {
	s.db.User(1)
	// Create first client with a specific token
	s.svc.generateToken = testTokens("Cunique12345678901ab")
	c1, err := s.svc.Create(1, "client1", PersistentClientPolicy{}, s.tokenExists)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "Cunique12345678901ab", c1.Token)

	// Create second client — generateToken would produce the same token first,
	// but GenerateNotExistingToken should retry and get a different one.
	s.svc.generateToken = testTokens("Cunique12345678901ab", "Cdifferent2345678ab")
	c2, err := s.svc.Create(1, "client2", PersistentClientPolicy{}, s.tokenExists)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "Cdifferent2345678ab", c2.Token)
}

// --- Renew tests ---

func (s *ServiceSuite) TestRenewUpdatesLastUsed() {
	s.db.User(1)
	client, err := s.svc.Create(1, "renew-test", BrowserSessionPolicy{}, s.tokenExists)
	require.NoError(s.T(), err)

	// Advance time by 10 minutes
	later := testdb.Now.Add(10 * time.Minute)
	s.svc.timeNow = func() time.Time { return later }

	updated, err := s.svc.Renew(client, 5*time.Minute)
	require.NoError(s.T(), err)
	assert.True(s.T(), updated)
	require.NotNil(s.T(), client.LastUsed)
	assert.Equal(s.T(), later, *client.LastUsed)
}

func (s *ServiceSuite) TestRenewThrottled() {
	s.db.User(1)
	client, err := s.svc.Create(1, "throttle-test", BrowserSessionPolicy{}, s.tokenExists)
	require.NoError(s.T(), err)

	// Manually set LastUsed to "now" to simulate recent activity
	now := testdb.Now
	client.LastUsed = &now

	// Advance only 3 minutes — within 5 minute throttle
	soon := testdb.Now.Add(3 * time.Minute)
	s.svc.timeNow = func() time.Time { return soon }

	updated, err := s.svc.Renew(client, 5*time.Minute)
	require.NoError(s.T(), err)
	assert.False(s.T(), updated)
}

func (s *ServiceSuite) TestRenewAfterThrottleExpires() {
	s.db.User(1)
	client, err := s.svc.Create(1, "throttle-exp", BrowserSessionPolicy{}, s.tokenExists)
	require.NoError(s.T(), err)

	now := testdb.Now
	client.LastUsed = &now

	// Advance 6 minutes — past the 5 minute throttle
	later := testdb.Now.Add(6 * time.Minute)
	s.svc.timeNow = func() time.Time { return later }

	updated, err := s.svc.Renew(client, 5*time.Minute)
	require.NoError(s.T(), err)
	assert.True(s.T(), updated)
}

func (s *ServiceSuite) TestRenewWithZeroThrottleAlwaysUpdates() {
	s.db.User(1)
	client, err := s.svc.Create(1, "zero-throttle", BrowserSessionPolicy{}, s.tokenExists)
	require.NoError(s.T(), err)

	// Set LastUsed to now
	now := testdb.Now
	client.LastUsed = &now

	// Zero throttle should always update, even immediately
	updated, err := s.svc.Renew(client, 0)
	require.NoError(s.T(), err)
	assert.True(s.T(), updated)
}

// --- RenewTokens batch tests ---

func (s *ServiceSuite) TestRenewTokensBatch() {
	s.db.User(1)
	s.svc.generateToken = testTokens("Cbatchtok11234567ab")
	c1, err := s.svc.Create(1, "batch1", BrowserSessionPolicy{}, s.tokenExists)
	require.NoError(s.T(), err)
	s.svc.generateToken = testTokens("Cbatchtok21234567ab")
	c2, err := s.svc.Create(1, "batch2", BrowserSessionPolicy{}, s.tokenExists)
	require.NoError(s.T(), err)

	later := testdb.Now.Add(10 * time.Minute)
	s.svc.timeNow = func() time.Time { return later }

	err = s.svc.RenewTokens([]string{c1.Token, c2.Token})
	require.NoError(s.T(), err)

	// Verify both clients were updated
	fetched1, _ := s.db.GetClientByID(c1.ID)
	fetched2, _ := s.db.GetClientByID(c2.ID)
	require.NotNil(s.T(), fetched1)
	require.NotNil(s.T(), fetched2)
	require.NotNil(s.T(), fetched1.LastUsed)
	require.NotNil(s.T(), fetched2.LastUsed)
	assert.Equal(s.T(), later, *fetched1.LastUsed)
	assert.Equal(s.T(), later, *fetched2.LastUsed)
}

func (s *ServiceSuite) TestRenewTokensEmptyList() {
	err := s.svc.RenewTokens(nil)
	assert.NoError(s.T(), err)
	err = s.svc.RenewTokens([]string{})
	assert.NoError(s.T(), err)
}

// --- Elevate tests ---

func (s *ServiceSuite) TestElevate() {
	s.db.User(1)
	client, err := s.svc.Create(1, "elev-test", PersistentClientPolicy{}, s.tokenExists)
	require.NoError(s.T(), err)
	assert.Nil(s.T(), client.ElevatedUntil) // PersistentClientPolicy has no initial elevation

	err = s.svc.Elevate(client.ID, 30*time.Minute)
	require.NoError(s.T(), err)

	// Re-fetch to verify
	fetched, err := s.db.GetClientByID(client.ID)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), fetched.ElevatedUntil)
	assert.Equal(s.T(), testdb.Now.Add(30*time.Minute), *fetched.ElevatedUntil)
}

func (s *ServiceSuite) TestElevateExtendsFromNow() {
	s.db.User(1)
	client, err := s.svc.Create(1, "elev-ext", BrowserSessionPolicy{}, s.tokenExists)
	require.NoError(s.T(), err)

	// Advance time by 30 minutes
	later := testdb.Now.Add(30 * time.Minute)
	s.svc.timeNow = func() time.Time { return later }

	// Elevate for another hour from "now" (which is now later)
	err = s.svc.Elevate(client.ID, time.Hour)
	require.NoError(s.T(), err)

	fetched, err := s.db.GetClientByID(client.ID)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), fetched.ElevatedUntil)
	// ElevatedUntil should be later + 1h, NOT original + 1h
	assert.Equal(s.T(), later.Add(time.Hour), *fetched.ElevatedUntil)
}

// --- IsElevated tests ---

func (s *ServiceSuite) TestIsElevatedWhenElevated() {
	s.db.User(1)
	client, err := s.svc.Create(1, "elev-check", BrowserSessionPolicy{}, s.tokenExists)
	require.NoError(s.T(), err)

	assert.True(s.T(), s.svc.IsElevated(client))
}

func (s *ServiceSuite) TestIsElevatedAfterExpiry() {
	s.db.User(1)
	client, err := s.svc.Create(1, "elev-exp", BrowserSessionPolicy{}, s.tokenExists)
	require.NoError(s.T(), err)

	// Advance past the 1-hour elevation
	s.svc.timeNow = func() time.Time { return testdb.Now.Add(2 * time.Hour) }
	assert.False(s.T(), s.svc.IsElevated(client))
}

func (s *ServiceSuite) TestIsElevatedWhenNil() {
	client := &model.Client{}
	assert.False(s.T(), s.svc.IsElevated(client))
}

func (s *ServiceSuite) TestIsElevatedPersistentClient() {
	s.db.User(1)
	client, err := s.svc.Create(1, "persist-elev", PersistentClientPolicy{}, s.tokenExists)
	require.NoError(s.T(), err)

	// PersistentClientPolicy has no initial elevation
	assert.False(s.T(), s.svc.IsElevated(client))
}

// --- Logout tests ---

func (s *ServiceSuite) TestLogout() {
	s.db.User(1)
	client, err := s.svc.Create(1, "logout-test", BrowserSessionPolicy{}, s.tokenExists)
	require.NoError(s.T(), err)

	err = s.svc.Logout(client.ID)
	require.NoError(s.T(), err)

	s.db.AssertClientNotExist(client.ID)
}

// --- CleanupExpired tests ---

func (s *ServiceSuite) TestCleanupExpired() {
	s.db.User(1)

	// Create a browser session (expires in 7 days)
	s.svc.generateToken = testTokens("Cbrowserexp12345ab")
	browser, err := s.svc.Create(1, "browser-exp", BrowserSessionPolicy{}, s.tokenExists)
	require.NoError(s.T(), err)

	// Create a persistent client (never expires)
	s.svc.generateToken = testTokens("Cpersistkeep1234ab")
	persistent, err := s.svc.Create(1, "persist-keep", PersistentClientPolicy{}, s.tokenExists)
	require.NoError(s.T(), err)

	// Advance time past 7 days
	afterExpiry := testdb.Now.Add(8 * 24 * time.Hour)

	// Run cleanup
	deleted, err := s.svc.CleanupExpired(afterExpiry)
	require.NoError(s.T(), err)

	// Browser session should be deleted
	found := false
	for _, c := range deleted {
		if c.ID == browser.ID {
			found = true
			break
		}
	}
	assert.True(s.T(), found, "browser session should be in deleted list")
	s.db.AssertClientNotExist(browser.ID)

	// Persistent client should survive
	s.db.AssertClientExist(persistent.ID)
}

func (s *ServiceSuite) TestCleanupMixedScenario() {
	s.db.User(1)

	// Create browser session with normal expiry (7 days)
	s.svc.generateToken = testTokens("Cmixed1234567890ab")
	_, err := s.svc.Create(1, "browser1", BrowserSessionPolicy{}, s.tokenExists)
	require.NoError(s.T(), err)

	// Create persistent client with short custom expiry (1 hour)
	short := uint(3600)
	s.svc.generateToken = testTokens("Cshortlived12345ab")
	shortLived, err := s.svc.Create(1, "short-lived", PersistentClientPolicy{CustomExpiry: &short}, s.tokenExists)
	require.NoError(s.T(), err)

	// Create permanent persistent client
	s.svc.generateToken = testTokens("Cpermanent123456ab")
	permanent, err := s.svc.Create(1, "permanent", PersistentClientPolicy{}, s.tokenExists)
	require.NoError(s.T(), err)

	// Advance time 2 hours — short-lived should expire, browser should not
	after2h := testdb.Now.Add(2 * time.Hour)
	deleted, err := s.svc.CleanupExpired(after2h)
	require.NoError(s.T(), err)

	// short-lived should be deleted
	found := false
	for _, c := range deleted {
		if c.ID == shortLived.ID {
			found = true
		}
	}
	assert.True(s.T(), found, "short-lived client should be deleted")
	s.db.AssertClientNotExist(shortLived.ID)

	// browser and permanent should survive
	s.db.AssertClientExist(1) // browser1
	s.db.AssertClientExist(permanent.ID)
}

// --- Cross-scenario integration tests ---

func (s *ServiceSuite) TestBrowserSessionFullLifecycle() {
	s.db.User(1)

	// 1. Create browser session
	client, err := s.svc.Create(1, "lifecycle", BrowserSessionPolicy{}, s.tokenExists)
	require.NoError(s.T(), err)
	assert.True(s.T(), s.svc.IsElevated(client))

	// 2. Renew after 10 minutes
	later10 := testdb.Now.Add(10 * time.Minute)
	s.svc.timeNow = func() time.Time { return later10 }
	updated, err := s.svc.Renew(client, 5*time.Minute)
	require.NoError(s.T(), err)
	assert.True(s.T(), updated)

	// 3. Elevation still valid (within 1 hour)
	assert.True(s.T(), s.svc.IsElevated(client))

	// 4. Advance past elevation (1h + 1min) but within session expiry (7 days)
	pastElevation := testdb.Now.Add(61 * time.Minute)
	s.svc.timeNow = func() time.Time { return pastElevation }
	assert.False(s.T(), s.svc.IsElevated(client))

	// Session should still exist (not expired yet)
	s.db.AssertClientExist(client.ID)

	// 5. Extend elevation
	err = s.svc.Elevate(client.ID, 30*time.Minute)
	require.NoError(s.T(), err)
	// Re-fetch client to pick up the DB-level elevation change
	client, err = s.db.GetClientByID(client.ID)
	require.NoError(s.T(), err)
	assert.True(s.T(), s.svc.IsElevated(client))

	// 6. Advance past session expiry (8 days from creation)
	pastExpiry := testdb.Now.Add(8 * 24 * time.Hour)
	deleted, err := s.svc.CleanupExpired(pastExpiry)
	require.NoError(s.T(), err)
	found := false
	for _, c := range deleted {
		if c.ID == client.ID {
			found = true
		}
	}
	assert.True(s.T(), found)
	s.db.AssertClientNotExist(client.ID)
}

func (s *ServiceSuite) TestNativeSessionConsistentWithBrowser() {
	s.db.User(1)

	s.svc.generateToken = testTokens("Cbrowser12345678ab")
	browser, err := s.svc.Create(1, "browser", BrowserSessionPolicy{}, s.tokenExists)
	require.NoError(s.T(), err)

	s.svc.generateToken = testTokens("Cnative123456789ab")
	native, err := s.svc.Create(1, "native", NativeSessionPolicy{}, s.tokenExists)
	require.NoError(s.T(), err)

	// Both should have same expiration and elevation
	assert.Equal(s.T(), browser.ExpiresAfterInactivitySeconds, native.ExpiresAfterInactivitySeconds)
	require.NotNil(s.T(), browser.ElevatedUntil)
	require.NotNil(s.T(), native.ElevatedUntil)
	assert.Equal(s.T(), *browser.ElevatedUntil, *native.ElevatedUntil)

	// Both should renew identically
	later := testdb.Now.Add(10 * time.Minute)
	s.svc.timeNow = func() time.Time { return later }

	updatedB, _ := s.svc.Renew(browser, 5*time.Minute)
	updatedN, _ := s.svc.Renew(native, 5*time.Minute)
	assert.True(s.T(), updatedB)
	assert.True(s.T(), updatedN)

	// Policy difference: cookie vs no cookie
	assert.True(s.T(), BrowserSessionPolicy{}.HasCookie())
	assert.False(s.T(), NativeSessionPolicy{}.HasCookie())
}

func (s *ServiceSuite) TestThreePoliciesLogout() {
	s.db.User(1)

	s.svc.generateToken = testTokens("Clogoutbrowser123ab")
	browser, _ := s.svc.Create(1, "b", BrowserSessionPolicy{}, s.tokenExists)
	s.svc.generateToken = testTokens("Clogoutnative1234ab")
	native, _ := s.svc.Create(1, "n", NativeSessionPolicy{}, s.tokenExists)
	s.svc.generateToken = testTokens("Clogoutpersist123ab")
	persistent, _ := s.svc.Create(1, "p", PersistentClientPolicy{}, s.tokenExists)

	for _, c := range []*model.Client{browser, native, persistent} {
		err := s.svc.Logout(c.ID)
		require.NoError(s.T(), err)
		s.db.AssertClientNotExist(c.ID)
	}
}

func (s *ServiceSuite) TestCreate100ClientsTokenUniqueness() {
	s.db.User(1)

	// Generate 100 unique tokens
	tokens := make([]string, 100)
	for i := range tokens {
		tokens[i] = "Ctoken" + padNum(i)
	}
	s.svc.generateToken = testTokens(tokens...)

	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		client, err := s.svc.Create(1, "client", PersistentClientPolicy{}, s.tokenExists)
		require.NoError(s.T(), err)
		assert.False(s.T(), seen[client.Token], "duplicate token: %s", client.Token)
		seen[client.Token] = true
	}
	assert.Len(s.T(), seen, 100)
}

// --- helpers ---

func (s *ServiceSuite) tokenExists(token string) bool {
	c, _ := s.db.GetClientByToken(token)
	return c != nil
}

// testTokens returns a token generation function that cycles through the given tokens.
func testTokens(tokens ...string) func() string {
	i := 0
	return func() string {
		res := tokens[i%len(tokens)]
		i++
		return res
	}
}

func padNum(n int) string {
	s := ""
	for i := 0; i < 16; i++ {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	return s
}
