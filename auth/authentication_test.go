package auth

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gotify/server/v2/auth/password"
	"github.com/gotify/server/v2/mode"
	"github.com/gotify/server/v2/model"
	"github.com/gotify/server/v2/session"
	"github.com/gotify/server/v2/test/testdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func TestSuite(t *testing.T) {
	suite.Run(t, new(AuthenticationSuite))
}

type AuthenticationSuite struct {
	suite.Suite
	auth *Auth
	DB   *testdb.Database
}

func (s *AuthenticationSuite) SetupSuite() {
	mode.Set(mode.TestDev)
	s.DB = testdb.NewDB(s.T())
	s.auth = &Auth{DB: s.DB}

	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	timeNow = func() time.Time { return now }

	elevated := now.Add(time.Hour)
	expired := now.Add(-time.Hour)

	s.DB.CreateUser(&model.User{
		Name:         "existing",
		Pass:         password.CreatePassword("pw", 5),
		Admin:        false,
		Applications: []model.Application{{Token: "apptoken", Name: "backup server1", Description: "irrelevant"}},
		Clients: []model.Client{
			{Token: "clienttoken", Name: "android phone1"},
			{Token: "clienttoken_elevated", Name: "elevated phone1", ElevatedUntil: &elevated},
			{Token: "clienttoken_expired", Name: "expired phone1", ElevatedUntil: &expired},
		},
	})

	s.DB.CreateUser(&model.User{
		Name:         "admin",
		Pass:         password.CreatePassword("pw", 5),
		Admin:        true,
		Applications: []model.Application{{Token: "apptoken_admin", Name: "backup server2", Description: "irrelevant"}},
		Clients: []model.Client{
			{Token: "clienttoken_admin", Name: "android phone2"},
			{Token: "clienttoken_admin_elevated", Name: "elevated phone2", ElevatedUntil: &elevated},
		},
	})
}

func (s *AuthenticationSuite) TearDownSuite() {
	timeNow = time.Now
	s.DB.Close()
}

func (s *AuthenticationSuite) TestQueryToken() {
	// not existing token
	s.assertQueryRequest("token", "ergerogerg", s.auth.RequireApplicationToken, 401)
	s.assertQueryRequest("token", "ergerogerg", s.auth.RequireClient, 401)
	s.assertQueryRequest("token", "ergerogerg", s.auth.RequireAdmin, 401)
	s.assertQueryRequest("token", "ergerogerg", s.auth.RequireElevatedClient, 401)

	// not existing key
	s.assertQueryRequest("tokenx", "clienttoken", s.auth.RequireApplicationToken, 401)
	s.assertQueryRequest("tokenx", "clienttoken", s.auth.RequireClient, 401)
	s.assertQueryRequest("tokenx", "clienttoken", s.auth.RequireAdmin, 401)
	s.assertQueryRequest("tokenx", "clienttoken", s.auth.RequireElevatedClient, 401)

	// apptoken
	s.assertQueryRequest("token", "apptoken", s.auth.RequireApplicationToken, 200)
	s.assertQueryRequest("token", "apptoken", s.auth.RequireClient, 401)
	s.assertQueryRequest("token", "apptoken", s.auth.RequireAdmin, 401)
	s.assertQueryRequest("token", "apptoken", s.auth.RequireElevatedClient, 401)
	s.assertQueryRequest("token", "apptoken_admin", s.auth.RequireApplicationToken, 200)
	s.assertQueryRequest("token", "apptoken_admin", s.auth.RequireClient, 401)
	s.assertQueryRequest("token", "apptoken_admin", s.auth.RequireAdmin, 401)
	s.assertQueryRequest("token", "apptoken_admin", s.auth.RequireElevatedClient, 401)

	// clienttoken (non-admin, not elevated)
	s.assertQueryRequest("token", "clienttoken", s.auth.RequireApplicationToken, 401)
	s.assertQueryRequest("token", "clienttoken", s.auth.RequireClient, 200)
	s.assertQueryRequest("token", "clienttoken", s.auth.RequireAdmin, 403)
	s.assertQueryRequest("token", "clienttoken", s.auth.RequireElevatedClient, 403)

	// clienttoken_elevated (non-admin, elevated)
	s.assertQueryRequest("token", "clienttoken_elevated", s.auth.RequireApplicationToken, 401)
	s.assertQueryRequest("token", "clienttoken_elevated", s.auth.RequireClient, 200)
	s.assertQueryRequest("token", "clienttoken_elevated", s.auth.RequireAdmin, 403)
	s.assertQueryRequest("token", "clienttoken_elevated", s.auth.RequireElevatedClient, 200)

	// clienttoken_expired (non-admin, elevation expired)
	s.assertQueryRequest("token", "clienttoken_expired", s.auth.RequireApplicationToken, 401)
	s.assertQueryRequest("token", "clienttoken_expired", s.auth.RequireClient, 200)
	s.assertQueryRequest("token", "clienttoken_expired", s.auth.RequireAdmin, 403)
	s.assertQueryRequest("token", "clienttoken_expired", s.auth.RequireElevatedClient, 403)

	// clienttoken_admin (not elevated)
	s.assertQueryRequest("token", "clienttoken_admin", s.auth.RequireApplicationToken, 401)
	s.assertQueryRequest("token", "clienttoken_admin", s.auth.RequireClient, 200)
	s.assertQueryRequest("token", "clienttoken_admin", s.auth.RequireAdmin, 403)
	s.assertQueryRequest("token", "clienttoken_admin", s.auth.RequireElevatedClient, 403)

	// clienttoken_admin_elevated
	s.assertQueryRequest("token", "clienttoken_admin_elevated", s.auth.RequireApplicationToken, 401)
	s.assertQueryRequest("token", "clienttoken_admin_elevated", s.auth.RequireClient, 200)
	s.assertQueryRequest("token", "clienttoken_admin_elevated", s.auth.RequireAdmin, 200)
	s.assertQueryRequest("token", "clienttoken_admin_elevated", s.auth.RequireElevatedClient, 200)
}

func (s *AuthenticationSuite) assertQueryRequest(key, value string, f fMiddleware, code int) (ctx *gin.Context) {
	recorder := httptest.NewRecorder()
	ctx, _ = gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest("GET", fmt.Sprintf("/?%s=%s", key, value), nil)
	f(ctx)
	assert.Equal(s.T(), code, recorder.Code)
	return ctx
}

func (s *AuthenticationSuite) TestNothingProvided() {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest("GET", "/", nil)
	s.auth.RequireApplicationToken(ctx)
	assert.Equal(s.T(), 401, recorder.Code)
}

func (s *AuthenticationSuite) TestHeaderApiKeyToken() {
	// not existing token
	s.assertHeaderRequest("X-Gotify-Key", "ergerogerg", s.auth.RequireApplicationToken, 401)
	s.assertHeaderRequest("X-Gotify-Key", "ergerogerg", s.auth.RequireClient, 401)
	s.assertHeaderRequest("X-Gotify-Key", "ergerogerg", s.auth.RequireAdmin, 401)
	s.assertHeaderRequest("X-Gotify-Key", "ergerogerg", s.auth.RequireElevatedClient, 401)

	// not existing key
	s.assertHeaderRequest("X-Gotify-Keyx", "clienttoken", s.auth.RequireApplicationToken, 401)
	s.assertHeaderRequest("X-Gotify-Keyx", "clienttoken", s.auth.RequireClient, 401)
	s.assertHeaderRequest("X-Gotify-Keyx", "clienttoken", s.auth.RequireAdmin, 401)
	s.assertHeaderRequest("X-Gotify-Keyx", "clienttoken", s.auth.RequireElevatedClient, 401)

	// apptoken
	s.assertHeaderRequest("X-Gotify-Key", "apptoken", s.auth.RequireApplicationToken, 200)
	s.assertHeaderRequest("X-Gotify-Key", "apptoken", s.auth.RequireClient, 401)
	s.assertHeaderRequest("X-Gotify-Key", "apptoken", s.auth.RequireAdmin, 401)
	s.assertHeaderRequest("X-Gotify-Key", "apptoken", s.auth.RequireElevatedClient, 401)
	s.assertHeaderRequest("X-Gotify-Key", "apptoken_admin", s.auth.RequireApplicationToken, 200)
	s.assertHeaderRequest("X-Gotify-Key", "apptoken_admin", s.auth.RequireClient, 401)
	s.assertHeaderRequest("X-Gotify-Key", "apptoken_admin", s.auth.RequireAdmin, 401)
	s.assertHeaderRequest("X-Gotify-Key", "apptoken_admin", s.auth.RequireElevatedClient, 401)

	// clienttoken (non-admin, not elevated)
	s.assertHeaderRequest("X-Gotify-Key", "clienttoken", s.auth.RequireApplicationToken, 401)
	s.assertHeaderRequest("X-Gotify-Key", "clienttoken", s.auth.RequireClient, 200)
	s.assertHeaderRequest("X-Gotify-Key", "clienttoken", s.auth.RequireAdmin, 403)
	s.assertHeaderRequest("X-Gotify-Key", "clienttoken", s.auth.RequireElevatedClient, 403)

	// clienttoken_elevated (non-admin, elevated)
	s.assertHeaderRequest("X-Gotify-Key", "clienttoken_elevated", s.auth.RequireApplicationToken, 401)
	s.assertHeaderRequest("X-Gotify-Key", "clienttoken_elevated", s.auth.RequireClient, 200)
	s.assertHeaderRequest("X-Gotify-Key", "clienttoken_elevated", s.auth.RequireAdmin, 403)
	s.assertHeaderRequest("X-Gotify-Key", "clienttoken_elevated", s.auth.RequireElevatedClient, 200)

	// clienttoken_admin (not elevated)
	s.assertHeaderRequest("X-Gotify-Key", "clienttoken_admin", s.auth.RequireApplicationToken, 401)
	s.assertHeaderRequest("X-Gotify-Key", "clienttoken_admin", s.auth.RequireClient, 200)
	s.assertHeaderRequest("X-Gotify-Key", "clienttoken_admin", s.auth.RequireAdmin, 403)
	s.assertHeaderRequest("X-Gotify-Key", "clienttoken_admin", s.auth.RequireElevatedClient, 403)

	// clienttoken_admin_elevated
	s.assertHeaderRequest("X-Gotify-Key", "clienttoken_admin_elevated", s.auth.RequireApplicationToken, 401)
	s.assertHeaderRequest("X-Gotify-Key", "clienttoken_admin_elevated", s.auth.RequireClient, 200)
	s.assertHeaderRequest("X-Gotify-Key", "clienttoken_admin_elevated", s.auth.RequireAdmin, 200)
	s.assertHeaderRequest("X-Gotify-Key", "clienttoken_admin_elevated", s.auth.RequireElevatedClient, 200)
}

func (s *AuthenticationSuite) TestAuthorizationHeaderApiKeyToken() {
	// not existing token
	s.assertHeaderRequest("Authorization", "Bearer ergerogerg", s.auth.RequireApplicationToken, 401)
	s.assertHeaderRequest("Authorization", "Bearer ergerogerg", s.auth.RequireClient, 401)
	s.assertHeaderRequest("Authorization", "Bearer ergerogerg", s.auth.RequireAdmin, 401)
	s.assertHeaderRequest("Authorization", "Bearer ergerogerg", s.auth.RequireElevatedClient, 401)

	// no authentication schema
	s.assertHeaderRequest("Authorization", "ergerogerg", s.auth.RequireApplicationToken, 401)
	s.assertHeaderRequest("Authorization", "ergerogerg", s.auth.RequireClient, 401)
	s.assertHeaderRequest("Authorization", "ergerogerg", s.auth.RequireAdmin, 401)
	s.assertHeaderRequest("Authorization", "ergerogerg", s.auth.RequireElevatedClient, 401)

	// wrong authentication schema
	s.assertHeaderRequest("Authorization", "ApiKeyx clienttoken", s.auth.RequireApplicationToken, 401)
	s.assertHeaderRequest("Authorization", "ApiKeyx clienttoken", s.auth.RequireClient, 401)
	s.assertHeaderRequest("Authorization", "ApiKeyx clienttoken", s.auth.RequireAdmin, 401)
	s.assertHeaderRequest("Authorization", "ApiKeyx clienttoken", s.auth.RequireElevatedClient, 401)

	// Authorization Bearer apptoken
	s.assertHeaderRequest("Authorization", "Bearer apptoken", s.auth.RequireApplicationToken, 200)
	s.assertHeaderRequest("Authorization", "Bearer apptoken", s.auth.RequireClient, 401)
	s.assertHeaderRequest("Authorization", "Bearer apptoken", s.auth.RequireAdmin, 401)
	s.assertHeaderRequest("Authorization", "Bearer apptoken", s.auth.RequireElevatedClient, 401)
	s.assertHeaderRequest("Authorization", "Bearer apptoken_admin", s.auth.RequireApplicationToken, 200)
	s.assertHeaderRequest("Authorization", "Bearer apptoken_admin", s.auth.RequireClient, 401)
	s.assertHeaderRequest("Authorization", "Bearer apptoken_admin", s.auth.RequireAdmin, 401)
	s.assertHeaderRequest("Authorization", "Bearer apptoken_admin", s.auth.RequireElevatedClient, 401)

	// Authorization Bearer clienttoken (non-admin, not elevated)
	s.assertHeaderRequest("Authorization", "Bearer clienttoken", s.auth.RequireApplicationToken, 401)
	s.assertHeaderRequest("Authorization", "Bearer clienttoken", s.auth.RequireClient, 200)
	s.assertHeaderRequest("Authorization", "Bearer clienttoken", s.auth.RequireAdmin, 403)
	s.assertHeaderRequest("Authorization", "Bearer clienttoken", s.auth.RequireElevatedClient, 403)

	// Authorization Bearer clienttoken_elevated (non-admin, elevated)
	s.assertHeaderRequest("Authorization", "Bearer clienttoken_elevated", s.auth.RequireApplicationToken, 401)
	s.assertHeaderRequest("Authorization", "Bearer clienttoken_elevated", s.auth.RequireClient, 200)
	s.assertHeaderRequest("Authorization", "Bearer clienttoken_elevated", s.auth.RequireAdmin, 403)
	s.assertHeaderRequest("Authorization", "Bearer clienttoken_elevated", s.auth.RequireElevatedClient, 200)

	// Authorization bearer clienttoken_admin (not elevated)
	s.assertHeaderRequest("Authorization", "bearer clienttoken_admin", s.auth.RequireApplicationToken, 401)
	s.assertHeaderRequest("Authorization", "bearer clienttoken_admin", s.auth.RequireClient, 200)
	s.assertHeaderRequest("Authorization", "bearer clienttoken_admin", s.auth.RequireAdmin, 403)
	s.assertHeaderRequest("Authorization", "bearer clienttoken_admin", s.auth.RequireElevatedClient, 403)

	// Authorization Bearer clienttoken_admin_elevated
	s.assertHeaderRequest("Authorization", "Bearer clienttoken_admin_elevated", s.auth.RequireApplicationToken, 401)
	s.assertHeaderRequest("Authorization", "Bearer clienttoken_admin_elevated", s.auth.RequireClient, 200)
	s.assertHeaderRequest("Authorization", "Bearer clienttoken_admin_elevated", s.auth.RequireAdmin, 200)
	s.assertHeaderRequest("Authorization", "Bearer clienttoken_admin_elevated", s.auth.RequireElevatedClient, 200)
}

func (s *AuthenticationSuite) TestBasicAuth() {
	s.assertHeaderRequest("Authorization", "Basic ergerogerg", s.auth.RequireApplicationToken, 401)
	s.assertHeaderRequest("Authorization", "Basic ergerogerg", s.auth.RequireClient, 401)
	s.assertHeaderRequest("Authorization", "Basic ergerogerg", s.auth.RequireAdmin, 401)
	s.assertHeaderRequest("Authorization", "Basic ergerogerg", s.auth.RequireElevatedClient, 401)

	// user existing:pw
	s.assertHeaderRequest("Authorization", "Basic ZXhpc3Rpbmc6cHc=", s.auth.RequireApplicationToken, 403)
	s.assertHeaderRequest("Authorization", "Basic ZXhpc3Rpbmc6cHc=", s.auth.RequireClient, 200)
	s.assertHeaderRequest("Authorization", "Basic ZXhpc3Rpbmc6cHc=", s.auth.RequireAdmin, 403)
	s.assertHeaderRequest("Authorization", "Basic ZXhpc3Rpbmc6cHc=", s.auth.RequireElevatedClient, 200)

	// user admin:pw
	s.assertHeaderRequest("Authorization", "Basic YWRtaW46cHc=", s.auth.RequireApplicationToken, 403)
	s.assertHeaderRequest("Authorization", "Basic YWRtaW46cHc=", s.auth.RequireClient, 200)
	s.assertHeaderRequest("Authorization", "Basic YWRtaW46cHc=", s.auth.RequireAdmin, 200)
	s.assertHeaderRequest("Authorization", "Basic YWRtaW46cHc=", s.auth.RequireElevatedClient, 200)

	// user admin:pwx
	s.assertHeaderRequest("Authorization", "Basic YWRtaW46cHd4", s.auth.RequireApplicationToken, 401)
	s.assertHeaderRequest("Authorization", "Basic YWRtaW46cHd4", s.auth.RequireClient, 401)
	s.assertHeaderRequest("Authorization", "Basic YWRtaW46cHd4", s.auth.RequireAdmin, 401)
	s.assertHeaderRequest("Authorization", "Basic YWRtaW46cHd4", s.auth.RequireElevatedClient, 401)

	// user notexisting:pw
	s.assertHeaderRequest("Authorization", "Basic bm90ZXhpc3Rpbmc6cHc=", s.auth.RequireApplicationToken, 401)
	s.assertHeaderRequest("Authorization", "Basic bm90ZXhpc3Rpbmc6cHc=", s.auth.RequireClient, 401)
	s.assertHeaderRequest("Authorization", "Basic bm90ZXhpc3Rpbmc6cHc=", s.auth.RequireAdmin, 401)
	s.assertHeaderRequest("Authorization", "Basic bm90ZXhpc3Rpbmc6cHc=", s.auth.RequireElevatedClient, 401)
}

func (s *AuthenticationSuite) TestOptionalAuth() {
	// various invalid users
	ctx := s.assertQueryRequest("token", "ergerogerg", s.auth.Optional, 200)
	assert.Nil(s.T(), TryGetUserID(ctx))
	ctx = s.assertHeaderRequest("X-Gotify-Key", "ergerogerg", s.auth.Optional, 200)
	assert.Nil(s.T(), TryGetUserID(ctx))
	ctx = s.assertHeaderRequest("Authorization", "Basic bm90ZXhpc3Rpbmc6cHc=", s.auth.Optional, 200)
	assert.Nil(s.T(), TryGetUserID(ctx))
	ctx = s.assertHeaderRequest("Authorization", "Basic YWRtaW46cHd4", s.auth.Optional, 200)
	assert.Nil(s.T(), TryGetUserID(ctx))
	ctx = s.assertQueryRequest("tokenx", "clienttoken", s.auth.Optional, 200)
	assert.Nil(s.T(), TryGetUserID(ctx))
	ctx = s.assertQueryRequest("token", "apptoken_admin", s.auth.Optional, 200)
	assert.Nil(s.T(), TryGetUserID(ctx))

	// user existing:pw
	ctx = s.assertHeaderRequest("Authorization", "Basic ZXhpc3Rpbmc6cHc=", s.auth.Optional, 200)
	assert.Equal(s.T(), uint(1), *TryGetUserID(ctx))
	ctx = s.assertQueryRequest("token", "clienttoken", s.auth.Optional, 200)
	assert.Equal(s.T(), uint(1), *TryGetUserID(ctx))

	// user admin:pw
	ctx = s.assertHeaderRequest("Authorization", "Basic YWRtaW46cHc=", s.auth.Optional, 200)
	assert.Equal(s.T(), uint(2), *TryGetUserID(ctx))
	ctx = s.assertQueryRequest("token", "clienttoken_admin", s.auth.Optional, 200)
	assert.Equal(s.T(), uint(2), *TryGetUserID(ctx))
}

func (s *AuthenticationSuite) TestCookieToken() {
	// not existing token
	s.assertCookieRequest("ergerogerg", s.auth.RequireApplicationToken, 401)
	s.assertCookieRequest("ergerogerg", s.auth.RequireClient, 401)
	s.assertCookieRequest("ergerogerg", s.auth.RequireAdmin, 401)
	s.assertCookieRequest("ergerogerg", s.auth.RequireElevatedClient, 401)

	// apptoken
	s.assertCookieRequest("apptoken", s.auth.RequireApplicationToken, 200)
	s.assertCookieRequest("apptoken", s.auth.RequireClient, 401)
	s.assertCookieRequest("apptoken", s.auth.RequireAdmin, 401)
	s.assertCookieRequest("apptoken", s.auth.RequireElevatedClient, 401)

	// clienttoken (non-admin, not elevated)
	s.assertCookieRequest("clienttoken", s.auth.RequireApplicationToken, 401)
	s.assertCookieRequest("clienttoken", s.auth.RequireClient, 200)
	s.assertCookieRequest("clienttoken", s.auth.RequireAdmin, 403)
	s.assertCookieRequest("clienttoken", s.auth.RequireElevatedClient, 403)

	// clienttoken_elevated (non-admin, elevated)
	s.assertCookieRequest("clienttoken_elevated", s.auth.RequireApplicationToken, 401)
	s.assertCookieRequest("clienttoken_elevated", s.auth.RequireClient, 200)
	s.assertCookieRequest("clienttoken_elevated", s.auth.RequireAdmin, 403)
	s.assertCookieRequest("clienttoken_elevated", s.auth.RequireElevatedClient, 200)

	// clienttoken_admin (not elevated)
	s.assertCookieRequest("clienttoken_admin", s.auth.RequireClient, 200)
	s.assertCookieRequest("clienttoken_admin", s.auth.RequireAdmin, 403)
	s.assertCookieRequest("clienttoken_admin", s.auth.RequireElevatedClient, 403)

	// clienttoken_admin_elevated
	s.assertCookieRequest("clienttoken_admin_elevated", s.auth.RequireClient, 200)
	s.assertCookieRequest("clienttoken_admin_elevated", s.auth.RequireAdmin, 200)
	s.assertCookieRequest("clienttoken_admin_elevated", s.auth.RequireElevatedClient, 200)
}

func (s *AuthenticationSuite) assertCookieRequest(token string, f fMiddleware, code int) (ctx *gin.Context) {
	recorder := httptest.NewRecorder()
	ctx, _ = gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest("GET", "/", nil)
	ctx.Request.AddCookie(&http.Cookie{Name: cookieName, Value: token})
	f(ctx)
	assert.Equal(s.T(), code, recorder.Code)
	return ctx
}

func (s *AuthenticationSuite) assertHeaderRequest(key, value string, f fMiddleware, code int) (ctx *gin.Context) {
	recorder := httptest.NewRecorder()
	ctx, _ = gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest("GET", "/", nil)
	ctx.Request.Header.Set(key, value)
	f(ctx)
	assert.Equal(s.T(), code, recorder.Code)
	return ctx
}

type fMiddleware gin.HandlerFunc

// --- SessionService integration tests ---

func TestSessionServiceAuthSuite(t *testing.T) {
	suite.Run(t, new(SessionServiceAuthSuite))
}

type SessionServiceAuthSuite struct {
	suite.Suite
	auth *Auth
	DB   *testdb.Database
}

func (s *SessionServiceAuthSuite) SetupSuite() {
	mode.Set(mode.TestDev)
	s.DB = testdb.NewDB(s.T())

	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	timeNow = func() time.Time { return now }

	svc := session.NewService(s.DB.GormDatabase, func() string { return "Cdummy" })
	svc.SetTimeNow(func() time.Time { return now })
	s.auth = &Auth{DB: s.DB, SessionService: svc}

	elevated := now.Add(time.Hour)

	s.DB.CreateUser(&model.User{
		Name: "svcuser",
		Pass: password.CreatePassword("pw", 5),
		Clients: []model.Client{
			{Token: "svc_client", Name: "svc phone"},
			{Token: "svc_elevated", Name: "svc elevated", ElevatedUntil: &elevated},
		},
	})
}

func (s *SessionServiceAuthSuite) TearDownSuite() {
	timeNow = time.Now
	s.DB.Close()
}

func (s *SessionServiceAuthSuite) TestRequireClient_WithSessionService() {
	s.assertRequest("token", "svc_client", s.auth.RequireClient, 200)
	s.assertRequest("token", "svc_elevated", s.auth.RequireClient, 200)
	s.assertRequest("token", "nonexistent", s.auth.RequireClient, 401)
}

func (s *SessionServiceAuthSuite) TestRequireElevated_WithSessionService() {
	s.assertRequest("token", "svc_client", s.auth.RequireElevatedClient, 403)
	s.assertRequest("token", "svc_elevated", s.auth.RequireElevatedClient, 200)
}

func (s *SessionServiceAuthSuite) TestCookieRenewal_WithSessionService() {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest("GET", "/", nil)
	ctx.Request.AddCookie(&http.Cookie{Name: cookieName, Value: "svc_client"})

	s.auth.RequireClient(ctx)

	assert.Equal(s.T(), 200, recorder.Code)
	// Cookie should be re-set (refreshed) since LastUsed was nil
	cookies := recorder.Result().Cookies()
	var refreshed *http.Cookie
	for _, c := range cookies {
		if c.Name == CookieName {
			refreshed = c
			break
		}
	}
	assert.NotNil(s.T(), refreshed, "cookie should be refreshed on first cookie-based request")
	assert.Equal(s.T(), "svc_client", refreshed.Value)
}

func (s *SessionServiceAuthSuite) TestNonCookieRequest_NoCookieRefresh() {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest("GET", "/?token=svc_client", nil)

	s.auth.RequireClient(ctx)

	assert.Equal(s.T(), 200, recorder.Code)
	// No cookie should be set for query-param based requests
	cookies := recorder.Result().Cookies()
	for _, c := range cookies {
		assert.NotEqual(s.T(), CookieName, c.Name, "cookie should NOT be set for non-cookie requests")
	}
}

func (s *SessionServiceAuthSuite) assertRequest(key, value string, f fMiddleware, code int) {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest("GET", fmt.Sprintf("/?%s=%s", key, value), nil)
	f(ctx)
	assert.Equal(s.T(), code, recorder.Code)
}
