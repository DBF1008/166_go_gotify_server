package auth

import (
	"net/http"
)

// CookieMaxAge is the default lifetime of the session cookie in seconds (7 days).
// This value is also used as ExpiresAfterInactivitySeconds for browser sessions.
// For policy-driven session management, see the session package.
const CookieMaxAge = 7 * 24 * 60 * 60

// CookieName is the name of the session cookie used for browser-based authentication.
const CookieName = "gotify-client-token"

// SetCookie sets a session cookie on the response.
// Use maxAge=-1 to delete the cookie (logout).
func SetCookie(w http.ResponseWriter, token string, maxAge int, secure bool) {
	http.SetCookie(w, &http.Cookie{
		Name:     CookieName,
		Value:    token,
		Path:     "/",
		MaxAge:   maxAge,
		Secure:   secure,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
	})
}
