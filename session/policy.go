package session

import "time"

// Policy defines the lifecycle behavior for a particular class of session/client.
// Each login method maps to a specific policy that controls expiration, elevation,
// and cookie behavior.
type Policy interface {
	// ExpiresAfterInactivity returns the inactivity timeout duration.
	// A value of 0 means the client never expires (persistent API clients).
	ExpiresAfterInactivity() time.Duration

	// InitialElevationDuration returns the initial elevation duration upon creation.
	// A value of 0 means the client is not elevated at creation time.
	InitialElevationDuration() time.Duration

	// HasCookie reports whether sessions of this type are delivered via browser cookie.
	HasCookie() bool

	// CookieMaxAge returns the browser-side max-age for the cookie in seconds.
	// Only meaningful when HasCookie() returns true.
	CookieMaxAge() int
}

// BrowserSessionPolicy is the policy for browser-based sessions created via
// local login or OIDC browser callback. Sessions expire after 7 days of inactivity
// and start with 1 hour of initial elevation.
type BrowserSessionPolicy struct{}

func (BrowserSessionPolicy) ExpiresAfterInactivity() time.Duration { return 7 * 24 * time.Hour }
func (BrowserSessionPolicy) InitialElevationDuration() time.Duration { return time.Hour }
func (BrowserSessionPolicy) HasCookie() bool                       { return true }
func (BrowserSessionPolicy) CookieMaxAge() int                     { return 7 * 24 * 60 * 60 }

// NativeSessionPolicy is the policy for native app sessions created via the OIDC
// external (PKCE) flow. Same expiration and elevation as browser sessions but
// tokens are delivered in the JSON response body instead of a cookie.
type NativeSessionPolicy struct{}

func (NativeSessionPolicy) ExpiresAfterInactivity() time.Duration { return 7 * 24 * time.Hour }
func (NativeSessionPolicy) InitialElevationDuration() time.Duration { return time.Hour }
func (NativeSessionPolicy) HasCookie() bool                       { return false }
func (NativeSessionPolicy) CookieMaxAge() int                     { return 0 }

// PersistentClientPolicy is the policy for API-created clients (POST /client).
// By default these never expire and are not elevated at creation.
// The caller may override the expiry via CustomExpiry.
type PersistentClientPolicy struct {
	// CustomExpiry allows the API caller to specify a custom inactivity timeout.
	// When nil, the client never expires.
	CustomExpiry *uint
}

func (p PersistentClientPolicy) ExpiresAfterInactivity() time.Duration {
	if p.CustomExpiry != nil && *p.CustomExpiry > 0 {
		return time.Duration(*p.CustomExpiry) * time.Second
	}
	return 0
}

func (PersistentClientPolicy) InitialElevationDuration() time.Duration { return 0 }
func (PersistentClientPolicy) HasCookie() bool                       { return false }
func (PersistentClientPolicy) CookieMaxAge() int                     { return 0 }
