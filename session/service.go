package session

import (
	"time"

	"github.com/gotify/server/v2/model"
)

// ClientStore abstracts the persistence operations needed by the session service.
// database.GormDatabase satisfies this interface.
type ClientStore interface {
	CreateClient(client *model.Client) error
	GetClientByID(id uint) (*model.Client, error)
	UpdateClientTokensLastUsedAndExpiresAt(tokens []string, t *time.Time) error
	UpdateClientElevatedUntil(id uint, t *time.Time) error
	DeleteClientByID(id uint) error
	CleanupExpiredClients(now time.Time) ([]*model.Client, error)
}

// Service centralizes all session lifecycle operations: creation, renewal,
// elevation, logout, and expiration cleanup. All login entry points and
// middleware should delegate to this service for consistent behavior.
type Service struct {
	store         ClientStore
	generateToken func() string
	timeNow       func() time.Time
}

// NewService creates a new session service.
// The generateToken function is typically auth.GenerateClientToken.
func NewService(store ClientStore, generateToken func() string) *Service {
	return &Service{
		store:         store,
		generateToken: generateToken,
		timeNow:       time.Now,
	}
}

// SetTimeNow overrides the time source used by the service.
// This is primarily intended for testing; production code should use the default time.Now.
func (s *Service) SetTimeNow(timeNow func() time.Time) {
	s.timeNow = timeNow
}

// Create creates a new client/session record with the given policy applied.
// The tokenExists callback is used to ensure token uniqueness.
// Returns the created client (with ID populated by the database).
func (s *Service) Create(userID uint, name string, policy Policy, tokenExists func(string) bool) (*model.Client, error) {
	token := s.generateUniqueToken(tokenExists)
	client := &model.Client{
		Name:   name,
		Token:  token,
		UserID: userID,
	}

	// Apply expiration policy
	inactivity := policy.ExpiresAfterInactivity()
	client.ExpiresAfterInactivitySeconds = uint(inactivity.Seconds())

	// Apply initial elevation
	elevDur := policy.InitialElevationDuration()
	if elevDur > 0 {
		elevatedUntil := s.timeNow().Add(elevDur)
		client.ElevatedUntil = &elevatedUntil
	}

	if err := s.store.CreateClient(client); err != nil {
		return nil, err
	}
	return client, nil
}

// Renew updates LastUsed and recomputes ExpiresAt for the given client.
// Returns true if the client was actually updated (respects throttle interval).
// When throttle is 0, the client is always updated.
func (s *Service) Renew(client *model.Client, throttle time.Duration) (bool, error) {
	now := s.timeNow()
	if throttle > 0 && client.LastUsed != nil && client.LastUsed.Add(throttle).After(now) {
		return false, nil
	}
	if err := s.store.UpdateClientTokensLastUsedAndExpiresAt([]string{client.Token}, &now); err != nil {
		return false, err
	}
	client.LastUsed = &now
	return true, nil
}

// RenewTokens batch-updates LastUsed and ExpiresAt for multiple tokens.
// Unlike Renew, this does not check throttle — it always updates.
// Intended for the WebSocket heartbeat that keeps streaming clients alive.
func (s *Service) RenewTokens(tokens []string) error {
	if len(tokens) == 0 {
		return nil
	}
	now := s.timeNow()
	return s.store.UpdateClientTokensLastUsedAndExpiresAt(tokens, &now)
}

// Elevate sets the ElevatedUntil timestamp on an existing client.
func (s *Service) Elevate(clientID uint, duration time.Duration) error {
	elevatedUntil := s.timeNow().Add(duration)
	return s.store.UpdateClientElevatedUntil(clientID, &elevatedUntil)
}

// IsElevated checks whether the client is currently elevated.
func (s *Service) IsElevated(client *model.Client) bool {
	return client.ElevatedUntil != nil && s.timeNow().Before(*client.ElevatedUntil)
}

// Logout deletes the client record, ending the session.
func (s *Service) Logout(clientID uint) error {
	return s.store.DeleteClientByID(clientID)
}

// CleanupExpired removes all clients whose ExpiresAt has passed.
// Returns the list of deleted clients so callers can notify stream handlers.
func (s *Service) CleanupExpired(now time.Time) ([]*model.Client, error) {
	return s.store.CleanupExpiredClients(now)
}

// generateUniqueToken generates a unique token by retrying until tokenExists returns false.
func (s *Service) generateUniqueToken(tokenExists func(string) bool) string {
	for {
		token := s.generateToken()
		if !tokenExists(token) {
			return token
		}
	}
}
