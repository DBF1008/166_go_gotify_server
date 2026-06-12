package database

import (
	"time"

	"github.com/gotify/server/v2/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMessageExpiration_DifferentAppConfigs verifies that each message's expiry
// is derived from its application's retention setting and that reads and cleanup
// honor those per-application configurations independently.
func (s *DatabaseSuite) TestMessageExpiration_DifferentAppConfigs() {
	user := &model.User{Name: "u", Pass: []byte{1}}
	require.NoError(s.T(), s.db.CreateUser(user))

	appForever := &model.Application{ID: 1, UserID: user.ID, Token: "AF", DefaultMessageExpirationSeconds: 0}
	appShort := &model.Application{ID: 2, UserID: user.ID, Token: "AS", DefaultMessageExpirationSeconds: 100}
	appLong := &model.Application{ID: 3, UserID: user.ID, Token: "AL", DefaultMessageExpirationSeconds: 3600}
	require.NoError(s.T(), s.db.CreateApplication(appForever))
	require.NoError(s.T(), s.db.CreateApplication(appShort))
	require.NoError(s.T(), s.db.CreateApplication(appLong))

	mForever := &model.Message{ID: 10, ApplicationID: appForever.ID, Date: now}
	mShort := &model.Message{ID: 11, ApplicationID: appShort.ID, Date: now}
	mLong := &model.Message{ID: 12, ApplicationID: appLong.ID, Date: now}
	require.NoError(s.T(), s.db.CreateMessage(mForever))
	require.NoError(s.T(), s.db.CreateMessage(mShort))
	require.NoError(s.T(), s.db.CreateMessage(mLong))

	// Expiry is computed from each application's retention.
	assert.Nil(s.T(), mForever.ExpiresAt, "retention 0 must keep messages forever")
	if assert.NotNil(s.T(), mShort.ExpiresAt) {
		assert.Equal(s.T(), now.Add(100*time.Second).Unix(), mShort.ExpiresAt.Unix())
	}
	if assert.NotNil(s.T(), mLong.ExpiresAt) {
		assert.Equal(s.T(), now.Add(3600*time.Second).Unix(), mLong.ExpiresAt.Unix())
	}

	// At `now` nothing has expired yet, so all three are visible.
	if msgs, err := s.db.GetMessagesByUser(user.ID); assert.NoError(s.T(), err) {
		assert.Len(s.T(), msgs, 3)
	}

	// Advance the clock past appShort's retention but not appLong's.
	s.db.DB.NowFunc = func() time.Time { return now.Add(200 * time.Second) }

	msgs, err := s.db.GetMessagesByUser(user.ID)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), []uint{12, 10}, ids(msgs), "appShort's message must be filtered, appForever/appLong remain")

	// Cleanup at the same instant removes only the expired (appShort) message.
	deleted, err := s.db.CleanupExpiredMessages(now.Add(200 * time.Second))
	require.NoError(s.T(), err)
	assert.Equal(s.T(), int64(1), deleted)
	s.assertMessagePresent(10)
	s.assertMessageGone(11)
	s.assertMessagePresent(12)
}

// TestMessageExpiration_ImmediateAndBoundary covers a message that is already
// expired ("immediate" expiration) and the exact-boundary case where expiry
// equals the current time. The read filter (expires_at > now) and cleanup
// (expires_at <= now) are complementary at equality, so a message expiring
// exactly at `now` is consistently treated as expired by both.
func (s *DatabaseSuite) TestMessageExpiration_ImmediateAndBoundary() {
	user := &model.User{Name: "u", Pass: []byte{1}}
	require.NoError(s.T(), s.db.CreateUser(user))
	app := &model.Application{ID: 1, UserID: user.ID, Token: "A"}
	require.NoError(s.T(), s.db.CreateApplication(app))

	past := now.Add(-time.Second)
	atNow := now
	future := now.Add(time.Second)

	// CreateMessage preserves an explicitly set ExpiresAt.
	require.NoError(s.T(), s.db.CreateMessage(&model.Message{ID: 1, ApplicationID: app.ID, Date: now, ExpiresAt: &past}))
	require.NoError(s.T(), s.db.CreateMessage(&model.Message{ID: 2, ApplicationID: app.ID, Date: now, ExpiresAt: &atNow}))
	require.NoError(s.T(), s.db.CreateMessage(&model.Message{ID: 3, ApplicationID: app.ID, Date: now, ExpiresAt: &future}))

	// Only the live (future) message is visible — across every list query.
	if msgs, err := s.db.GetMessagesByApplication(app.ID); assert.NoError(s.T(), err) {
		assert.Equal(s.T(), []uint{3}, ids(msgs))
	}
	if msgs, err := s.db.GetMessagesByApplicationSince(app.ID, 100, 0); assert.NoError(s.T(), err) {
		assert.Equal(s.T(), []uint{3}, ids(msgs))
	}
	if msgs, err := s.db.GetMessagesByUser(user.ID); assert.NoError(s.T(), err) {
		assert.Equal(s.T(), []uint{3}, ids(msgs))
	}
	if msgs, err := s.db.GetMessagesByUserSince(user.ID, 100, 0); assert.NoError(s.T(), err) {
		assert.Equal(s.T(), []uint{3}, ids(msgs))
	}

	// Cleanup at `now` removes the past and the exactly-at-now message.
	deleted, err := s.db.CleanupExpiredMessages(now)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), int64(2), deleted)
	s.assertMessageGone(1)
	s.assertMessageGone(2)
	s.assertMessagePresent(3)
}

// TestMessageExpiration_Pagination verifies that paged reads return only live
// messages in correct order and chain correctly via `since`, even when expired
// messages are interleaved across page boundaries.
func (s *DatabaseSuite) TestMessageExpiration_Pagination() {
	user := &model.User{Name: "u", Pass: []byte{1}}
	require.NoError(s.T(), s.db.CreateUser(user))
	app := &model.Application{ID: 1, UserID: user.ID, Token: "A"}
	require.NoError(s.T(), s.db.CreateApplication(app))

	past := now.Add(-time.Second)
	future := now.Add(time.Hour)
	// IDs 1..20: even IDs expired, odd IDs live (10 live, 10 expired).
	for i := uint(1); i <= 20; i++ {
		m := &model.Message{ID: i, ApplicationID: app.ID, Date: now}
		if i%2 == 0 {
			m.ExpiresAt = &past
		} else {
			m.ExpiresAt = &future
		}
		require.NoError(s.T(), s.db.CreateMessage(m))
	}

	page1, err := s.db.GetMessagesByApplicationSince(app.ID, 3, 0)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), []uint{19, 17, 15}, ids(page1))

	page2, err := s.db.GetMessagesByApplicationSince(app.ID, 3, 15)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), []uint{13, 11, 9}, ids(page2))

	// `since` landing on an expired id still chains correctly.
	page3, err := s.db.GetMessagesByApplicationSince(app.ID, 3, 10)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), []uint{9, 7, 5}, ids(page3))

	// Tail page returns fewer than the limit.
	page4, err := s.db.GetMessagesByApplicationSince(app.ID, 3, 5)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), []uint{3, 1}, ids(page4))

	// The user-scoped query returns all live messages.
	all, err := s.db.GetMessagesByUserSince(user.ID, 100, 0)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), []uint{19, 17, 15, 13, 11, 9, 7, 5, 3, 1}, ids(all))

	// A window past the last live message is empty.
	empty, err := s.db.GetMessagesByApplicationSince(app.ID, 3, 1)
	require.NoError(s.T(), err)
	assert.Empty(s.T(), empty)
}

// TestCleanupExpiredMessages_RestartRecovery simulates the cleanup task running
// on startup after downtime: it is invoked with a `now` well past the expiry of
// several messages and must purge exactly those, idempotently, leaving messages
// that are still within their retention window.
func (s *DatabaseSuite) TestCleanupExpiredMessages_RestartRecovery() {
	user := &model.User{Name: "u", Pass: []byte{1}}
	require.NoError(s.T(), s.db.CreateUser(user))
	app := &model.Application{ID: 1, UserID: user.ID, Token: "A", DefaultMessageExpirationSeconds: 60}
	require.NoError(s.T(), s.db.CreateApplication(app))

	require.NoError(s.T(), s.db.CreateMessage(&model.Message{ID: 1, ApplicationID: app.ID, Date: now}))                       // expires now+60
	require.NoError(s.T(), s.db.CreateMessage(&model.Message{ID: 2, ApplicationID: app.ID, Date: now.Add(30 * time.Second)})) // expires now+90
	require.NoError(s.T(), s.db.CreateMessage(&model.Message{ID: 3, ApplicationID: app.ID, Date: now.Add(10 * time.Minute)})) // expires now+10m60s

	// Server was "down"; time advanced to now+5m. The startup sweep purges 1 and 2.
	startupNow := now.Add(5 * time.Minute)
	deleted, err := s.db.CleanupExpiredMessages(startupNow)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), int64(2), deleted)
	s.assertMessageGone(1)
	s.assertMessageGone(2)
	s.assertMessagePresent(3)

	// Re-running the sweep at the same time changes nothing (idempotent).
	deleted, err = s.db.CleanupExpiredMessages(startupNow)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), int64(0), deleted)

	// After a longer downtime the remaining message is purged too.
	deleted, err = s.db.CleanupExpiredMessages(now.Add(time.Hour))
	require.NoError(s.T(), err)
	assert.Equal(s.T(), int64(1), deleted)
	s.assertMessageGone(3)
}

// TestMessageExpiration_NilNeverExpires guards backward compatibility: messages
// without an expiry (e.g. rows created before the retention feature, or from
// applications with retention disabled) are never filtered out nor cleaned up.
func (s *DatabaseSuite) TestMessageExpiration_NilNeverExpires() {
	user := &model.User{Name: "u", Pass: []byte{1}}
	require.NoError(s.T(), s.db.CreateUser(user))
	app := &model.Application{ID: 1, UserID: user.ID, Token: "A"} // retention disabled
	require.NoError(s.T(), s.db.CreateApplication(app))

	m := &model.Message{ID: 1, ApplicationID: app.ID, Date: now}
	require.NoError(s.T(), s.db.CreateMessage(m))
	assert.Nil(s.T(), m.ExpiresAt)

	// Even a sweep far in the future leaves nil-expiry messages untouched.
	deleted, err := s.db.CleanupExpiredMessages(now.Add(100 * 365 * 24 * time.Hour))
	require.NoError(s.T(), err)
	assert.Equal(s.T(), int64(0), deleted)

	if msgs, err := s.db.GetMessagesByApplication(app.ID); assert.NoError(s.T(), err) {
		assert.Len(s.T(), msgs, 1)
	}
}

func (s *DatabaseSuite) assertMessagePresent(id uint) {
	if msg, err := s.db.GetMessageByID(id); assert.NoError(s.T(), err) {
		assert.NotNil(s.T(), msg, "message %d should still exist", id)
	}
}

func (s *DatabaseSuite) assertMessageGone(id uint) {
	if msg, err := s.db.GetMessageByID(id); assert.NoError(s.T(), err) {
		assert.Nil(s.T(), msg, "message %d should be deleted", id)
	}
}

func ids(msgs []*model.Message) []uint {
	result := make([]uint, len(msgs))
	for i, m := range msgs {
		result[i] = m.ID
	}
	return result
}
