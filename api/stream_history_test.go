package api

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/gotify/server/v2/model"
	"github.com/gotify/server/v2/test/testdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamMessageHistory_GetMessagesAfter(t *testing.T) {
	db := testdb.NewDB(t)
	defer db.Close()

	userBuilder := db.User(1)
	app1 := userBuilder.NewAppWithToken(10, "app-ten")
	app2 := userBuilder.NewAppWithToken(20, "app-twenty")
	// A second user's messages must never leak into another user's replay.
	db.User(2).App(30).Message(1000)

	now := time.Now()
	extras, err := json.Marshal(map[string]interface{}{"x::y": 1.0})
	require.NoError(t, err)
	require.NoError(t, db.CreateMessage(&model.Message{ID: 1, ApplicationID: app1.ID, Message: "one", Title: "t1", Priority: 4, Date: now}))
	require.NoError(t, db.CreateMessage(&model.Message{ID: 2, ApplicationID: app2.ID, Message: "two", Date: now}))
	require.NoError(t, db.CreateMessage(&model.Message{ID: 3, ApplicationID: app1.ID, Message: "three", Extras: extras, Date: now}))

	history := &StreamMessageHistory{DB: db}

	// Replay from the start: ordered ascending, mixed apps, converted to the external form.
	msgs, err := history.GetMessagesAfter(1, 0, 100)
	require.NoError(t, err)
	require.Len(t, msgs, 3)
	assert.Equal(t, uint(1), msgs[0].ID)
	assert.Equal(t, app1.ID, msgs[0].ApplicationID)
	require.NotNil(t, msgs[0].Priority)
	assert.Equal(t, 4, *msgs[0].Priority) // priority converted from the internal representation
	assert.Equal(t, uint(2), msgs[1].ID)
	assert.Equal(t, app2.ID, msgs[1].ApplicationID)
	assert.Equal(t, uint(3), msgs[2].ID)
	assert.Equal(t, map[string]interface{}{"x::y": 1.0}, msgs[2].Extras) // extras decoded back to a map

	// A cursor in the middle only replays the newer messages.
	msgs, err = history.GetMessagesAfter(1, 2, 100)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, uint(3), msgs[0].ID)

	// The limit is honored.
	msgs, err = history.GetMessagesAfter(1, 0, 2)
	require.NoError(t, err)
	require.Len(t, msgs, 2)
	assert.Equal(t, uint(1), msgs[0].ID)
	assert.Equal(t, uint(2), msgs[1].ID)

	// A cursor beyond the newest message replays nothing.
	msgs, err = history.GetMessagesAfter(1, 99, 100)
	require.NoError(t, err)
	assert.Empty(t, msgs)
}
