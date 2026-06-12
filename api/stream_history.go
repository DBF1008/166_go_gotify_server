package api

import "github.com/gotify/server/v2/model"

// MessageHistoryDatabase is the subset of database access needed to replay the messages a
// streaming client missed while it was disconnected.
type MessageHistoryDatabase interface {
	GetMessagesByUserAfter(userID uint, limit int, after uint) ([]*model.Message, error)
}

// StreamMessageHistory adapts the message database to the streaming API: it fetches the
// messages created after a client's resume cursor and converts them to the external
// representation that is delivered over the WebSocket stream.
//
// It is wired separately from MessageAPI so the stream can be constructed before the message
// handler (which depends on the stream as its notifier), avoiding a construction cycle.
type StreamMessageHistory struct {
	DB MessageHistoryDatabase
}

// GetMessagesAfter returns up to limit messages for the user with an id greater than 'after',
// ordered ascending (oldest first), as external messages ready to be streamed.
func (h *StreamMessageHistory) GetMessagesAfter(userID, after uint, limit int) ([]*model.MessageExternal, error) {
	messages, err := h.DB.GetMessagesByUserAfter(userID, limit, after)
	if err != nil {
		return nil, err
	}
	return toExternalMessages(messages), nil
}
