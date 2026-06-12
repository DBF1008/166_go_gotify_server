package stream

import "github.com/gotify/server/v2/model"

// MessageFetcher retrieves messages for replay.
// Implemented by database.GormDatabase via structural typing.
type MessageFetcher interface {
	// GetMessagesByUserAfter returns messages for userID with ID > since,
	// ordered by ID ascending. Returns an empty slice if none found.
	GetMessagesByUserAfter(userID uint, since uint) ([]*model.MessageExternal, error)

	// MessageExistsForUser checks whether a message with the given ID exists
	// and belongs to the specified user.
	MessageExistsForUser(userID uint, messageID uint) (bool, error)
}
