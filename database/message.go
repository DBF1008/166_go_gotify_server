package database

import (
	"time"

	"github.com/gotify/server/v2/model"
	"gorm.io/gorm"
)

// GetMessageByID returns the messages for the given id or nil.
func (d *GormDatabase) GetMessageByID(id uint) (*model.Message, error) {
	msg := new(model.Message)
	err := d.DB.Find(msg, id).Error
	if err == gorm.ErrRecordNotFound {
		err = nil
	}
	if msg.ID == id {
		return msg, err
	}
	return nil, err
}

// CreateMessage creates a message. When the message has no expiry set, it is
// derived from the owning application's retention setting
// (Application.DefaultMessageExpirationSeconds).
func (d *GormDatabase) CreateMessage(message *model.Message) error {
	if message.ExpiresAt == nil {
		if app, err := d.GetApplicationByID(message.ApplicationID); err == nil && app != nil {
			message.ExpiresAt = app.MessageExpiresAt(message.Date, d.DB.NowFunc())
		}
	}
	return d.DB.Create(message).Error
}

// GetMessagesByUser returns all messages from a user.
func (d *GormDatabase) GetMessagesByUser(userID uint) ([]*model.Message, error) {
	var messages []*model.Message
	db := d.DB.Joins("JOIN applications ON applications.user_id = ?", userID).
		Where("messages.application_id = applications.id").Order("messages.id desc")
	err := d.messagesNotExpired(db).Find(&messages).Error
	if err == gorm.ErrRecordNotFound {
		err = nil
	}
	return messages, err
}

// GetMessagesByUserSince returns limited messages from a user.
// If since is 0 it will be ignored.
func (d *GormDatabase) GetMessagesByUserSince(userID uint, limit int, since uint) ([]*model.Message, error) {
	var messages []*model.Message
	db := d.DB.Joins("JOIN applications ON applications.user_id = ?", userID).
		Where("messages.application_id = applications.id").Order("messages.id desc").Limit(limit)
	if since != 0 {
		db = db.Where("messages.id < ?", since)
	}
	err := d.messagesNotExpired(db).Find(&messages).Error
	if err == gorm.ErrRecordNotFound {
		err = nil
	}
	return messages, err
}

// GetMessagesByApplication returns all messages from an application.
func (d *GormDatabase) GetMessagesByApplication(tokenID uint) ([]*model.Message, error) {
	var messages []*model.Message
	db := d.DB.Where("application_id = ?", tokenID).Order("messages.id desc")
	err := d.messagesNotExpired(db).Find(&messages).Error
	if err == gorm.ErrRecordNotFound {
		err = nil
	}
	return messages, err
}

// GetMessagesByApplicationSince returns limited messages from an application.
// If since is 0 it will be ignored.
func (d *GormDatabase) GetMessagesByApplicationSince(appID uint, limit int, since uint) ([]*model.Message, error) {
	var messages []*model.Message
	db := d.DB.Where("application_id = ?", appID).Order("messages.id desc").Limit(limit)
	if since != 0 {
		db = db.Where("messages.id < ?", since)
	}
	err := d.messagesNotExpired(db).Find(&messages).Error
	if err == gorm.ErrRecordNotFound {
		err = nil
	}
	return messages, err
}

// DeleteMessageByID deletes a message by its id.
func (d *GormDatabase) DeleteMessageByID(id uint) error {
	return d.DB.Where("id = ?", id).Delete(&model.Message{}).Error
}

// DeleteMessagesByApplication deletes all messages from an application.
func (d *GormDatabase) DeleteMessagesByApplication(applicationID uint) error {
	return d.DB.Where("application_id = ?", applicationID).Delete(&model.Message{}).Error
}

// DeleteMessagesByUser deletes all messages from a user.
func (d *GormDatabase) DeleteMessagesByUser(userID uint) error {
	app, _ := d.GetApplicationsByUser(userID)
	for _, app := range app {
		d.DeleteMessagesByApplication(app.ID)
	}
	return nil
}

// CleanupExpiredMessages deletes all messages whose expiry has passed and
// returns the number of deleted messages. Messages without an expiry are never
// removed.
func (d *GormDatabase) CleanupExpiredMessages(now time.Time) (int64, error) {
	res := d.DB.Where("expires_at IS NOT NULL AND expires_at <= ?", now).Delete(&model.Message{})
	return res.RowsAffected, res.Error
}

// messagesNotExpired scopes a query to messages that have not yet expired. The
// column is qualified because some queries join the applications table.
func (d *GormDatabase) messagesNotExpired(tx *gorm.DB) *gorm.DB {
	return tx.Where("messages.expires_at IS NULL OR messages.expires_at > ?", d.DB.NowFunc())
}
