package database

import (
	"github.com/gotify/server/v2/model"
	"gorm.io/gorm"
)

// GetUserByName returns the user by the given name or nil.
func (d *GormDatabase) GetUserByName(name string) (*model.User, error) {
	user := new(model.User)
	err := d.DB.Where("name = ?", name).Find(user).Error
	if err == gorm.ErrRecordNotFound {
		err = nil
	}
	if user.Name == name {
		return user, err
	}
	return nil, err
}

// GetUserByID returns the user by the given id or nil.
func (d *GormDatabase) GetUserByID(id uint) (*model.User, error) {
	user := new(model.User)
	err := d.DB.Find(user, id).Error
	if err == gorm.ErrRecordNotFound {
		err = nil
	}
	if user.ID == id {
		return user, err
	}
	return nil, err
}

// CountUser returns the user count which satisfies the given condition.
func (d *GormDatabase) CountUser(condition ...interface{}) (int64, error) {
	c := int64(-1)
	handle := d.DB.Model(new(model.User))
	if len(condition) == 1 {
		handle = handle.Where(condition[0])
	} else if len(condition) > 1 {
		handle = handle.Where(condition[0], condition[1:]...)
	}
	err := handle.Count(&c).Error
	return c, err
}

// GetUsers returns all users.
func (d *GormDatabase) GetUsers() ([]*model.User, error) {
	var users []*model.User
	err := d.DB.Find(&users).Error
	return users, err
}

// DeleteUserByID deletes a user by its id.
// All associated records (messages, plugin configs, applications, clients) are deleted
// atomically within a single transaction.
func (d *GormDatabase) DeleteUserByID(id uint) error {
	return d.DB.Transaction(func(tx *gorm.DB) error {
		// Delete messages belonging to the user's applications first,
		// since messages reference application_id.
		var appIDs []uint
		if err := tx.Model(&model.Application{}).Where("user_id = ?", id).Pluck("id", &appIDs).Error; err != nil {
			return err
		}
		if len(appIDs) > 0 {
			if err := tx.Where("application_id IN ?", appIDs).Delete(&model.Message{}).Error; err != nil {
				return err
			}
		}
		if err := tx.Where("user_id = ?", id).Delete(&model.PluginConf{}).Error; err != nil {
			return err
		}
		if err := tx.Where("user_id = ?", id).Delete(&model.Application{}).Error; err != nil {
			return err
		}
		if err := tx.Where("user_id = ?", id).Delete(&model.Client{}).Error; err != nil {
			return err
		}
		return tx.Where("id = ?", id).Delete(&model.User{}).Error
	})
}

// UpdateUser updates a user.
func (d *GormDatabase) UpdateUser(user *model.User) error {
	return d.DB.Save(user).Error
}

// CreateUser creates a user.
func (d *GormDatabase) CreateUser(user *model.User) error {
	return d.DB.Create(user).Error
}
