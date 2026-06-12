package plugin

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"plugin"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/gotify/server/v2/auth"
	"github.com/gotify/server/v2/model"
	"github.com/gotify/server/v2/plugin/compat"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

// The Database interface for encapsulating database access.
type Database interface {
	GetUsers() ([]*model.User, error)
	GetPluginConfByUserAndPath(userid uint, path string) (*model.PluginConf, error)
	CreatePluginConf(p *model.PluginConf) error
	GetPluginConfByApplicationID(appid uint) (*model.PluginConf, error)
	UpdatePluginConf(p *model.PluginConf) error
	CreateMessage(message *model.Message) error
	GetPluginConfByID(id uint) (*model.PluginConf, error)
	GetPluginConfByToken(token string) (*model.PluginConf, error)
	GetUserByID(id uint) (*model.User, error)
	CreateApplication(application *model.Application) error
	UpdateApplication(app *model.Application) error
	GetApplicationsByUser(userID uint) ([]*model.Application, error)
	GetApplicationByToken(token string) (*model.Application, error)
}

// Notifier notifies when a new message was created.
type Notifier interface {
	Notify(userID uint, message *model.MessageExternal)
}

// Manager is the coordination layer for plugins. It owns the catalog of loaded plugins, the
// registry of live instance state machines, and the lock that guards them, and it reconciles
// the persisted internal-application flags. All per-instance lifecycle work (wiring, config
// validation, enable/disable, teardown) is delegated to instanceState, which keeps the runtime
// instance and the persisted PluginConf in sync.
//
// Locking discipline: every mutator (InitializeForUserID, SetPluginEnabled, SetConfig,
// RemoveUser, and the NewManager boot init) acquires the write lock; the unexported transition
// helpers and per-user init assume it is held. Readers (Instance, HasInstance, PluginInfo) take
// the read lock.
type Manager struct {
	mutex    *sync.RWMutex
	states   map[uint]*instanceState
	plugins  map[string]compat.Plugin
	messages chan MessageWithUserID
	db       Database
	mux      *gin.RouterGroup
}

// NewManager created a Manager from configurations.
func NewManager(db Database, directory string, mux *gin.RouterGroup, notifier Notifier) (*Manager, error) {
	manager := &Manager{
		mutex:    &sync.RWMutex{},
		states:   map[uint]*instanceState{},
		plugins:  map[string]compat.Plugin{},
		messages: make(chan MessageWithUserID),
		db:       db,
		mux:      mux,
	}

	go func() {
		for {
			message := <-manager.messages
			internalMsg := &model.Message{
				ApplicationID: message.Message.ApplicationID,
				Title:         message.Message.Title,
				Priority:      *message.Message.Priority,
				Date:          message.Message.Date,
				Message:       message.Message.Message,
			}
			if message.Message.Extras != nil {
				internalMsg.Extras, _ = json.Marshal(message.Message.Extras)
			}
			db.CreateMessage(internalMsg)
			message.Message.ID = internalMsg.ID
			notifier.Notify(message.UserID, &message.Message)
		}
	}()

	if err := manager.loadPlugins(directory); err != nil {
		return nil, err
	}

	users, err := manager.db.GetUsers()
	if err != nil {
		return nil, err
	}
	if err := func() error {
		manager.mutex.Lock()
		defer manager.mutex.Unlock()
		for _, user := range users {
			if err := manager.initializeForUser(*user); err != nil {
				return err
			}
		}
		return nil
	}(); err != nil {
		return nil, err
	}

	return manager, nil
}

// ErrAlreadyEnabledOrDisabled is returned on SetPluginEnabled call when a plugin is already enabled or disabled.
var ErrAlreadyEnabledOrDisabled = errors.New("config is already enabled/disabled")

func (m *Manager) applicationExists(token string) bool {
	app, _ := m.db.GetApplicationByToken(token)
	return app != nil
}

func (m *Manager) pluginConfExists(token string) bool {
	pluginConf, _ := m.db.GetPluginConfByToken(token)
	return pluginConf != nil
}

// SetPluginEnabled sets the plugins enabled state.
func (m *Manager) SetPluginEnabled(pluginID uint, enabled bool) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	state, ok := m.states[pluginID]
	if !ok {
		return errors.New("instance not found")
	}
	return state.setEnabled(enabled)
}

// SetConfig validates and persists a new configuration for a plugin instance. It is the shared
// entry point for the API config-update flow: the instance only receives the config if it is
// valid, and the bytes are persisted only after validation succeeds. A persistence failure is
// reported via ErrConfigPersistence so the caller can distinguish it from an invalid config.
func (m *Manager) SetConfig(pluginID uint, raw []byte) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	state, ok := m.states[pluginID]
	if !ok {
		return errors.New("instance not found")
	}
	return state.setConfig(raw)
}

// PluginInfo returns plugin info.
func (m *Manager) PluginInfo(modulePath string) compat.Info {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if p, ok := m.plugins[modulePath]; ok {
		return p.PluginInfo()
	}
	log.Warn().Str("module_path", modulePath).Msg("Could not get plugin info")
	return compat.Info{
		Name:        "UNKNOWN",
		ModulePath:  modulePath,
		Description: "Oops something went wrong",
	}
}

// Instance returns an instance with the given ID.
func (m *Manager) Instance(pluginID uint) (compat.PluginInstance, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if state, ok := m.states[pluginID]; ok {
		return state.instance, nil
	}
	return nil, errors.New("instance not found")
}

// HasInstance returns whether the given plugin ID has a corresponding instance.
func (m *Manager) HasInstance(pluginID uint) bool {
	instance, err := m.Instance(pluginID)
	return err == nil && instance != nil
}

// RemoveUser tears down all plugin instances of a user when the user is deleted. It is
// best-effort: every instance is disabled and unregistered even if disabling one of them fails,
// so the registry is always left clean. The first error encountered (if any) is returned.
func (m *Manager) RemoveUser(userID uint) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	var firstErr error
	for _, state := range m.userStates(userID) {
		if err := state.teardown(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// userStates returns the live instance states that belong to a user.
func (m *Manager) userStates(userID uint) []*instanceState {
	var states []*instanceState
	for _, state := range m.states {
		if state.conf.UserID == userID {
			states = append(states, state)
		}
	}
	return states
}

type pluginFileLoadError struct {
	Filename        string
	UnderlyingError error
}

func (c pluginFileLoadError) Error() string {
	return fmt.Sprintf("error while loading plugin %s: %s", c.Filename, c.UnderlyingError)
}

func (m *Manager) loadPlugins(directory string) error {
	if directory == "" {
		return nil
	}

	pluginFiles, err := os.ReadDir(directory)
	if err != nil {
		return fmt.Errorf("error while reading directory %s", err)
	}
	for _, f := range pluginFiles {
		if f.IsDir() {
			continue
		}

		name := f.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}

		pluginPath := filepath.Join(directory, "./", name)

		log.Info().Str("path", pluginPath).Msg("Loading plugin")
		pRaw, err := plugin.Open(pluginPath)
		if err != nil {
			return pluginFileLoadError{name, err}
		}
		compatPlugin, err := compat.Wrap(pRaw)
		if err != nil {
			return pluginFileLoadError{name, err}
		}
		if err := m.LoadPlugin(compatPlugin); err != nil {
			return pluginFileLoadError{name, err}
		}
	}
	return nil
}

// LoadPlugin loads a compat plugin, exported to sideload plugins for testing purposes.
func (m *Manager) LoadPlugin(compatPlugin compat.Plugin) error {
	modulePath := compatPlugin.PluginInfo().ModulePath
	if _, ok := m.plugins[modulePath]; ok {
		return fmt.Errorf("plugin with module path %s is present at least twice", modulePath)
	}
	m.plugins[modulePath] = compatPlugin
	return nil
}

// InitializeForUserID initializes all plugin instances for a given user.
func (m *Manager) InitializeForUserID(userID uint) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	user, err := m.db.GetUserByID(userID)
	if err != nil {
		return err
	}
	if user != nil {
		return m.initializeForUser(*user)
	}
	return fmt.Errorf("user with id %d not found", userID)
}

// initializeForUser instantiates every loaded plugin for the user and reconciles the user's
// internal-application flags. The caller must hold the write lock.
func (m *Manager) initializeForUser(user model.User) error {
	userCtx := compat.UserContext{
		ID:    user.ID,
		Name:  user.Name,
		Admin: user.Admin,
	}

	for _, p := range m.plugins {
		if err := m.initializeSingleUserPlugin(userCtx, p); err != nil {
			return err
		}
	}

	return m.reconcileInternalApps(user.ID)
}

// reconcileInternalApps is the single authority for the Application.Internal flag. An
// application is internal exactly when it is backed by a plugin configuration whose plugin is
// currently loaded. It is recomputed on every (re)initialization so the flag never drifts when
// plugins are added or removed. The caller must hold the write lock.
func (m *Manager) reconcileInternalApps(userID uint) error {
	apps, err := m.db.GetApplicationsByUser(userID)
	if err != nil {
		return err
	}
	for _, app := range apps {
		conf, err := m.db.GetPluginConfByApplicationID(app.ID)
		if err != nil {
			return err
		}
		internal := false
		if conf != nil {
			_, loaded := m.plugins[conf.ModulePath]
			internal = loaded
		}
		if app.Internal != internal {
			app.Internal = internal
			if err := m.db.UpdateApplication(app); err != nil {
				return err
			}
		}
	}
	return nil
}

// initializeSingleUserPlugin constructs the instance for a single plugin, wires it, publishes it
// into the registry, and enables it if its persisted config says so. The instance is only
// registered once it is fully wired, so a half-initialized instance is never exposed. The caller
// must hold the write lock.
func (m *Manager) initializeSingleUserPlugin(userCtx compat.UserContext, p compat.Plugin) error {
	info := p.PluginInfo()
	instance := p.NewPluginInstance(userCtx)

	pluginConf, err := m.db.GetPluginConfByUserAndPath(userCtx.ID, info.ModulePath)
	if err != nil {
		return err
	}
	if pluginConf == nil {
		pluginConf, err = m.createPluginConf(instance, info, userCtx.ID)
		if err != nil {
			return err
		}
	}

	state := &instanceState{
		m:        m,
		instance: instance,
		conf:     pluginConf,
		phase:    phaseNew,
	}
	state.wire()
	m.states[pluginConf.ID] = state
	state.activateIfEnabled(userCtx.Name)

	return nil
}

func (m *Manager) createPluginConf(instance compat.PluginInstance, info compat.Info, userID uint) (*model.PluginConf, error) {
	pluginConf := &model.PluginConf{
		UserID:     userID,
		ModulePath: info.ModulePath,
		Token:      auth.GenerateNotExistingToken(auth.GeneratePluginToken, m.pluginConfExists),
	}
	if compat.HasSupport(instance, compat.Configurer) {
		pluginConf.Config, _ = yaml.Marshal(instance.DefaultConfig())
	}
	if compat.HasSupport(instance, compat.Messenger) {
		app := &model.Application{
			Token:       auth.GenerateNotExistingToken(auth.GenerateApplicationToken, m.applicationExists),
			Name:        info.String(),
			UserID:      userID,
			Internal:    true,
			Description: fmt.Sprintf("auto generated application for %s", info.ModulePath),
		}
		if err := m.db.CreateApplication(app); err != nil {
			return nil, err
		}
		pluginConf.ApplicationID = app.ID
	}
	if err := m.db.CreatePluginConf(pluginConf); err != nil {
		return nil, err
	}
	return pluginConf, nil
}
