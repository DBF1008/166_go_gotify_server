package plugin

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/gotify/server/v2/model"
	"github.com/gotify/server/v2/plugin/compat"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

// ErrConfigPersistence indicates that a validated plugin configuration could not be
// persisted. It is distinct from a validation error so callers can tell a bad request
// (invalid config) apart from a server error (storage failure).
var ErrConfigPersistence = errors.New("could not persist plugin config")

// phase is the lifecycle phase of a single plugin instance.
//
// The transitions form the state machine that every plugin flow (boot/init, config
// update, enable/disable, user deletion) shares:
//
//	phaseNew --wire--> phaseInactive <--setEnabled--> phaseActive
//	                        |                              |
//	                        +-----------teardown-----------+--> phaseRemoved
type phase int

const (
	// phaseNew is a freshly constructed instance that has not been wired or registered.
	phaseNew phase = iota
	// phaseInactive is a fully wired, registered instance that is currently disabled.
	phaseInactive
	// phaseActive is a wired, registered instance that is enabled.
	phaseActive
	// phaseRemoved is an instance that has been torn down and unregistered.
	phaseRemoved
)

// instanceState is the lifecycle state machine for a single (user, plugin) pair. It is the
// sole owner of the coupling between the runtime PluginInstance and the persisted PluginConf
// and guarantees they stay reconciled: an instance that is registered in the Manager is always
// fully wired, and conf.Enabled in the database always matches the runtime instance after any
// successful transition. On a failed transition the runtime is rolled back to match the
// persisted state so the two never silently diverge.
//
// All transition methods assume the Manager's write lock is held by the caller (see Manager).
type instanceState struct {
	m        *Manager
	instance compat.PluginInstance
	conf     *model.PluginConf
	phase    phase
}

// wire attaches the capability handlers (messenger, storager, configurer, webhooker) to the
// instance and restores its persisted configuration. It does not enable the instance; call
// activateIfEnabled for that. After wire returns the instance is consistent and ready to be
// published into the registry.
func (s *instanceState) wire() {
	inst := s.instance
	if compat.HasSupport(inst, compat.Messenger) {
		inst.SetMessageHandler(redirectToChannel{
			ApplicationID: s.conf.ApplicationID,
			UserID:        s.conf.UserID,
			Messages:      s.m.messages,
		})
	}
	if compat.HasSupport(inst, compat.Storager) {
		inst.SetStorageHandler(dbStorageHandler{s.conf.ID, s.m.db})
	}
	if compat.HasSupport(inst, compat.Configurer) {
		s.restoreConfig()
	}
	if compat.HasSupport(inst, compat.Webhooker) {
		id := s.conf.ID
		g := s.m.mux.Group(s.conf.Token+"/", requirePluginEnabled(id, s.m.db))
		inst.RegisterWebhook(strings.Replace(g.BasePath(), ":id", strconv.Itoa(int(id)), 1), g)
	}
}

// validateConfig is the single configuration code path shared by boot restore and the API
// config update: it unmarshals the raw YAML onto the instance's default config and applies it
// via ValidateAndSetConfig. On success the instance holds the new config; on failure the
// instance is left untouched and the underlying error is returned. It performs no persistence.
func (s *instanceState) validateConfig(raw []byte) error {
	c := s.instance.DefaultConfig()
	if err := yaml.Unmarshal(raw, c); err != nil {
		return err
	}
	return s.instance.ValidateAndSetConfig(c)
}

// restoreConfig applies the persisted configuration to the instance during initialization. If
// no config has been stored yet (newly implemented Configurer) the default config is stored. If
// the stored config is rejected — it may be outdated — the instance is disabled, a commented
// fallback config (default plus the original, commented out) is persisted, and the instance is
// reset to its default config so it stays in a valid runtime state until the user re-enables it.
func (s *instanceState) restoreConfig() {
	if len(s.conf.Config) == 0 {
		// The Configurer is newly implemented; use the default config.
		s.conf.Config, _ = yaml.Marshal(s.instance.DefaultConfig())
		s.m.db.UpdatePluginConf(s.conf)
	}

	if err := s.validateConfig(s.conf.Config); err != nil {
		s.conf.Enabled = false

		log.Warn().
			Str("module_path", s.conf.ModulePath).
			Uint("user_id", s.conf.UserID).
			Err(err).
			Msg("Plugin failed to initialize because it rejected the current config. It might be outdated. A default config is used and the user would need to enable it again.")

		newConf := bytes.NewBufferString("# Plugin initialization failed because it rejected the current config. It might be outdated.\r\n# A default plugin configuration is used:\r\n")

		d, _ := yaml.Marshal(s.instance.DefaultConfig())
		newConf.Write(d)
		newConf.WriteString("\r\n")

		newConf.WriteString("# The original configuration: \r\n")
		oldConf := bufio.NewScanner(bytes.NewReader(s.conf.Config))
		for oldConf.Scan() {
			newConf.WriteString("# ")
			newConf.WriteString(oldConf.Text())
			newConf.WriteString("\r\n")
		}

		s.conf.Config = newConf.Bytes()

		s.m.db.UpdatePluginConf(s.conf)
		s.instance.ValidateAndSetConfig(s.instance.DefaultConfig())
	}
}

// setConfig validates and persists a new configuration coming from the API. The instance only
// receives the config if it is valid, and the new bytes are persisted only after validation
// succeeds, so an invalid update leaves both the runtime instance and the stored config
// untouched. A persistence failure is wrapped in ErrConfigPersistence so the caller can map it
// to a server error rather than a bad request.
func (s *instanceState) setConfig(raw []byte) error {
	if err := s.validateConfig(raw); err != nil {
		return err
	}
	// Re-read the conf so we do not clobber storage the instance may have written since init.
	if fresh, err := s.m.db.GetPluginConfByID(s.conf.ID); err == nil && fresh != nil {
		s.conf = fresh
	}
	s.conf.Config = raw
	if err := s.m.db.UpdatePluginConf(s.conf); err != nil {
		return fmt.Errorf("%w: %v", ErrConfigPersistence, err)
	}
	return nil
}

// activateIfEnabled enables the instance when its persisted config says it should be enabled.
// This is the boot-time soft-enable: if the plugin cannot be enabled (e.g. it rejects its
// config) it is disabled and persisted rather than failing initialization, so the user can fix
// the configuration and enable it again.
func (s *instanceState) activateIfEnabled(userName string) {
	if !s.conf.Enabled {
		s.phase = phaseInactive
		return
	}
	if err := s.instance.Enable(); err != nil {
		// A single user plugin cannot be enabled. Don't panic; disable for now and wait for
		// the user to update the config.
		log.Warn().Err(err).Str("user", userName).Msg("Plugin initialize failed, disabling now")
		s.conf.Enabled = false
		s.m.db.UpdatePluginConf(s.conf)
		s.phase = phaseInactive
		return
	}
	s.phase = phaseActive
}

// setEnabled is the single start/stop transition shared by the API and any other caller. It is
// a no-op error (ErrAlreadyEnabledOrDisabled) when the instance is already in the target state.
// Otherwise it toggles the runtime instance, then reconciles and persists conf.Enabled. If the
// instance toggles but persistence fails, the runtime is rolled back so it keeps matching the
// database.
func (s *instanceState) setEnabled(target bool) error {
	if s.conf.Enabled == target {
		return ErrAlreadyEnabledOrDisabled
	}

	if err := s.toggleInstance(target); err != nil {
		return err
	}

	// The instance might have updated its conf row (e.g. storage) while toggling; re-read it
	// before persisting the new enabled state so we do not clobber those writes.
	if fresh, err := s.m.db.GetPluginConfByID(s.conf.ID); err == nil && fresh != nil {
		s.conf = fresh
	}
	s.conf.Enabled = target
	if err := s.m.db.UpdatePluginConf(s.conf); err != nil {
		// Roll the runtime back so it keeps matching the persisted (unchanged) state.
		_ = s.toggleInstance(!target)
		s.conf.Enabled = !target
		return err
	}

	if target {
		s.phase = phaseActive
	} else {
		s.phase = phaseInactive
	}
	return nil
}

func (s *instanceState) toggleInstance(enable bool) error {
	if enable {
		return s.instance.Enable()
	}
	return s.instance.Disable()
}

// teardown disables the instance if it is active and removes it from the registry. It is used
// when a user is deleted. The instance is always unregistered, even if disabling fails, so the
// registry never keeps a reference to a half-removed instance; the disable error (if any) is
// returned to the caller.
func (s *instanceState) teardown() error {
	var err error
	if s.conf.Enabled {
		err = s.instance.Disable()
	}
	delete(s.m.states, s.conf.ID)
	s.phase = phaseRemoved
	return err
}
