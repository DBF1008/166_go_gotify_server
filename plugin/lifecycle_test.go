package plugin

import (
	"errors"
	"testing"

	"github.com/gotify/server/v2/model"
	"github.com/gotify/server/v2/plugin/testing/mock"
	"github.com/gotify/server/v2/test/testdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// These tests exercise the instanceState lifecycle through the public Manager surface using the
// sideloaded mock plugin (no compiled .so needed). They assert the core invariant of the
// refactor: after every transition the runtime instance and the persisted PluginConf agree, and
// on a failed transition the runtime is rolled back to match persistence.

type nopNotifier struct{}

func (nopNotifier) Notify(uint, *model.MessageExternal) {}

func sideloadManager(t *testing.T, db Database) *Manager {
	t.Helper()
	m, err := NewManager(db, "", nil, nopNotifier{})
	require.NoError(t, err)
	require.NoError(t, m.LoadPlugin(new(mock.Plugin)))
	return m
}

func mockConf(t *testing.T, db *testdb.Database, uid uint) *model.PluginConf {
	t.Helper()
	conf, err := db.GetPluginConfByUserAndPath(uid, mock.ModulePath)
	require.NoError(t, err)
	require.NotNil(t, conf)
	return conf
}

func mockInstance(t *testing.T, m *Manager, confID uint) *mock.PluginInstance {
	t.Helper()
	inst, err := m.Instance(confID)
	require.NoError(t, err)
	return inst.(*mock.PluginInstance)
}

func TestLifecycle_StartStop_runtimeAndDBStayConsistent(t *testing.T) {
	db := testdb.NewDB(t)
	defer db.Close()
	m := sideloadManager(t, db)
	db.User(100)
	require.NoError(t, m.InitializeForUserID(100))

	conf := mockConf(t, db, 100)
	inst := mockInstance(t, m, conf.ID)

	assert.False(t, inst.Enabled)
	assert.False(t, mockConf(t, db, 100).Enabled)

	// enable: runtime and DB both flip on
	require.NoError(t, m.SetPluginEnabled(conf.ID, true))
	assert.True(t, inst.Enabled, "runtime should be enabled")
	assert.True(t, mockConf(t, db, 100).Enabled, "db should be enabled")

	// enabling again is a no-op error and changes nothing
	assert.ErrorIs(t, m.SetPluginEnabled(conf.ID, true), ErrAlreadyEnabledOrDisabled)
	assert.True(t, inst.Enabled)
	assert.True(t, mockConf(t, db, 100).Enabled)

	// disable: runtime and DB both flip off
	require.NoError(t, m.SetPluginEnabled(conf.ID, false))
	assert.False(t, inst.Enabled, "runtime should be disabled")
	assert.False(t, mockConf(t, db, 100).Enabled, "db should be disabled")

	assert.ErrorIs(t, m.SetPluginEnabled(conf.ID, false), ErrAlreadyEnabledOrDisabled)
}

func TestLifecycle_SetConfig_invalidLeavesRuntimeAndDBUntouched(t *testing.T) {
	db := testdb.NewDB(t)
	defer db.Close()
	m := sideloadManager(t, db)
	db.User(101)
	require.NoError(t, m.InitializeForUserID(101))
	conf := mockConf(t, db, 101)
	inst := mockInstance(t, m, conf.ID)

	origRuntime := inst.Config
	origStored := mockConf(t, db, 101).Config

	invalid, err := yaml.Marshal(&mock.PluginConfig{TestKey: "x", IsNotValid: true})
	require.NoError(t, err)

	err = m.SetConfig(conf.ID, invalid)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrConfigPersistence, "invalid config is a validation error, not a persistence error")

	assert.Equal(t, origRuntime, inst.Config, "runtime config unchanged on invalid update")
	assert.Equal(t, origStored, mockConf(t, db, 101).Config, "stored config unchanged on invalid update")
}

func TestLifecycle_SetConfig_validUpdatesRuntimeAndDB(t *testing.T) {
	db := testdb.NewDB(t)
	defer db.Close()
	m := sideloadManager(t, db)
	db.User(102)
	require.NoError(t, m.InitializeForUserID(102))
	conf := mockConf(t, db, 102)
	inst := mockInstance(t, m, conf.ID)

	want := &mock.PluginConfig{TestKey: "updated"}
	raw, err := yaml.Marshal(want)
	require.NoError(t, err)
	require.NoError(t, m.SetConfig(conf.ID, raw))

	assert.Equal(t, want, inst.Config, "runtime received the new config")

	stored := new(mock.PluginConfig)
	require.NoError(t, yaml.Unmarshal(mockConf(t, db, 102).Config, stored))
	assert.Equal(t, want, stored, "db persisted the new config")
}

func TestLifecycle_RestoreConfig_rejectedConfigDisablesAndResetsToDefault(t *testing.T) {
	db := testdb.NewDB(t)
	defer db.Close()
	m := sideloadManager(t, db)
	db.User(103)

	// A stored config that is well-formed YAML but rejected by the plugin (e.g. it became
	// outdated). It is also marked enabled, so restore must reconcile it back to disabled.
	rejected, err := yaml.Marshal(&mock.PluginConfig{TestKey: "stale", IsNotValid: true})
	require.NoError(t, err)
	require.NoError(t, db.CreatePluginConf(&model.PluginConf{
		UserID:     103,
		ModulePath: mock.ModulePath,
		Token:      "Plifecycle103",
		Enabled:    true,
		Config:     rejected,
	}))

	require.NoError(t, m.InitializeForUserID(103))

	conf := mockConf(t, db, 103)
	inst := mockInstance(t, m, conf.ID)

	assert.False(t, inst.Enabled, "runtime is disabled after the config is rejected")
	assert.False(t, conf.Enabled, "db is reconciled to disabled")
	assert.Equal(t, inst.DefaultConfig(), inst.Config, "runtime is reset to the default config")
	assert.Contains(t, string(conf.Config), "default plugin configuration is used",
		"stored config is rewritten with the explanatory fallback")
}

// failingPersistDB wraps the test database and can be told to fail UpdatePluginConf, to exercise
// the persistence-failure rollback path.
type failingPersistDB struct {
	*testdb.Database
	failUpdate bool
}

func (d *failingPersistDB) UpdatePluginConf(p *model.PluginConf) error {
	if d.failUpdate {
		return errors.New("persist boom")
	}
	return d.Database.UpdatePluginConf(p)
}

func TestLifecycle_SetEnabled_persistFailureRollsBackRuntime(t *testing.T) {
	base := testdb.NewDB(t)
	defer base.Close()
	fdb := &failingPersistDB{Database: base}
	m := sideloadManager(t, fdb)
	base.User(104)
	require.NoError(t, m.InitializeForUserID(104))

	conf := mockConf(t, base, 104)
	inst := mockInstance(t, m, conf.ID)
	require.False(t, inst.Enabled)

	fdb.failUpdate = true
	err := m.SetPluginEnabled(conf.ID, true)
	fdb.failUpdate = false

	require.Error(t, err)
	assert.EqualError(t, err, "persist boom")
	assert.False(t, inst.Enabled, "runtime is rolled back when the enabled state cannot be persisted")
	assert.False(t, mockConf(t, base, 104).Enabled, "db stays disabled")
}
