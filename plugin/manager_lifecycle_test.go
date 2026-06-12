package plugin

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/gotify/server/v2/model"
	"github.com/gotify/server/v2/plugin/compat"
	"github.com/gotify/server/v2/plugin/testing/mock"
	"github.com/gotify/server/v2/test/testdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// failingDB wraps a real Database and can be configured to return errors
// on specific method calls.
type failingDB struct {
	Database
	updateConfErr     error
	updateConfCallCnt int32
	failOnUpdateN     int32 // fail on the N-th call (0-based, -1 = never)
}

func newFailingDB(inner Database) *failingDB {
	return &failingDB{Database: inner, failOnUpdateN: -1}
}

func (f *failingDB) UpdatePluginConf(p *model.PluginConf) error {
	n := atomic.AddInt32(&f.updateConfCallCnt, 1)
	if f.failOnUpdateN >= 0 && n-1 == f.failOnUpdateN {
		return f.updateConfErr
	}
	return f.Database.UpdatePluginConf(p)
}

func (f *failingDB) resetCounter() {
	atomic.StoreInt32(&f.updateConfCallCnt, 0)
	f.failOnUpdateN = -1
	f.updateConfErr = nil
}

type lifecycleNotifier struct{}

func (n lifecycleNotifier) Notify(userID uint, message *model.MessageExternal) {}

// helper: create a manager with mock plugin loaded, no .so files, no mux.
func newLifecycleManager(t *testing.T, db Database) *Manager {
	t.Helper()
	m, err := NewManager(db, "", nil, lifecycleNotifier{})
	require.NoError(t, err)
	require.NoError(t, m.LoadPlugin(new(mock.Plugin)))
	return m
}

func getLifecycleConf(t *testing.T, db *testdb.Database, uid uint) *model.PluginConf {
	t.Helper()
	conf, err := db.GetPluginConfByUserAndPath(uid, mock.ModulePath)
	require.NoError(t, err)
	require.NotNil(t, conf)
	return conf
}

// ===== Start/Stop Scenarios =====

func TestLifecycle_EnableDisable_HappyPath(t *testing.T) {
	db := testdb.NewDB(t)
	defer db.Close()
	m := newLifecycleManager(t, db)
	uid := uint(100)
	db.NewUserWithName(uid, "lc_happy")
	require.NoError(t, m.InitializeForUserID(uid))

	conf := getLifecycleConf(t, db, uid)
	assert.False(t, conf.Enabled)

	// After init, should be StateDisabled (conf was not pre-enabled).
	state, err := m.InstanceState(conf.ID)
	require.NoError(t, err)
	assert.Equal(t, StateDisabled, state)

	// Enable.
	require.NoError(t, m.SetPluginEnabled(conf.ID, true))
	state, err = m.InstanceState(conf.ID)
	require.NoError(t, err)
	assert.Equal(t, StateEnabled, state)

	conf = getLifecycleConf(t, db, uid)
	assert.True(t, conf.Enabled, "DB should reflect enabled")

	// Disable.
	require.NoError(t, m.SetPluginEnabled(conf.ID, false))
	state, err = m.InstanceState(conf.ID)
	require.NoError(t, err)
	assert.Equal(t, StateDisabled, state)

	conf = getLifecycleConf(t, db, uid)
	assert.False(t, conf.Enabled, "DB should reflect disabled")
}

func TestLifecycle_EnableFails_StaysFailed(t *testing.T) {
	db := testdb.NewDB(t)
	defer db.Close()
	m := newLifecycleManager(t, db)

	uid := uint(101)
	mock.ReturnErrorOnEnableForUser(uid, errors.New("enable boom 101"))
	db.NewUserWithName(uid, "lc_enable_fail")
	require.NoError(t, m.InitializeForUserID(uid))

	conf := getLifecycleConf(t, db, uid)
	assert.False(t, conf.Enabled)

	// Not pre-enabled, so state is Disabled (Enable was never attempted).
	state, err := m.InstanceState(conf.ID)
	require.NoError(t, err)
	assert.Equal(t, StateDisabled, state)

	// Trying to enable via SetPluginEnabled should fail.
	err = m.SetPluginEnabled(conf.ID, true)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "enable boom 101")

	// DB should remain disabled.
	conf = getLifecycleConf(t, db, uid)
	assert.False(t, conf.Enabled)
}

func TestLifecycle_DisableFails_StaysEnabled(t *testing.T) {
	db := testdb.NewDB(t)
	defer db.Close()
	m := newLifecycleManager(t, db)

	uid := uint(102)
	db.NewUserWithName(uid, "lc_disable_fail")
	require.NoError(t, m.InitializeForUserID(uid))

	conf := getLifecycleConf(t, db, uid)
	require.NoError(t, m.SetPluginEnabled(conf.ID, true))

	mock.ReturnErrorOnDisableForUser(uid, errors.New("disable boom 102"))

	err := m.SetPluginEnabled(conf.ID, false)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "disable boom 102")

	// DB should still be enabled.
	conf = getLifecycleConf(t, db, uid)
	assert.True(t, conf.Enabled, "DB should remain enabled after disable failure")

	state, err := m.InstanceState(conf.ID)
	require.NoError(t, err)
	assert.Equal(t, StateEnabled, state, "runtime should remain enabled")
}

func TestLifecycle_EnableSuccess_DBFail_RollsBackRuntime(t *testing.T) {
	db := testdb.NewDB(t)
	defer db.Close()
	fdb := newFailingDB(db)
	m := newLifecycleManager(t, fdb)

	uid := uint(103)
	db.NewUserWithName(uid, "lc_rollback_enable")
	require.NoError(t, m.InitializeForUserID(uid))

	conf, err := db.GetPluginConfByUserAndPath(uid, mock.ModulePath)
	require.NoError(t, err)
	require.NotNil(t, conf)

	fdb.resetCounter()
	fdb.failOnUpdateN = 0
	fdb.updateConfErr = errors.New("db write failure")

	err = m.SetPluginEnabled(conf.ID, true)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "db write failure")

	// Runtime should be rolled back — instance should be disabled.
	inst, err := m.Instance(conf.ID)
	require.NoError(t, err)
	mockInst := inst.(*mock.PluginInstance)
	assert.False(t, mockInst.Enabled, "instance should be rolled back to disabled")

	// DB should remain disabled.
	conf, err = db.GetPluginConfByUserAndPath(uid, mock.ModulePath)
	require.NoError(t, err)
	assert.False(t, conf.Enabled)
}

func TestLifecycle_DisableSuccess_DBFail_RollsBackRuntime(t *testing.T) {
	db := testdb.NewDB(t)
	defer db.Close()
	fdb := newFailingDB(db)
	m := newLifecycleManager(t, fdb)

	uid := uint(104)
	db.NewUserWithName(uid, "lc_rollback_disable")
	require.NoError(t, m.InitializeForUserID(uid))

	conf, err := db.GetPluginConfByUserAndPath(uid, mock.ModulePath)
	require.NoError(t, err)
	require.NotNil(t, conf)
	require.NoError(t, m.SetPluginEnabled(conf.ID, true))

	fdb.resetCounter()
	fdb.failOnUpdateN = 0
	fdb.updateConfErr = errors.New("db write failure on disable")

	err = m.SetPluginEnabled(conf.ID, false)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "db write failure on disable")

	// Runtime should be rolled back — instance should be re-enabled.
	inst, err := m.Instance(conf.ID)
	require.NoError(t, err)
	mockInst := inst.(*mock.PluginInstance)
	assert.True(t, mockInst.Enabled, "instance should be rolled back to enabled")

	// DB should remain enabled.
	conf, err = db.GetPluginConfByUserAndPath(uid, mock.ModulePath)
	require.NoError(t, err)
	assert.True(t, conf.Enabled)
}

func TestLifecycle_DoubleEnable_ReturnsAlreadyError(t *testing.T) {
	db := testdb.NewDB(t)
	defer db.Close()
	m := newLifecycleManager(t, db)

	uid := uint(105)
	db.NewUserWithName(uid, "lc_double_enable")
	require.NoError(t, m.InitializeForUserID(uid))

	conf := getLifecycleConf(t, db, uid)
	require.NoError(t, m.SetPluginEnabled(conf.ID, true))

	err := m.SetPluginEnabled(conf.ID, true)
	assert.Equal(t, ErrAlreadyEnabledOrDisabled, err)
}

// ===== Config Invalidation Scenarios =====

func TestLifecycle_InvalidConfig_AutoDisable(t *testing.T) {
	db := testdb.NewDB(t)
	defer db.Close()
	m := newLifecycleManager(t, db)

	uid := uint(106)
	db.NewUserWithName(uid, "lc_bad_config")
	require.NoError(t, db.CreatePluginConf(&model.PluginConf{
		UserID:     uid,
		ModulePath: mock.ModulePath,
		Token:      "Pbadconfig106",
		Enabled:    true,
		Config:     []byte(`invalid: """`),
	}))

	require.NoError(t, m.InitializeForUserID(uid))

	conf := getLifecycleConf(t, db, uid)
	assert.False(t, conf.Enabled, "plugin should be auto-disabled on bad config")

	state, err := m.InstanceState(conf.ID)
	require.NoError(t, err)
	assert.Equal(t, StateFailed, state, "state should be Failed after config rejection")

	// Config should contain the commented-out original.
	assert.Contains(t, string(conf.Config), "# The original configuration:")
}

func TestLifecycle_ReEnableAfterConfigFix(t *testing.T) {
	db := testdb.NewDB(t)
	defer db.Close()
	m := newLifecycleManager(t, db)

	uid := uint(107)
	db.NewUserWithName(uid, "lc_fix_config")
	require.NoError(t, db.CreatePluginConf(&model.PluginConf{
		UserID:     uid,
		ModulePath: mock.ModulePath,
		Token:      "Pfixconfig107",
		Enabled:    true,
		Config:     []byte(`invalid: """`),
	}))

	require.NoError(t, m.InitializeForUserID(uid))

	conf := getLifecycleConf(t, db, uid)
	assert.False(t, conf.Enabled)

	state, err := m.InstanceState(conf.ID)
	require.NoError(t, err)
	assert.Equal(t, StateFailed, state)

	// Simulate the user fixing the config through UpdateConfig API.
	inst, err := m.Instance(conf.ID)
	require.NoError(t, err)
	validConfig := inst.DefaultConfig()
	validBytes, err := yaml.Marshal(validConfig)
	require.NoError(t, err)

	require.NoError(t, inst.ValidateAndSetConfig(validConfig))

	conf.Config = validBytes
	conf.Enabled = false
	require.NoError(t, db.UpdatePluginConf(conf))

	// Now re-enable.
	require.NoError(t, m.SetPluginEnabled(conf.ID, true))

	state, err = m.InstanceState(conf.ID)
	require.NoError(t, err)
	assert.Equal(t, StateEnabled, state)

	conf = getLifecycleConf(t, db, uid)
	assert.True(t, conf.Enabled)
}

// ===== User Deletion Scenarios =====

func TestLifecycle_RemoveUser_DisablesAllAndCleansInstances(t *testing.T) {
	db := testdb.NewDB(t)
	defer db.Close()
	m := newLifecycleManager(t, db)

	uid := uint(108)
	db.NewUserWithName(uid, "lc_remove_user")
	require.NoError(t, m.InitializeForUserID(uid))

	conf := getLifecycleConf(t, db, uid)
	require.NoError(t, m.SetPluginEnabled(conf.ID, true))

	state, err := m.InstanceState(conf.ID)
	require.NoError(t, err)
	assert.Equal(t, StateEnabled, state)

	// Remove the user.
	require.NoError(t, m.RemoveUser(uid))

	// Instance should be gone.
	assert.False(t, m.HasInstance(conf.ID), "instance should be removed after RemoveUser")

	// State query should fail (instance removed).
	_, err = m.InstanceState(conf.ID)
	assert.Error(t, err)
}

func TestLifecycle_RemoveUser_DisableError_BestEffortReturnsAggregate(t *testing.T) {
	db := testdb.NewDB(t)
	defer db.Close()
	m := newLifecycleManager(t, db)

	uid := uint(109)
	db.NewUserWithName(uid, "lc_disable_err_remove")
	require.NoError(t, m.InitializeForUserID(uid))

	conf := getLifecycleConf(t, db, uid)
	require.NoError(t, m.SetPluginEnabled(conf.ID, true))

	// Make disable fail.
	mock.ReturnErrorOnDisableForUser(uid, errors.New("disable fail 109"))

	// RemoveUser should return an error but still clean up the instance.
	err := m.RemoveUser(uid)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "disable fail 109")

	// Instance should still be removed from the map.
	assert.False(t, m.HasInstance(conf.ID), "instance should be removed even on disable error")
}

func TestLifecycle_RemoveUser_ReconcilesInternalApps(t *testing.T) {
	db := testdb.NewDB(t)
	defer db.Close()
	m := newLifecycleManager(t, db)

	uid := uint(110)
	db.NewUserWithName(uid, "lc_internal_reconcile")
	require.NoError(t, m.InitializeForUserID(uid))

	// The mock plugin has Messenger capability, so an internal app should be created.
	conf := getLifecycleConf(t, db, uid)
	require.NotZero(t, conf.ApplicationID, "messenger plugin should have an application")

	apps, err := db.GetApplicationsByUser(uid)
	require.NoError(t, err)
	var internalApp *model.Application
	for _, a := range apps {
		if a.ID == conf.ApplicationID {
			internalApp = a
			break
		}
	}
	require.NotNil(t, internalApp, "internal app should exist")
	assert.True(t, internalApp.Internal, "app should be marked as internal")

	// Remove the user (runtime cleanup only).
	require.NoError(t, m.RemoveUser(uid))

	// After RemoveUser, PluginConf records still exist in DB (deleted by DeleteUserByID separately).
	// So reconcileInternalApps correctly keeps the Internal flag because the plugin is loaded.
	// Now simulate what DeleteUserByID would do — delete the PluginConf.
	require.NoError(t, db.DeletePluginConfByID(conf.ID))

	// Re-initialize to trigger reconcileInternalApps.
	require.NoError(t, m.InitializeForUserID(uid))

	// After PluginConf is gone, the internal flag should be reset.
	apps, err = db.GetApplicationsByUser(uid)
	require.NoError(t, err)
	for _, a := range apps {
		if a.ID == conf.ApplicationID {
			assert.False(t, a.Internal, "internal flag should be reset after PluginConf deletion")
		}
	}
}

func TestLifecycle_RemoveUser_DanglingConf_NoError(t *testing.T) {
	db := testdb.NewDB(t)
	defer db.Close()
	m := newLifecycleManager(t, db)

	uid := uint(111)
	db.NewUserWithName(uid, "lc_dangling")

	// Create a dangling conf (no corresponding loaded plugin).
	require.NoError(t, db.CreatePluginConf(&model.PluginConf{
		UserID:     uid,
		ModulePath: "github.com/nonexistent/plugin",
		Token:      "Pdangling111",
		Enabled:    true,
	}))

	// Initialize (only the mock plugin will be initialized, dangling conf is ignored).
	require.NoError(t, m.InitializeForUserID(uid))

	// RemoveUser should succeed despite the dangling conf.
	require.NoError(t, m.RemoveUser(uid))
}

// ===== Internal App Sync Scenarios =====

func TestLifecycle_InternalApp_MarkedOnInit(t *testing.T) {
	db := testdb.NewDB(t)
	defer db.Close()
	m := newLifecycleManager(t, db)

	uid := uint(112)
	db.NewUserWithName(uid, "lc_messenger")
	require.NoError(t, m.InitializeForUserID(uid))

	conf := getLifecycleConf(t, db, uid)
	require.NotZero(t, conf.ApplicationID)

	apps, err := db.GetApplicationsByUser(uid)
	require.NoError(t, err)

	var found bool
	for _, a := range apps {
		if a.ID == conf.ApplicationID {
			assert.True(t, a.Internal, "messenger plugin app should be internal")
			found = true
		}
	}
	assert.True(t, found, "internal app should exist for messenger plugin")
}

func TestLifecycle_InternalApp_UnmarkedWhenPluginNotLoaded(t *testing.T) {
	db := testdb.NewDB(t)
	defer db.Close()

	uid := uint(113)
	db.NewUserWithName(uid, "lc_stale_internal")
	app := &model.Application{
		Token:    "Astale_lc",
		Internal: true,
		Name:     "stale plugin app",
		UserID:   uid,
	}
	require.NoError(t, db.CreateApplication(app))
	require.NoError(t, db.CreatePluginConf(&model.PluginConf{
		ApplicationID: app.ID,
		UserID:        uid,
		Enabled:       true,
		ModulePath:    "github.com/nonexistent/plugin",
		Token:         "Pstale_lc",
	}))

	// Verify it's internal before.
	appBefore, err := db.GetApplicationByToken("Astale_lc")
	require.NoError(t, err)
	assert.True(t, appBefore.Internal)

	// Create manager — the non-existent plugin won't be loaded.
	_, err = NewManager(db, "", nil, lifecycleNotifier{})
	require.NoError(t, err)

	// After initialization, the internal flag should be reset.
	appAfter, err := db.GetApplicationByToken("Astale_lc")
	require.NoError(t, err)
	assert.False(t, appAfter.Internal, "stale internal app should be unmarked")
}

func TestLifecycle_InstanceState_UnknownPlugin(t *testing.T) {
	db := testdb.NewDB(t)
	defer db.Close()
	m := newLifecycleManager(t, db)

	_, err := m.InstanceState(9999)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "instance not found")
}

func TestLifecycle_InstanceState_String(t *testing.T) {
	assert.Equal(t, "loaded", StateLoaded.String())
	assert.Equal(t, "initialized", StateInitialized.String())
	assert.Equal(t, "enabled", StateEnabled.String())
	assert.Equal(t, "disabled", StateDisabled.String())
	assert.Equal(t, "failed", StateFailed.String())
	assert.Equal(t, "removed", StateRemoved.String())
	assert.Equal(t, "unknown", InstanceState(99).String())
}

func TestLifecycle_ManagedInstanceByID(t *testing.T) {
	db := testdb.NewDB(t)
	defer db.Close()
	m := newLifecycleManager(t, db)

	uid := uint(114)
	db.NewUserWithName(uid, "lc_managed_lookup")
	require.NoError(t, m.InitializeForUserID(uid))

	conf := getLifecycleConf(t, db, uid)
	managed, err := m.ManagedInstanceByID(conf.ID)
	require.NoError(t, err)
	assert.NotNil(t, managed.Instance)
	assert.Equal(t, StateDisabled, managed.State)
	assert.Equal(t, conf.ID, managed.ConfID)
	assert.Equal(t, mock.ModulePath, managed.ModulePath)
	assert.Equal(t, uid, managed.UserID)

	_, err = m.ManagedInstanceByID(9999)
	assert.Error(t, err)
}

// Compile-time interface check.
var _ compat.PluginInstance = (*mock.PluginInstance)(nil)
