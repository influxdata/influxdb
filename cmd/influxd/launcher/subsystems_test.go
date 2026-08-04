package launcher

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	nethttp "net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// checkEnvelope decodes either endpoint's body. /health and /ready use
// different shapes, but every field asserted on here is common to both, and
// the fields each one omits decode as zero values. Note that the two do not
// share a status vocabulary: /health reports pass/fail and /ready reports
// ready/starting, so the field is only meaningful per endpoint.
// check.Response is an interface and cannot be a decode target, so nested
// checks decode as BasicResponse.
type checkEnvelope struct {
	Status  check.Status          `json:"status"`
	Message string                `json:"message"`
	Checks  []check.BasicResponse `json:"checks"`
}

// newCheckLauncher returns a Launcher wired up with only what is needed to
// register and serve /health and /ready: no subsystem is started, so every
// gate is in the state it would be in at the very beginning of run.
func newCheckLauncher(t *testing.T) *Launcher {
	t.Helper()

	m := NewLauncher()
	m.log = zaptest.NewLogger(t)
	m.checkHandler = http.NewHealthReadyHandler(m.log)
	m.initReadyChecks()
	return m
}

// serveCheck issues a GET against the launcher's check handler and returns the
// decoded body along with the HTTP status. Health auth is off here, so the
// full envelope — messages included — is served to this anonymous caller.
func serveCheck(t *testing.T, m *Launcher, path string) (checkEnvelope, int) {
	t.Helper()

	w := httptest.NewRecorder()
	m.checkHandler.ServeHTTP(w, httptest.NewRequest(nethttp.MethodGet, path, nil))

	resp := w.Result()
	defer func() { require.NoError(t, resp.Body.Close()) }()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var body checkEnvelope
	require.NoErrorf(t, json.Unmarshal(raw, &body), "body: %s", raw)
	return body, resp.StatusCode
}

// checksByName indexes a checks slice, failing if any name appears twice. The
// duplicate assertion is the point: nothing in kit/check enforces unique
// names, so a subsystem that both fails and later registers its normal check
// would silently produce two entries under one name.
func checksByName(t *testing.T, rs []check.BasicResponse) map[string]check.BasicResponse {
	t.Helper()

	out := make(map[string]check.BasicResponse, len(rs))
	for _, r := range rs {
		_, dup := out[r.Name()]
		require.Falsef(t, dup, "duplicate check name %q in %v", r.Name(), rs)
		out[r.Name()] = r
	}
	return out
}

// TestSubsystemNames_Distinct pins that no two subsystems share a name. A
// collision would put two unrelated phases under one /health entry, and the
// one that registered second would be invisible.
//
// SubsystemKV and BoltStore share the value "bolt" by design — one is a
// subsystem name, the other the deprecated --store value — so this covers the
// Subsystem* set only.
func TestSubsystemNames_Distinct(t *testing.T) {
	byConst := map[string]string{
		"SubsystemEngine":            SubsystemEngine,
		"SubsystemReplications":      SubsystemReplications,
		"SubsystemQuery":             SubsystemQuery,
		"SubsystemInfluxQL":          SubsystemInfluxQL,
		"SubsystemTaskScheduler":     SubsystemTaskScheduler,
		"SubsystemTasks":             SubsystemTasks,
		"SubsystemScraper":           SubsystemScraper,
		"SubsystemJaeger":            SubsystemJaeger,
		"SubsystemPIDFile":           SubsystemPIDFile,
		"SubsystemKV":                SubsystemKV,
		"SubsystemSQLite":            SubsystemSQLite,
		"SubsystemHTTPServer":        SubsystemHTTPServer,
		"SubsystemShards":            SubsystemShards,
		"SubsystemFlagger":           SubsystemFlagger,
		"SubsystemAuthorization":     SubsystemAuthorization,
		"SubsystemAuthorizationV1":   SubsystemAuthorizationV1,
		"SubsystemSecrets":           SubsystemSecrets,
		"SubsystemMetaClient":        SubsystemMetaClient,
		"SubsystemNotificationRules": SubsystemNotificationRules,
		"SubsystemLabels":            SubsystemLabels,
		"SubsystemAPI":               SubsystemAPI,
		"SubsystemMetaStore":         SubsystemMetaStore,
	}

	seen := make(map[string]string, len(byConst))
	for name, value := range byConst {
		require.NotEmptyf(t, value, "%s has no value", name)
		other, dup := seen[value]
		require.Falsef(t, dup, "%s and %s share the value %q", other, name, value)
		seen[value] = name
	}
}

// TestLauncher_InitReadyChecks pins the wiring failSubsystem depends on: every
// gate is filed under its own name, and only the gated subsystems are in the
// map. A gate filed under the wrong name would latch an unrelated subsystem.
func TestLauncher_InitReadyChecks(t *testing.T) {
	m := newCheckLauncher(t)

	for name, gate := range m.readyGates {
		require.Equalf(t, name, gate.CheckName(),
			"gate filed under %q reports the name %q", name, gate.CheckName())
	}

	require.Equal(t, map[string]*check.ReadyGate{
		SubsystemKV:            m.kvReady,
		SubsystemSQLite:        m.sqliteReady,
		SubsystemEngine:        m.engineReady,
		SubsystemReplications:  m.replicationsReady,
		SubsystemQuery:         m.queryReady,
		SubsystemTasks:         m.tasksReady,
		SubsystemTaskScheduler: m.schedulerReady,
	}, m.readyGates)

	// SubsystemShards is deliberately absent: m.startupProgress owns that name
	// and latches its own failure through Finish(err).
	require.NotContains(t, m.readyGates, SubsystemShards)

	require.Equal(t, []string{
		SubsystemKV,
		SubsystemSQLite,
		SubsystemEngine,
		SubsystemReplications,
		SubsystemQuery,
		SubsystemTasks,
		SubsystemTaskScheduler,
		SubsystemShards,
	}, m.ReadyCheckNames())
}

func TestLauncher_FailSubsystem_GatedName(t *testing.T) {
	m := newCheckLauncher(t)
	before := m.ReadyCheckNames()

	cause := errors.New("no such file or directory")
	got := m.failSubsystem(SubsystemEngine, "Failed to open engine", cause)

	// The error goes back to influxd untouched, so the exit message does not
	// change; only the check detail is composed.
	require.ErrorIs(t, got, cause)
	require.Equal(t, cause.Error(), got.Error())
	require.Equal(t, SubsystemEngine, m.failedSubsystem)

	const wantMsg = "Failed to open engine: no such file or directory"

	health, status := serveCheck(t, m, "/health")
	require.Equal(t, nethttp.StatusServiceUnavailable, status)
	require.Equal(t, check.StatusFail, health.Status)
	// firstFailureMessage sorts failing checks by name, and the only failing
	// health check registered here is the engine's.
	require.Equal(t, wantMsg, health.Message)

	healthChecks := checksByName(t, health.Checks)
	require.Contains(t, healthChecks, SubsystemEngine)
	require.Equal(t, check.StatusFail, healthChecks[SubsystemEngine].Status())
	require.Equal(t, wantMsg, healthChecks[SubsystemEngine].Message())

	// A gated subsystem latches the entry it already has; registering a second
	// /ready check under the same name is what this branch exists to avoid.
	require.Equal(t, before, m.ReadyCheckNames())

	ready, status := serveCheck(t, m, "/ready")
	require.Equal(t, nethttp.StatusServiceUnavailable, status)
	readyChecks := checksByName(t, ready.Checks)
	require.Equal(t, wantMsg, readyChecks[SubsystemEngine].Message())
	// Every other gate is still merely un-fired, and says so.
	require.Equal(t, check.MsgNotReady, readyChecks[SubsystemQuery].Message())
}

// TestLauncher_FailSubsystem_UngatedName covers a failure late in run, after
// every gate has fired. Without a check of its own such a failure leaves
// /ready reporting "ready" until the process exits.
func TestLauncher_FailSubsystem_UngatedName(t *testing.T) {
	m := newCheckLauncher(t)
	for _, gate := range m.readyGates {
		gate.Ready()
	}
	m.startupProgress.Finish(nil)

	ready, status := serveCheck(t, m, "/ready")
	require.Equal(t, nethttp.StatusOK, status)
	require.Equal(t, "ready", string(ready.Status),
		"test setup: /ready must report ready before the late failure")

	cause := errors.New("duplicate option registered")
	got := m.failSubsystem(SubsystemAPI, "Failed creating config handler", cause)
	require.ErrorIs(t, got, cause)

	const wantMsg = "Failed creating config handler: duplicate option registered"

	require.Contains(t, m.ReadyCheckNames(), SubsystemAPI)

	ready, status = serveCheck(t, m, "/ready")
	require.Equal(t, nethttp.StatusServiceUnavailable, status)
	readyChecks := checksByName(t, ready.Checks)
	require.Contains(t, readyChecks, SubsystemAPI)
	require.Equal(t, wantMsg, readyChecks[SubsystemAPI].Message())

	health, status := serveCheck(t, m, "/health")
	require.Equal(t, nethttp.StatusServiceUnavailable, status)
	healthChecks := checksByName(t, health.Checks)
	require.Equal(t, wantMsg, healthChecks[SubsystemAPI].Message())
}

// TestLauncher_FailSubsystem_MetaStoreNaming pins the shape produced when a
// store-type-agnostic phase fails: two /ready entries, meta-store carrying the
// reason and bolt not blamed for it. That is the visible consequence of naming
// the KV migrations for the role rather than for one implementation of it —
// under --store=memory there is no bolt file to blame — and without this
// assertion the next person to touch openMetaStores will "fix" it by latching
// the bolt gate too.
//
// check.MsgNotReady on the bolt entry is the state during run. The deferred
// sweep replaces it with "not reached" before run returns, which
// TestLauncher_FailUnreachedGates covers; what must not happen either way is
// bolt carrying the migration error.
func TestLauncher_FailSubsystem_MetaStoreNaming(t *testing.T) {
	m := newCheckLauncher(t)

	cause := errors.New("migration 42: disk full")
	m.failSubsystem(SubsystemMetaStore, "Failed to apply KV migrations", cause)

	const wantMsg = "Failed to apply KV migrations: migration 42: disk full"

	health, _ := serveCheck(t, m, "/health")
	healthChecks := checksByName(t, health.Checks)
	require.Equal(t, wantMsg, healthChecks[SubsystemMetaStore].Message())
	require.NotContains(t, healthChecks, SubsystemKV,
		"a migration failure must not be reported as a bolt failure")

	ready, _ := serveCheck(t, m, "/ready")
	readyChecks := checksByName(t, ready.Checks)
	require.Equal(t, wantMsg, readyChecks[SubsystemMetaStore].Message())
	require.Equal(t, check.MsgNotReady, readyChecks[SubsystemKV].Message())
}

// TestLauncher_FailUnreachedGates pins what /ready says once run has given up:
// one entry carrying the reason, the subsystems that did start still passing,
// and every remaining gate reporting that it was never reached rather than
// check.MsgNotReady — which means "not yet", and is a different claim.
func TestLauncher_FailUnreachedGates(t *testing.T) {
	m := newCheckLauncher(t)

	// A SQL migration failure: bolt is up by the time the migrations run,
	// sqlite is what failed, and nothing after it is ever reached.
	m.kvReady.Ready()
	cause := errors.New("migration 0012: disk full")
	m.failSubsystem(SubsystemSQLite, "Failed to apply SQL migrations", cause)

	m.failUnreachedGates(context.Background())

	const (
		wantReason    = "Failed to apply SQL migrations: migration 0012: disk full"
		wantUnreached = "not reached: startup failed at sqlite"
	)

	// bolt started, and must be left alone. ReadyGate.Fail outranks Ready, so
	// a sweep that did not skip a passing gate would report a working
	// subsystem as failed -- and /ready carries only failing checks, so the
	// gate itself is what has to be asked.
	require.Equal(t, check.StatusPass, m.kvReady.Check(context.Background()).Status(),
		"the sweep failed a subsystem that had already started")

	ready, status := serveCheck(t, m, "/ready")
	require.Equal(t, nethttp.StatusServiceUnavailable, status)
	readyChecks := checksByName(t, ready.Checks)
	require.NotContains(t, readyChecks, SubsystemKV, "a passing gate must not be listed")

	// The gate holding the real cause keeps it: ReadyGate.Fail keeps the first
	// error, so the sweep cannot overwrite an attributed failure with the
	// generic one no matter which runs first.
	require.Equal(t, wantReason, readyChecks[SubsystemSQLite].Message())

	for _, name := range []string{
		SubsystemEngine, SubsystemReplications, SubsystemQuery,
		SubsystemTasks, SubsystemTaskScheduler,
	} {
		require.Containsf(t, readyChecks, name, "%s is missing from /ready", name)
		require.Equalf(t, check.StatusFail, readyChecks[name].Status(), "%s", name)
		require.Equalf(t, wantUnreached, readyChecks[name].Message(),
			"%s still reports a message that reads as startup in progress", name)
	}

	// shards is m.startupProgress, not a ReadyGate, so the sweep does not
	// reach it and it goes on reporting shard-loading progress on a server
	// that never got as far as opening the engine. Asserted rather than left
	// implicit: it is the remaining half of this defect, not an oversight.
	require.Equal(t, "waiting for shard enumeration", readyChecks[SubsystemShards].Message())
}

// TestLauncher_FailUnreachedGates_Unattributed covers a failure that reached no
// failSubsystem call at all. There is no phase to name, so the message says
// only that startup failed — naming an empty subsystem would be worse than
// naming none.
func TestLauncher_FailUnreachedGates_Unattributed(t *testing.T) {
	m := newCheckLauncher(t)
	require.Empty(t, m.failedSubsystem, "test setup: nothing may be attributed")

	m.failUnreachedGates(context.Background())

	ready, _ := serveCheck(t, m, "/ready")
	readyChecks := checksByName(t, ready.Checks)
	require.Equal(t, "not reached: startup failed", readyChecks[SubsystemEngine].Message())
}

// TestLauncher_FailUnreachedGates_BeforeInitReadyChecks covers the sites that
// fail before the gates exist — feature flag overrides and the PID file. The
// deferred sweep still runs for them, over a nil map.
func TestLauncher_FailUnreachedGates_BeforeInitReadyChecks(t *testing.T) {
	m := NewLauncher()
	m.log = zaptest.NewLogger(t)
	require.Nil(t, m.readyGates, "test setup: the gates must not exist yet")

	require.NotPanics(t, func() { m.failUnreachedGates(context.Background()) })
}

// TestLauncher_FailSubsystem_BeforeCheckHandler covers the two sites that run
// before m.checkHandler exists — feature flag overrides and the PID file.
// Nothing is listening then, so attribution is the log line alone.
func TestLauncher_FailSubsystem_BeforeCheckHandler(t *testing.T) {
	m := NewLauncher()
	m.log = zaptest.NewLogger(t)
	require.Nil(t, m.checkHandler, "test setup: the handler must not exist yet")

	cause := errors.New("unknown feature flag")
	var got error
	require.NotPanics(t, func() {
		got = m.failSubsystem(SubsystemFlagger, "Failed to configure feature flag overrides", cause)
	})
	require.ErrorIs(t, got, cause)
	require.Equal(t, SubsystemFlagger, m.failedSubsystem)
}
