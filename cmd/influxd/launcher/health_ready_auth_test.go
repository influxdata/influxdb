package launcher_test

import (
	nethttp "net/http"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// getCheckEndpoint decodes a check endpoint into a generic object, so the test
// can assert on which keys are present rather than on a fixed struct.
func getCheckEndpoint(t *testing.T, l *launcher.TestLauncher, path, token string) (int, map[string]any) {
	t.Helper()
	var out map[string]any
	return httpGetJSON(t, l.URL().String()+path, token, &out), out
}

// assertReducedHealthy asserts the reduced /health shape on a healthy server:
// the documented envelope minus the build fields and minus every per-check
// message. On this path the checks array is the list of registered subsystems,
// all passing -- the same list on any install of this configuration -- so it
// stays available to callers who cannot prove operator permissions. What a real
// launcher adds over the unit tests is that the list is the real one.
func assertReducedHealthy(t *testing.T, body map[string]any) {
	t.Helper()
	assert.Equal(t, "influxdb", body["name"])
	assert.Equal(t, "pass", body["status"])
	assert.Equal(t, "healthy", body["message"])
	// Build fields are the withholding that survives on the passing path.
	assert.NotContains(t, body, "version")
	assert.NotContains(t, body, "commit")

	checks, ok := body["checks"].([]any)
	require.Truef(t, ok, "expected a checks array, got %#v", body["checks"])
	assert.NotEmpty(t, checks)
	for _, entry := range checks {
		c, ok := entry.(map[string]any)
		require.Truef(t, ok, "expected a check object, got %#v", entry)
		assert.Equal(t, "pass", c["status"])
		assert.NotContains(t, c, "message", "check %v leaked its message", c["name"])
	}
}

func TestLauncher_HealthAuth_Enabled(t *testing.T) {
	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t, func(o *launcher.InfluxdOpts) {
		o.HealthAuthEnabled = true
	})
	defer l.ShutdownOrFail(t, ctx)

	// A limited token: real credentials, but nowhere near operator.
	limited := &influxdb.Authorization{
		OrgID:  l.Org.ID,
		UserID: l.User.ID,
		Permissions: []influxdb.Permission{{
			Action:   influxdb.ReadAction,
			Resource: influxdb.Resource{Type: influxdb.BucketsResourceType, OrgID: &l.Org.ID},
		}},
	}
	require.NoError(t, l.AuthorizationService(t).CreateAuthorization(ctx, limited))
	require.NotEmpty(t, limited.Token)

	t.Run("anonymous health keeps the healthy envelope", func(t *testing.T) {
		// The status code is the part a liveness probe reads, and it must be
		// correct without any credential at all.
		status, body := getCheckEndpoint(t, l, "/health", "")
		require.Equal(t, nethttp.StatusOK, status)
		assertReducedHealthy(t, body)
	})

	t.Run("limited token keeps the healthy envelope", func(t *testing.T) {
		status, body := getCheckEndpoint(t, l, "/health", limited.Token)
		require.Equal(t, nethttp.StatusOK, status)
		assertReducedHealthy(t, body)
	})

	t.Run("invalid token keeps the healthy envelope", func(t *testing.T) {
		status, body := getCheckEndpoint(t, l, "/health", "not-a-real-token")
		require.Equal(t, nethttp.StatusOK, status)
		assertReducedHealthy(t, body)
	})

	t.Run("operator token sees detail", func(t *testing.T) {
		// l.Auth is the onboarding authorization, which carries OperPermissions.
		status, body := getCheckEndpoint(t, l, "/health", l.Auth.Token)
		require.Equal(t, nethttp.StatusOK, status)
		assert.Equal(t, "influxdb", body["name"])
		assert.Equal(t, "healthy", body["message"])
		assert.Contains(t, body, "version")
		assert.Contains(t, body, "commit")
		checks, ok := body["checks"].([]any)
		require.Truef(t, ok, "expected a checks array, got %#v", body["checks"])
		assert.NotEmpty(t, checks)
	})

	t.Run("ready reports status without a credential", func(t *testing.T) {
		status, body := getCheckEndpoint(t, l, "/ready", "")
		require.Equal(t, nethttp.StatusOK, status)
		assert.Equal(t, "ready", body["status"])
		// started and up are not sensitive and stay available to probes.
		assert.Contains(t, body, "started")
		assert.Contains(t, body, "up")
	})
}

// TestLauncher_HealthAuth_ImpliedByHardening pins the OR in the launcher:
// --hardening-enabled turns on every hardening feature, health auth included,
// without --health-auth-enabled being set.
func TestLauncher_HealthAuth_ImpliedByHardening(t *testing.T) {
	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t, func(o *launcher.InfluxdOpts) {
		o.HardeningEnabled = true
	})
	defer l.ShutdownOrFail(t, ctx)

	status, body := getCheckEndpoint(t, l, "/health", "")
	require.Equal(t, nethttp.StatusOK, status)
	assertReducedHealthy(t, body)

	// The build fields are what separates an operator from everyone else on a
	// healthy server; the checks array is served either way.
	status, body = getCheckEndpoint(t, l, "/health", l.Auth.Token)
	require.Equal(t, nethttp.StatusOK, status)
	assert.Contains(t, body, "version")
	assert.Contains(t, body, "commit")

	// The implication is resolved into opts before the config handler is built,
	// so /api/v2/config reports what is actually enforced rather than the raw
	// flag the operator happened to pass.
	status, cfg := getCheckEndpoint(t, l, "/api/v2/config", l.Auth.Token)
	require.Equal(t, nethttp.StatusOK, status)
	config, ok := cfg["config"].(map[string]any)
	require.Truef(t, ok, "expected a config object, got %#v", cfg["config"])
	assert.Equal(t, true, config["health-auth-enabled"])
}

// TestLauncher_HealthAuth_HardeningOptOut pins the escape hatch. An operator
// who hardens the instance but whose monitoring parses the /health body can set
// --health-auth-enabled=false and keep both: the full anonymous envelope, and
// every other hardening feature -- including the flux/pkger IP validator, which
// has no per-feature flag of its own and is therefore unreachable if dropping
// --hardening-enabled were the only way out.
//
// HealthAuthEnabledSet is what newInfluxdCommand sets when the operator names
// the option on the command line, in INFLUXD_HEALTH_AUTH_ENABLED, or in the
// config file; the three sources are covered by the cmd tests.
func TestLauncher_HealthAuth_HardeningOptOut(t *testing.T) {
	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t, func(o *launcher.InfluxdOpts) {
		o.HardeningEnabled = true
		o.HealthAuthEnabled = false
		o.HealthAuthEnabledSet = true
	})
	defer l.ShutdownOrFail(t, ctx)

	status, body := getCheckEndpoint(t, l, "/health", "")
	require.Equal(t, nethttp.StatusOK, status)
	assert.Equal(t, "healthy", body["message"])
	assert.Contains(t, body, "checks")
	assert.Contains(t, body, "version")
	assert.Contains(t, body, "commit")

	// Hardening is otherwise untouched. HSTS is the feature observable from
	// here, and it is served by the same handler the opt-out just disarmed.
	req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodGet, l.URL().String()+"/health", nil)
	require.NoError(t, err)
	res, err := nethttp.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, res.Body.Close()) }()
	assert.NotEmpty(t, res.Header.Get("Strict-Transport-Security"),
		"opting out of health auth must not disarm the rest of --hardening-enabled")

	// And /api/v2/config reports what is enforced, not the flag that lost.
	status, cfg := getCheckEndpoint(t, l, "/api/v2/config", l.Auth.Token)
	require.Equal(t, nethttp.StatusOK, status)
	config, ok := cfg["config"].(map[string]any)
	require.Truef(t, ok, "expected a config object, got %#v", cfg["config"])
	assert.Equal(t, false, config["health-auth-enabled"])
	assert.Equal(t, true, config["hardening-enabled"])
}

// The flag-off case is pinned by TestLauncher_HealthEndpoint, which already
// boots a default launcher and asserts the full anonymous envelope. No test
// here repeats that boot.
