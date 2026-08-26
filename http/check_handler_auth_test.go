package http

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/check"
	kitplatform "github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"golang.org/x/time/rate"
)

// stubResolver is a CredentialResolver returning fixed results and counting
// calls, so tests can assert both what the gate decided and whether it
// consulted the credential at all.
type stubResolver struct {
	auth   platform.Authorizer
	err    error
	called atomic.Int64
}

func (s *stubResolver) Authorize(*http.Request) (platform.Authorizer, error) {
	s.called.Add(1)
	return s.auth, s.err
}

// instancePermissions is what a session for the initial setup user carries. It
// is derived from the real URM the onboarding flow creates rather than a copied
// literal, so if that mapping ever stops granting instance permissions this test
// stops claiming to cover the setup-user path.
func instancePermissions(t *testing.T) []platform.Permission {
	t.Helper()
	urm := &platform.UserResourceMapping{
		UserType:     platform.Owner,
		ResourceType: platform.InstanceResourceType,
		ResourceID:   kitplatform.ID(1),
	}
	perms, err := urm.ToPermissions()
	require.NoError(t, err)
	return perms
}

// authHandler builds a handler with health auth enabled and the given permission
// set presented by every caller.
func authHandler(t *testing.T, perms []platform.Permission) (*HealthReadyHandler, *stubResolver) {
	t.Helper()
	h := NewHealthReadyHandler(zaptest.NewLogger(t))
	h.SetHealthAuthRequired(true)
	r := &stubResolver{auth: mock.NewMockAuthorizer(false, perms)}
	h.SetCredentialResolver(r)
	return h, r
}

// doAuthRequest issues a request carrying a token, so that it reaches the
// credential resolver at all. The value is never parsed -- every resolver in
// this file is a stub -- but resolve probes for a credential before spending a
// resolution slot, and a request carrying none is answered without consulting
// the resolver. Any test whose subject is what a resolved credential decides
// must use this rather than doRequest, which models the credential-free
// liveness probe.
func doAuthRequest(t *testing.T, h http.Handler, method, target string) *http.Response {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	SetToken("stub", req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Result()
}

func decodeBody(t *testing.T, res *http.Response) map[string]any {
	t.Helper()
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, json.Unmarshal(body, &got))
	return got
}

// TestHealthReadyHandler_Auth_Disabled_ServesFullDetail is the backward-compat
// pin: with the flag off, detail is served to a caller with no credential at
// all, exactly as before this feature existed.
func TestHealthReadyHandler_Auth_Disabled_ServesFullDetail(t *testing.T) {
	h := NewHealthReadyHandler(zaptest.NewLogger(t))
	h.AddNamedHealthCheck(failingChecker{name: "kv", message: "open /var/lib/influxdb/influxd.bolt: permission denied"})

	res := doRequest(t, h, http.MethodGet, "/health")
	defer closeBody(t, res)

	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
	got := decodeBody(t, res)
	assert.Equal(t, "open /var/lib/influxdb/influxd.bolt: permission denied", got["message"])
	assert.Contains(t, got, "checks")
	assert.Contains(t, got, "version")
	assert.Contains(t, got, "commit")
}

func TestHealthReadyHandler_Auth_PermissionMatrix(t *testing.T) {
	orgID := kitplatform.ID(1)

	tests := []struct {
		name     string
		perms    []platform.Permission
		wantFull bool
	}{
		{
			name:     "operator permissions",
			perms:    platform.OperPermissions(),
			wantFull: true,
		},
		{
			// The setup user's session route: instance is a wildcard.
			name:     "instance permissions",
			perms:    instancePermissions(t),
			wantFull: true,
		},
		{
			name:     "read all permissions",
			perms:    platform.ReadAllPermissions(),
			wantFull: false,
		},
		{
			// Deliberately pinned: an org owner is NOT sufficient. Every
			// permission OwnerPermissions grants carries a non-nil OrgID, which
			// cannot satisfy OperPermissions' org-wide (nil OrgID) requirement.
			name:     "org owner permissions",
			perms:    platform.OwnerPermissions(orgID),
			wantFull: false,
		},
		{
			name:     "no permissions",
			perms:    nil,
			wantFull: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, resolver := authHandler(t, tt.perms)
			h.AddNamedHealthCheck(failingChecker{name: "kv", message: "secret detail"})

			res := doAuthRequest(t, h, http.MethodGet, "/health")
			defer closeBody(t, res)

			// The status code must not depend on authorization: a liveness
			// probe reads it and must see the truth either way.
			require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
			require.Equal(t, int64(1), resolver.called.Load())

			got := decodeBody(t, res)
			assert.Equal(t, "influxdb", got["name"])
			assert.Equal(t, "fail", got["status"])

			if tt.wantFull {
				assert.Equal(t, "secret detail", got["message"])
				assert.Contains(t, got, "checks")
				return
			}
			assert.NotContains(t, got, "message")
			assert.NotContains(t, got, "checks")
			assert.NotContains(t, got, "version")
			assert.NotContains(t, got, "commit")
		})
	}
}

// TestHealthReadyHandler_Auth_UnresolvableCredential pins that a caller whose
// credential does not resolve gets the reduced body. This is the other way the
// gate can fail to identify a caller; the startup window below is deliberately
// not the same outcome.
func TestHealthReadyHandler_Auth_UnresolvableCredential(t *testing.T) {
	h := NewHealthReadyHandler(zaptest.NewLogger(t))
	h.SetHealthAuthRequired(true)
	h.SetCredentialResolver(&stubResolver{err: errors.New("token required")})
	h.AddNamedHealthCheck(failingChecker{name: "kv", message: "secret detail"})

	res := doAuthRequest(t, h, http.MethodGet, "/health")
	defer closeBody(t, res)

	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
	got := decodeBody(t, res)
	assert.Equal(t, "fail", got["status"])
	assert.NotContains(t, got, "message")
	assert.NotContains(t, got, "checks")
}

// TestHealthReadyHandler_Auth_NoCredentialSkipsResolution pins the other half of
// that distinction: a caller presenting nothing is answered without the resolver
// being consulted at all. The outcome is the same reduced body an unresolvable
// credential gets, but it is reached without a store read and without occupying
// one of the resolution slots -- which is what keeps a fleet of credential-free
// liveness probes from crowding out the credentialed operator probe the slots
// exist to bound. See HealthReadyHandler.resolve.
func TestHealthReadyHandler_Auth_NoCredentialSkipsResolution(t *testing.T) {
	h, resolver := authHandler(t, platform.OperPermissions())
	h.AddNamedHealthCheck(failingChecker{name: "kv", message: "secret detail"})

	res := doRequest(t, h, http.MethodGet, "/health")
	defer closeBody(t, res)

	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
	assert.Equal(t, int64(0), resolver.called.Load(),
		"a request with no credential has nothing to resolve")

	// Answered as rejected, not as unidentifiable: no names, no statuses.
	got := decodeBody(t, res)
	assert.Equal(t, "fail", got["status"])
	assert.NotContains(t, got, "message")
	assert.NotContains(t, got, "checks")
}

// namesAndStatuses returns the name/status pairs from a body's checks array and
// fails the test if any entry carries a message or a sub-check. Withholding the
// messages is the whole basis on which the startup window releases anything at
// all, so every assertion about that window runs through here.
func namesAndStatuses(t *testing.T, body map[string]any) map[string]string {
	t.Helper()
	raw, ok := body["checks"].([]any)
	require.Truef(t, ok, "expected a checks array, got %#v", body["checks"])
	out := make(map[string]string, len(raw))
	for _, entry := range raw {
		c, ok := entry.(map[string]any)
		require.Truef(t, ok, "expected a check object, got %#v", entry)
		name, ok := c["name"].(string)
		require.Truef(t, ok, "expected a check name, got %#v", c["name"])
		status, ok := c["status"].(string)
		require.Truef(t, ok, "expected a check status, got %#v", c["status"])
		assert.NotContains(t, c, "message", "check %q leaked its message", name)
		assert.NotContains(t, c, "checks", "check %q leaked its sub-checks", name)
		out[name] = status
	}
	return out
}

// startupWindowHandler builds a handler with health auth enabled and no
// credential resolver: the state the launcher is in from the first served
// request until the authorization store opens.
func startupWindowHandler(t *testing.T) *HealthReadyHandler {
	t.Helper()
	h := NewHealthReadyHandler(zaptest.NewLogger(t))
	h.SetHealthAuthRequired(true)
	h.AddNamedHealthCheck(failingChecker{name: "kv", message: "open /var/lib/influxdb/influxd.bolt: permission denied"})
	h.AddNamedReadyCheck(failingChecker{name: "engine", message: "loading shards 34.0% (17 / 50)"})
	h.AddNamedReadyCheck(staticChecker{name: "sqlite", resp: check.NamedPass("sqlite")})
	return h
}

// TestHealthReadyHandler_Auth_StartupWindow covers the deliberate relaxation
// before any resolver exists. The launcher cannot open the authorization store
// until the KV migrations finish, so for that window there is nobody to ask;
// withholding attribution from everyone would blind an operator during the one
// phase -- migrating, or hung mid-migration -- when these endpoints are all
// they have. Names and statuses go out; messages, which is where startup error
// text lives, do not.
func TestHealthReadyHandler_Auth_StartupWindow(t *testing.T) {
	t.Run("health", func(t *testing.T) {
		h := startupWindowHandler(t)

		res := doRequest(t, h, http.MethodGet, "/health")
		defer closeBody(t, res)

		require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
		got := decodeBody(t, res)
		assert.Equal(t, "influxdb", got["name"])
		assert.Equal(t, "fail", got["status"])
		assert.Equal(t, map[string]string{"kv": "fail"}, namesAndStatuses(t, got))
		// The top-level message is the failing check's message verbatim, so it
		// is withheld with the rest of them.
		assert.NotContains(t, got, "message")
		assert.NotContains(t, got, "version")
		assert.NotContains(t, got, "commit")
	})

	t.Run("ready", func(t *testing.T) {
		h := startupWindowHandler(t)

		res := doRequest(t, h, http.MethodGet, "/ready")
		defer closeBody(t, res)

		require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
		got := decodeBody(t, res)
		assert.Equal(t, "starting", got["status"])
		assert.Contains(t, got, "started")
		assert.Contains(t, got, "up")
		// Only the failing gate, exactly as on the authorized path: the
		// relaxation changes what each entry carries, never which entries.
		assert.Equal(t, map[string]string{"engine": "fail"}, namesAndStatuses(t, got))
	})
}

// TestHealthReadyHandler_Auth_StartupWindowWireFormat pins the exact bytes of
// the relaxed /health body. "no message field anywhere" is the security
// property the window rests on, and a structural assertion cannot see a field
// that a future marshaling change adds back.
func TestHealthReadyHandler_Auth_StartupWindowWireFormat(t *testing.T) {
	h := startupWindowHandler(t)

	res := doRequest(t, h, http.MethodGet, "/health")
	defer closeBody(t, res)

	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	assert.Equal(t, `{"name":"influxdb","status":"fail","checks":[{"name":"kv","status":"fail"}]}`+"\n", string(body))

	// Still the type the in-repo remote-health client decodes.
	var basic check.BasicResponse
	require.NoError(t, json.Unmarshal(body, &basic))
	assert.Equal(t, "influxdb", basic.Name())
	require.Len(t, basic.Checks(), 1)
	assert.Equal(t, "kv", basic.Checks()[0].Name())
	assert.Empty(t, basic.Checks()[0].Message())
}

// TestHealthReadyHandler_Auth_StartupWindowClosesOnResolver pins that the
// relaxation is startup-only. Installing a resolver ends it for good -- the
// launcher never removes one -- and from then on the caller's permissions
// decide, including deciding on less than the window gave away.
func TestHealthReadyHandler_Auth_StartupWindowClosesOnResolver(t *testing.T) {
	h := startupWindowHandler(t)

	get := func(t *testing.T) map[string]any {
		t.Helper()
		res := doAuthRequest(t, h, http.MethodGet, "/health")
		defer closeBody(t, res)
		require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
		return decodeBody(t, res)
	}

	got := get(t)
	assert.Equal(t, map[string]string{"kv": "fail"}, namesAndStatuses(t, got))

	// A caller who cannot prove operator permissions now gets strictly less
	// than the window gave them, which is the point: the window was a
	// concession to there being nobody to ask, not a permission grant.
	h.SetCredentialResolver(&stubResolver{auth: mock.NewMockAuthorizer(false, platform.ReadAllPermissions())})
	got = get(t)
	assert.NotContains(t, got, "checks")
	assert.NotContains(t, got, "message")

	// An operator gets everything, messages included.
	h.SetCredentialResolver(&stubResolver{auth: mock.NewMockAuthorizer(false, platform.OperPermissions())})
	got = get(t)
	assert.Equal(t, "open /var/lib/influxdb/influxd.bolt: permission denied", got["message"])
	assert.Contains(t, got, "checks")
}

// TestHealthReadyHandler_Auth_ReadyDetailForOperator is the authorized half of
// the /ready gate, and the feature's headline use case: with health auth on, an
// operator token watches shard-loading progress on /ready throughout startup.
// Nothing else asserts that /ready ever serves a message to anyone while the
// gate is enabled.
func TestHealthReadyHandler_Auth_ReadyDetailForOperator(t *testing.T) {
	h, resolver := authHandler(t, platform.OperPermissions())
	h.AddNamedReadyCheck(failingChecker{name: "engine", message: "loading shards 34.0% (17 / 50)"})
	h.AddNamedReadyCheck(staticChecker{name: "kv", resp: check.NamedPass("kv")})

	res := doAuthRequest(t, h, http.MethodGet, "/ready")
	defer closeBody(t, res)

	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
	require.Equal(t, int64(1), resolver.called.Load())

	got := decodeBody(t, res)
	assert.Equal(t, "starting", got["status"])
	checks, ok := got["checks"].([]any)
	require.Truef(t, ok, "expected a checks array, got %#v", got["checks"])
	require.Len(t, checks, 1, "only the failing gate is reported")
	engine, ok := checks[0].(map[string]any)
	require.Truef(t, ok, "expected a check object, got %#v", checks[0])
	assert.Equal(t, "engine", engine["name"])
	assert.Equal(t, "fail", engine["status"])
	assert.Equal(t, "loading shards 34.0% (17 / 50)", engine["message"],
		"an operator must see the progress message, not just the gate name")
}

// TestHealthReadyHandler_Auth_DependencyFailingSkipsResolution pins the guard
// against a wedged store, and what an operator still gets while it holds.
// Resolving a token opens a bolt View, which cannot be cancelled, so while the
// KV checker reports fail the resolver must not be consulted at all --
// otherwise /health would hang for credentialed callers in exactly the failure
// the background prober exists to survive.
//
// Not consulting it means nobody can be identified, operator included. That is
// a reason to release the attribution, not to withhold it: the caller standing
// in front of a KV incident learns which checks are failing, and still not the
// messages, which is where the raw error text lives.
func TestHealthReadyHandler_Auth_DependencyFailingSkipsResolution(t *testing.T) {
	h, resolver := authHandler(t, platform.OperPermissions())
	h.SetAuthDependencyChecker(failingChecker{name: "kv", message: "stale"})
	h.AddNamedHealthCheck(failingChecker{name: "kv", message: "secret detail"})
	h.AddNamedReadyCheck(failingChecker{name: "engine", message: "secret detail"})

	want := map[string]map[string]string{
		"/health": {"kv": "fail"},
		"/ready":  {"engine": "fail"},
	}
	for path, wantChecks := range want {
		t.Run(path, func(t *testing.T) {
			res := doAuthRequest(t, h, http.MethodGet, path)
			defer closeBody(t, res)

			require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
			got := decodeBody(t, res)
			// namesAndStatuses fails the test if any entry kept its message.
			assert.Equal(t, wantChecks, namesAndStatuses(t, got))
			assert.NotContains(t, got, "message",
				"the failing store's error text stays withheld")
		})
	}
	assert.Equal(t, int64(0), resolver.called.Load(),
		"the credential must not be resolved while the store it reads is failing")
}

// blockingResolver stands in for credential resolution against a wedged bolt:
// Authorize blocks, and like a bbolt View it cannot be cancelled. entered
// reports each arrival so a test can wait until every slot is occupied.
type blockingResolver struct {
	entered chan struct{}
	release chan struct{}
}

func (b *blockingResolver) Authorize(*http.Request) (platform.Authorizer, error) {
	b.entered <- struct{}{}
	<-b.release
	return mock.NewMockAuthorizer(false, platform.OperPermissions()), nil
}

// TestHealthReadyHandler_Auth_ResolutionIsBounded pins the cap on concurrent
// credential resolutions, and what a request that hits the cap is told. The
// dependency guard is up to DefaultProbeStaleness late, so a store that wedges
// just after its last successful probe still looks healthy and resolutions
// begun in that window never return. Unbounded, each one strands its own
// goroutine and bolt read transaction for the life of the process; bounded, the
// damage stops at maxInflightResolutions and every later request is answered
// immediately from check state that needs no store at all.
//
// Answered as rejected, specifically. The bound makes the pool exhaustible, and
// a caller can exhaust it on purpose, so this is the one "could not ask"
// condition that must not release more than a rejection does.
func TestHealthReadyHandler_Auth_ResolutionIsBounded(t *testing.T) {
	h := NewHealthReadyHandler(zaptest.NewLogger(t))
	h.SetHealthAuthRequired(true)
	h.AddNamedHealthCheck(failingChecker{name: "kv", message: "secret detail"})

	resolver := &blockingResolver{
		entered: make(chan struct{}, maxInflightResolutions),
		release: make(chan struct{}),
	}
	h.SetCredentialResolver(resolver)

	var wg sync.WaitGroup
	release := sync.OnceFunc(func() {
		close(resolver.release)
		wg.Wait()
	})
	// The stranded requests outlive the goroutines that made them, exactly as a
	// wedged View would. Release them however the test ends.
	t.Cleanup(release)

	// Credential-bearing, since a request with nothing to resolve never reaches
	// the resolver and so cannot occupy a slot. That is also the shape of the
	// threat: saturating the pool takes credentials, even garbage ones.
	for range maxInflightResolutions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res := doAuthRequest(t, h, http.MethodGet, "/health")
			// assert, not require: FailNow off the test goroutine is illegal.
			assert.NoError(t, res.Body.Close())
		}()
	}
	// A slot is taken before Authorize is called, so one signal per goroutine
	// means every slot is occupied.
	for range maxInflightResolutions {
		<-resolver.entered
	}

	res := doAuthRequest(t, h, http.MethodGet, "/health")
	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode,
		"a saturated resolver must not change what a probe reads")
	got := decodeBody(t, res)
	closeBody(t, res)
	// Saturation is the one "could not ask" condition a caller can manufacture,
	// so it must not release more than being asked and rejected does: enough
	// concurrent credentialed requests would otherwise let a flood grant itself
	// the subsystem attribution, and demote the operator probe it crowds out.
	assert.NotContains(t, got, "checks",
		"a saturated pool must not release what a rejected caller cannot have")
	assert.NotContains(t, got, "message")

	// Slots come back when a resolution returns -- the stranded ones never do,
	// which is the point, but a store that recovers must not leave the handler
	// permanently unable to identify anyone.
	release()

	res = doAuthRequest(t, h, http.MethodGet, "/health")
	defer closeBody(t, res)
	assert.Equal(t, "secret detail", decodeBody(t, res)["message"],
		"an operator is identified again once the slots are free")
}

// TestHealthReadyHandler_Auth_OverBudgetIsAnsweredAsRejected pins what an
// over-budget request is told. Resolving a token opens a bolt View whether or
// not the token is any good, so the budget is what stops an anonymous caller
// from driving unbounded reads against the metadata store through an endpoint
// conventionally exempt from rate limiting. Like a saturated slot pool, running
// out is caller-inducible, so it must not release more than a rejection does.
//
// The budget is replaced outright rather than drained, so nothing here depends
// on how fast the test machine issues requests.
func TestHealthReadyHandler_Auth_OverBudgetIsAnsweredAsRejected(t *testing.T) {
	h, resolver := authHandler(t, platform.OperPermissions())
	h.AddNamedHealthCheck(failingChecker{name: "kv", message: "secret detail"})
	h.resolveBudget = rate.NewLimiter(0, 0) // never allows

	res := doAuthRequest(t, h, http.MethodGet, "/health")
	defer closeBody(t, res)

	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode,
		"the status code never depends on the budget")
	assert.Equal(t, int64(0), resolver.called.Load(),
		"an over-budget request must not reach the store at all")

	got := decodeBody(t, res)
	assert.NotContains(t, got, "checks",
		"over budget must not release what a rejected caller cannot have")
	assert.NotContains(t, got, "message")
}

// TestHealthReadyHandler_Auth_ResolutionIsRateLimited is the wiring half: the
// budget a real handler is built with actually bounds resolutions, rather than
// the limiter sitting there unconsulted.
//
// Asserted as a range because the bucket refills while the loop runs. At
// maxResolutionsPerSecond that is 2 tokens a second against a loop that takes
// microseconds, so the exact count is resolutionBurst in practice; the range
// keeps a slow or heavily loaded machine from turning that into a flake.
func TestHealthReadyHandler_Auth_ResolutionIsRateLimited(t *testing.T) {
	const extra = 8

	h, resolver := authHandler(t, platform.OperPermissions())
	h.AddNamedHealthCheck(failingChecker{name: "kv", message: "secret detail"})

	detailed := 0
	rejected := 0
	for range resolutionBurst + extra {
		res := doAuthRequest(t, h, http.MethodGet, "/health")
		require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
		got := decodeBody(t, res)
		closeBody(t, res)

		if _, ok := got["message"]; ok {
			detailed++
			continue
		}
		rejected++
		assert.NotContains(t, got, "checks")
	}

	assert.Positive(t, detailed, "the budget must let an operator through at all")
	assert.LessOrEqual(t, detailed, resolutionBurst+extra/2,
		"the budget must turn requests away well before the loop ends")
	assert.Positive(t, rejected)
	assert.Equal(t, int64(detailed), resolver.called.Load(),
		"exactly the resolved requests reached the store")
}

// TestHealthReadyHandler_Auth_NoCredentialCostsNoBudget pins that the ordering
// in resolve is cheapest-first. A credential-free liveness probe is most of this
// endpoint's traffic; if it spent budget, a probe fleet could starve the
// credentialed operator probe the budget exists to protect.
func TestHealthReadyHandler_Auth_NoCredentialCostsNoBudget(t *testing.T) {
	h, _ := authHandler(t, platform.OperPermissions())
	h.AddNamedHealthCheck(failingChecker{name: "kv", message: "secret detail"})

	for range resolutionBurst * 4 {
		closeBody(t, doRequest(t, h, http.MethodGet, "/health"))
	}

	res := doAuthRequest(t, h, http.MethodGet, "/health")
	defer closeBody(t, res)
	assert.Equal(t, "secret detail", decodeBody(t, res)["message"],
		"credential-free traffic must not have spent the operator's budget")
}

// TestHealthReadyHandler_Auth_StartupWindowSurvivesWedgedDependency pins the
// order of the two guards. The dependency check exists to keep a wedged store
// from hanging credential resolution; during the startup window there is no
// resolution to hang, and the names and statuses come from check responses
// already computed without touching the store. So a store that wedges before
// the launcher gets that far must not also cost the operator the attribution.
func TestHealthReadyHandler_Auth_StartupWindowSurvivesWedgedDependency(t *testing.T) {
	h := startupWindowHandler(t)
	h.SetAuthDependencyChecker(failingChecker{name: "kv", message: "stale"})

	res := doRequest(t, h, http.MethodGet, "/health")
	defer closeBody(t, res)

	got := decodeBody(t, res)
	assert.Equal(t, map[string]string{"kv": "fail"}, namesAndStatuses(t, got))
	assert.NotContains(t, got, "message")
}

// TestHealthReadyHandler_Auth_DependencyPassingAllowsResolution is the converse:
// a healthy dependency must not block an authorized caller.
func TestHealthReadyHandler_Auth_DependencyPassingAllowsResolution(t *testing.T) {
	h, resolver := authHandler(t, platform.OperPermissions())
	h.SetAuthDependencyChecker(staticChecker{name: "kv", resp: check.NamedPass("kv")})
	h.AddNamedHealthCheck(failingChecker{name: "engine", message: "secret detail"})

	res := doAuthRequest(t, h, http.MethodGet, "/health")
	defer closeBody(t, res)

	require.Equal(t, int64(1), resolver.called.Load())
	got := decodeBody(t, res)
	assert.Equal(t, "secret detail", got["message"])
}

// TestHealthReadyHandler_Auth_RedactedHealthWireFormat pins the exact bytes of
// the reduced /health body. It is a check.BasicResponse, which is the type
// QueryHealthCheck already decodes /health into.
//
// The two passing cases are the contract this endpoint has always had on a 200:
// a "healthy" message and a checks array. On that path the array is the list of
// registered subsystems, identical on every install of the same configuration,
// so gating it protected nothing and broke every consumer that read it. The
// failing case is where withholding earns its keep.
func TestHealthReadyHandler_Auth_RedactedHealthWireFormat(t *testing.T) {
	tests := []struct {
		name    string
		checker check.NamedChecker
		want    string
		code    int
	}{
		{
			name: "passing with no checks registered",
			want: `{"name":"influxdb","status":"pass","message":"healthy"}` + "\n",
			code: http.StatusOK,
		},
		{
			// A passing check may still carry a message -- the task-scheduler
			// reports its next-run timing this way. That is state, so it goes,
			// while the name and status stay.
			name:    "passing check keeps its name and loses its message",
			checker: staticChecker{name: "task-scheduler", resp: check.NewBasicResponse("task-scheduler", check.StatusPass, "next run in 12s", nil)},
			want:    `{"name":"influxdb","status":"pass","message":"healthy","checks":[{"name":"task-scheduler","status":"pass"}]}` + "\n",
			code:    http.StatusOK,
		},
		{
			name:    "failing",
			checker: failingChecker{name: "kv", message: "open /var/lib/influxdb/influxd.bolt: permission denied"},
			want:    `{"name":"influxdb","status":"fail"}` + "\n",
			code:    http.StatusServiceUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := authHandler(t, nil)
			if tt.checker != nil {
				h.AddNamedHealthCheck(tt.checker)
			}

			res := doAuthRequest(t, h, http.MethodGet, "/health")
			defer closeBody(t, res)

			require.Equal(t, tt.code, res.StatusCode)
			body, err := io.ReadAll(res.Body)
			require.NoError(t, err)
			assert.Equal(t, tt.want, string(body))

			// The reduced body must still decode into the type the in-repo
			// client uses for a remote /health.
			var basic check.BasicResponse
			require.NoError(t, json.Unmarshal(body, &basic))
			assert.Equal(t, "influxdb", basic.Name())
		})
	}
}

// TestHealthReadyHandler_Auth_PassingBodyKeepsDocumentedShape is the structural
// half of the wire-format pin above, and the regression guard for the break it
// fixes: with health auth on, a healthy server must still answer a caller with
// no operator permissions the way it always has, minus the build fields. A
// monitor asserting message == "healthy" or a non-empty checks array is reading
// a documented contract, and it must not start reporting a healthy instance as
// broken the day --hardening-enabled is turned on.
func TestHealthReadyHandler_Auth_PassingBodyKeepsDocumentedShape(t *testing.T) {
	h, resolver := authHandler(t, platform.ReadAllPermissions())
	h.AddNamedHealthCheck(staticChecker{name: "kv", resp: check.NamedPass("kv")})
	h.AddNamedHealthCheck(staticChecker{name: "task-scheduler", resp: check.NewBasicResponse("task-scheduler", check.StatusPass, "idle", nil)})

	res := doAuthRequest(t, h, http.MethodGet, "/health")
	defer closeBody(t, res)

	require.Equal(t, http.StatusOK, res.StatusCode)
	require.Equal(t, int64(1), resolver.called.Load())

	got := decodeBody(t, res)
	assert.Equal(t, "influxdb", got["name"])
	assert.Equal(t, "pass", got["status"])
	assert.Equal(t, "healthy", got["message"])
	assert.Equal(t, map[string]string{"kv": "pass", "task-scheduler": "pass"}, namesAndStatuses(t, got))

	// Build info is the one thing the passing path still withholds: commit
	// pins the exact build, and version is already in the response header.
	assert.NotContains(t, got, "version")
	assert.NotContains(t, got, "commit")
	assert.Equal(t, platform.GetBuildInfo().Version, res.Header.Get("X-Influxdb-Version"))
}

// TestHealthReadyHandler_Auth_PassingBodyOperatorGetsBuildInfo is the converse:
// the passing path still distinguishes an operator, who gets the messages and
// the build fields the reduced body drops.
func TestHealthReadyHandler_Auth_PassingBodyOperatorGetsBuildInfo(t *testing.T) {
	h, _ := authHandler(t, platform.OperPermissions())
	h.AddNamedHealthCheck(staticChecker{name: "task-scheduler", resp: check.NewBasicResponse("task-scheduler", check.StatusPass, "next run in 12s", nil)})

	res := doAuthRequest(t, h, http.MethodGet, "/health")
	defer closeBody(t, res)

	require.Equal(t, http.StatusOK, res.StatusCode)
	got := decodeBody(t, res)
	assert.Equal(t, "healthy", got["message"])
	assert.Equal(t, platform.GetBuildInfo().Version, got["version"])
	assert.Contains(t, got, "commit")

	checks, ok := got["checks"].([]any)
	require.Truef(t, ok, "expected a checks array, got %#v", got["checks"])
	require.Len(t, checks, 1)
	sched, ok := checks[0].(map[string]any)
	require.Truef(t, ok, "expected a check object, got %#v", checks[0])
	assert.Equal(t, "next run in 12s", sched["message"],
		"an operator sees the pass-path message a reduced body strips")
}

// TestHealthReadyHandler_Auth_RedactedReady pins that /ready drops only the
// checks array. Started and up stay: neither is sensitive, and a probe reading
// uptime should keep working.
func TestHealthReadyHandler_Auth_RedactedReady(t *testing.T) {
	h, _ := authHandler(t, nil)
	h.AddNamedReadyCheck(failingChecker{name: "engine", message: "loading shards 34.0% (17 / 50)"})

	res := doAuthRequest(t, h, http.MethodGet, "/ready")
	defer closeBody(t, res)

	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
	got := decodeBody(t, res)
	assert.Equal(t, "starting", got["status"])
	assert.Contains(t, got, "started")
	assert.Contains(t, got, "up")
	assert.NotContains(t, got, "checks")
}

// TestHealthReadyHandler_Auth_RedactedKeepsBuildHeaders documents that the
// build-info headers survive redaction -- which is why the redacted body drops
// version without claiming to conceal it.
func TestHealthReadyHandler_Auth_RedactedKeepsBuildHeaders(t *testing.T) {
	h, _ := authHandler(t, nil)

	for _, path := range []string{"/health", "/ready"} {
		t.Run(path, func(t *testing.T) {
			res := doRequest(t, h, http.MethodGet, path)
			defer closeBody(t, res)

			assert.Equal(t, "OSS", res.Header.Get("X-Influxdb-Build"))
			assert.Equal(t, platform.GetBuildInfo().Version, res.Header.Get("X-Influxdb-Version"))
			assert.Equal(t, "application/json; charset=utf-8", res.Header.Get("Content-Type"))
		})
	}
}

// TestHealthReadyHandler_Auth_StatusCodesMatch is the property a liveness probe
// depends on: whether or not the caller is authorized changes the body, never
// the status code.
func TestHealthReadyHandler_Auth_StatusCodesMatch(t *testing.T) {
	build := func(t *testing.T, withAuth bool) *HealthReadyHandler {
		t.Helper()
		h := NewHealthReadyHandler(zaptest.NewLogger(t))
		if withAuth {
			h.SetHealthAuthRequired(true)
			h.SetCredentialResolver(&stubResolver{err: errors.New("token required")})
		}
		h.AddNamedHealthCheck(failingChecker{name: "kv", message: "secret detail"})
		h.AddNamedReadyCheck(failingChecker{name: "engine", message: "secret detail"})
		return h
	}

	for _, path := range []string{"/health", "/ready"} {
		t.Run(path, func(t *testing.T) {
			open := doRequest(t, build(t, false), http.MethodGet, path)
			defer closeBody(t, open)
			gated := doRequest(t, build(t, true), http.MethodGet, path)
			defer closeBody(t, gated)

			assert.Equal(t, open.StatusCode, gated.StatusCode)
		})
	}
}
