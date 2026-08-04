package http

import (
	"encoding/json"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/influxdata/influxdb/v2/toml"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// operPermissions is the bar a caller must clear to read check detail, built
// once rather than per request: platform.OperPermissions allocates a 44-element
// slice, and /health is polled continuously. Read-only — never hand this slice
// to anything that might retain or mutate it.
var operPermissions = platform.OperPermissions()

const (
	// statusStarting is the body-level status value reported by /ready when
	// any ready check is still failing, by ServeHTTP's pre-delegate 503
	// fallback, and as the firstFailureMessage fallback when a failing
	// health check supplies no message of its own. statusReady is its
	// counterpart on the /ready 200 path; messageHealthy is the top-level
	// message on the /health 200 path.
	statusStarting = "starting"
	statusReady    = "ready"
	messageHealthy = "healthy"

	// healthName is the top-level Name reported on the /health response.
	// QueryHealthCheck reuses it so the check identity it returns is the
	// same whether the remote /health succeeds or fails.
	healthName = "influxdb"

	// startingBody is the response body for non-/health-or-/ready requests
	// received before SetHandler installs a delegate.
	startingBody = `{"status":"` + statusStarting + `"}` + "\n"

	// contentTypeKey and contentTypeJSON are the header name and value
	// used on every response body this handler writes itself.
	contentTypeKey  = "Content-Type"
	contentTypeJSON = "application/json; charset=utf-8"

	// pathKey is the zap field key carrying the request path on
	// write-error log lines.
	pathKey = "path"

	// maxInflightResolutions caps concurrent credential resolutions, and so
	// caps the goroutines and bolt read transactions a wedged store can strand
	// (see HealthReadyHandler.resolve). It is well above the concurrency of
	// real credentialed probe traffic -- a handful of monitors, not a fleet --
	// and low enough that stranding that many costs nothing that matters.
	maxInflightResolutions = 8

	// maxResolutionsPerSecond and resolutionBurst cap the *rate* of credential
	// resolutions, and so the rate of store reads a caller can drive through
	// these endpoints. maxInflightResolutions caps concurrency, which is a
	// different quantity and not the one at risk here: eight slots turning over
	// at bolt speed is thousands of reads a second, and /health is the endpoint
	// operators routinely exempt from the rate limiting that fronts everything
	// else. Resolving a token opens a bolt View and an index lookup, and neither
	// requires the credential to be valid -- a garbage token costs the same read
	// a real one does, and can be sent as fast as the network allows.
	//
	// Sized for real credentialed probe traffic -- a handful of monitors on a
	// 5-15s interval -- with the burst absorbing their alignment. A resolution
	// that does not fit in the budget is not a rejection on the credential's
	// merits; it simply does not happen, which detail answers the same way it
	// answers a saturated pool.
	maxResolutionsPerSecond = 2
	resolutionBurst         = 16

	// resolveBudgetLogInterval throttles the line logged when the budget is
	// exhausted. The condition is caller-inducible, so logging per occurrence
	// would hand a flood the log volume as well; one line per interval is enough
	// for an operator to tell "I am being flooded" from "my token is wrong",
	// which are otherwise the same symptom -- a reduced body.
	resolveBudgetLogInterval = time.Minute
)

// delegateHandler wraps an http.Handler so the atomic pointer targets a
// concrete struct instead of an interface, avoiding the pointer-to-interface
// awkwardness of atomic.Pointer[http.Handler]. The embedded Handler promotes
// ServeHTTP so callers write d.ServeHTTP(w, r).
type delegateHandler struct{ http.Handler }

// resolverHolder and checkerHolder wrap their interfaces for the same reason
// delegateHandler wraps http.Handler. The embedded interface promotes its
// method so callers write rh.Authorize(r) and ch.Check(ctx).
type resolverHolder struct{ CredentialResolver }
type checkerHolder struct{ check.Checker }

// HealthReadyHandler serves /health and /ready backed by a *check.Check and
// forwards any other request to an optional delegate handler. Before the
// delegate is installed, non-check requests get a 503 "starting" response.
// It is safe for concurrent use; checkers may be registered while it is
// serving.
type HealthReadyHandler struct {
	check     *check.Check
	startTime time.Time
	delegate  atomic.Pointer[delegateHandler] // nil == no delegate installed
	headers   *AddHeader
	log       *zap.Logger

	// authRequired gates check detail behind operator permissions. It is a
	// plain bool rather than an atomic because it is set during setup, before
	// the handler begins serving -- the same lifetime as headers.
	authRequired bool

	// resolver identifies the caller behind a request. It stays nil until the
	// launcher installs one, which happens after the KV migrations and so long
	// after serving starts, so it must be atomic. nil is not "deny": it is
	// "cannot ask yet", which detail treats differently.
	resolver atomic.Pointer[resolverHolder]

	// authDep stands in for the store that credential resolution reads; when
	// it reports fail, resolution is skipped. nil == no guard installed.
	authDep atomic.Pointer[checkerHolder]

	// resolveSlots bounds concurrent credential resolutions; see resolve. Its
	// capacity is the number of goroutines a wedged store may strand.
	resolveSlots chan struct{}

	// resolveBudget bounds the rate of credential resolutions, and so the rate
	// of store reads a caller can drive; see resolve. One bucket for the
	// handler rather than one per caller: the resource being protected is the
	// store, per-IP state is unbounded memory an anonymous caller controls, and
	// X-Forwarded-For is not trustworthy on the path these endpoints sit on.
	resolveBudget *rate.Limiter

	// budgetLog throttles the resolveBudget-exhausted log line so a flood
	// cannot turn itself into log volume.
	budgetLog *rate.Limiter
}

// healthBody is the full /health envelope: a check response plus build info.
// When the caller is not authorized for detail, writeHealth serves a
// check.BasicResponse instead -- see there for what survives, which depends on
// whether the aggregate is passing. A field added here is withheld by default,
// so consider whether it belongs in that reduced body too.
type healthBody struct {
	Name    string          `json:"name"`
	Status  string          `json:"status"`
	Message string          `json:"message"`
	Checks  check.Responses `json:"checks"`
	Version string          `json:"version"`
	Commit  string          `json:"commit"`
}

type readyBody struct {
	Status string          `json:"status"`
	Start  time.Time       `json:"started"`
	Up     toml.Duration   `json:"up"`
	Checks check.Responses `json:"checks,omitempty"`
}

// NewHealthReadyHandler returns a HealthReadyHandler with no registered
// checkers and no delegate installed. A nil log is replaced with zap.NewNop
// so write-error logging is always safe to call.
func NewHealthReadyHandler(log *zap.Logger) *HealthReadyHandler {
	if log == nil {
		log = zap.NewNop()
	}
	return &HealthReadyHandler{
		check:         check.NewCheck(),
		startTime:     time.Now(),
		headers:       &AddHeader{WriteHeader: serverHeaderWriter(false, 0)},
		log:           log,
		resolveSlots:  make(chan struct{}, maxInflightResolutions),
		resolveBudget: rate.NewLimiter(maxResolutionsPerSecond, resolutionBurst),
		budgetLog:     rate.NewLimiter(rate.Every(resolveBudgetLogInterval), 1),
	}
}

// SetStrictTransportSecurity makes the handler emit the Strict-Transport-Security
// (HSTS) header, with the given max-age in seconds, on the /health, /ready, and
// pre-delegate "starting" responses it renders itself. Like the root handler,
// this is opt-in via --hardening-enabled so the header is consistent across all
// endpoints. Call during setup, before the handler begins serving.
func (h *HealthReadyHandler) SetStrictTransportSecurity(maxAge int) {
	if maxAge < 0 {
		maxAge = 0
	}
	h.headers = &AddHeader{WriteHeader: serverHeaderWriter(true, maxAge)}
}

// SetHealthAuthRequired makes /health and /ready withhold check detail from
// callers that cannot prove operator permissions. Unauthorized callers still
// receive the correct 200/503 status code and a reduced body, so credential-free
// liveness probes keep working unchanged; only the diagnostic detail is gated.
// Opt-in via --health-auth-enabled or --hardening-enabled. Call during setup,
// before the handler begins serving.
//
// Until a credential resolver is installed no caller can be identified, and the
// bodies carry check names and statuses without their messages instead; see
// detail.
func (h *HealthReadyHandler) SetHealthAuthRequired(required bool) {
	h.authRequired = required
}

// SetCredentialResolver installs the resolver used to identify callers when
// health auth is required. Installing the first one closes the startup window
// in which detail cannot be gated on a credential at all (see detail); from
// then on a caller who cannot prove operator permissions gets the reduced body.
// A nil cr is ignored.
//
// Safe to call concurrently with ServeHTTP, and safe to call more than once:
// the launcher installs a token-only resolver as soon as the authorization
// store opens and replaces it with the full resolver once sessions are wired.
func (h *HealthReadyHandler) SetCredentialResolver(cr CredentialResolver) {
	if cr == nil {
		return
	}
	h.resolver.Store(&resolverHolder{cr})
}

// SetAuthDependencyChecker installs a checker standing in for the store that
// credential resolution reads. While that checker reports fail, resolution is
// skipped, and since no caller can then be identified, every one of them is
// served check names and statuses without their messages.
//
// This guard exists because resolving a token calls FindAuthorizationByToken,
// which opens a bolt View, and bbolt's View cannot be cancelled (see
// KVStore.runOneProbe in bolt/kv.go). A wedged store would therefore hang the
// probe -- precisely the failure the background prober behind this checker was
// built to survive. Pass the store's own Checker rather than relying on a check
// name: /ready's KV entry is a ReadyGate that never un-fires once startup
// completes, so a name lookup would leave /ready exposed. A nil c is ignored.
// Safe to call concurrently with ServeHTTP.
func (h *HealthReadyHandler) SetAuthDependencyChecker(c check.Checker) {
	if c == nil {
		return
	}
	h.authDep.Store(&checkerHolder{c})
}

// detailLevel is how much of a check response a caller may see.
type detailLevel int

// These are permissions, not response shapes: writeHealth and writeReady each
// decide what a level yields on their own passing and failing paths.
const (
	// detailNone withholds every message and every per-check status the
	// aggregate does not already imply, along with the build info.
	detailNone detailLevel = iota
	// detailNames also permits per-check names and statuses, still with the
	// messages stripped. It is the answer whenever the handler could not ask
	// who the caller is -- not when it asked and did not like the answer, which
	// is detailNone.
	detailNames
	// detailFull is the whole envelope, the only level that carries messages.
	detailFull
)

// detail reports how much check detail the caller may see. When health auth is
// disabled -- the default -- it is detailFull for everyone and costs a single
// bool load, so the atomics are never touched on the common path.
//
// Note that a caller presenting no credential at all is rejected by
// ProbeAuthScheme without any store access and without occupying a resolution
// slot, which is what keeps credential-free liveness probes free of the cost
// (and of the wedged-store risk) entirely.
//
// Two of the paths below end in detailNames rather than detailNone, and they
// are the same situation: the handler could not ask who the caller is. No
// resolver exists yet, or the store resolution would read is failing. Neither
// is anything a caller did -- both are global server state, reached identically
// by every request in flight -- and answering them as though the caller had
// been rejected costs an operator the subsystem attribution during exactly the
// incidents these endpoints exist to report. Being unable to ask releases shape
// -- check names and statuses -- and never content: messages and build info
// still require a credential.
//
// Slot exhaustion is deliberately not a third; see the !asked branch below.
// See HEALTH_READY.md ("The startup window", "Behavior when the KV store is
// wedged") for the operator-facing description.
func (h *HealthReadyHandler) detail(r *http.Request) detailLevel {
	if !h.authRequired {
		return detailFull
	}
	rh := h.resolver.Load()
	if rh == nil {
		// No resolver has been installed yet. Credential resolution reads the
		// authorization store, which cannot be opened until the KV migrations
		// finish, so this window is a floor rather than a wiring oversight: for
		// the whole of it every caller, operator included, would otherwise see a
		// body with no subsystem attribution at all -- during precisely the
		// phase (migrating, or hung mid-migration) when attribution is what an
		// operator needs. Names and statuses are released instead. They come
		// from check responses already computed above the gate, so nothing here
		// touches the store, and stripDetail removes the messages, which is
		// where startup error text (paths, addresses, DSNs) lives.
		//
		// This is startup-only in practice: the launcher installs a resolver as
		// soon as the authorization store opens and never removes one. A server
		// that never gets there is one that failed to start.
		return detailNames
	}
	if c := h.authDep.Load(); c != nil && c.Check(r.Context()).Status() == check.StatusFail {
		// The store a credential would be resolved against is failing, so the
		// credential does not get resolved -- see SetAuthDependencyChecker. The
		// caller is unidentifiable for the duration, which is the one time an
		// operator most needs to know which subsystem is failing.
		return detailNames
	}
	auth, asked := h.resolve(r, rh)
	if !asked {
		// The resolution was turned away by one of resolve's bounds: the rate
		// budget was empty, or every slot was occupied. Unlike the two cases
		// above, both are caller-inducible -- enough credential-bearing
		// requests exhaust either. Answering detailNames here would let a flood
		// grant itself more than any single one of its requests earns on its
		// own -- a rejected caller gets detailNone -- while demoting the
		// operator probe it crowds out. So neither bound ever escalates: a
		// caller that does not get resolved is answered as though it had been
		// asked and rejected.
		//
		// The wedged-store incident the slot bound exists for still reaches
		// detailNames, by the authDep guard above, once the prober notices.
		// The window where the slots are gone but the guard has not yet
		// flipped is bounded by bolt.DefaultProbeStaleness, and costs an
		// operator names and statuses for that long -- never the correct
		// status code, which writeHealth and writeReady compute above the gate.
		return detailNone
	}
	if auth == nil {
		return detailNone
	}
	if authorizer.AllowedAll(auth, operPermissions) {
		return detailFull
	}
	return detailNone
}

// resolve identifies the caller behind r, bounding both how many resolutions
// may be in flight at once and how fast they may be started. It reports
// asked=false when either bound turned the request away -- which is not a
// denial, the caller was never asked, though detail answers it as one -- and a
// nil Authorizer with asked=true both when the request carries no credential to
// resolve and when the credential was resolved and rejected.
//
// The three checks run cheapest-first, so a request that will not be resolved is
// turned away before it consumes anything scarcer than the check itself:
//
//  1. The credential probe reads a header and a cookie and touches no store, so
//     a credential-free request -- every plain liveness probe, which is most of
//     this endpoint's traffic -- is answered without spending a slot or a unit
//     of budget, and cannot crowd out the credentialed probe those bound.
//  2. The rate budget caps store reads per second. Resolving a token opens a
//     bolt View whether or not the token is any good, so without it an
//     anonymous caller can drive unbounded reads against the metadata store
//     through an endpoint that is conventionally exempt from the rate limiting
//     in front of everything else. See maxResolutionsPerSecond.
//  3. The in-flight slot caps what a wedged store can strand, below.
//
// The rate budget is global, so a flood costs a real operator the check detail
// for as long as it lasts -- their probe finds the same empty bucket. That is
// the same trade the slot bound already makes, and it is deliberate: the status
// code is computed above the gate and never depends on any of this, so a
// liveness probe reads the truth throughout.
//
// The slot bound exists because the freshness guard above it is up to
// bolt.DefaultProbeStaleness late: a store that wedges just after its last
// successful probe still looks healthy for seconds, and every resolution begun
// in that window opens a bolt View, which bbolt cannot cancel. Those requests
// never return. Without the bound each one strands its own goroutine and read
// transaction for the life of the process; with it, at most
// maxInflightResolutions do, and every later request is answered immediately
// from cached check state. The slot is released by the deferred receive, which
// a stranded resolution never reaches -- deliberately, since the resource it is
// standing in for is not free either.
func (h *HealthReadyHandler) resolve(r *http.Request, rh *resolverHolder) (platform.Authorizer, bool) {
	// A caller carrying no credential has nothing to resolve, which is an
	// answer, not a slot or a unit of budget spent: report it as asked and
	// rejected. Authorize probes again on the path below -- a second header read
	// and cookie lookup, no store access -- which keeps CredentialResolver a
	// single-method interface.
	if _, err := ProbeAuthScheme(r); err != nil {
		return nil, true
	}
	if !h.resolveBudget.Allow() {
		if h.budgetLog.Allow() {
			// Warn, not Error: nothing is broken, and the endpoints keep
			// reporting the truth. But an operator whose probe just lost its
			// detail has no other way to learn why, since a starved probe and a
			// rejected credential produce the same body.
			h.log.Warn("Credential resolution for /health and /ready is over budget; check detail is being withheld from callers that would otherwise be authorized",
				zap.Int("resolutions_per_second", maxResolutionsPerSecond),
				zap.Int("burst", resolutionBurst))
		}
		return nil, false
	}
	select {
	case h.resolveSlots <- struct{}{}:
	default:
		return nil, false
	}
	defer func() { <-h.resolveSlots }()

	// Discard the error rather than returning it: every caller of resolve
	// treats "rejected" as one outcome, and Authorize is documented not to log,
	// so there is nothing further to do with it here. Returning an untyped nil
	// keeps auth == nil a reliable test -- Authorize's own contract, since
	// extractSession's concrete *platform.Session would otherwise make the
	// interface non-nil.
	auth, err := rh.Authorize(r)
	if err != nil {
		return nil, true
	}
	return auth, true
}

// AddHealthCheck registers an anonymous health check.
func (h *HealthReadyHandler) AddHealthCheck(c check.Checker) { h.check.AddHealthCheck(c) }

// AddNamedReadyCheck registers nc as a ready check under nc.CheckName().
func (h *HealthReadyHandler) AddNamedReadyCheck(nc check.NamedChecker) {
	h.check.AddNamedReadyCheck(nc)
}

// AddNamedHealthCheck registers nc as a health check under nc.CheckName().
func (h *HealthReadyHandler) AddNamedHealthCheck(nc check.NamedChecker) {
	h.check.AddNamedHealthCheck(nc)
}

// ReadyCheckNames returns the names of currently-registered ready checks
// in registration order.
func (h *HealthReadyHandler) ReadyCheckNames() []string { return h.check.ReadyCheckNames() }

// SetHandler installs the delegate handler used for any request that is not
// /health or /ready. A nil next is ignored to prevent a nil delegate from
// being published. Note: Go's typed-nil-through-interface gotcha means a
// concrete typed nil (e.g. (*T)(nil)) will still pass this guard; the
// delegate's own ServeHTTP needs to tolerate a nil receiver if that
// pattern is possible. Safe to call concurrently with ServeHTTP.
func (h *HealthReadyHandler) SetHandler(next http.Handler) {
	if next == nil {
		return
	}
	h.delegate.Store(&delegateHandler{next})
}

// ServeHTTP dispatches /health and /ready to the local renderers. Other
// paths are forwarded to the installed delegate; if no delegate has been
// installed, it returns 503 with a small "starting" body. Build-info
// headers are added to responses this handler renders itself; delegated
// responses rely on the delegate's own header middleware to avoid
// double-adding them.
func (h *HealthReadyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case HealthPath, HealthPath + "/":
		h.headers.WriteHeader(w.Header())
		h.writeHealth(w, r)
	case ReadyPath, ReadyPath + "/":
		h.headers.WriteHeader(w.Header())
		h.writeReady(w, r)
	default:
		if d := h.delegate.Load(); d != nil {
			d.ServeHTTP(w, r)
			return
		}
		h.headers.WriteHeader(w.Header())
		w.Header().Set(contentTypeKey, contentTypeJSON)
		w.WriteHeader(http.StatusServiceUnavailable)
		if _, err := io.WriteString(w, startingBody); err != nil {
			h.log.Debug("failed to write starting body",
				zap.String(pathKey, r.URL.Path),
				zap.Error(err))
		}
	}
}

// writeHealth evaluates the health checks and renders them, withholding the
// failure message and the per-check detail when the caller is not authorized
// for them. The checks are evaluated before the credential is consulted: check
// evaluation never blocks, and the outcome decides the status code regardless
// of who is asking.
//
// What an unauthorized caller keeps depends on the aggregate: a passing /health
// serves its documented shape to everyone, while a failing one withholds the
// attribution. See the reduced-body branch below.
func (h *HealthReadyHandler) writeHealth(w http.ResponseWriter, r *http.Request) {
	resp := h.check.CheckHealth(r.Context())
	failed := resp.Status() == check.StatusFail
	status := http.StatusOK
	if failed {
		status = http.StatusServiceUnavailable
	}
	// check.BasicResponse is reused for the reduced body because its message and
	// checks are omitempty, so an empty one yields exactly {"name","status"} --
	// and it is the type QueryHealthCheck decodes /health into, so the reduced
	// body stays something the existing client understands. healthBody cannot be
	// reused: it carries omitempty on no field, and omitempty cannot be added to
	// Checks because resp.Checks() is a non-nil empty slice when nothing is
	// registered, a case the wire-format tests pin as "checks":[].
	//
	// Nothing withheld is computed above: firstFailureMessage builds the very
	// string this branch exists to suppress, and for a FreshnessResponse that
	// means a fmt.Sprintf per probe.
	if level := h.detail(r); level != detailFull {
		var (
			message string
			checks  check.Responses
		)
		switch {
		case !failed:
			// A passing /health keeps its documented shape for everyone. The
			// aggregate is pass, so every entry is a registered subsystem name
			// with status pass: the same list on every install of the same
			// configuration, saying nothing the 200 does not already say. The
			// message is the constant "healthy". Withholding those broke the
			// envelope on the path that is true almost all of the time, and
			// protected no secret.
			//
			// Messages and build info still reach only an operator: stripDetail
			// drops the task-scheduler's pass-path timing (see
			// cmd/influxd/run/scheduler_pulse.go), and version/commit stay out
			// because commit pins the exact build.
			message = messageHealthy
			checks = stripDetail(resp.Checks())
		case level == detailNames:
			// Failing, so which check is failing is this server's state rather
			// than its shape, and it is gated: names and statuses during the
			// startup window, nothing once a credential can be resolved.
			checks = stripDetail(resp.Checks())
		}
		h.writeJSON(w, r, status, check.NewBasicResponse(healthName, resp.Status(), message, checks))
		return
	}
	message := messageHealthy
	if failed {
		message = firstFailureMessage(resp.Checks())
	}
	info := platform.GetBuildInfo()
	h.writeJSON(w, r, status, healthBody{
		Name:    healthName,
		Status:  string(resp.Status()),
		Message: message,
		Checks:  resp.Checks(),
		Version: info.Version,
		Commit:  info.Commit,
	})
}

func (h *HealthReadyHandler) writeReady(w http.ResponseWriter, r *http.Request) {
	resp := h.check.CheckReady(r.Context())
	status := http.StatusOK
	readyStatus := statusReady
	var checks check.Responses
	if resp.Status() == check.StatusFail {
		status = http.StatusServiceUnavailable
		readyStatus = statusStarting
		// readyBody.Checks is omitempty, so simply not collecting the failing
		// checks is the whole redaction. Started and up stay: neither is
		// sensitive, and Start is a time.Time, for which omitempty does nothing
		// -- zeroing it would emit a bogus timestamp rather than omit the
		// field. On a ready instance nothing is withheld at all, since checks
		// is only populated here on the failure path.
		//
		// Gate before filtering rather than after: the startup window, when
		// /ready 503s for the whole of shard loading, is when the readiness
		// probe polls hardest.
		switch h.detail(r) {
		case detailFull:
			checks = failingChecks(resp.Checks())
		case detailNames:
			checks = failingChecksStripped(resp.Checks())
		}
	}
	h.writeJSON(w, r, status, readyBody{
		Status: readyStatus,
		Start:  h.startTime,
		Up:     toml.Duration(time.Since(h.startTime)),
		Checks: checks,
	})
}

// writeJSON marshals body and writes it with the given status. If marshaling
// fails the response collapses to a 500 with a fixed JSON error body, matching
// kit/check.writeResponse. Marshal errors are not expected for the shape-pinned
// bodies this handler emits today; this guards against future changes that
// introduce a field that can fail to encode. Write errors (client hung up
// mid-response) are logged at debug level — the headers are already on the
// wire at that point, so there is no recovery.
func (h *HealthReadyHandler) writeJSON(w http.ResponseWriter, r *http.Request, status int, body interface{}) {
	buf, err := json.Marshal(body)
	if err != nil {
		h.log.Error("failed to marshal response body",
			zap.String(pathKey, r.URL.Path),
			zap.Error(err))
		buf = []byte(`{"message":"error marshaling response","status":"fail"}`)
		status = http.StatusInternalServerError
	}
	buf = append(buf, '\n')
	w.Header().Set(contentTypeKey, contentTypeJSON)
	w.WriteHeader(status)
	if _, err := w.Write(buf); err != nil {
		h.log.Debug("failed to write response body",
			zap.String(pathKey, r.URL.Path),
			zap.Error(err))
	}
}

func firstFailureMessage(checks check.Responses) string {
	for _, c := range checks {
		if c.Status() == check.StatusFail {
			if msg := c.Message(); msg != "" {
				return msg
			}
			return string(check.StatusFail)
		}
	}
	return statusStarting
}

// stripDetail returns a copy of checks carrying each response's name and status
// and nothing else. Messages go because a gate that failed during startup
// carries the startup error text -- filesystem paths, addresses, DSNs -- which
// is the whole reason health auth exists. Sub-checks go with them: they carry
// messages of their own, and no check registered on this handler nests any.
//
// A nil return rather than an empty slice matters: BasicResponse.Checks is
// omitempty, so an empty result must omit the field rather than emit
// "checks":[]. /ready reaches the same shape through failingChecksStripped.
func stripDetail(checks check.Responses) check.Responses {
	if len(checks) == 0 {
		return nil
	}
	out := make(check.Responses, len(checks))
	for i, c := range checks {
		out[i] = check.NewBasicResponse(c.Name(), c.Status(), "", nil)
	}
	return out
}

func failingChecks(checks check.Responses) check.Responses {
	var out check.Responses
	for _, c := range checks {
		if c.Status() == check.StatusFail {
			out = append(out, c)
		}
	}
	return out
}

// failingChecksStripped is failingChecks followed by stripDetail in a single
// pass. Composing the two builds the filtered slice only to copy it and throw it
// away, and /ready takes this path for the whole of the startup window, when the
// readiness probe polls hardest and every gate is failing.
//
// The single pass also reads each Status once rather than twice, which matters
// beyond the saved call: a live Response (check.FreshnessResponse) can report
// differently on a second invocation, and selecting an entry on one reading
// while rendering it from another can emit a check the body says is failing
// with a status of pass.
//
// Like both halves it returns nil rather than an empty slice, which
// readyBody.Checks's omitempty needs in order to omit the field.
func failingChecksStripped(checks check.Responses) check.Responses {
	var out check.Responses
	for _, c := range checks {
		if status := c.Status(); status == check.StatusFail {
			out = append(out, check.NewBasicResponse(c.Name(), status, "", nil))
		}
	}
	return out
}
