package launcher

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io/fs"
	"net"
	nethttp "net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/dependencies/testing"
	"github.com/influxdata/flux/dependencies/url"
	"github.com/influxdata/flux/execute/executetest"
	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/annotations"
	annotationTransport "github.com/influxdata/influxdb/v2/annotations/transport"
	"github.com/influxdata/influxdb/v2/authorization"
	"github.com/influxdata/influxdb/v2/authorizer"
	"github.com/influxdata/influxdb/v2/backup"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/checks"
	"github.com/influxdata/influxdb/v2/cmd/influxd/run"
	"github.com/influxdata/influxdb/v2/dashboards"
	dashboardTransport "github.com/influxdata/influxdb/v2/dashboards/transport"
	"github.com/influxdata/influxdb/v2/dbrp"
	"github.com/influxdata/influxdb/v2/gather"
	"github.com/influxdata/influxdb/v2/http"
	iqlcontrol "github.com/influxdata/influxdb/v2/influxql/control"
	iqlquery "github.com/influxdata/influxdb/v2/influxql/query"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/internal/resource"
	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/influxdata/influxdb/v2/kit/feature"
	overrideflagger "github.com/influxdata/influxdb/v2/kit/feature/override"
	"github.com/influxdata/influxdb/v2/kit/metric"
	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/prom"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/label"
	"github.com/influxdata/influxdb/v2/notebooks"
	notebookTransport "github.com/influxdata/influxdb/v2/notebooks/transport"
	endpointservice "github.com/influxdata/influxdb/v2/notification/endpoint/service"
	ruleservice "github.com/influxdata/influxdb/v2/notification/rule/service"
	"github.com/influxdata/influxdb/v2/pkger"
	infprom "github.com/influxdata/influxdb/v2/prometheus"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/query/control"
	"github.com/influxdata/influxdb/v2/query/fluxlang"
	"github.com/influxdata/influxdb/v2/query/stdlib/influxdata/influxdb"
	"github.com/influxdata/influxdb/v2/remotes"
	remotesTransport "github.com/influxdata/influxdb/v2/remotes/transport"
	"github.com/influxdata/influxdb/v2/replications"
	replicationTransport "github.com/influxdata/influxdb/v2/replications/transport"
	"github.com/influxdata/influxdb/v2/secret"
	"github.com/influxdata/influxdb/v2/session"
	"github.com/influxdata/influxdb/v2/snowflake"
	"github.com/influxdata/influxdb/v2/source"
	"github.com/influxdata/influxdb/v2/sqlite"
	sqliteMigrations "github.com/influxdata/influxdb/v2/sqlite/migrations"
	"github.com/influxdata/influxdb/v2/storage"
	storageflux "github.com/influxdata/influxdb/v2/storage/flux"
	"github.com/influxdata/influxdb/v2/storage/readservice"
	taskbackend "github.com/influxdata/influxdb/v2/task/backend"
	"github.com/influxdata/influxdb/v2/task/backend/coordinator"
	"github.com/influxdata/influxdb/v2/task/backend/executor"
	"github.com/influxdata/influxdb/v2/task/backend/middleware"
	"github.com/influxdata/influxdb/v2/task/backend/scheduler"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
	telegrafservice "github.com/influxdata/influxdb/v2/telegraf/service"
	"github.com/influxdata/influxdb/v2/telemetry"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/prometheus/client_golang/prometheus/collectors"

	// needed for tsm1
	_ "github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"

	// needed for tsi1
	_ "github.com/influxdata/influxdb/v2/tsdb/index/tsi1"
	authv1 "github.com/influxdata/influxdb/v2/v1/authorization"
	iqlcoordinator "github.com/influxdata/influxdb/v2/v1/coordinator"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	storage2 "github.com/influxdata/influxdb/v2/v1/services/storage"
	"github.com/influxdata/influxdb/v2/vault"
	pzap "github.com/influxdata/influxdb/v2/zap"
	"github.com/opentracing/opentracing-go"
	jaegerconfig "github.com/uber/jaeger-client-go/config"
	"go.uber.org/zap"
)

const (
	// DiskStore stores all REST resources to disk in boltdb and sqlite.
	DiskStore = "disk"
	// BoltStore also stores all REST resources to disk in boltdb and sqlite. Kept for backwards-compatibility.
	BoltStore = "bolt"
	// MemoryStore stores all REST resources in memory (useful for testing).
	MemoryStore = "memory"

	// LogTracing enables tracing via zap logs
	LogTracing = "log"
	// JaegerTracing enables tracing via the Jaeger client library
	JaegerTracing = "jaeger"
)

var (
	// ErrPIDFileExists indicates that a PID file already exists.
	ErrPIDFileExists = errors.New("PID file exists (possible unclean shutdown or another instance already running)")
)

type labeledCloser struct {
	label  string
	closer func(context.Context) error
}

// Launcher represents the main program execution.
type Launcher struct {
	wg       sync.WaitGroup
	cancel   func()
	doneChan <-chan struct{}
	closers  []labeledCloser
	flushers flushers

	flagger feature.Flagger

	kvStore   kv.Store
	kvService *kv.Service
	sqlStore  *sqlite.SqlStore

	// storage engine
	engine Engine

	// InfluxQL query engine
	queryController *control.Controller

	httpPort   int
	tlsEnabled bool

	scheduler stoppingScheduler
	executor  *executor.Executor

	log *zap.Logger
	reg *prom.Registry

	apibackend *http.APIBackend

	checkHandler      *http.HealthReadyHandler
	kvReady           *check.ReadyGate
	sqliteReady       *check.ReadyGate
	engineReady       *check.ReadyGate
	replicationsReady *check.ReadyGate
	queryReady        *check.ReadyGate
	tasksReady        *check.ReadyGate
	schedulerReady    *check.ReadyGate
	startupProgress   *run.StartupProgressLogger

	// readyGates maps a subsystem name to the ReadyGate registered under it,
	// so failSubsystem can latch the gate belonging to a failing phase instead
	// of registering a second /ready check with the same name. Populated by
	// initReadyChecks; only the gated subsystems appear.
	readyGates map[string]*check.ReadyGate

	// failedSubsystem is the name of the first phase failSubsystem attributed
	// a startup failure to, used only to notice a failure that reached no
	// attribution at all. Written and read from run's goroutine.
	failedSubsystem string

	// shutdownMu guards the closer list, the accumulated teardown state, and
	// httpServing. Teardown runs in two phases on the startup-failure path --
	// see shutdownSubsystems -- so a closer is consumed from m.closers as it
	// runs rather than the whole teardown being gated by a single sync.Once.
	shutdownMu sync.Mutex

	// httpServing reports whether runHTTP bound a listener. Consulted by
	// holdForStartupError: with no listener there is nothing to scrape, and
	// waiting only delays the error. It is guarded rather than plain because
	// run writes it from its own goroutine while holdForStartupError, which is
	// exported to tests and is the only reader, is reachable from another.
	httpServing  bool
	shutdownErrs []error
	shutdownDone bool
}

type stoppingScheduler interface {
	scheduler.Scheduler
	Stop()
}

// NewLauncher returns a new instance of Launcher with a no-op logger.
func NewLauncher() *Launcher {
	return &Launcher{
		log: zap.NewNop(),
	}
}

// Registry returns the prometheus metrics registry.
func (m *Launcher) Registry() *prom.Registry {
	return m.reg
}

// Engine returns a reference to the storage engine. It should only be called
// for end-to-end testing purposes.
func (m *Launcher) Engine() Engine {
	return m.engine
}

// ReadyCheckNames returns the names of currently-registered /ready checks
// in registration order. Intended for tests that want to assert which
// subsystems gate readiness.
func (m *Launcher) ReadyCheckNames() []string {
	return m.checkHandler.ReadyCheckNames()
}

// Shutdown closes whatever is left of the launcher and waits for all services
// to clean up. It is the final teardown phase: after it returns, nothing the
// launcher registered is still running.
//
// Every registered closer runs at most once across all calls, because each is
// consumed as it runs. A caller that cannot tell whether the launcher was
// already torn down — in whole, or in part via shutdownSubsystems — can call
// Shutdown unconditionally without double-closing a store or reporting a
// spurious error for already-released state. The returned error accumulates
// every phase's closer failures, so a single call site reports the whole
// teardown.
func (m *Launcher) Shutdown(ctx context.Context) error {
	m.shutdownMu.Lock()
	defer m.shutdownMu.Unlock()
	if m.shutdownDone {
		return m.shutdownError()
	}

	m.runClosers(ctx)

	// Safe only here, and not in shutdownSubsystems: the HTTP serve goroutine
	// is tracked in m.wg and returns only once the server closes, which the
	// closer above has now done.
	m.wg.Wait()

	// N.B. We ignore any errors here because Sync is known to fail with EINVAL
	// when logging to Stdout on certain OS's.
	//
	// Uber made the same change within the core of the logger implementation.
	// See: https://github.com/uber-go/zap/issues/328
	_ = m.log.Sync()

	m.shutdownDone = true
	return m.shutdownError()
}

// shutdownSubsystems runs every registered closer except the two that describe
// the process itself — the HTTP server's and the PID file's — releasing the
// bolt flock, the sqlite file and the engine directory while leaving the
// listener, and so /health and /ready, serving. It is the first of the two
// teardown phases used by holdForStartupError; Shutdown is the second and
// releases both of the ones kept here.
//
// The PID file is retained deliberately, and not merely as a companion to the
// listener. It is the interlock that stops a second influxd starting against
// the same data directory, and this process is still running and still holding
// its port: releasing it early would let a concurrent start past the check that
// exists to catch exactly this, only to fail it on "address already in use" —
// a worse error, naming the wrong cause. A PID file must describe a live
// process for as long as the process is alive.
//
// It deliberately does not wait on m.wg. The HTTP serve goroutine is tracked
// there and returns only once the server closes, so waiting here would block
// for exactly as long as the listener is retained.
func (m *Launcher) shutdownSubsystems(ctx context.Context) error {
	m.shutdownMu.Lock()
	defer m.shutdownMu.Unlock()
	if m.shutdownDone {
		return m.shutdownError()
	}

	m.runClosers(ctx, SubsystemHTTPServer, SubsystemPIDFile)
	return m.shutdownError()
}

// runClosers runs the registered closers in reverse registration order,
// skipping any whose label is in keep, and records each failure. The closers
// it is about to run are removed from m.closers before any of them run, so no
// closer can run twice even if one panics. The kept closers stay in
// registration order, so a later phase still tears down in reverse.
//
// Caller must hold shutdownMu.
func (m *Launcher) runClosers(ctx context.Context, keep ...string) {
	kept := make([]labeledCloser, 0, len(keep))
	pending := make([]labeledCloser, 0, len(m.closers))
	for _, lc := range m.closers {
		if slices.Contains(keep, lc.label) {
			kept = append(kept, lc)
			continue
		}
		pending = append(pending, lc)
	}
	m.closers = kept

	// Shut down subsystems in the reverse order of their registration.
	for i := len(pending); i > 0; i-- {
		lc := pending[i-1]
		m.log.Info("Stopping subsystem", zap.String("subsystem", lc.label))
		if err := lc.closer(ctx); err != nil {
			m.log.Error("Failed to stop subsystem", zap.String("subsystem", lc.label), zap.Error(err))
			m.shutdownErrs = append(m.shutdownErrs, fmt.Errorf("%s: %w", lc.label, err))
		}
	}
}

// shutdownError renders the closer failures accumulated across every teardown
// phase run so far. errors.Join rather than a flattened message: every failure
// stays reachable through errors.Is and errors.As, and each one already names
// the subsystem it came from. Caller must hold shutdownMu.
func (m *Launcher) shutdownError() error {
	if len(m.shutdownErrs) == 0 {
		return nil
	}
	return fmt.Errorf("failed to shut down server: %w", errors.Join(m.shutdownErrs...))
}

// freezeChecks pins /health and /ready to the report they serve right now, so
// the teardown that follows cannot rewrite it. This is the whole reason the
// freeze exists rather than an optimization: sqlite.SqlStore.Check pings a
// closed handle, and bolt.KVStore.Check ages into "stale: last probe ..." once
// its prober stops. check.Responses sorts failures first and then by name, and
// /health's top-level message is the first of them, so a closed bolt would
// outrank and mask the engine failure the hold exists to publish.
//
// The freeze is bounded at two levels, and the distinction between them is
// load-bearing. check.Check.Freeze gives every probe a context of its own,
// bounded at check.DefaultProbeTimeout, so a wedged subsystem holds the process
// open for the length of its own probe and no longer -- that is what stops an
// early slow checker leaving the rest to snapshot as failed probes, which,
// since failures sort ahead of passes by name, could outrank and mask the very
// attribution this freeze is taken to preserve. freezeTimeout then caps the sum
// as a backstop, sized so a healthy freeze never reaches it.
func (m *Launcher) freezeChecks(ctx context.Context) {
	// m.httpServing implies m.checkHandler is non-nil: the handler is built
	// before runHTTP is called, so a bound listener means both exist. No guard
	// here would ever fire.
	ctx, cancel := context.WithTimeout(ctx, freezeTimeout)
	defer cancel()
	m.checkHandler.FreezeChecks(ctx)
}

// setHTTPServing records that runHTTP bound a listener, so a failure from here
// on has somewhere to be read from.
func (m *Launcher) setHTTPServing() {
	m.shutdownMu.Lock()
	defer m.shutdownMu.Unlock()
	m.httpServing = true
}

// httpIsServing reports whether runHTTP bound a listener. It takes and releases
// shutdownMu, so a caller must not already hold it -- holdForStartupError reads
// this before the phased teardown that acquires it.
func (m *Launcher) httpIsServing() bool {
	m.shutdownMu.Lock()
	defer m.shutdownMu.Unlock()
	return m.httpServing
}

// cappedLinger bounds d at maxStartupErrorLinger, warning when it has to.
//
// The cap is enforced here rather than on the option so that it cannot be
// bypassed -- every path into the window goes through holdForStartupError --
// and so print-config keeps reporting the configured value rather than a
// rewritten one. The warning is the operator's only notice that the duration
// they chose is not the duration they will get, and it names the flag so the
// line is actionable on its own.
func (m *Launcher) cappedLinger(d time.Duration) time.Duration {
	if d <= maxStartupErrorLinger {
		return d
	}
	m.log.Warn("Startup error linger exceeds the maximum; capping it",
		zap.String("flag", startupErrorLingerFlag),
		zap.Duration("requested", d),
		zap.Duration("maximum", maxStartupErrorLinger))
	return maxStartupErrorLinger
}

// holdForStartupError releases everything the failed process no longer needs
// and then keeps /health and /ready scrapeable for d, so the startup error
// latched by failSubsystem can be retrieved before the process exits.
//
// The check set is frozen first — see freezeChecks — and then teardown is
// split around the wait. Every subsystem except the HTTP listener and the PID
// file is closed before the process parks, so the bolt flock, the sqlite file
// and the engine directory belong to the next run rather than to one that
// already failed. The listener is what the wait needs; the PID file is what
// keeps the next run from starting on top of this one while it still holds the
// port (see shutdownSubsystems). The Shutdown that follows releases both.
//
// Non-check requests are unaffected: the delegate handler is installed as the
// last statement of a successful run, so on this path there is none and they
// still get the 503 "starting" body.
//
// It returns immediately, tearing nothing down and freezing nothing, when d is
// non-positive or no listener was ever established: there is then nothing to
// scrape, and the caller's Shutdown does the whole teardown in one phase
// exactly as before. At the other end d is capped; see cappedLinger.
//
// The wait also ends when the launcher's context is done, which covers a
// SIGINT and a serve goroutine that already gave up and cancelled. It is NOT
// cut short by SIGTERM: influxd traps only os.Interrupt (see
// kit/signals.WithStandardSignals), so a SIGTERM during the window kills the
// process where it stands. Splitting the teardown is what limits the damage:
// the file locks are already gone, and what is left behind is a stale PID file
// — the same thing any uncatchable signal leaves behind at any other point in
// the process's life, and what --overwrite-pid-file is for.
//
// ctx bounds the teardown at shutdownTimeout and the freeze at freezeTimeout,
// not the wait. Pass the process context rather than the signal-wrapped one, so
// a signal racing either neither truncates the teardown nor poisons the freeze.
func (m *Launcher) holdForStartupError(ctx context.Context, d time.Duration) {
	if d <= 0 || !m.httpIsServing() {
		return
	}
	d = m.cappedLinger(d)
	m.freezeChecks(ctx)

	subsysCtx, cancel := context.WithTimeout(ctx, shutdownTimeout)
	// Failures are logged per subsystem by runClosers and accumulate into the
	// error the caller's Shutdown returns, so there is nothing to report here.
	_ = m.shutdownSubsystems(subsysCtx)
	cancel()

	m.log.Warn("Startup failed; serving /health and /ready before exiting",
		zap.Duration(startupErrorLingerFlag, d), zap.Int("port", m.httpPort))

	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-m.Done():
	}

	// Cancel the launcher context so the final Shutdown's m.wg.Wait() has the
	// same precondition it has on the normal exit path, where cmdRunE reaches
	// Shutdown only after <-l.Done(). Not load-bearing today — the serve
	// goroutine returns when its closer shuts the server down, not on this
	// cancel, and runReporter is the only other wg member and never starts on
	// a failure path — so it is tidiness, not a fix.
	m.cancel()
}

func (m *Launcher) Done() <-chan struct{} {
	return m.doneChan
}

// initReadyChecks creates every /ready check the launcher owns and registers
// it on m.checkHandler, in the order it should appear. Call once, before any
// subsystem starts, so /ready enumerates the full set of phases from the very
// first probe rather than growing as startup proceeds.
//
// The gates are recorded in m.readyGates keyed by their own CheckName, which
// is what makes failSubsystem unable to pair a subsystem name with another
// subsystem's gate. m.startupProgress is deliberately not a gate: it owns
// SubsystemShards and latches its own failure through Finish.
func (m *Launcher) initReadyChecks() {
	m.readyGates = make(map[string]*check.ReadyGate)
	newGate := func(name string) *check.ReadyGate {
		g := check.NewReadyGate(name)
		m.readyGates[name] = g
		m.checkHandler.AddNamedReadyCheck(g)
		return g
	}

	m.kvReady = newGate(SubsystemKV)
	m.sqliteReady = newGate(SubsystemSQLite)
	m.engineReady = newGate(SubsystemEngine)
	m.replicationsReady = newGate(SubsystemReplications)
	m.queryReady = newGate(SubsystemQuery)
	m.tasksReady = newGate(SubsystemTasks)
	m.schedulerReady = newGate(SubsystemTaskScheduler)

	m.startupProgress = run.NewStartupProgressLogger(
		SubsystemShards,
		m.log.With(zap.String("service", "startup-progress")))
	m.checkHandler.AddNamedReadyCheck(m.startupProgress.ReadyChecker())
	m.checkHandler.AddNamedHealthCheck(m.startupProgress.HealthChecker())
}

// failSubsystem records err as the initialization failure of the subsystem
// named name and returns err unchanged, for the caller to return from run.
//
// It logs msg — the message the call site used to log itself, so log-based
// alerting keeps matching — with the subsystem name attached, and publishes
// "msg: err" as the message of a /health check named for the failing phase.
// The returned error is not wrapped, so influxd's exit message is unchanged.
// Any fields the call site logged alongside the error are passed through.
//
// On /ready, a subsystem that owns a ReadyGate has that gate latched, so the
// entry already registered for it carries the reason instead of a bare
// check.MsgNotReady. A subsystem with no gate — one that runs after every gate
// has fired — gets a failing /ready check registered for it, which is what
// stops /ready reporting "ready" through a late startup failure.
//
// The no-duplicate-names invariant: every subsystem that registers a health
// check of its own does so only after it is fully up (bolt, sqlite, query,
// task-scheduler, influxql), and every error site for that subsystem precedes
// that registration, so a subsystem's failure check and its normal check are
// mutually exclusive by construction. check.Check holds health checks in an
// append-only slice with no name map: nothing but this ordering enforces it.
func (m *Launcher) failSubsystem(name, msg string, err error, fields ...zap.Field) error {
	m.log.Error(msg, append([]zap.Field{zap.String("subsystem", name), zap.Error(err)}, fields...)...)

	if m.failedSubsystem == "" {
		m.failedSubsystem = name
	}

	// The checkers below are read from an HTTP handler long after this call
	// site returned. Build the detail with fmt.Errorf so its message is
	// precomputed and check.Error can never call Error() on a moving target.
	detail := fmt.Errorf("%s: %w", msg, err)

	// m.checkHandler does not exist until partway into run. The sites that
	// precede it also precede runHTTP, so there is no listener to observe
	// what they would have registered.
	if m.checkHandler == nil {
		return err
	}

	m.checkHandler.AddNamedHealthCheck(check.Named(name, check.ErrCheck(func() error { return detail })))
	if gate, ok := m.readyGates[name]; ok {
		gate.Fail(detail)
	} else {
		m.checkHandler.AddNamedReadyCheck(check.Named(name, check.ErrCheck(func() error { return detail })))
	}
	return err
}

// failUnreachedGates latches every ready gate that never fired, so /ready
// reports that the phase was never reached rather than a bare
// check.MsgNotReady. Call it only when run is returning an error: that is the
// point at which "has not become ready yet" and "will never become ready"
// stop being the same statement, and check.MsgNotReady only ever meant the
// first one.
//
// It exists because the gate that failed is not usually the only gate left
// hanging. A failure at the SQL migrations latches sqlite and leaves engine,
// query, tasks and task-scheduler all reading "not ready" forever, which
// reads as a server still working through startup. Afterwards exactly one
// entry carries a reason, the ones that came up still say so, and the rest say
// they never got their turn.
//
// A gate reporting StatusPass is skipped. Its subsystem genuinely started, and
// ReadyGate.Fail outranks Ready, so sweeping it would report a working
// subsystem as failed. The gate carrying the real reason needs no such guard:
// Fail keeps the first error, so it survives this regardless of ordering.
//
// m.startupProgress is not a ReadyGate and is not swept, so on a failure
// before engine.Open the shards entry goes on reporting shard-loading
// progress. See the startup failure checks section of HEALTH_READY.md.
func (m *Launcher) failUnreachedGates(ctx context.Context) {
	// m.failedSubsystem is empty when a failure reached no attribution at all;
	// the caller has already warned about that, and naming no phase is better
	// than naming an empty one.
	notReached := errors.New("not reached: startup failed")
	if m.failedSubsystem != "" {
		notReached = fmt.Errorf("not reached: startup failed at %s", m.failedSubsystem)
	}

	// Ranging a nil map is a no-op, which covers the sites that fail before
	// initReadyChecks has run.
	for _, gate := range m.readyGates {
		if gate.Check(ctx).Status() == check.StatusPass {
			continue
		}
		gate.Fail(notReached)
	}
}

func (m *Launcher) run(ctx context.Context, opts *InfluxdOpts) (err error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Every failure path below is expected to go through failSubsystem, so
	// that /health and /ready name the phase that failed. A return that skips
	// it leaves the endpoints saying nothing useful; say so in the log rather
	// than let it pass silently.
	//
	// This is also where the gates that never fired are closed out. It runs on
	// every failure path there is, including any added later, which per-site
	// calls would not.
	defer func() {
		if err == nil {
			return
		}
		if m.failedSubsystem == "" {
			m.log.Warn("Startup failed without subsystem attribution", zap.Error(err))
		}
		m.failUnreachedGates(ctx)
	}()

	ctx, m.cancel = context.WithCancel(ctx)
	m.doneChan = ctx.Done()

	info := platform.GetBuildInfo()
	m.log.Info("Welcome to InfluxDB",
		zap.String("version", info.Version),
		zap.String("commit", info.Commit),
		zap.String("build_date", info.Date),
		zap.String("log_level", opts.LogLevel.String()),
	)
	m.initTracing(opts)

	if p := opts.Viper.ConfigFileUsed(); p != "" {
		m.log.Debug("loaded config file", zap.String("path", p))
	}

	if opts.NatsPort != 0 {
		m.log.Warn("nats-port argument is deprecated and unused")
	}

	if opts.NatsMaxPayloadBytes != 0 {
		m.log.Warn("nats-max-payload-bytes argument is deprecated and unused")
	}

	// Parse feature flags.
	// These flags can be used to modify the remaining setup logic in this method.
	// They will also be injected into the contexts of incoming HTTP requests at runtime,
	// for use in modifying behavior there.
	if m.flagger == nil {
		m.flagger = feature.DefaultFlagger()
		if len(opts.FeatureFlags) > 0 {
			f, err := overrideflagger.Make(opts.FeatureFlags, feature.ByKey)
			if err != nil {
				return m.failSubsystem(SubsystemFlagger, "Failed to configure feature flag overrides", err,
					zap.Any("overrides", opts.FeatureFlags))
			}
			m.log.Info("Running with feature flag overrides", zap.Any("overrides", opts.FeatureFlags))
			m.flagger = f
		}
	}

	if err := m.writePIDFile(opts.PIDFile, opts.OverwritePIDFile); err != nil {
		// The wrap is what influxd prints, so it stays the returned error.
		return m.failSubsystem(SubsystemPIDFile, "Failed writing PID file",
			fmt.Errorf("error writing PIDFile %q: %w", opts.PIDFile, err))
	}

	// Bring up /health and /ready early so k8s probes have a responsive
	// endpoint while slower subsystems (meta stores, storage engine, tasks,
	// scheduler) finish initializing. Non-check requests return 503
	// "starting" until SetHandler is called at the end of construction.
	httpLogger := m.log.With(zap.String("service", "http"))
	m.checkHandler = http.NewHealthReadyHandler(httpLogger)
	if opts.HardeningEnabled {
		// Match the root handler so /health and /ready also carry the header.
		m.checkHandler.SetStrictTransportSecurity(opts.StrictTransportSecurityMaxAge)
	}
	// Fold --hardening-enabled's implications into the individual options,
	// which is what NewConfigHandler below reports and what everything past
	// this point reads. An option the operator set for themselves survives;
	// see applyHardeningImplications. The CLI has already done this in PreRunE
	// -- print-config needs it too, and shares the path -- so this call covers
	// callers that build an InfluxdOpts and invoke run directly, and is
	// otherwise a no-op.
	opts.applyHardeningImplications()
	if opts.HardeningEnabled && !opts.HealthAuthEnabled {
		// The operator asked for hardening and then supplied a value of their
		// own for health auth, so the implication stood down. That is
		// supported, and deliberately so -- the reduced bodies can break
		// monitoring that parses them -- but "I turned hardening on" is not the
		// same claim as "I know /health serves startup error text, filesystem
		// paths and vault addresses to anyone who can reach the port". Say so
		// once, at startup, rather than let the gap live only in a config file.
		m.log.Warn("Hardening is enabled but health auth is not: /health and /ready serve full check detail, including startup error messages, to unauthenticated callers",
			zap.String("flag", healthAuthEnabledFlag))
	}
	// Set before runHTTP so it is in place for the very first request.
	m.checkHandler.SetHealthAuthRequired(opts.HealthAuthEnabled)
	if opts.HealthAuthEnabled {
		// Bracket the window in the log: no credential can be resolved until
		// the authorization store opens, and an operator who sees a body with
		// no messages in it should be able to tell that phase apart from a
		// rejected credential. The matching line is at SetCredentialResolver.
		m.log.Info("Check detail on /health and /ready requires operator permissions; until the authorization store opens, both report check names and statuses without messages")
	}
	m.initReadyChecks()

	// Under NoTasks the tasks subsystem and scheduler never start; pre-fire
	// their gates so /ready does not block forever waiting on subsystems
	// that will never come up. The gates remain registered and refer to the
	// same fields the !NoTasks scheduler-init path fires later (see below),
	// so /ready output is uniform between modes.
	if opts.NoTasks {
		m.tasksReady.Ready()
		m.schedulerReady.Ready()
	}

	registerCloser, err := m.runHTTP(opts, m.checkHandler, httpLogger)
	// Register the HTTP server's shutdown closer last (deferred until run
	// returns) so that during Shutdown — which iterates closers in reverse —
	// the listener is closed before subsystems are torn down.
	defer registerCloser()
	if err != nil {
		return m.failSubsystem(SubsystemHTTPServer, "Failed starting HTTP server", err)
	}
	// A listener is bound, so a failure from here on has somewhere to be read
	// from; see holdForStartupError.
	m.setHTTPServing()

	m.reg = prom.NewRegistry(m.log.With(zap.String("service", "prom_registry")))
	m.reg.MustRegister(collectors.NewGoCollector())

	// Open the KV and SQL stores and migrate the KV store, firing m.kvReady.
	// The SQL migrations are deliberately not part of this call: everything
	// between here and SetCredentialResolver below runs with /health and /ready
	// unable to identify any caller, so the only work admitted into that gap is
	// work the resolver itself depends on.
	procID, err := m.openMetaStores(ctx, opts)
	if err != nil {
		// Attributed at the failing site inside openMetaStores, which knows
		// which of its phases failed.
		return err
	}

	// Surface KV liveness on /health. In-memory KV (testing mode) has no
	// meaningful failure surface; skip it. boltKV is registered via its own
	// NamedChecker impl; its CheckName was set to SubsystemKV at construction
	// (see bolt.WithCheckName), so no extra Named wrap is needed here.
	if boltKV, ok := m.kvStore.(*bolt.KVStore); ok {
		m.checkHandler.AddNamedHealthCheck(boltKV)
		// Credential resolution for /health and /ready reads this store, and a
		// bolt View cannot be cancelled, so the store's own prober-backed check
		// gates whether resolution is attempted at all. In-memory KV (testing)
		// leaves this unset, which is correct: it cannot wedge. Install it
		// before the resolver: the reverse order leaves a window in which
		// resolution is attempted with no wedged-store guard in place.
		m.checkHandler.SetAuthDependencyChecker(boltKV)
	}

	tenantStore := tenant.NewStore(m.kvStore)
	ts := tenant.NewSystem(tenantStore, m.log.With(zap.String("store", "new")), m.reg, opts.StrongPasswords, metric.WithSuffix("new"))

	var authSvc platform.AuthorizationService
	{
		hasherVariantName := authorization.DefaultHashVariantName // This value could come from opts in the future.
		authStoreLogger := m.log.With(zap.String("store", "auth"))
		authStore, err := authorization.NewStore(ctx, m.kvStore, opts.UseHashedTokens, authorization.WithAuthorizationHashVariantName(hasherVariantName), authorization.WithLogger(authStoreLogger))
		if err != nil {
			return m.failSubsystem(SubsystemAuthorization, "Failed creating new authorization store", err,
				zap.Bool("UseHashedTokens", opts.UseHashedTokens), zap.String("hasherVariant", hasherVariantName))
		}
		authSvc = authorization.NewService(authStore, ts)
	}

	// Give /health and /ready a token-only resolver here, the earliest point at
	// which any credential can be resolved: the authorization store reads the
	// migrated KV schema (its setup rejects a pre-migration bolt file outright)
	// and nothing else, so it cannot be built sooner and must not be built
	// later. Everything slow in startup — the SQL migrations just below,
	// engine.Open, shard loading — now happens with an operator able to read
	// check detail over HTTP. Sessions are not wired until much later; the full
	// resolver replaces this one once they are.
	//
	// KEEP THIS IMMEDIATELY AFTER openMetaStores. Anything inserted above it
	// widens the window in which no caller, operator included, can see more
	// than check names and statuses.
	m.checkHandler.SetCredentialResolver(
		newHealthCredentialResolver(httpLogger, authSvc, ts.UndecoratedUserService(), nil))
	if opts.HealthAuthEnabled {
		m.log.Info("Check detail on /health and /ready now gated on operator permissions",
			zap.String("credentials", "token"))
	}

	// Migrate the SQL store, firing m.sqliteReady. Registering its health check
	// stays on this side of the migrations: the checker pings the database, and
	// a ping that times out under migration load would flip /health to 503 and
	// invite an orchestrator to restart a server that is migrating correctly.
	if err := m.migrateSQLStore(ctx, opts); err != nil {
		// Attributed inside migrateSQLStore.
		return err
	}
	m.checkHandler.AddNamedHealthCheck(m.sqlStore)
	m.reg.MustRegister(infprom.NewInfluxCollector(procID, info))

	serviceConfig := kv.ServiceConfig{
		FluxLanguageService: fluxlang.DefaultService,
	}

	m.kvService = kv.NewService(m.log.With(zap.String("store", "kv")), m.kvStore, ts, serviceConfig)
	ts.Apply(tenant.WithTaskService(m.kvService))

	var (
		opLogSvc                                              = tenant.NewOpLogService(m.kvStore, m.kvService)
		userLogSvc   platform.UserOperationLogService         = opLogSvc
		bucketLogSvc platform.BucketOperationLogService       = opLogSvc
		orgLogSvc    platform.OrganizationOperationLogService = opLogSvc
	)
	var (
		variableSvc      platform.VariableService           = m.kvService
		sourceSvc        platform.SourceService             = m.kvService
		scraperTargetSvc platform.ScraperTargetStoreService = m.kvService
	)

	secretStore, err := secret.NewStore(m.kvStore)
	if err != nil {
		return m.failSubsystem(SubsystemSecrets, "Failed creating new secret store", err)
	}

	var secretSvc platform.SecretService = secret.NewMetricService(m.reg, secret.NewLogger(m.log.With(zap.String("service", "secret")), secret.NewService(secretStore)))

	switch opts.SecretStore {
	case "bolt":
		// If it is bolt, then we already set it above.
	case "vault":
		// The vault secret service is configured using the standard vault environment variables.
		// https://www.vaultproject.io/docs/commands/index.html#environment-variables
		svc, err := vault.NewSecretService(vault.WithConfig(opts.VaultConfig))
		if err != nil {
			return m.failSubsystem(SubsystemSecrets, "Failed initializing vault secret service", err)
		}
		secretSvc = svc
	default:
		err := fmt.Errorf("unknown secret service %q, expected \"bolt\" or \"vault\"", opts.SecretStore)
		return m.failSubsystem(SubsystemSecrets, "Failed setting secret service", err)
	}

	metaClient := meta.NewClient(meta.NewConfig(), m.kvStore)
	if err := metaClient.Open(); err != nil {
		return m.failSubsystem(SubsystemMetaClient, "Failed to open meta client", err)
	}

	if opts.Testing {
		// the testing engine will write/read into a temporary directory
		engine := NewTemporaryEngine(
			opts.StorageConfig,
			storage.WithMetaClient(metaClient),
		)
		m.flushers = append(m.flushers, engine)
		m.engine = engine
	} else {
		// check for 2.x data / state from a prior 2.x. The message stays
		// neutral: checkForPriorVersion returns either a bucket-read failure
		// or an incompatible-version error, and it logs which one itself.
		if err := checkForPriorVersion(ctx, m.log, opts.BoltPath, opts.EnginePath, ts.BucketService, metaClient); err != nil {
			return m.failSubsystem(SubsystemEngine, "Prior version check failed", err)
		}

		m.engine = storage.NewEngine(
			opts.EnginePath,
			opts.StorageConfig,
			storage.WithMetricsDisabled(opts.MetricsDisabled),
			storage.WithMetaClient(metaClient),
		)
	}
	m.engine.WithLogger(m.log)
	m.engine.WithStartupMetrics(m.startupProgress)
	err = m.engine.Open(ctx)
	// Finish startup progress whether Open succeeded or failed so /ready
	// stops reporting a stale percentage. On failure the shards check
	// latches into a terminal Fail that surfaces the error.
	m.startupProgress.Finish(err)
	if err != nil {
		return m.failSubsystem(SubsystemEngine, "Failed to open engine", err)
	}
	m.engineReady.Ready()
	m.closers = append(m.closers, labeledCloser{
		label: SubsystemEngine,
		closer: func(context.Context) error {
			m.engineReady.Unready()
			return m.engine.Close()
		},
	})
	// The Engine's metrics must be registered after it opens.
	m.reg.MustRegister(m.engine.PrometheusCollectors()...)

	var (
		deleteService  platform.DeleteService  = m.engine
		pointsWriter   storage.PointsWriter    = m.engine
		backupService  platform.BackupService  = m.engine
		restoreService platform.RestoreService = m.engine
	)

	remotesSvc := remotes.NewService(m.sqlStore)
	remotesServer := remotesTransport.NewInstrumentedRemotesHandler(
		m.log.With(zap.String("handler", "remotes")), m.reg, m.kvStore, remotesSvc)

	replicationSvc, replicationsMetrics := replications.NewService(m.sqlStore, ts, pointsWriter, m.log.With(zap.String("service", "replications")), opts.EnginePath, opts.InstanceID)
	replicationServer := replicationTransport.NewInstrumentedReplicationHandler(
		m.log.With(zap.String("handler", "replications")), m.reg, m.kvStore, replicationSvc)
	ts.BucketService = replications.NewBucketService(
		m.log.With(zap.String("service", "replication_buckets")), ts.BucketService, replicationSvc)

	m.reg.MustRegister(replicationsMetrics.PrometheusCollectors()...)

	if err = replicationSvc.Open(ctx); err != nil {
		return m.failSubsystem(SubsystemReplications, "Failed to open replications service", err)
	}
	m.replicationsReady.Ready()
	m.closers = append(m.closers, labeledCloser{
		label: SubsystemReplications,
		closer: func(context.Context) error {
			m.replicationsReady.Unready()
			return replicationSvc.Close()
		},
	})

	pointsWriter = replicationSvc

	// When --hardening-enabled, use an HTTP IP validator that restricts
	// flux and pkger HTTP requests to private addressess.
	var urlValidator url.Validator
	if opts.HardeningEnabled {
		urlValidator = url.PrivateIPValidator{}
	} else {
		urlValidator = url.PassValidator{}
	}

	deps, err := influxdb.NewDependencies(
		storageflux.NewReader(storage2.NewStore(m.engine.TSDBStore(), m.engine.MetaClient())),
		pointsWriter,
		authorizer.NewBucketService(ts.BucketService),
		authorizer.NewOrgService(ts.OrganizationService),
		authorizer.NewSecretService(secretSvc),
		nil,
		influxdb.WithURLValidator(urlValidator),
	)
	if err != nil {
		return m.failSubsystem(SubsystemQuery, "Failed to get query controller dependencies", err)
	}

	dependencyList := []flux.Dependency{deps}
	if opts.Testing {
		dependencyList = append(dependencyList, executetest.NewDefaultTestFlagger())
		dependencyList = append(dependencyList, testing.FrameworkConfig{})
	}

	m.queryController, err = control.New(control.Config{
		ConcurrencyQuota:                opts.ConcurrencyQuota,
		InitialMemoryBytesQuotaPerQuery: int64(opts.InitialMemoryBytesQuotaPerQuery),
		MemoryBytesQuotaPerQuery:        int64(opts.MemoryBytesQuotaPerQuery),
		MaxMemoryBytes:                  int64(opts.MaxMemoryBytes),
		QueueSize:                       opts.QueueSize,
		ExecutorDependencies:            dependencyList,
		FluxLogEnabled:                  opts.FluxLogEnabled,
	}, m.log.With(zap.String("service", "storage-reads")))
	if err != nil {
		return m.failSubsystem(SubsystemQuery, "Failed to create query controller", err)
	}
	m.queryReady.Ready()
	m.closers = append(m.closers, labeledCloser{
		label: SubsystemQuery,
		closer: func(ctx context.Context) error {
			m.queryReady.Unready()
			return m.queryController.Shutdown(ctx)
		},
	})

	m.reg.MustRegister(m.queryController.PrometheusCollectors()...)

	var storageQueryService = readservice.NewProxyQueryService(m.queryController)
	m.checkHandler.AddNamedHealthCheck(check.Named(SubsystemQuery, storageQueryService))
	var taskSvc taskmodel.TaskService
	{
		// create the task stack
		combinedTaskService := taskbackend.NewAnalyticalStorage(
			m.log.With(zap.String("service", "task-analytical-store")),
			m.kvService,
			ts.BucketService,
			m.kvService,
			pointsWriter,
			query.QueryServiceBridge{AsyncQueryService: m.queryController},
		)

		executor, executorMetrics := executor.NewExecutor(
			m.log.With(zap.String("service", "task-executor")),
			query.QueryServiceBridge{AsyncQueryService: m.queryController},
			ts.UserService,
			combinedTaskService,
			combinedTaskService,
			executor.WithFlagger(m.flagger),
		)
		err = executor.LoadExistingScheduleRuns(ctx)
		if err != nil {
			return m.failSubsystem(SubsystemTasks, "could not load existing scheduled runs", err)
		}
		m.executor = executor
		m.reg.MustRegister(executorMetrics.PrometheusCollectors()...)
		schLogger := m.log.With(zap.String("service", "task-scheduler"))

		var sch stoppingScheduler = &scheduler.NoopScheduler{}
		if !opts.NoTasks {
			var (
				sm      *scheduler.SchedulerMetrics
				err     error
				treeSch *scheduler.TreeScheduler
			)
			treeSch, sm, err = scheduler.NewScheduler(
				executor,
				taskbackend.NewSchedulableTaskService(m.kvService),
				scheduler.WithOnErrorFn(func(ctx context.Context, taskID scheduler.ID, scheduledAt time.Time, err error) {
					schLogger.Info(
						"error in scheduler run",
						zap.String("taskID", platform2.ID(taskID).String()),
						zap.Time("scheduledAt", scheduledAt),
						zap.Error(err))
				}),
			)
			// Assign only after the error check: on failure treeSch is a nil
			// *TreeScheduler, and assigning it would leave sch a non-nil
			// stoppingScheduler wrapping nil.
			if err != nil {
				return m.failSubsystem(SubsystemTaskScheduler, "could not start task scheduler", err)
			}
			sch = treeSch
			m.closers = append(m.closers, labeledCloser{
				label: SubsystemTaskScheduler,
				closer: func(context.Context) error {
					m.schedulerReady.Unready()
					m.tasksReady.Unready()
					sch.Stop()
					return nil
				},
			})
			m.reg.MustRegister(sm.PrometheusCollectors()...)
			m.tasksReady.Ready()
			m.schedulerReady.Ready()
			// Register a pulse health check for the real tree scheduler.
			// NoopScheduler has no pulse to monitor; skip it.
			m.checkHandler.AddNamedHealthCheck(check.Named(SubsystemTaskScheduler, run.NewSchedulerPulseCheck(treeSch, run.DefaultSchedulerPulseThreshold)))
		}

		m.scheduler = sch
		coordLogger := m.log.With(zap.String("service", "task-coordinator"))
		taskCoord := coordinator.NewCoordinator(
			coordLogger,
			sch,
			executor)

		taskSvc = middleware.New(combinedTaskService, taskCoord)
		if err := taskbackend.TaskNotifyCoordinatorOfExisting(
			ctx,
			taskSvc,
			combinedTaskService,
			taskCoord,
			func(ctx context.Context, taskID platform2.ID, runID platform2.ID) error {
				_, err := executor.ResumeCurrentRun(ctx, taskID, runID)
				return err
			},
			coordLogger); err != nil {
			m.log.Error("Failed to resume existing tasks", zap.Error(err))
		}
	}

	dbrpSvc := dbrp.NewAuthorizedService(dbrp.NewService(ctx, authorizer.NewBucketService(ts.BucketService), m.kvStore))

	cm := iqlcontrol.NewControllerMetrics([]string{})
	m.reg.MustRegister(cm.PrometheusCollectors()...)

	mapper := &iqlcoordinator.LocalShardMapper{
		MetaClient: metaClient,
		TSDBStore:  m.engine.TSDBStore(),
		DBRP:       dbrpSvc,
	}

	m.log.Info("Configuring InfluxQL statement executor (zeros indicate unlimited).",
		zap.Int("max_select_point", opts.CoordinatorConfig.MaxSelectPointN),
		zap.Int("max_select_series", opts.CoordinatorConfig.MaxSelectSeriesN),
		zap.Int("max_select_buckets", opts.CoordinatorConfig.MaxSelectBucketsN))

	qe := iqlquery.NewExecutor(m.log, cm)
	influxqlProxy := iqlquery.NewProxyExecutor(m.log, qe)
	m.checkHandler.AddNamedHealthCheck(check.Named(SubsystemInfluxQL, influxqlProxy))
	se := &iqlcoordinator.StatementExecutor{
		MetaClient:        metaClient,
		TSDBStore:         m.engine.TSDBStore(),
		ShardMapper:       mapper,
		DBRP:              dbrpSvc,
		MaxSelectPointN:   opts.CoordinatorConfig.MaxSelectPointN,
		MaxSelectSeriesN:  opts.CoordinatorConfig.MaxSelectSeriesN,
		MaxSelectBucketsN: opts.CoordinatorConfig.MaxSelectBucketsN,
	}
	qe.StatementExecutor = se
	qe.StatementNormalizer = se

	var checkSvc platform.CheckService
	{
		coordinator := coordinator.NewCoordinator(m.log, m.scheduler, m.executor)
		checkSvc = checks.NewService(m.log.With(zap.String("svc", "checks")), m.kvStore, ts.OrganizationService, m.kvService)
		checkSvc = middleware.NewCheckService(checkSvc, m.kvService, coordinator)
	}

	var notificationEndpointSvc platform.NotificationEndpointService
	{
		notificationEndpointSvc = endpointservice.New(endpointservice.NewStore(m.kvStore), secretSvc)
	}

	var notificationRuleSvc platform.NotificationRuleStore
	{
		coordinator := coordinator.NewCoordinator(m.log, m.scheduler, m.executor)
		notificationRuleSvc, err = ruleservice.New(m.log, m.kvStore, m.kvService, ts.OrganizationService, notificationEndpointSvc)
		if err != nil {
			return m.failSubsystem(SubsystemNotificationRules, "Failed creating notification rule store", err)
		}

		// tasks service notification middleware which keeps task service up to date
		// with persisted changes to notification rules.
		notificationRuleSvc = middleware.NewNotificationRuleStore(notificationRuleSvc, m.kvService, coordinator)
	}

	var telegrafSvc platform.TelegrafConfigStore
	{
		telegrafSvc = telegrafservice.New(m.kvStore)
	}

	scraperScheduler, err := gather.NewScheduler(m.log.With(zap.String("service", "scraper")), 100, 10, scraperTargetSvc, pointsWriter, 10*time.Second)
	if err != nil {
		return m.failSubsystem(SubsystemScraper, "Failed to create scraper subscriber", err)
	}
	m.closers = append(m.closers, labeledCloser{
		label: SubsystemScraper,
		closer: func(ctx context.Context) error {
			scraperScheduler.Close()
			return nil
		},
	})

	var sessionSvc platform.SessionService
	// healthSessionSvc is the same session service without the metrics and
	// logging middleware, for the /health and /ready credential resolver; see
	// newHealthCredentialResolver. It must be the instance the decorated chain
	// wraps rather than a second one, since the session store is in-memory and
	// a separate instance would find no sessions at all.
	var healthSessionSvc platform.SessionService
	{
		healthSessionSvc = session.NewService(
			session.NewStorage(inmem.NewSessionStore()),
			ts.UserService,
			ts.UserResourceMappingService,
			authSvc,
			session.WithSessionLength(time.Duration(opts.SessionLength)*time.Minute),
		)
		sessionSvc = session.NewSessionMetrics(m.reg, healthSessionSvc)
		sessionSvc = session.NewSessionLogger(m.log.With(zap.String("service", "session")), sessionSvc)
	}

	var labelSvc platform.LabelService
	{
		labelsStore, err := label.NewStore(m.kvStore)
		if err != nil {
			return m.failSubsystem(SubsystemLabels, "Failed creating new labels store", err)
		}
		labelSvc = label.NewService(labelsStore)
	}

	ts.BucketService = storage.NewBucketService(m.log, ts.BucketService, m.engine)
	ts.BucketService = dbrp.NewBucketService(m.log, ts.BucketService, dbrpSvc)

	bucketManifestWriter := backup.NewBucketManifestWriter(ts, metaClient)
	bucketManifestWriter.WithLogger(m.log.With(zap.String("service", "bucket-manifest-writer")))

	onboardingLogger := m.log.With(zap.String("handler", "onboard"))
	onboardOpts := []tenant.OnboardServiceOptionFn{tenant.WithOnboardingLogger(onboardingLogger)}
	if opts.TestingAlwaysAllowSetup {
		onboardOpts = append(onboardOpts, tenant.WithAlwaysAllowInitialUser())
	}

	onboardSvc := tenant.NewOnboardService(ts, authSvc, onboardOpts...)                   // basic service
	onboardSvc = tenant.NewAuthedOnboardSvc(onboardSvc)                                   // with auth
	onboardSvc = tenant.NewOnboardingMetrics(m.reg, onboardSvc, metric.WithSuffix("new")) // with metrics
	onboardSvc = tenant.NewOnboardingLogger(onboardingLogger, onboardSvc)                 // with logging

	var (
		passwordV1 platform.PasswordsService
		authSvcV1  *authv1.Service
	)
	{
		authStore, err := authv1.NewStore(m.kvStore)
		if err != nil {
			return m.failSubsystem(SubsystemAuthorizationV1, "Failed creating new authorization store", err)
		}

		authSvcV1 = authv1.NewService(authStore, ts, authv1.WithPasswordChecking(opts.StrongPasswords))
		passwordV1 = authv1.NewCachingPasswordsService(authSvcV1)
	}

	var (
		dashboardSvc    platform.DashboardService
		dashboardLogSvc platform.DashboardOperationLogService
	)
	{
		dashboardService := dashboards.NewService(m.kvStore, m.kvService)
		dashboardSvc = dashboardService
		dashboardLogSvc = dashboardService
	}

	// resourceResolver is a deprecated type which combines the lookups
	// of multiple resources into one type, used to resolve the resources
	// associated org ID or name . It is a stop-gap while we move this
	// behaviour off of *kv.Service to aid in reducing the coupling on this type.
	resourceResolver := &resource.Resolver{
		AuthorizationFinder:        authSvc,
		BucketFinder:               ts.BucketService,
		OrganizationFinder:         ts.OrganizationService,
		DashboardFinder:            dashboardSvc,
		SourceFinder:               sourceSvc,
		TaskFinder:                 taskSvc,
		TelegrafConfigFinder:       telegrafSvc,
		VariableFinder:             variableSvc,
		TargetFinder:               scraperTargetSvc,
		CheckFinder:                checkSvc,
		NotificationEndpointFinder: notificationEndpointSvc,
		NotificationRuleFinder:     notificationRuleSvc,
	}

	errorHandler := kithttp.NewErrorHandler(m.log.With(zap.String("handler", "error_logger")))
	m.apibackend = &http.APIBackend{
		AssetsPath:           opts.AssetsPath,
		UIDisabled:           opts.UIDisabled,
		HTTPErrorHandler:     errorHandler,
		Logger:               m.log,
		FluxLogEnabled:       opts.FluxLogEnabled,
		SessionRenewDisabled: opts.SessionRenewDisabled,
		NewQueryService:      source.NewQueryService,
		PointsWriter: &storage.LoggingPointsWriter{
			Underlying:    pointsWriter,
			BucketFinder:  ts.BucketService,
			LogBucketName: platform.MonitoringSystemBucketName,
		},
		DeleteService:           deleteService,
		BackupService:           backupService,
		SqlBackupRestoreService: m.sqlStore,
		BucketManifestWriter:    bucketManifestWriter,
		RestoreService:          restoreService,
		AuthorizationService:    authSvc,
		AuthorizationV1Service:  authSvcV1,
		PasswordV1Service:       passwordV1,
		AuthorizerV1: &authv1.Authorizer{
			AuthV1:   authSvcV1,
			AuthV2:   authSvc,
			Comparer: passwordV1,
			User:     ts,
		},
		AlgoWProxy: &http.NoopProxyHandler{},
		// Wrap the BucketService in a storage backed one that will ensure deleted buckets are removed from the storage engine.
		BucketService:                   ts.BucketService,
		SessionService:                  sessionSvc,
		UserService:                     ts.UserService,
		OnboardingService:               onboardSvc,
		DBRPService:                     dbrpSvc,
		OrganizationService:             ts.OrganizationService,
		UserResourceMappingService:      ts.UserResourceMappingService,
		LabelService:                    labelSvc,
		DashboardService:                dashboardSvc,
		DashboardOperationLogService:    dashboardLogSvc,
		BucketOperationLogService:       bucketLogSvc,
		UserOperationLogService:         userLogSvc,
		OrganizationOperationLogService: orgLogSvc,
		SourceService:                   sourceSvc,
		VariableService:                 variableSvc,
		PasswordsService:                ts.PasswordsService,
		InfluxqldService:                influxqlProxy,
		FluxService:                     storageQueryService,
		FluxLanguageService:             fluxlang.DefaultService,
		TaskService:                     taskSvc,
		TelegrafService:                 telegrafSvc,
		NotificationRuleStore:           notificationRuleSvc,
		NotificationEndpointService:     notificationEndpointSvc,
		CheckService:                    checkSvc,
		ScraperTargetStoreService:       scraperTargetSvc,
		SecretService:                   secretSvc,
		LookupService:                   resourceResolver,
		DocumentService:                 m.kvService,
		OrgLookupService:                resourceResolver,
		WriteEventRecorder:              infprom.NewEventRecorder("write"),
		QueryEventRecorder:              infprom.NewEventRecorder("query"),
		Flagger:                         m.flagger,
		FlagsHandler:                    feature.NewFlagsHandler(errorHandler, feature.ByKey),
	}

	// Replace the token-only resolver installed during startup now that the
	// session service exists, so a UI session can read check detail too.
	m.checkHandler.SetCredentialResolver(
		newHealthCredentialResolver(httpLogger, authSvc, ts.UndecoratedUserService(), healthSessionSvc))

	m.reg.MustRegister(m.apibackend.PrometheusCollectors()...)

	authAgent := new(authorizer.AuthAgent)

	var pkgSVC pkger.SVC
	{
		b := m.apibackend
		authedOrgSVC := authorizer.NewOrgService(b.OrganizationService)
		authedUrmSVC := authorizer.NewURMService(b.OrgLookupService, b.UserResourceMappingService)
		pkgerLogger := m.log.With(zap.String("service", "pkger"))
		disableFileUrls := opts.HardeningEnabled || opts.TemplateFileUrlsDisabled
		pkgSVC = pkger.NewService(
			pkger.WithHTTPClient(pkger.NewDefaultHTTPClient(urlValidator)),
			pkger.WithFileUrlsDisabled(disableFileUrls),
			pkger.WithLogger(pkgerLogger),
			pkger.WithStore(pkger.NewStoreKV(m.kvStore)),
			pkger.WithBucketSVC(authorizer.NewBucketService(b.BucketService)),
			pkger.WithCheckSVC(authorizer.NewCheckService(b.CheckService, authedUrmSVC, authedOrgSVC)),
			pkger.WithDashboardSVC(authorizer.NewDashboardService(b.DashboardService)),
			pkger.WithLabelSVC(label.NewAuthedLabelService(labelSvc, b.OrgLookupService)),
			pkger.WithNotificationEndpointSVC(authorizer.NewNotificationEndpointService(b.NotificationEndpointService, authedUrmSVC, authedOrgSVC)),
			pkger.WithNotificationRuleSVC(authorizer.NewNotificationRuleStore(b.NotificationRuleStore, authedUrmSVC, authedOrgSVC)),
			pkger.WithOrganizationService(authorizer.NewOrgService(b.OrganizationService)),
			pkger.WithSecretSVC(authorizer.NewSecretService(b.SecretService)),
			pkger.WithTaskSVC(authorizer.NewTaskService(pkgerLogger, b.TaskService)),
			pkger.WithTelegrafSVC(authorizer.NewTelegrafConfigService(b.TelegrafService, b.UserResourceMappingService)),
			pkger.WithVariableSVC(authorizer.NewVariableService(b.VariableService)),
		)
		pkgSVC = pkger.MWTracing()(pkgSVC)
		pkgSVC = pkger.MWMetrics(m.reg)(pkgSVC)
		pkgSVC = pkger.MWLogging(pkgerLogger)(pkgSVC)
		pkgSVC = pkger.MWAuth(authAgent)(pkgSVC)
	}

	var stacksHTTPServer *pkger.HTTPServerStacks
	{
		tLogger := m.log.With(zap.String("handler", "stacks"))
		stacksHTTPServer = pkger.NewHTTPServerStacks(tLogger, pkgSVC)
	}

	var templatesHTTPServer *pkger.HTTPServerTemplates
	{
		tLogger := m.log.With(zap.String("handler", "templates"))
		templatesHTTPServer = pkger.NewHTTPServerTemplates(tLogger, pkgSVC, pkger.NewDefaultHTTPClient(urlValidator))
	}

	userHTTPServer := ts.NewUserHTTPHandler(m.log)
	meHTTPServer := ts.NewMeHTTPHandler(m.log)
	onboardHTTPServer := tenant.NewHTTPOnboardHandler(m.log, onboardSvc)

	// feature flagging for new labels service
	var labelHandler *label.LabelHandler
	{
		b := m.apibackend

		labelSvc = label.NewAuthedLabelService(labelSvc, b.OrgLookupService)
		labelSvc = label.NewLabelLogger(m.log.With(zap.String("handler", "labels")), labelSvc)
		labelSvc = label.NewLabelMetrics(m.reg, labelSvc)
		labelHandler = label.NewHTTPLabelHandler(m.log, labelSvc)
	}

	// feature flagging for new authorization service
	var authHTTPServer *authorization.AuthHandler
	{
		authLogger := m.log.With(zap.String("handler", "authorization"))

		var authService platform.AuthorizationService
		authService = authorization.NewAuthedAuthorizationService(authSvc, ts)
		authService = authorization.NewAuthMetrics(m.reg, authService)
		authService = authorization.NewAuthLogger(authLogger, authService)

		authHTTPServer = authorization.NewHTTPAuthHandler(m.log, authService, ts)
	}

	var v1AuthHTTPServer *authv1.AuthHandler
	{
		authLogger := m.log.With(zap.String("handler", "v1_authorization"))

		var authService platform.AuthorizationService
		authService = authorization.NewAuthedAuthorizationService(authSvcV1, ts)
		authService = authorization.NewAuthLogger(authLogger, authService)

		passService := authv1.NewAuthedPasswordService(authv1.AuthFinder(authSvcV1), passwordV1)
		v1AuthHTTPServer = authv1.NewHTTPAuthHandler(m.log, authService, passService, ts)
	}

	var sessionHTTPServer *session.SessionHandler
	{
		sessionHTTPServer = session.NewSessionHandler(m.log.With(zap.String("handler", "session")), sessionSvc, ts.UserService, ts.PasswordsService)
	}

	orgHTTPServer := ts.NewOrgHTTPHandler(m.log, secret.NewAuthedService(secretSvc))

	bucketHTTPServer := ts.NewBucketHTTPHandler(m.log, labelSvc)

	var dashboardServer *dashboardTransport.DashboardHandler
	{
		urmHandler := tenant.NewURMHandler(
			m.log.With(zap.String("handler", "urm")),
			platform.DashboardsResourceType,
			"id",
			ts.UserService,
			tenant.NewAuthedURMService(ts.OrganizationService, ts.UserResourceMappingService),
		)

		labelHandler := label.NewHTTPEmbeddedHandler(
			m.log.With(zap.String("handler", "label")),
			platform.DashboardsResourceType,
			labelSvc,
		)

		dashboardServer = dashboardTransport.NewDashboardHandler(
			m.log.With(zap.String("handler", "dashboards")),
			authorizer.NewDashboardService(dashboardSvc),
			labelSvc,
			ts.UserService,
			ts.OrganizationService,
			urmHandler,
			labelHandler,
		)
	}

	notebookSvc := notebooks.NewService(m.sqlStore)
	notebookServer := notebookTransport.NewNotebookHandler(
		m.log.With(zap.String("handler", "notebooks")),
		authorizer.NewNotebookService(
			notebooks.NewLoggingService(
				m.log.With(zap.String("service", "notebooks")),
				notebooks.NewMetricCollectingService(m.reg, notebookSvc),
			),
		),
	)

	annotationSvc := annotations.NewService(m.sqlStore)
	annotationServer := annotationTransport.NewAnnotationHandler(
		m.log.With(zap.String("handler", "annotations")),
		authorizer.NewAnnotationService(
			annotations.NewLoggingService(
				m.log.With(zap.String("service", "annotations")),
				annotations.NewMetricCollectingService(m.reg, annotationSvc),
			),
		),
	)

	configHandler, err := http.NewConfigHandler(m.log.With(zap.String("handler", "config")), opts.BindCliOpts())
	if err != nil {
		return m.failSubsystem(SubsystemAPI, "Failed creating config handler", err)
	}

	platformHandler := http.NewPlatformHandler(
		m.apibackend,
		http.WithResourceHandler(stacksHTTPServer),
		http.WithResourceHandler(templatesHTTPServer),
		http.WithResourceHandler(onboardHTTPServer),
		http.WithResourceHandler(authHTTPServer),
		http.WithResourceHandler(labelHandler),
		http.WithResourceHandler(sessionHTTPServer.SignInResourceHandler()),
		http.WithResourceHandler(sessionHTTPServer.SignOutResourceHandler()),
		http.WithResourceHandler(userHTTPServer),
		http.WithResourceHandler(meHTTPServer),
		http.WithResourceHandler(orgHTTPServer),
		http.WithResourceHandler(bucketHTTPServer),
		http.WithResourceHandler(v1AuthHTTPServer),
		http.WithResourceHandler(dashboardServer),
		http.WithResourceHandler(notebookServer),
		http.WithResourceHandler(annotationServer),
		http.WithResourceHandler(remotesServer),
		http.WithResourceHandler(replicationServer),
		http.WithResourceHandler(configHandler),
	)

	rootHandlerOpts := []http.HandlerOptFn{
		http.WithLog(httpLogger),
		http.WithAPIHandler(platformHandler),
		http.WithPprofEnabled(!opts.ProfilingDisabled),
		http.WithMetrics(m.reg, !opts.MetricsDisabled),
	}
	if opts.HardeningEnabled {
		rootHandlerOpts = append(rootHandlerOpts, http.WithStrictTransportSecurity(opts.StrictTransportSecurityMaxAge))
	}

	var httpHandler nethttp.Handler = http.NewRootHandler("platform", rootHandlerOpts...)

	if opts.LogLevel == zap.DebugLevel {
		httpHandler = http.LoggingMW(httpLogger)(httpHandler)
	}
	// If we are in testing mode we allow all data to be flushed and removed.
	if opts.Testing {
		httpHandler = http.Debug(ctx, httpHandler, m.flushers, onboardSvc)
	}

	if !opts.ReportingDisabled {
		m.runReporter(ctx)
	}
	m.checkHandler.SetHandler(httpHandler)

	return nil
}

// initTracing sets up the global tracer for the influxd process.
// Any errors encountered during setup are logged, but don't crash the process.
func (m *Launcher) initTracing(opts *InfluxdOpts) {
	switch opts.TracingType {
	case LogTracing:
		m.log.Info("Tracing via zap logging")
		opentracing.SetGlobalTracer(pzap.NewTracer(m.log, snowflake.NewIDGenerator()))

	case JaegerTracing:
		m.log.Info("Tracing via Jaeger")
		cfg, err := jaegerconfig.FromEnv()
		if err != nil {
			m.log.Error("Failed to get Jaeger client config from environment variables", zap.Error(err))
			return
		}
		tracer, closer, err := cfg.NewTracer()
		if err != nil {
			m.log.Error("Failed to instantiate Jaeger tracer", zap.Error(err))
			return
		}
		m.closers = append(m.closers, labeledCloser{
			label: SubsystemJaeger,
			closer: func(context.Context) error {
				return closer.Close()
			},
		})
		opentracing.SetGlobalTracer(tracer)
	}
}

// writePIDFile will write the process ID to pidFilename and register a cleanup function to delete it during
// shutdown. If pidFilename is empty, then no PID file is written and no cleanup function is registered.
// If pidFilename already exists and overwrite is false, then pidFilename is not overwritten and a
// ErrPIDFileExists error is returned. If pidFilename already exists and overwrite is true, then pidFilename
// will be overwritten but a warning will be logged.
func (m *Launcher) writePIDFile(pidFilename string, overwrite bool) error {
	if pidFilename == "" {
		return nil
	}

	// Create directory to PIDfile if needed.
	if err := os.MkdirAll(filepath.Dir(pidFilename), 0777); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}

	// Write PID to file, but don't clobber an existing PID file.
	pidBytes := []byte(strconv.Itoa(os.Getpid()))
	pidMode := fs.FileMode(0666)
	openFlags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	pidFile, err := os.OpenFile(pidFilename, openFlags|os.O_EXCL, pidMode)
	if err != nil {
		if !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("open file: %w", err)
		}
		if !overwrite {
			return ErrPIDFileExists
		} else {
			m.log.Warn("PID file already exists, attempting to overwrite", zap.String("pidFile", pidFilename))
			pidFile, err = os.OpenFile(pidFilename, openFlags, pidMode)
			if err != nil {
				return fmt.Errorf("overwrite file: %w", err)
			}
		}
	}
	_, writeErr := pidFile.Write(pidBytes) // Contract says Write must return an error if count < len(pidBytes).
	closeErr := pidFile.Close()            // always close the file
	if writeErr != nil || closeErr != nil {
		var errs []error
		if writeErr != nil {
			errs = append(errs, fmt.Errorf("write file: %w", writeErr))
		}
		if closeErr != nil {
			errs = append(errs, fmt.Errorf("close file: %w", closeErr))
		}

		// Let's make sure we don't leave a PID file behind on error.
		removeErr := os.Remove(pidFilename)
		if removeErr != nil {
			errs = append(errs, fmt.Errorf("remove file: %w", removeErr))
		}

		return errors.Join(errs...)
	}

	// Add a cleanup function.
	m.closers = append(m.closers, labeledCloser{
		label: SubsystemPIDFile,
		closer: func(context.Context) error {
			if err := os.Remove(pidFilename); err != nil {
				return fmt.Errorf("removing PID file %q: %w", pidFilename, err)
			}
			return nil
		},
	})

	return nil
}

// openMetaStores opens the embedded DBs used to store metadata about influxd resources, migrating the KV
// store to the latest schema expected by the server. The SQL store is opened here but migrated by
// migrateSQLStore, which the caller invokes once the /health credential resolver is installed.
// On success, a unique ID is returned to be used as an identifier for the influxd instance in telemetry.
func (m *Launcher) openMetaStores(ctx context.Context, opts *InfluxdOpts) (string, error) {
	type flushableKVStore interface {
		kv.SchemaStore
		http.Flusher
	}
	var kvStore flushableKVStore
	var sqlStore *sqlite.SqlStore

	var procID string
	var err error
	switch opts.StoreType {
	case BoltStore:
		m.log.Warn("Using --store=bolt is deprecated. Use --store=disk instead.")
		fallthrough
	case DiskStore:
		boltClient := bolt.NewClient(m.log.With(zap.String("service", "bolt")))
		boltClient.Path = opts.BoltPath

		if err := boltClient.Open(ctx); err != nil {
			return "", m.failSubsystem(SubsystemKV, "Failed opening bolt", err)
		}
		m.reg.MustRegister(boltClient)
		procID = boltClient.ID().String()

		boltKV := bolt.NewKVStore(m.log.With(zap.String("service", SubsystemKV)), opts.BoltPath, bolt.WithCheckName(SubsystemKV))
		boltKV.WithDB(boltClient.DB())
		kvStore = boltKV
		// boltKV's prober shares the *bolt.DB owned by boltClient; stop the
		// prober before closing the DB so it does not see a torn handle.
		m.closers = append(m.closers, labeledCloser{
			label: SubsystemKV,
			closer: func(context.Context) error {
				m.kvReady.Unready()
				boltKV.StopProber()
				return boltClient.Close()
			},
		})

		// If a sqlite-path is not specified, store sqlite db in the same directory as bolt with the default filename.
		if opts.SqLitePath == "" {
			opts.SqLitePath = filepath.Join(filepath.Dir(opts.BoltPath), sqlite.DefaultFilename)
		}
		sqlStore, err = sqlite.NewSqlStore(opts.SqLitePath, m.log.With(zap.String("service", "sqlite")), sqlite.WithCheckName(SubsystemSQLite))
		if err != nil {
			return "", m.failSubsystem(SubsystemSQLite, "Failed opening sqlite store", err)
		}

	case MemoryStore:
		kvStore = inmem.NewKVStore()
		sqlStore, err = sqlite.NewSqlStore(sqlite.InmemPath, m.log.With(zap.String("service", "sqlite")), sqlite.WithCheckName(SubsystemSQLite))
		if err != nil {
			return "", m.failSubsystem(SubsystemSQLite, "Failed opening sqlite store", err)
		}

	default:
		err := fmt.Errorf("unknown store type %s; expected disk or memory", opts.StoreType)
		return "", m.failSubsystem(SubsystemMetaStore, "Failed opening metadata store", err)
	}

	m.closers = append(m.closers, labeledCloser{
		label: SubsystemSQLite,
		closer: func(context.Context) error {
			m.sqliteReady.Unready()
			return sqlStore.Close()
		},
	})
	if opts.Testing {
		m.flushers = append(m.flushers, kvStore, sqlStore)
	}

	// Apply migrations to the KV metadata store. The SQL store is migrated
	// separately, by migrateSQLStore, so that the /health credential resolver
	// can be installed between the two.
	kvMigrator, err := migration.NewMigrator(
		m.log.With(zap.String("service", "KV migrations")),
		kvStore,
		all.Migrations[:]...,
	)
	if err != nil {
		return "", m.failSubsystem(SubsystemMetaStore, "Failed to initialize kv migrator", err)
	}
	if persistentStore(opts) {
		kvMigrator.SetBackupPath(preMigrationBackupPath(opts.BoltPath))
	}
	if err := kvMigrator.Up(ctx); err != nil {
		return "", m.failSubsystem(SubsystemMetaStore, "Failed to apply KV migrations", err)
	}
	m.kvReady.Ready()

	m.kvStore = kvStore
	m.sqlStore = sqlStore
	return procID, nil
}

// migrateSQLStore applies the SQL migrations to the store openMetaStores
// opened, firing m.sqliteReady on success.
//
// It is split out of openMetaStores because these migrations are slow on a
// large database and depend on nothing that /health's credential resolver needs
// — the authorization store the resolver reads lives in KV — so the resolver is
// installed first and this window is served with check detail available to an
// operator rather than to nobody. See Launcher.run.
func (m *Launcher) migrateSQLStore(ctx context.Context, opts *InfluxdOpts) error {
	sqlMigrator := sqlite.NewMigrator(m.sqlStore, m.log.With(zap.String("service", "SQL migrations")))
	if persistentStore(opts) {
		sqlMigrator.SetBackupPath(preMigrationBackupPath(opts.SqLitePath))
	}
	if err := sqlMigrator.Up(ctx, sqliteMigrations.AllUp); err != nil {
		return m.failSubsystem(SubsystemSQLite, "Failed to apply SQL migrations", err)
	}
	m.sqliteReady.Ready()
	return nil
}

// persistentStore reports whether the metadata stores are on disk, and so
// whether a pre-migration backup is worth taking for rollback.
func persistentStore(opts *InfluxdOpts) bool {
	return opts.StoreType == DiskStore || opts.StoreType == BoltStore
}

// preMigrationBackupPath names the rollback copy taken before a metadata store
// is migrated. Shared by the KV and SQL migrators so the two backups stay
// identically named.
func preMigrationBackupPath(storePath string) string {
	return fmt.Sprintf("%s.pre-%s-upgrade.backup", storePath, platform.GetBuildInfo().Version)
}

// runHTTP configures and launches a listener for incoming HTTP(S) requests.
// The listener is run in a separate goroutine. If it fails to start up, it
// will cancel the launcher. Returns a closer func to be called for shutdown.
func (m *Launcher) runHTTP(opts *InfluxdOpts, handler nethttp.Handler, httpLogger *zap.Logger) (func(), error) {
	log := m.log.With(zap.String("service", "tcp-listener"))

	httpServer := &nethttp.Server{
		Addr:              opts.HttpBindAddress,
		Handler:           handler,
		ReadHeaderTimeout: opts.HttpReadHeaderTimeout,
		ReadTimeout:       opts.HttpReadTimeout,
		WriteTimeout:      opts.HttpWriteTimeout,
		IdleTimeout:       opts.HttpIdleTimeout,
		ErrorLog:          zap.NewStdLog(httpLogger),
	}
	registerCloser := func() {
		m.closers = append(m.closers, labeledCloser{
			label:  SubsystemHTTPServer,
			closer: httpServer.Shutdown,
		})
	}

	ln, err := net.Listen("tcp", opts.HttpBindAddress)
	if err != nil {
		log.Error("Failed to set up TCP listener", zap.String("addr", opts.HttpBindAddress), zap.Error(err))
		return registerCloser, err
	}
	if addr, ok := ln.Addr().(*net.TCPAddr); ok {
		m.httpPort = addr.Port
	}
	m.wg.Add(1)

	m.tlsEnabled = opts.HttpTLSCert != "" && opts.HttpTLSKey != ""
	if !m.tlsEnabled {
		if opts.HttpTLSCert != "" || opts.HttpTLSKey != "" {
			log.Warn("TLS requires specifying both cert and key, falling back to HTTP")
		}

		go func(log *zap.Logger) {
			defer m.wg.Done()
			log.Info("Listening", zap.String("transport", "http"), zap.String("addr", opts.HttpBindAddress), zap.Int("port", m.httpPort))

			if err := httpServer.Serve(ln); err != nethttp.ErrServerClosed {
				log.Error("Failed to serve HTTP", zap.Error(err))
				m.cancel()
			}
			log.Info("Stopping")
		}(log)

		return registerCloser, nil
	}

	// Cleanup for paths that fail after Listen but before Serve. The
	// registered closer (httpServer.Shutdown) only closes listeners that
	// the server tracks via Serve/ServeTLS, so it cannot release ln here;
	// and m.wg.Add(1) above has no goroutine to decrement it.
	cleanupBeforeServe := func() {
		if cerr := ln.Close(); cerr != nil {
			log.Warn("Failed to close TCP listener after error", zap.Error(cerr))
		}
		m.wg.Done()
	}

	if _, err = tls.LoadX509KeyPair(opts.HttpTLSCert, opts.HttpTLSKey); err != nil {
		log.Error("Failed to load x509 key pair", zap.String("cert-path", opts.HttpTLSCert), zap.String("key-path", opts.HttpTLSKey))
		cleanupBeforeServe()
		return registerCloser, err
	}

	var tlsMinVersion uint16
	var useStrictCiphers = opts.HttpTLSStrictCiphers
	switch opts.HttpTLSMinVersion {
	case "1.0":
		log.Warn("Setting the minimum version of TLS to 1.0 - this is discouraged. Please use 1.2 or 1.3")
		tlsMinVersion = tls.VersionTLS10
	case "1.1":
		log.Warn("Setting the minimum version of TLS to 1.1 - this is discouraged. Please use 1.2 or 1.3")
		tlsMinVersion = tls.VersionTLS11
	case "1.2":
		tlsMinVersion = tls.VersionTLS12
	case "1.3":
		if useStrictCiphers {
			log.Warn("TLS version 1.3 does not support configuring strict ciphers")
			useStrictCiphers = false
		}
		tlsMinVersion = tls.VersionTLS13
	default:
		cleanupBeforeServe()
		return registerCloser, fmt.Errorf("unsupported TLS version: %s", opts.HttpTLSMinVersion)
	}

	// nil uses the default cipher suite
	var cipherConfig []uint16 = nil
	if useStrictCiphers {
		// See https://ssl-config.mozilla.org/#server=go&version=1.14.4&config=intermediate&guideline=5.6
		cipherConfig = []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
		}
	}

	httpServer.TLSConfig = &tls.Config{
		CurvePreferences:         []tls.CurveID{tls.CurveP521, tls.CurveP384, tls.CurveP256},
		PreferServerCipherSuites: !useStrictCiphers,
		MinVersion:               tlsMinVersion,
		CipherSuites:             cipherConfig,
	}

	go func(log *zap.Logger) {
		defer m.wg.Done()
		log.Info("Listening", zap.String("transport", "https"), zap.String("addr", opts.HttpBindAddress), zap.Int("port", m.httpPort))

		if err := httpServer.ServeTLS(ln, opts.HttpTLSCert, opts.HttpTLSKey); err != nethttp.ErrServerClosed {
			log.Error("Failed to serve HTTPS", zap.Error(err))
			m.cancel()
		}
		log.Info("Stopping")
	}(log)

	return registerCloser, nil
}

// runReporter configures and launches a periodic telemetry report for the server.
func (m *Launcher) runReporter(ctx context.Context) {
	reporter := telemetry.NewReporter(m.log, m.reg)
	reporter.Interval = 8 * time.Hour
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		reporter.Report(ctx)
	}()
}

func checkForPriorVersion(ctx context.Context, log *zap.Logger, boltPath string, enginePath string, bs platform.BucketService, metaClient *meta.Client) error {
	buckets, _, err := bs.FindBuckets(ctx, platform.BucketFilter{})
	if err != nil {
		log.Error("Failed to retrieve buckets", zap.Error(err))
		return err
	}

	hasErrors := false

	// if there are no buckets, we will be fine
	if len(buckets) > 0 {
		log.Info("Checking InfluxDB metadata for prior version.", zap.String("bolt_path", boltPath))

		for i := range buckets {
			bucket := buckets[i]
			if dbi := metaClient.Database(bucket.ID.String()); dbi == nil {
				log.Error("Missing metadata for bucket.", zap.String("bucket", bucket.Name), zap.Stringer("bucket_id", bucket.ID))
				hasErrors = true
			}
		}

		if hasErrors {
			log.Error("Incompatible InfluxDB 2.0 metadata found. File must be moved before influxd will start.", zap.String("path", boltPath))
		}
	}

	// see if there are existing files which match the old directory structure
	{
		for _, name := range []string{"_series", "index"} {
			dir := filepath.Join(enginePath, name)
			if fi, err := os.Stat(dir); err == nil {
				if fi.IsDir() {
					log.Error("Found directory that is incompatible with this version of InfluxDB.", zap.String("path", dir))
					hasErrors = true
				}
			}
		}
	}

	if hasErrors {
		log.Error("Incompatible InfluxDB 2.0 version found. Move all files outside of engine_path before influxd will start.", zap.String("engine_path", enginePath))
		return errors.New("incompatible InfluxDB version")
	}

	return nil
}

// OrganizationService returns the internal organization service.
func (m *Launcher) OrganizationService() platform.OrganizationService {
	return m.apibackend.OrganizationService
}

// QueryController returns the internal query service.
func (m *Launcher) QueryController() *control.Controller {
	return m.queryController
}

// BucketService returns the internal bucket service.
func (m *Launcher) BucketService() platform.BucketService {
	return m.apibackend.BucketService
}

// UserService returns the internal user service.
func (m *Launcher) UserService() platform.UserService {
	return m.apibackend.UserService
}

// AuthorizationService returns the internal authorization service.
func (m *Launcher) AuthorizationService() platform.AuthorizationService {
	return m.apibackend.AuthorizationService
}

func (m *Launcher) AuthorizationV1Service() platform.AuthorizationService {
	return m.apibackend.AuthorizationV1Service
}

// SecretService returns the internal secret service.
func (m *Launcher) SecretService() platform.SecretService {
	return m.apibackend.SecretService
}

// CheckService returns the internal check service.
func (m *Launcher) CheckService() platform.CheckService {
	return m.apibackend.CheckService
}

func (m *Launcher) DBRPMappingService() platform.DBRPMappingService {
	return m.apibackend.DBRPService
}

func (m *Launcher) SessionService() platform.SessionService {
	return m.apibackend.SessionService
}

// newHealthCredentialResolver builds the resolver /health and /ready use to
// identify callers when health auth is enabled.
//
// It is deliberately a separate AuthenticationHandler from the one inside the
// platform handler. That one is not retained anywhere and is built much later,
// but more importantly it inherits --session-renew-disabled, which defaults to
// off: a browser polling /health every few seconds through it would keep its
// session alive indefinitely, silently defeating --session-length. Renewal is
// therefore always disabled here.
//
// sessionSvc may be nil, which it is for the token-only resolver installed
// during startup; cookie-bearing callers are simply unresolvable until the full
// resolver replaces it.
//
// Pass userSvc and sessionSvc UNDECORATED -- without the metrics and logging
// middleware the rest of the server uses. Identifying the caller behind a probe
// is not that caller using InfluxDB, and recording it as though it were makes
// the two indistinguishable afterwards: a monitor polling every ten seconds is
// six find_user_by_id calls a minute, forever, in the same counters and the
// same log an operator reads to see what real users are doing. It is the
// steadiness that does the damage -- the probe traffic is constant, so it does
// not look like noise, it looks like a user. authSvc has no middleware to
// strip, and its token lookup reads only the authorization store.
func newHealthCredentialResolver(
	log *zap.Logger,
	authSvc platform.AuthorizationService,
	userSvc platform.UserService,
	sessionSvc platform.SessionService,
) http.CredentialResolver {
	h := http.NewAuthenticationHandler(log, kithttp.NewErrorHandler(log))
	h.AuthorizationService = authSvc
	h.UserService = userSvc
	h.SessionService = sessionSvc
	h.SessionRenewDisabled = true
	return h
}
