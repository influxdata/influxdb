package launcher

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/fluxinit"
	"github.com/influxdata/influxdb/v2/internal/fs"
	"github.com/influxdata/influxdb/v2/kit/cli"
	"github.com/influxdata/influxdb/v2/kit/exit"
	"github.com/influxdata/influxdb/v2/kit/signals"
	influxlogger "github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/pprof"
	"github.com/influxdata/influxdb/v2/sqlite"
	"github.com/influxdata/influxdb/v2/storage"
	"github.com/influxdata/influxdb/v2/toml"
	"github.com/influxdata/influxdb/v2/v1/coordinator"
	"github.com/influxdata/influxdb/v2/vault"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap/zapcore"
)

// healthAuthEnabledFlag names the --health-auth-enabled option. It is shared by
// the option's definition, the probe that decides whether the operator set it,
// and the hardening implication that defers to that answer.
const healthAuthEnabledFlag = "health-auth-enabled"

// startupErrorLingerFlag names the --startup-error-linger option. Shared by the
// option's definition and the log line holdForStartupError writes when the wait
// begins, so an operator reading the log knows which flag produced it.
const startupErrorLingerFlag = "startup-error-linger"

// maxStartupErrorLinger caps --startup-error-linger. The window holds the HTTP
// port open on a process that has already failed, and every supervisor that
// would restart it -- systemd, a container runtime, a shell loop -- is waiting
// on that process to exit, so an unbounded value turns a failed start into an
// indefinite outage. That is a far worse failure than the one the window exists
// to report. Thirty minutes is well past any scrape interval a monitoring
// system uses and well short of the point at which nobody notices.
//
// Enforced in Launcher.holdForStartupError rather than on the option, so that
// every caller is covered and so print-config keeps reporting what the operator
// configured rather than silently rewriting it -- print-config output is
// routinely redirected into a config file.
const maxStartupErrorLinger = 30 * time.Minute

// shutdownTimeout bounds teardown, giving in-progress requests a few seconds to
// finish. It applies to the normal exit path and to both phases of the
// startup-failure path. The check freeze that precedes them has a budget of its
// own; see freezeTimeout.
const shutdownTimeout = 2 * time.Second

// freezeTimeout is the backstop on the whole check freeze that precedes a
// startup-failure teardown. It is emphatically not the probe budget:
// kit/check.Check.Freeze bounds every probe individually at
// check.DefaultProbeTimeout, and that per-probe bound is what keeps one slow
// subsystem from spending the time the checks after it need. This caps only the
// sum, so a check set that has grown, or a run in which many subsystems are
// slow at once, cannot hold open a process that has already failed.
//
// It is sized so that a healthy freeze never reaches it. Once it expires the
// remaining probes run on a dead context, and a cancelled probe snapshots as a
// failure that can outrank the attribution the freeze exists to preserve --
// precisely the drift the per-probe bound was introduced to stop. The launcher
// registers on the order of fifteen checks, so the worst case at
// DefaultProbeTimeout apiece is around 7.5s; this leaves that room and then
// some for the set to grow.
//
// Separate from shutdownTimeout because the two bound unrelated work -- probing
// subsystems that are still up, versus draining in-flight HTTP requests -- and
// resizing one must not silently resize the other.
const freezeTimeout = 15 * time.Second

// errInvalidFlags reports 1.x configuration keys found in a 2.x config file.
// The status is exit.CodeConfig: the config file has to be edited, so a
// supervisor configured to stop retrying on a configuration error will not
// restart into the same failure.
func errInvalidFlags(flags []string, configFile string) error {
	return exit.WithCode(exit.CodeConfig, fmt.Errorf(
		"error: found flags from an InfluxDB 1.x configuration in config file at %s - see https://docs.influxdata.com/influxdb/latest/reference/config-options/ for flags supported on this version of InfluxDB: %s",
		configFile,
		strings.Join(flags, ","),
	))
}

// NewInfluxdCommand constructs the root of the influxd CLI, along with a `run` subcommand.
// The `run` subcommand is set as the default to execute.
func NewInfluxdCommand(ctx context.Context, v *viper.Viper) (*cobra.Command, error) {
	return newInfluxdCommand(ctx, NewOpts(v))
}

// newInfluxdCommand builds the command around an InfluxdOpts the caller owns,
// so a test can observe the option resolution the wiring performs without
// executing the server.
func newInfluxdCommand(ctx context.Context, o *InfluxdOpts) (*cobra.Command, error) {
	v := o.Viper
	cliOpts := o.BindCliOpts()

	prog := cli.Program{
		Name: "influxd",
		Run:  cmdRunE(ctx, o),
	}
	cmd, err := cli.NewCommand(o.Viper, &prog)
	if err != nil {
		return nil, err
	}

	// Record whether the operator supplied a value for --health-auth-enabled,
	// which decides whether --hardening-enabled may imply it (see
	// applyHardeningImplications). This has to happen here, after
	// cli.NewCommand has read the config file and turned on AutomaticEnv but
	// before BindOptions below: once the flag is bound, viper falls back to its
	// default and every key looks set. The command line is the other half of
	// the answer and is not parsed yet -- PreRunE adds it.
	o.HealthAuthEnabledSet = v.Get(healthAuthEnabledFlag) != nil

	// Error out if invalid flags are found in the config file. This may indicate trying to launch 2.x using a 1.x config.
	if invalidFlags := invalidFlags(v); len(invalidFlags) > 0 {
		return nil, errInvalidFlags(invalidFlags, v.ConfigFileUsed())
	}

	runCmd := &cobra.Command{
		Use:  "run",
		RunE: cmd.RunE,
		Args: cli.UsageArgs(cobra.NoArgs),
	}
	for _, c := range []*cobra.Command{cmd, runCmd} {
		setCmdDescriptions(c)
		if err := cli.BindOptions(o.Viper, c, cliOpts); err != nil {
			return nil, err
		}
		c.PreRunE = resolveOptions(o)
	}
	cmd.AddCommand(runCmd)
	printCmd, err := NewInfluxdPrintConfigCommand(v, cliOpts)
	if err != nil {
		return nil, err
	}
	// print-config resolves the options the same way the server does, because
	// reporting what the server would do is its entire purpose. Cobra does not
	// inherit PreRunE from a parent command, so this cannot come from the loop
	// above -- and without it print-config is not merely wrong about
	// health-auth-enabled, it is a trap; see resolveOptions.
	printCmd.PreRunE = resolveOptions(o)
	cmd.AddCommand(printCmd)

	return cmd, nil
}

// resolveOptions returns the PreRunE that finishes resolving o once the command
// line has been parsed: it completes HealthAuthEnabledSet and folds
// --hardening-enabled's implications into the individual options. Every command
// that reads those options installs it -- the server and print-config alike.
//
// print-config especially. Printing health-auth-enabled: false for a hardened
// server would be worse than merely wrong, because the documented workflow is to
// redirect that output into a config file: doing so makes the key present, and a
// present key is exactly what HealthAuthEnabledSet reads as "the operator
// supplied a value". The printed false would come back as an explicit false and
// suppress the implication for good, silently ungating /health and /ready detail
// on a server still running with --hardening-enabled.
func resolveOptions(o *InfluxdOpts) func(*cobra.Command, []string) error {
	return func(c *cobra.Command, _ []string) error {
		// Flags are parsed by the time PreRunE fires, so this is the earliest
		// point at which an explicit --health-auth-enabled=false on the command
		// line is distinguishable from the flag sitting at its default.
		o.HealthAuthEnabledSet = o.HealthAuthEnabledSet || c.Flags().Changed(healthAuthEnabledFlag)
		o.applyHardeningImplications()
		return nil
	}
}

func invalidFlags(v *viper.Viper) []string {
	var invalid []string
	for _, k := range v.AllKeys() {
		if inOneDotExFlagsList(k) {
			invalid = append(invalid, k)
		}
	}

	return invalid
}

func setCmdDescriptions(cmd *cobra.Command) {
	cmd.Short = "Start the influxd server"
	cmd.Long = `
	Start up the daemon configured with flags/env vars/config file.

	The order of precedence for config options are as follows (1 highest, 3 lowest):
		1. flags
		2. env vars
		3. config file

	A config file can be provided via the INFLUXD_CONFIG_PATH env var. If a file is
	not provided via an env var, influxd will look in the current directory for a
	config.{json|toml|yaml|yml} file. If one does not exist, then it will continue unchanged.
`
}

func cmdRunE(ctx context.Context, o *InfluxdOpts) func() error {
	return func() error {
		// Set this as early as possible, since it affects global profiling rates.
		pprof.SetGlobalProfiling(!o.ProfilingDisabled)

		fluxinit.FluxInit()

		l := NewLauncher()

		// Create top level logger
		logconf := &influxlogger.Config{
			Format: "auto",
			Level:  o.LogLevel,
		}
		logger, err := logconf.New(os.Stdout)
		if err != nil {
			// Unreachable as the config above stands, and kept as a guard
			// rather than dropped. Config.New fails only on a format it cannot
			// encode, and Format is fixed to "auto" two lines up, which
			// resolves to "console" or "logfmt"; Level is never validated here
			// at all, having already been rejected by pflag on the command line
			// (EX_USAGE) or by cli.BindOptions in the environment (EX_CONFIG).
			// Should a format or level that does fail arrive later, it is a
			// configured value an operator has to edit, which is what
			// CodeConfig says -- and this is the one startup failure that never
			// reaches Launcher.run, where every other status is pinned.
			return exit.WithCode(exit.CodeConfig, err)
		}
		l.log = logger

		// Start the launcher and wait for it to exit on SIGINT. SIGTERM is not
		// trapped — kit/signals registers os.Interrupt and os.Kill, and SIGKILL
		// cannot be caught — so a SIGTERM kills the process where it stands.
		runErr := l.run(signals.WithStandardSignals(ctx), o)
		if runErr != nil {
			// Startup failed. Release everything a restart needs and, if the
			// operator asked for it, keep /health and /ready answering long
			// enough for a scraper to read which subsystem failed and why.
			l.holdForStartupError(ctx, o.StartupErrorLinger)
		} else {
			<-l.Done()
		}

		// Tear down whatever is left, allowing it a few seconds to finish any
		// in-progress requests. Derived from the outer ctx rather than the
		// signal-wrapped one so a signal cannot truncate teardown.
		//
		// This runs on the startup-failure path too, which it did not before:
		// that path used to return above and leave a --pid-file orphaned for
		// the next start to trip over.
		shutdownCtx, cancel := context.WithTimeout(ctx, shutdownTimeout)
		defer cancel()
		serr := l.Shutdown(shutdownCtx)

		return exitError(runErr, serr)
	}
}

// exitError combines what startup and teardown reported into the single error
// influxd exits on.
//
// Join rather than pick a winner. runErr leads, so the exit code and the first
// line influxd prints are what they have always been, while a teardown failure
// stays reachable through errors.Is and errors.As instead of living only in the
// log -- which is what a caller inspecting the error, a test among them, has to
// work with. errors.Join returns nil when both are nil and the surviving
// error's own message when only one is, so neither single-error case changes at
// all; only a startup failure whose teardown ALSO failed gains a second line.
//
// No aggregate log line here: runClosers already logs every closer failure at
// Error with the subsystem that produced it, which is the same reasoning
// holdForStartupError documents for discarding the error from its own phase.
//
// runErr arrives carrying an exit status, pinned by Launcher.run before it
// could be joined with anything, and exit.Code takes the leftmost -- so a
// startup failure decides the status even when teardown also failed. Only a
// clean startup whose teardown then failed needs a status assigned here: a
// signal that led to a successful shutdown still exits 0, and this is the sole
// path on which a stop does not.
//
// That path is not exotic. shutdownTimeout gives in-flight requests two
// seconds, and httpServer.Shutdown reports the deadline as its own error, so
// stopping a server with a longer query still running lands here and exits
// EX_TEMPFAIL rather than 0. It is the honest answer -- the requests were cut
// off -- but it means an operator who signals a busy server should expect 75,
// not treat it as a rare corner case. EXIT_CODES.md says so too.
//
// It is a function rather than the tail of cmdRunE because cmdRunE cannot be
// called from a test -- fluxinit.FluxInit panics on a second call, and the
// launcher test package has already made the first.
func exitError(runErr, serr error) error {
	if runErr == nil && serr != nil {
		return exit.WithCode(exit.Classify(serr), serr)
	}
	return errors.Join(runErr, serr)
}

// InfluxdOpts captures all arguments for running the InfluxDB server.
type InfluxdOpts struct {
	Testing                 bool
	TestingAlwaysAllowSetup bool

	LogLevel          zapcore.Level
	FluxLogEnabled    bool
	TracingType       string
	ReportingDisabled bool

	PIDFile          string
	OverwritePIDFile bool

	// StartupErrorLinger is how long a failed startup keeps /health and /ready
	// serving before the process exits, so a monitoring system can retrieve the
	// subsystem attribution that would otherwise die with the listener. Zero,
	// the default, exits immediately as before, and anything above
	// maxStartupErrorLinger is capped to it. Everything except the listener and
	// the PID file is released before the wait begins; see
	// Launcher.holdForStartupError.
	StartupErrorLinger time.Duration

	AssetsPath string
	BoltPath   string
	SqLitePath string
	EnginePath string

	StoreType   string
	SecretStore string
	VaultConfig vault.Config

	InstanceID string

	HttpBindAddress       string
	HttpReadHeaderTimeout time.Duration
	HttpReadTimeout       time.Duration
	HttpWriteTimeout      time.Duration
	HttpIdleTimeout       time.Duration
	HttpTLSCert           string
	HttpTLSKey            string
	HttpTLSMinVersion     string
	HttpTLSStrictCiphers  bool
	SessionLength         int // in minutes
	SessionRenewDisabled  bool

	ProfilingDisabled bool
	MetricsDisabled   bool
	UIDisabled        bool

	NatsPort            int
	NatsMaxPayloadBytes int

	NoTasks      bool
	FeatureFlags map[string]string

	// Query options.
	ConcurrencyQuota                int32
	InitialMemoryBytesQuotaPerQuery toml.SSize
	MemoryBytesQuotaPerQuery        toml.SSize
	MaxMemoryBytes                  toml.SSize
	QueueSize                       int32
	CoordinatorConfig               coordinator.Config

	// Storage options.
	StorageConfig storage.Config

	Viper *viper.Viper

	// HardeningEnabled toggles multiple best-practice hardening options on.
	HardeningEnabled bool
	// StrictTransportSecurityMaxAge is the max-age, in seconds, used for the
	// Strict-Transport-Security header when --hardening-enabled is set.
	StrictTransportSecurityMaxAge int
	// TemplateFileUrlsDisabled disables file protocol URIs in templates.
	TemplateFileUrlsDisabled bool
	// HealthAuthEnabled requires operator permissions to read check detail
	// from /health and /ready. Implied by HardeningEnabled unless the operator
	// supplied a value of their own; see HealthAuthEnabledSet.
	HealthAuthEnabled bool
	// HealthAuthEnabledSet reports whether HealthAuthEnabled holds a value the
	// operator supplied -- on the command line, in the environment, or in the
	// config file -- rather than its default. It is not an option itself: it
	// exists so HardeningEnabled's implication can leave an explicit
	// --health-auth-enabled=false alone. Set by newInfluxdCommand; anything
	// building InfluxdOpts directly (tests, embedding) leaves it false, which
	// keeps the implication unconditional as it was.
	HealthAuthEnabledSet bool
	StrongPasswords      bool
	UseHashedTokens      bool
}

// NewOpts constructs options with default values.
func NewOpts(viper *viper.Viper) *InfluxdOpts {
	dir, err := fs.InfluxDir()
	if err != nil {
		panic(fmt.Errorf("failed to determine influx directory: %v", err))
	}

	return &InfluxdOpts{
		Viper:             viper,
		StorageConfig:     storage.NewConfig(),
		CoordinatorConfig: coordinator.NewConfig(),

		LogLevel:          zapcore.InfoLevel,
		FluxLogEnabled:    false,
		ReportingDisabled: false,

		PIDFile:            "",
		OverwritePIDFile:   false,
		StartupErrorLinger: 0,

		BoltPath:   filepath.Join(dir, bolt.DefaultFilename),
		SqLitePath: filepath.Join(dir, sqlite.DefaultFilename),
		EnginePath: filepath.Join(dir, "engine"),

		HttpBindAddress:       ":8086",
		HttpReadHeaderTimeout: 10 * time.Second,
		HttpIdleTimeout:       3 * time.Minute,
		HttpTLSMinVersion:     "1.2",
		HttpTLSStrictCiphers:  false,
		SessionLength:         60, // 60 minutes
		SessionRenewDisabled:  false,

		ProfilingDisabled: false,
		MetricsDisabled:   false,
		UIDisabled:        false,

		StoreType:   DiskStore,
		SecretStore: BoltStore,

		NatsPort:            0,
		NatsMaxPayloadBytes: 0,

		NoTasks: false,

		ConcurrencyQuota:                1024,
		InitialMemoryBytesQuotaPerQuery: 0,
		MemoryBytesQuotaPerQuery:        0,
		MaxMemoryBytes:                  0,
		QueueSize:                       1024,

		Testing:                 false,
		TestingAlwaysAllowSetup: false,

		HardeningEnabled:              false,
		StrictTransportSecurityMaxAge: 31536000, // 1 year
		TemplateFileUrlsDisabled:      false,
		HealthAuthEnabled:             false,
		StrongPasswords:               false,
		UseHashedTokens:               true,
	}
}

// applyHardeningImplications turns on the options --hardening-enabled implies,
// leaving alone any option the operator supplied a value for. It is called from
// the PreRunE that resolveOptions installs, before anything reads the options it
// resolves, and again by Launcher.run to cover callers that build an
// InfluxdOpts and invoke run directly. It is idempotent, which is what lets both
// call it.
//
// --hardening-enabled means "every hardening feature", but an implication with
// no way out is a trap when the feature changes an API contract:
// --health-auth-enabled reshapes the /health and /ready bodies, and an operator
// whose monitoring parses them needs to keep the rest of the hardening --
// notably the flux/pkger IP validator, which has no per-feature flag -- without
// it. So an explicit --health-auth-enabled wins in either direction: false
// keeps the bodies as they were, true is the same answer the implication would
// have given anyway.
//
// The implication is resolved into the options rather than OR-ed at the use
// site because NewConfigHandler reports these fields verbatim: OR-ing would
// leave /api/v2/config claiming health-auth-enabled is false on a server that
// is enforcing it.
//
// --template-file-urls-disabled, the other feature --hardening-enabled implies,
// is still OR-ed at its use site and has no opt-out. That is deliberate for now:
// it changes no response body, so nothing downstream can silently break on it.
func (o *InfluxdOpts) applyHardeningImplications() {
	if o.HardeningEnabled && !o.HealthAuthEnabledSet {
		o.HealthAuthEnabled = true
	}
}

// BindCliOpts returns a list of options which can be added to a cobra command
// in order to set options over the CLI.
func (o *InfluxdOpts) BindCliOpts() []cli.Opt {
	return []cli.Opt{
		{
			DestP:   &o.LogLevel,
			Flag:    "log-level",
			Default: o.LogLevel,
			Desc:    "supported log levels are debug, info, and error",
		},
		{
			DestP:   &o.FluxLogEnabled,
			Flag:    "flux-log-enabled",
			Default: o.FluxLogEnabled,
			Desc:    "enables detailed logging for flux queries",
		},
		{
			DestP: &o.TracingType,
			Flag:  "tracing-type",
			Desc:  fmt.Sprintf("supported tracing types are %s, %s", LogTracing, JaegerTracing),
		},
		{
			DestP:   &o.BoltPath,
			Flag:    "bolt-path",
			Default: o.BoltPath,
			Desc:    "path to boltdb database",
		},
		{
			DestP: &o.SqLitePath,
			Flag:  "sqlite-path",
			Desc:  fmt.Sprintf("path to sqlite database. if not set, sqlite database will be stored in the bolt-path directory as %q.", sqlite.DefaultFilename),
		},
		{
			DestP: &o.AssetsPath,
			Flag:  "assets-path",
			Desc:  "override default assets by serving from a specific directory (developer mode)",
		},
		{
			DestP:   &o.StoreType,
			Flag:    "store",
			Default: o.StoreType,
			Desc:    "backing store for REST resources (disk or memory)",
		},
		{
			DestP:   &o.Testing,
			Flag:    "e2e-testing",
			Default: o.Testing,
			Desc:    "add /debug/flush endpoint to clear stores; used for end-to-end tests",
		},
		{
			DestP:   &o.TestingAlwaysAllowSetup,
			Flag:    "testing-always-allow-setup",
			Default: o.TestingAlwaysAllowSetup,
			Desc:    "ensures the /api/v2/setup endpoint always returns true to allow onboarding",
		},
		{
			DestP:   &o.EnginePath,
			Flag:    "engine-path",
			Default: o.EnginePath,
			Desc:    "path to persistent engine files",
		},
		{
			DestP:   &o.SecretStore,
			Flag:    "secret-store",
			Default: o.SecretStore,
			Desc:    "data store for secrets (bolt or vault)",
		},
		{
			DestP:   &o.ReportingDisabled,
			Flag:    "reporting-disabled",
			Default: o.ReportingDisabled,
			Desc:    "disable sending telemetry data to https://telemetry.influxdata.com every 8 hours",
		},
		{
			DestP:   &o.PIDFile,
			Flag:    "pid-file",
			Default: o.PIDFile,
			Desc:    "write process ID to a file",
		},
		{
			DestP:   &o.OverwritePIDFile,
			Flag:    "overwrite-pid-file",
			Default: o.OverwritePIDFile,
			Desc:    "overwrite PID file if it already exists instead of exiting",
		},
		{
			DestP:   &o.StartupErrorLinger,
			Flag:    startupErrorLingerFlag,
			Default: o.StartupErrorLinger,
			Desc:    fmt.Sprintf("how long to keep /health and /ready serving after a failed startup, so the error can be retrieved, before exiting. Set to 0 to exit immediately; capped at %s", maxStartupErrorLinger),
		},
		{
			DestP:   &o.SessionLength,
			Flag:    "session-length",
			Default: o.SessionLength,
			Desc:    "ttl in minutes for newly created sessions",
		},
		{
			DestP:   &o.SessionRenewDisabled,
			Flag:    "session-renew-disabled",
			Default: o.SessionRenewDisabled,
			Desc:    "disables automatically extending session ttl on request",
		},
		{
			DestP: &o.VaultConfig.Address,
			Flag:  "vault-addr",
			Desc:  "address of the Vault server expressed as a URL and port, for example: https://127.0.0.1:8200/.",
		},
		{
			DestP: &o.VaultConfig.ClientTimeout,
			Flag:  "vault-client-timeout",
			Desc:  "timeout variable. The default value is 60s.",
		},
		{
			DestP: &o.VaultConfig.MaxRetries,
			Flag:  "vault-max-retries",
			Desc:  "maximum number of retries when a 5xx error code is encountered. The default is 2, for three total attempts. Set this to 0 or less to disable retrying.",
		},
		{
			DestP: &o.VaultConfig.CACert,
			Flag:  "vault-cacert",
			Desc:  "path to a PEM-encoded CA certificate file on the local disk. This file is used to verify the Vault server's SSL certificate. This environment variable takes precedence over VAULT_CAPATH.",
		},
		{
			DestP: &o.VaultConfig.CAPath,
			Flag:  "vault-capath",
			Desc:  "path to a directory of PEM-encoded CA certificate files on the local disk. These certificates are used to verify the Vault server's SSL certificate.",
		},
		{
			DestP: &o.VaultConfig.ClientCert,
			Flag:  "vault-client-cert",
			Desc:  "path to a PEM-encoded client certificate on the local disk. This file is used for TLS communication with the Vault server.",
		},
		{
			DestP: &o.VaultConfig.ClientKey,
			Flag:  "vault-client-key",
			Desc:  "path to an unencrypted, PEM-encoded private key on disk which corresponds to the matching client certificate.",
		},
		{
			DestP: &o.VaultConfig.InsecureSkipVerify,
			Flag:  "vault-skip-verify",
			Desc:  "do not verify Vault's presented certificate before communicating with it. Setting this variable is not recommended and voids Vault's security model.",
		},
		{
			DestP: &o.VaultConfig.TLSServerName,
			Flag:  "vault-tls-server-name",
			Desc:  "name to use as the SNI host when connecting via TLS.",
		},
		{
			DestP: &o.VaultConfig.Token,
			Flag:  "vault-token",
			Desc:  "vault authentication token",
		},

		// HTTP options
		{
			DestP:   &o.HttpBindAddress,
			Flag:    "http-bind-address",
			Default: o.HttpBindAddress,
			Desc:    "bind address for the REST HTTP API",
		},
		{
			DestP:   &o.HttpReadHeaderTimeout,
			Flag:    "http-read-header-timeout",
			Default: o.HttpReadHeaderTimeout,
			Desc:    "max duration the server should spend trying to read HTTP headers for new requests. Set to 0 for no timeout",
		},
		{
			DestP:   &o.HttpReadTimeout,
			Flag:    "http-read-timeout",
			Default: o.HttpReadTimeout,
			Desc:    "max duration the server should spend trying to read the entirety of new requests. Set to 0 for no timeout",
		},
		{
			DestP:   &o.HttpWriteTimeout,
			Flag:    "http-write-timeout",
			Default: o.HttpWriteTimeout,
			Desc:    "max duration the server should spend on processing+responding to requests. Set to 0 for no timeout",
		},
		{
			DestP:   &o.HttpIdleTimeout,
			Flag:    "http-idle-timeout",
			Default: o.HttpIdleTimeout,
			Desc:    "max duration the server should keep established connections alive while waiting for new requests. Set to 0 for no timeout",
		},
		{
			DestP: &o.HttpTLSCert,
			Flag:  "tls-cert",
			Desc:  "TLS certificate for HTTPs",
		},
		{
			DestP: &o.HttpTLSKey,
			Flag:  "tls-key",
			Desc:  "TLS key for HTTPs",
		},
		{
			DestP:   &o.HttpTLSMinVersion,
			Flag:    "tls-min-version",
			Default: o.HttpTLSMinVersion,
			Desc:    "Minimum accepted TLS version",
		},
		{
			DestP:   &o.HttpTLSStrictCiphers,
			Flag:    "tls-strict-ciphers",
			Default: o.HttpTLSStrictCiphers,
			Desc:    "Restrict accept ciphers to: ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, ECDHE_RSA_WITH_AES_128_GCM_SHA256, ECDHE_ECDSA_WITH_AES_256_GCM_SHA384, ECDHE_RSA_WITH_AES_256_GCM_SHA384, ECDHE_ECDSA_WITH_CHACHA20_POLY1305, ECDHE_RSA_WITH_CHACHA20_POLY1305",
		},

		{
			DestP:   &o.NoTasks,
			Flag:    "no-tasks",
			Default: o.NoTasks,
			Desc:    "disables the task scheduler",
		},
		{
			DestP:   &o.ConcurrencyQuota,
			Flag:    "query-concurrency",
			Default: o.ConcurrencyQuota,
			Desc:    "the number of queries that are allowed to execute concurrently. Set to 0 to allow an unlimited number of concurrent queries",
		},
		// Default on the next three Opts is documentary: for a pflag.Value
		// DestP, pflag.Var already uses destP's NewOpts-set value as the help
		// default, so omitting Default would not change behavior.
		{
			DestP:   &o.InitialMemoryBytesQuotaPerQuery,
			Flag:    "query-initial-memory-bytes",
			Default: o.InitialMemoryBytesQuotaPerQuery,
			Desc:    "the initial number of bytes allocated for a query when it is started. If this is unset, then query-memory-bytes will be used",
		},
		{
			DestP:   &o.MemoryBytesQuotaPerQuery,
			Flag:    "query-memory-bytes",
			Default: o.MemoryBytesQuotaPerQuery,
			Desc:    "maximum number of bytes a query is allowed to use at any given time. This must be greater or equal to query-initial-memory-bytes",
		},
		{
			DestP:   &o.MaxMemoryBytes,
			Flag:    "query-max-memory-bytes",
			Default: o.MaxMemoryBytes,
			Desc:    "the maximum amount of memory used for queries. Can only be set when query-concurrency is limited. If this is unset, then this number is query-concurrency * query-memory-bytes",
		},
		{
			DestP:   &o.QueueSize,
			Flag:    "query-queue-size",
			Default: o.QueueSize,
			Desc:    "the number of queries that are allowed to be awaiting execution before new queries are rejected. Must be > 0 if query-concurrency is not unlimited",
		},
		{
			DestP: &o.FeatureFlags,
			Flag:  "feature-flags",
			Desc:  "feature flag overrides",
		},
		{
			DestP:   &o.InstanceID,
			Flag:    "instance-id",
			Default: "",
			Desc:    "add an instance id for replications to prevent collisions and allow querying by edge node",
		},

		// storage configuration
		{
			DestP:   &o.StorageConfig.WriteTimeout,
			Flag:    "storage-write-timeout",
			Default: o.StorageConfig.WriteTimeout,
			Desc:    "The max amount of time the engine will spend completing a write request before cancelling with a timeout.",
		},
		{
			DestP: &o.StorageConfig.Data.WALFsyncDelay,
			Flag:  "storage-wal-fsync-delay",
			Desc:  "The amount of time that a write will wait before fsyncing. A duration greater than 0 can be used to batch up multiple fsync calls. This is useful for slower disks or when WAL write contention is seen.",
		},
		{
			DestP: &o.StorageConfig.Data.WALMaxConcurrentWrites,
			Flag:  "storage-wal-max-concurrent-writes",
			Desc:  "The max number of writes that will attempt to write to the WAL at a time. (default <nprocs> * 2)",
		},
		{
			DestP:   &o.StorageConfig.Data.WALMaxWriteDelay,
			Flag:    "storage-wal-max-write-delay",
			Default: o.StorageConfig.Data.WALMaxWriteDelay,
			Desc:    "The max amount of time a write will wait when the WAL already has `storage-wal-max-concurrent-writes` active writes. Set to 0 to disable the timeout.",
		},
		{
			DestP: &o.StorageConfig.Data.WALFlushOnShutdown,
			Flag:  "storage-wal-flush-on-shutdown",
			Desc:  "Flushes and clears the WAL on shutdown",
		},
		{
			DestP: &o.StorageConfig.Data.ValidateKeys,
			Flag:  "storage-validate-keys",
			Desc:  "Validates incoming writes to ensure keys only have valid unicode characters.",
		},
		{
			DestP: &o.StorageConfig.Data.SkipFieldSizeValidation,
			Flag:  "storage-no-validate-field-size",
			Desc:  "Skip field-size validation on incoming writes.",
		},
		{
			DestP: &o.StorageConfig.Data.CacheMaxMemorySize,
			Flag:  "storage-cache-max-memory-size",
			Desc:  "The maximum size a shard's cache can reach before it starts rejecting writes.",
		},
		{
			DestP: &o.StorageConfig.Data.CacheSnapshotMemorySize,
			Flag:  "storage-cache-snapshot-memory-size",
			Desc:  "The size at which the engine will snapshot the cache and write it to a TSM file, freeing up memory.",
		},
		{
			DestP: &o.StorageConfig.Data.CacheSnapshotWriteColdDuration,
			Flag:  "storage-cache-snapshot-write-cold-duration",
			Desc:  "The length of time at which the engine will snapshot the cache and write it to a new TSM file if the shard hasn't received writes or deletes.",
		},
		{
			DestP: &o.StorageConfig.Data.CompactFullWriteColdDuration,
			Flag:  "storage-compact-full-write-cold-duration",
			Desc:  "The duration at which the engine will compact all TSM files in a shard if it hasn't received a write or delete.",
		},
		{
			DestP: &o.StorageConfig.Data.CompactThroughputBurst,
			Flag:  "storage-compact-throughput-burst",
			Desc:  "The maximum burst capacity in bytes per second that we will allow TSM compactions to write to disk.",
		},
		{
			DestP: &o.StorageConfig.Data.CompactThroughput,
			Flag:  "storage-compact-throughput",
			Desc:  "The rate in bytes per second that we will allow TSM compactions to write to disk.",
		},
		// limits
		{
			DestP: &o.StorageConfig.Data.MaxConcurrentCompactions,
			Flag:  "storage-max-concurrent-compactions",
			Desc:  "The maximum number of concurrent full and level compactions that can run at one time.  A value of 0 results in 50% of runtime.GOMAXPROCS(0) used at runtime.  Any number greater than 0 limits compactions to that value.  This setting does not apply to cache snapshotting.",
		},
		{
			DestP: &o.StorageConfig.Data.MaxIndexLogFileSize,
			Flag:  "storage-max-index-log-file-size",
			Desc:  "The threshold, in bytes, when an index write-ahead log file will compact into an index file. Lower sizes will cause log files to be compacted more quickly and result in lower heap usage at the expense of write throughput.",
		},
		{
			DestP: &o.StorageConfig.Data.SeriesIDSetCacheSize,
			Flag:  "storage-series-id-set-cache-size",
			Desc:  "The size of the internal cache used in the TSI index to store previously calculated series results.",
		},
		{
			DestP: &o.StorageConfig.Data.SeriesFileMaxConcurrentSnapshotCompactions,
			Flag:  "storage-series-file-max-concurrent-snapshot-compactions",
			Desc:  "The maximum number of concurrent snapshot compactions that can be running at one time across all series partitions in a database.",
		},
		{
			DestP: &o.StorageConfig.Data.TSMWillNeed,
			Flag:  "storage-tsm-use-madv-willneed",
			Desc:  "Controls whether we hint to the kernel that we intend to page in mmap'd sections of TSM files.",
		},
		{
			DestP: &o.StorageConfig.RetentionService.CheckInterval,
			Flag:  "storage-retention-check-interval",
			Desc:  "The interval of time when retention policy enforcement checks run.",
		},
		{
			DestP: &o.StorageConfig.PrecreatorConfig.CheckInterval,
			Flag:  "storage-shard-precreator-check-interval",
			Desc:  "The interval of time when the check to pre-create new shards runs.",
		},
		{
			DestP: &o.StorageConfig.PrecreatorConfig.AdvancePeriod,
			Flag:  "storage-shard-precreator-advance-period",
			Desc:  "The default period ahead of the endtime of a shard group that its successor group is created.",
		},

		// InfluxQL Coordinator Config
		{
			DestP: &o.CoordinatorConfig.MaxSelectPointN,
			Flag:  "influxql-max-select-point",
			Desc:  "The maximum number of points a SELECT can process. A value of 0 will make the maximum point count unlimited. This will only be checked every second so queries will not be aborted immediately when hitting the limit.",
		},
		{
			DestP: &o.CoordinatorConfig.MaxSelectSeriesN,
			Flag:  "influxql-max-select-series",
			Desc:  "The maximum number of series a SELECT can run. A value of 0 will make the maximum series count unlimited.",
		},
		{
			DestP: &o.CoordinatorConfig.MaxSelectBucketsN,
			Flag:  "influxql-max-select-buckets",
			Desc:  "The maximum number of group by time bucket a SELECT can create. A value of zero will max the maximum number of buckets unlimited.",
		},

		// NATS config
		{
			DestP:   &o.NatsPort,
			Flag:    "nats-port",
			Desc:    "deprecated: nats has been replaced",
			Default: o.NatsPort,
			Hidden:  true,
		},
		{
			DestP:   &o.NatsMaxPayloadBytes,
			Flag:    "nats-max-payload-bytes",
			Desc:    "deprecated: nats has been replaced",
			Default: o.NatsMaxPayloadBytes,
			Hidden:  true,
		},

		// Pprof config
		{
			DestP:   &o.ProfilingDisabled,
			Flag:    "pprof-disabled",
			Desc:    "Don't expose debugging information over HTTP at /debug/pprof",
			Default: o.ProfilingDisabled,
		},

		// Metrics config
		{
			DestP:   &o.MetricsDisabled,
			Flag:    "metrics-disabled",
			Desc:    "Don't expose metrics over HTTP at /metrics",
			Default: o.MetricsDisabled,
		},
		// UI Config
		{
			DestP:   &o.UIDisabled,
			Flag:    "ui-disabled",
			Default: o.UIDisabled,
			Desc:    "Disable the InfluxDB UI",
		},

		// hardening options
		// --hardening-enabled is meant to enable all hardening
		// options in one go. Today it enables the IP validator for
		// flux and pkger templates HTTP requests, disables file://
		// protocol for pkger templates, sets the
		// Strict-Transport-Security response header, and requires
		// operator permissions to read check detail from /health and
		// /ready. Per-feature flags exist for the features that have
		// them (--template-file-urls-disabled, --health-auth-enabled)
		// so that users can either opt into all features
		// (--hardening-enabled) or to precisely the features they
		// require. Setting --health-auth-enabled explicitly overrides
		// this flag's implication of it; see
		// applyHardeningImplications.
		{
			DestP:   &o.HardeningEnabled,
			Flag:    "hardening-enabled",
			Default: o.HardeningEnabled,
			Desc:    "enable hardening options (disallow private IPs within flux and templates HTTP requests; disable file URLs in templates; set the Strict-Transport-Security response header; require operator permissions for /health and /ready detail unless --health-auth-enabled says otherwise)",
		},

		// --strict-transport-security-max-age sets the max-age, in
		// seconds, for the Strict-Transport-Security (HSTS) header. The
		// header is only emitted when --hardening-enabled is set. Lower
		// values can be useful during initial deployment, before
		// committing to a longer policy. preload is never set since we
		// do not own the domain InfluxDB is hosted on.
		{
			DestP:   &o.StrictTransportSecurityMaxAge,
			Flag:    "strict-transport-security-max-age",
			Default: o.StrictTransportSecurityMaxAge,
			Desc:    "max-age, in seconds, for the Strict-Transport-Security header (only used when --hardening-enabled is set)",
		},

		// --template-file-urls-disabled prevents file protocol URIs
		// from being used for templates.
		{
			DestP:   &o.TemplateFileUrlsDisabled,
			Flag:    "template-file-urls-disabled",
			Default: o.TemplateFileUrlsDisabled,
			Desc:    "disable template file URLs",
		},

		// --health-auth-enabled withholds the failure message and the
		// per-check responses, which carry raw error text such as
		// filesystem paths and shard ids. Setting it explicitly wins over
		// the --hardening-enabled implication in either direction, so an
		// operator whose monitoring parses those bodies can harden
		// everything else.
		{
			DestP:   &o.HealthAuthEnabled,
			Flag:    healthAuthEnabledFlag,
			Default: o.HealthAuthEnabled,
			Desc:    "require operator permissions to read check detail from /health and /ready (unauthorized callers still receive the correct status code with a reduced body); set explicitly to override --hardening-enabled",
		},
		{
			DestP:   &o.StrongPasswords,
			Flag:    "strong-passwords",
			Default: o.StrongPasswords,
			Desc:    "enable password strength enforcement",
		},
		{
			DestP:   &o.UseHashedTokens,
			Flag:    "use-hashed-tokens",
			Default: o.UseHashedTokens,
			Desc:    "enable storing hashed API tokens on disk (enabled by default; pass --use-hashed-tokens=false to disable for < 2.8 compatibility)",
		},
	}
}

var (
	oneDotExFlagsList = []string{
		// "reporting-disabled" is valid in both 1x and 2x configs
		"bind-address", // global setting is called "http-bind-address" on 2x

		// Remaining flags, when parsed from a 1.x config file, will be in sub-sections prefixed by these headers:
		"collectd.",
		"continuous_queries.",
		"coordinator.",
		"data.",
		"graphite.",
		"http.",
		"logging.",
		"meta.",
		"monitor.",
		"opentsdb.",
		"retention.",
		"shard-precreation.",
		"subscriber.",
		"tls.",
		"udp.",
	}
)

// compareFlags checks if a given flag from the read configuration matches one from the list. If the value from the list
// ends in a ".", the given flag is check for that prefix. Otherwise, the flag is checked for equality.
func compareFlags(key, fromList string) bool {
	if strings.HasSuffix(fromList, ".") {
		return strings.HasPrefix(key, fromList)
	}

	return strings.EqualFold(key, fromList)
}

func inOneDotExFlagsList(key string) bool {
	for _, f := range oneDotExFlagsList {
		if compareFlags(key, f) {
			return true
		}
	}

	return false
}
