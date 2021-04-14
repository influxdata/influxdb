package launcher

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/fluxinit"
	"github.com/influxdata/influxdb/v2/internal/fs"
	"github.com/influxdata/influxdb/v2/kit/cli"
	"github.com/influxdata/influxdb/v2/kit/signals"
	influxlogger "github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/nats"
	"github.com/influxdata/influxdb/v2/pprof"
	"github.com/influxdata/influxdb/v2/storage"
	"github.com/influxdata/influxdb/v2/v1/coordinator"
	"github.com/influxdata/influxdb/v2/vault"
	natsserver "github.com/nats-io/gnatsd/server"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap/zapcore"
)

const (
	// Max Integer
	MaxInt = 1<<uint(strconv.IntSize-1) - 1
)

// NewInfluxdCommand constructs the root of the influxd CLI, along with a `run` subcommand.
// The `run` subcommand is set as the default to execute.
func NewInfluxdCommand(ctx context.Context, v *viper.Viper) (*cobra.Command, error) {
	o := newOpts(v)
	cliOpts := o.bindCliOpts()

	prog := cli.Program{
		Name: "influxd",
		Run:  cmdRunE(ctx, o),
	}
	cmd, err := cli.NewCommand(o.Viper, &prog)
	if err != nil {
		return nil, err
	}

	runCmd := &cobra.Command{
		Use:  "run",
		RunE: cmd.RunE,
		Args: cobra.NoArgs,
	}
	for _, c := range []*cobra.Command{cmd, runCmd} {
		setCmdDescriptions(c)
		if err := cli.BindOptions(o.Viper, c, cliOpts); err != nil {
			return nil, err
		}
	}
	cmd.AddCommand(runCmd)
	printCmd, err := NewInfluxdPrintConfigCommand(v, cliOpts)
	if err != nil {
		return nil, err
	}
	cmd.AddCommand(printCmd)

	return cmd, nil
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
			return err
		}
		l.log = logger

		// Start the launcher and wait for it to exit on SIGINT or SIGTERM.
		if err := l.run(signals.WithStandardSignals(ctx), o); err != nil {
			return err
		}
		<-l.Done()

		// Tear down the launcher, allowing it a few seconds to finish any
		// in-progress requests.
		shutdownCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		return l.Shutdown(shutdownCtx)
	}
}

// InfluxdOpts captures all arguments for running the InfluxDB server.
type InfluxdOpts struct {
	Testing                 bool
	TestingAlwaysAllowSetup bool

	LogLevel          zapcore.Level
	TracingType       string
	ReportingDisabled bool

	AssetsPath string
	BoltPath   string
	EnginePath string

	StoreType   string
	SecretStore string
	VaultConfig vault.Config

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

	NatsPort            int
	NatsMaxPayloadBytes int

	NoTasks      bool
	FeatureFlags map[string]string

	// Query options.
	ConcurrencyQuota                int32
	InitialMemoryBytesQuotaPerQuery int64
	MemoryBytesQuotaPerQuery        int64
	MaxMemoryBytes                  int64
	QueueSize                       int32
	CoordinatorConfig               coordinator.Config

	// Storage options.
	StorageConfig storage.Config

	Viper *viper.Viper
}

// newOpts constructs options with default values.
func newOpts(viper *viper.Viper) *InfluxdOpts {
	dir, err := fs.InfluxDir()
	if err != nil {
		panic(fmt.Errorf("failed to determine influx directory: %v", err))
	}

	return &InfluxdOpts{
		Viper:             viper,
		StorageConfig:     storage.NewConfig(),
		CoordinatorConfig: coordinator.NewConfig(),

		LogLevel:          zapcore.InfoLevel,
		ReportingDisabled: false,

		BoltPath:   filepath.Join(dir, bolt.DefaultFilename),
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

		StoreType:   BoltStore,
		SecretStore: BoltStore,

		NatsPort:            nats.RandomPort,
		NatsMaxPayloadBytes: natsserver.MAX_PAYLOAD_SIZE,

		NoTasks: false,

		ConcurrencyQuota:                0,
		InitialMemoryBytesQuotaPerQuery: 0,
		MemoryBytesQuotaPerQuery:        MaxInt,
		MaxMemoryBytes:                  0,
		QueueSize:                       0,

		Testing:                 false,
		TestingAlwaysAllowSetup: false,
	}
}

// bindCliOpts returns a list of options which can be added to a cobra command
// in order to set options over the CLI.
func (o *InfluxdOpts) bindCliOpts() []cli.Opt {
	return []cli.Opt{
		{
			DestP:   &o.LogLevel,
			Flag:    "log-level",
			Default: o.LogLevel,
			Desc:    "supported log levels are debug, info, and error",
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
			DestP: &o.AssetsPath,
			Flag:  "assets-path",
			Desc:  "override default assets by serving from a specific directory (developer mode)",
		},
		{
			DestP:   &o.StoreType,
			Flag:    "store",
			Default: o.StoreType,
			Desc:    "backing store for REST resources (bolt or memory)",
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

		// storage configuration
		{
			DestP: &o.StorageConfig.Data.WALFsyncDelay,
			Flag:  "storage-wal-fsync-delay",
			Desc:  "The amount of time that a write will wait before fsyncing. A duration greater than 0 can be used to batch up multiple fsync calls. This is useful for slower disks or when WAL write contention is seen.",
		},
		{
			DestP: &o.StorageConfig.Data.ValidateKeys,
			Flag:  "storage-validate-keys",
			Desc:  "Validates incoming writes to ensure keys only have valid unicode characters.",
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
			Desc:  "The rate limit in bytes per second that we will allow TSM compactions to write to disk.",
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
			Desc:    fmt.Sprintf("Port that should be bound by the NATS streaming server. A value of %d will cause a random port to be selected.", nats.RandomPort),
			Default: o.NatsPort,
		},
		{
			DestP:   &o.NatsMaxPayloadBytes,
			Flag:    "nats-max-payload-bytes",
			Desc:    "The maximum number of bytes allowed in a NATS message payload.",
			Default: o.NatsMaxPayloadBytes,
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
	}
}
