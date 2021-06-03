package upgrade

import (
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2/pkg/testing/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestConfigUpgrade(t *testing.T) {
	type testCase struct {
		name     string
		config1x string
		config2x string
	}

	var testCases = []testCase{
		{
			name:     "minimal",
			config1x: testConfigV1minimal,
			config2x: testConfigV2minimal,
		},
		{
			name:     "default",
			config1x: testConfigV1default,
			config2x: testConfigV2default,
		},
		{
			name:     "empty",
			config1x: testConfigV1empty,
			config2x: testConfigV2empty,
		},
		{
			name:     "obsolete / arrays",
			config1x: testConfigV1obsoleteArrays,
			config2x: testConfigV2obsoleteArrays,
		},
		{
			name:     "query concurrency",
			config1x: testConfigV1QueryConcurrency,
			config2x: testConfigV2QueryConcurrency,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tmpdir := t.TempDir()
			configFile := filepath.Join(tmpdir, "influxdb.conf")
			configFileV2 := filepath.Join(filepath.Dir(configFile), "config.toml")
			err := ioutil.WriteFile(configFile, []byte(tc.config1x), 0444)
			require.NoError(t, err)

			targetOtions := optionsV2{
				boltPath:   "/db/.influxdbv2/influxd.bolt",
				enginePath: "/db/.influxdbv2/engine",
				configPath: configFileV2,
			}

			var rawV1Config map[string]interface{}
			if _, err = toml.Decode(tc.config1x, &rawV1Config); err != nil {
				t.Fatal(err)
			}
			err = upgradeConfig(rawV1Config, targetOtions, zaptest.NewLogger(t))
			assert.NoError(t, err)

			var actual, expected map[string]interface{}
			if _, err = toml.Decode(tc.config2x, &expected); err != nil {
				t.Fatal(err)
			}
			if _, err = toml.DecodeFile(configFileV2, &actual); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(expected, actual); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestConfigLoadFile(t *testing.T) {
	var typicalRetval, emptyRetval configV1
	_, err := toml.Decode(
		"[meta]\ndir=\"/var/lib/influxdb/meta\"\n[data]\ndir=\"/var/lib/influxdb/data\"\nwal-dir=\"/var/lib/influxdb/wal\"\n[http]\nbind-address=\":8086\"\nhttps-enabled=false",
		&typicalRetval,
	)
	require.NoError(t, err)

	type testCase struct {
		name     string
		config1x string
		retval   *configV1
	}

	var testCases = []testCase{
		{
			name:     "minimal",
			config1x: testConfigV1minimal,
			retval:   &typicalRetval,
		},
		{
			name:     "default",
			config1x: testConfigV1default,
			retval:   &typicalRetval,
		},
		{
			name:     "empty",
			config1x: testConfigV1empty,
			retval:   &emptyRetval,
		},
		{
			name:     "obsolete / arrays",
			config1x: testConfigV1obsoleteArrays,
			retval:   &typicalRetval,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tmpdir := t.TempDir()
			configFile := filepath.Join(tmpdir, "influxdb.conf")
			err := ioutil.WriteFile(configFile, []byte(tc.config1x), 0444)
			require.NoError(t, err)
			retval, _, err := loadV1Config(configFile)
			require.NoError(t, err)

			if diff := cmp.Diff(tc.retval, retval); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestConfigLoadFileNotExists(t *testing.T) {
	configFile := "/there/is/no/such/path/influxdb.conf"

	// try upgrade

	_, _, err := loadV1Config(configFile)
	if err == nil {
		t.Fatal("error expected")
	}
}

// 1.x test configs

var testConfigV1minimal = `### Welcome to the InfluxDB configuration file.

# Change this option to true to disable reporting.
reporting-disabled = false

# Bind address to use for the RPC service for backup and restore.
bind-address = "127.0.0.1:8088"

[meta]
  dir = "/var/lib/influxdb/meta"

[data]
  dir = "/var/lib/influxdb/data"
  wal-dir = "/var/lib/influxdb/wal"
  wal-fsync-delay = "100s"
  index-version = "inmem"

[coordinator]
  max-select-point = 0

[retention]
  check-interval = "30m"

[shard-precreation]
  check-interval = "5m"

[monitor]
  store-enabled = true

[http]
  flux-enabled = false
  bind-address = ":8086"
  https-certificate = "/etc/ssl/influxdb.pem"
  https-private-key = "/etc/ssl/influxdb-key.pem"
  pprof-enabled = false

[logging]
  level = "debug"

[subscriber]

[[graphite]]

[[collectd]]

[[opentsdb]]

[[udp]]

[continuous_queries]
  query-stats-enabled = true

[tls]
`

var testConfigV1default = `reporting-disabled = false
bind-address = "127.0.0.1:8088"

[meta]
  dir = "/var/lib/influxdb/meta"
  retention-autocreate = true
  logging-enabled = true

[data]
  dir = "/var/lib/influxdb/data"
  wal-dir = "/var/lib/influxdb/wal"
  wal-fsync-delay = "0s"
  validate-keys = false
  index-version = "tsi1"
  query-log-enabled = true
  cache-max-memory-size = 1073741824
  cache-snapshot-memory-size = 26214400
  cache-snapshot-write-cold-duration = "10m0s"
  compact-full-write-cold-duration = "4h0m0s"
  compact-throughput = 50331648
  compact-throughput-burst = 50331648
  max-concurrent-compactions = 0
  max-index-log-file-size = 1048576
  series-id-set-cache-size = 100
  series-file-max-concurrent-snapshot-compactions = 0
  trace-logging-enabled = false
  tsm-use-madv-willneed = false

[coordinator]
  write-timeout = "10s"
  max-concurrent-queries = 0
  query-timeout = "0s"
  log-queries-after = "0s"
  max-select-point = 0
  max-select-series = 0
  max-select-buckets = 0

[retention]
  enabled = true
  check-interval = "30m0s"

[shard-precreation]
  enabled = true
  check-interval = "10m0s"
  advance-period = "30m0s"

[monitor]
  store-enabled = true
  store-database = "_internal"
  store-interval = "10s"

[subscriber]
  enabled = true
  http-timeout = "30s"
  insecure-skip-verify = false
  ca-certs = ""
  write-concurrency = 40
  write-buffer-size = 1000

[http]
  enabled = true
  bind-address = ":8086"
  auth-enabled = false
  log-enabled = true
  suppress-write-log = false
  write-tracing = false
  flux-enabled = false
  flux-log-enabled = false
  pprof-enabled = true
  pprof-auth-enabled = false
  debug-pprof-enabled = false
  ping-auth-enabled = false
  prom-read-auth-enabled = false
  https-enabled = false
  https-certificate = "/etc/ssl/influxdb.pem"
  https-private-key = ""
  max-row-limit = 0
  max-connection-limit = 0
  shared-secret = ""
  realm = "InfluxDB"
  unix-socket-enabled = false
  unix-socket-permissions = "0777"
  bind-socket = "/var/run/influxdb.sock"
  max-body-size = 25000000
  access-log-path = ""
  max-concurrent-write-limit = 0
  max-enqueued-write-limit = 0
  enqueued-write-timeout = 30000000000

[logging]
  format = "auto"
  level = "info"
  suppress-logo = false

[[graphite]]
  enabled = false
  bind-address = ":2003"
  database = "graphite"
  retention-policy = ""
  protocol = "tcp"
  batch-size = 5000
  batch-pending = 10
  batch-timeout = "1s"
  consistency-level = "one"
  separator = "."
  udp-read-buffer = 0

[[collectd]]
  enabled = false
  bind-address = ":25826"
  database = "collectd"
  retention-policy = ""
  batch-size = 5000
  batch-pending = 10
  batch-timeout = "10s"
  read-buffer = 0
  typesdb = "/usr/share/collectd/types.db"
  security-level = "none"
  auth-file = "/etc/collectd/auth_file"
  parse-multivalue-plugin = "split"

[[opentsdb]]
  enabled = false
  bind-address = ":4242"
  database = "opentsdb"
  retention-policy = ""
  consistency-level = "one"
  tls-enabled = false
  certificate = "/etc/ssl/influxdb.pem"
  batch-size = 1000
  batch-pending = 5
  batch-timeout = "1s"
  log-point-errors = true

[[udp]]
  enabled = false
  bind-address = ":8089"
  database = "udp"
  retention-policy = ""
  batch-size = 5000
  batch-pending = 10
  read-buffer = 0
  batch-timeout = "1s"
  precision = ""

[continuous_queries]
  log-enabled = true
  enabled = true
  query-stats-enabled = false
  run-interval = "1s"

[tls]
  min-version = "tls1.2"
  max-version = "tls1.3"
`

var testConfigV1obsoleteArrays = `
reporting-disabled = true

[meta]
  dir = "/var/lib/influxdb/meta"

[data]
  dir = "/var/lib/influxdb/data"
  wal-dir = "/var/lib/influxdb/wal"

[http]
  enabled = true
  bind-address = ":8086"

[[udp]]
  enabled = false
  bind-address = ":8089"
  database = "udp"
  retention-policy = ""
  batch-size = 5000
  batch-pending = 10
  read-buffer = 0
  batch-timeout = "1s"
  precision = ""

[[udp]]
  enabled = false
  bind-address = ":8090"
  database = "udp2"
  retention-policy = ""
  batch-size = 5000
  batch-pending = 10
  read-buffer = 0
  batch-timeout = "1s"
  precision = ""
`

var testConfigV1empty = `
`

var testConfigV1QueryConcurrency = `
[coordinator]
  max-concurrent-queries = 128
`

// 2.x test configs

var testConfigV2minimal = `reporting-disabled = false
bolt-path = "/db/.influxdbv2/influxd.bolt"
engine-path = "/db/.influxdbv2/engine"
http-bind-address = ":8086"
influxql-max-select-point = 0
log-level = "debug"
storage-retention-check-interval = "30m"
storage-shard-precreator-check-interval = "5m"
storage-wal-fsync-delay = "100s"
tls-cert = "/etc/ssl/influxdb.pem"
tls-key = "/etc/ssl/influxdb-key.pem"
pprof-disabled = true
`

var testConfigV2default = `reporting-disabled = false
bolt-path = "/db/.influxdbv2/influxd.bolt"
engine-path = "/db/.influxdbv2/engine"
http-bind-address = ":8086"
influxql-max-select-buckets = 0
influxql-max-select-point = 0
influxql-max-select-series = 0
log-level = "info"
query-concurrency = 0
storage-cache-max-memory-size = 1073741824
storage-cache-snapshot-memory-size = 26214400
storage-cache-snapshot-write-cold-duration = "10m0s"
storage-compact-full-write-cold-duration = "4h0m0s"
storage-compact-throughput-burst = 50331648
storage-max-concurrent-compactions = 0
storage-max-index-log-file-size = 1048576
storage-retention-check-interval = "30m0s"
storage-series-file-max-concurrent-snapshot-compactions = 0
storage-series-id-set-cache-size = 100
storage-shard-precreator-advance-period = "30m0s"
storage-shard-precreator-check-interval = "10m0s"
storage-tsm-use-madv-willneed = false
storage-validate-keys = false
storage-wal-fsync-delay = "0s"
tls-cert = "/etc/ssl/influxdb.pem"
tls-key = ""
pprof-disabled = false
`

var testConfigV2obsoleteArrays = `reporting-disabled = true
bolt-path = "/db/.influxdbv2/influxd.bolt"
engine-path = "/db/.influxdbv2/engine"
http-bind-address = ":8086"
`

var testConfigV2empty = `
bolt-path = "/db/.influxdbv2/influxd.bolt"
engine-path = "/db/.influxdbv2/engine"
`

var testConfigV2QueryConcurrency = `
bolt-path = "/db/.influxdbv2/influxd.bolt"
engine-path = "/db/.influxdbv2/engine"
query-concurrency = 128
query-queue-size = 128
`
