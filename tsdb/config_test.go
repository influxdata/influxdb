package tsdb_test

import (
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"

	"github.com/influxdata/influxdb/v2/tsdb"
)

func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	c := tsdb.NewConfig()
	_, err := toml.Decode(`
dir = "/var/lib/influxdb/data"
wal-dir = "/var/lib/influxdb/wal"
wal-fsync-delay = "10s"
wal-flush-on-shutdown = true
tsm-use-madv-willneed = true
`, &c)
	require.NoError(t, err)

	require.NoError(t, c.Validate())
	require.Equal(t, "/var/lib/influxdb/data", c.Dir)
	require.Equal(t, "/var/lib/influxdb/wal", c.WALDir)
	require.Equal(t, time.Duration(10*time.Second), time.Duration(c.WALFsyncDelay))
	require.True(t, c.WALFlushOnShutdown)
	require.True(t, c.TSMWillNeed)
}

func TestConfig_Validate_Error(t *testing.T) {
	c := tsdb.NewConfig()
	if err := c.Validate(); err == nil || err.Error() != "Data.Dir must be specified" {
		t.Errorf("unexpected error: %s", err)
	}

	c.Dir = "/var/lib/influxdb/data"
	if err := c.Validate(); err == nil || err.Error() != "Data.WALDir must be specified" {
		t.Errorf("unexpected error: %s", err)
	}

	c.WALDir = "/var/lib/influxdb/wal"
	c.Engine = "fake1"
	if err := c.Validate(); err == nil || err.Error() != "unrecognized engine fake1" {
		t.Errorf("unexpected error: %s", err)
	}

	c.Engine = "tsm1"
	c.Index = "foo"
	if err := c.Validate(); err == nil || err.Error() != "unrecognized index foo" {
		t.Errorf("unexpected error: %s", err)
	}

	c.Index = tsdb.TSI1IndexName
	if err := c.Validate(); err != nil {
		t.Error(err)
	}

	c.SeriesIDSetCacheSize = -1
	if err := c.Validate(); err == nil || err.Error() != "series-id-set-cache-size must be non-negative" {
		t.Errorf("unexpected error: %s", err)
	}
}

func TestConfig_ByteSizes(t *testing.T) {
	// Parse configuration.
	c := tsdb.NewConfig()
	if _, err := toml.Decode(`
dir = "/var/lib/influxdb/data"
wal-dir = "/var/lib/influxdb/wal"
wal-fsync-delay = "10s"
cache-max-memory-size = 5368709120
cache-snapshot-memory-size = 104857600
`, &c); err != nil {
		t.Fatal(err)
	}

	if err := c.Validate(); err != nil {
		t.Errorf("unexpected validate error: %s", err)
	}

	if got, exp := c.Dir, "/var/lib/influxdb/data"; got != exp {
		t.Errorf("unexpected dir:\n\nexp=%v\n\ngot=%v\n\n", exp, got)
	}
	if got, exp := c.WALDir, "/var/lib/influxdb/wal"; got != exp {
		t.Errorf("unexpected wal-dir:\n\nexp=%v\n\ngot=%v\n\n", exp, got)
	}
	if got, exp := c.WALFsyncDelay, time.Duration(10*time.Second); time.Duration(got).Nanoseconds() != exp.Nanoseconds() {
		t.Errorf("unexpected wal-fsync-delay:\n\nexp=%v\n\ngot=%v\n\n", exp, got)
	}
	if got, exp := c.CacheMaxMemorySize, uint64(5<<30); uint64(got) != exp {
		t.Errorf("unexpected cache-max-memory-size:\n\nexp=%v\n\ngot=%v\n\n", exp, got)
	}
	if got, exp := c.CacheSnapshotMemorySize, uint64(100<<20); uint64(got) != exp {
		t.Errorf("unexpected cache-snapshot-memory-size:\n\nexp=%v\n\ngot=%v\n\n", exp, got)
	}
}

func TestConfig_HumanReadableSizes(t *testing.T) {
	// Parse configuration.
	c := tsdb.NewConfig()
	if _, err := toml.Decode(`
dir = "/var/lib/influxdb/data"
wal-dir = "/var/lib/influxdb/wal"
wal-fsync-delay = "10s"
cache-max-memory-size = "5gib"
cache-snapshot-memory-size = "100mib"
`, &c); err != nil {
		t.Fatal(err)
	}

	if err := c.Validate(); err != nil {
		t.Errorf("unexpected validate error: %s", err)
	}

	if got, exp := c.Dir, "/var/lib/influxdb/data"; got != exp {
		t.Errorf("unexpected dir:\n\nexp=%v\n\ngot=%v\n\n", exp, got)
	}
	if got, exp := c.WALDir, "/var/lib/influxdb/wal"; got != exp {
		t.Errorf("unexpected wal-dir:\n\nexp=%v\n\ngot=%v\n\n", exp, got)
	}
	if got, exp := c.WALFsyncDelay, time.Duration(10*time.Second); time.Duration(got).Nanoseconds() != exp.Nanoseconds() {
		t.Errorf("unexpected wal-fsync-delay:\n\nexp=%v\n\ngot=%v\n\n", exp, got)
	}
	if got, exp := c.CacheMaxMemorySize, uint64(5<<30); uint64(got) != exp {
		t.Errorf("unexpected cache-max-memory-size:\n\nexp=%v\n\ngot=%v\n\n", exp, got)
	}
	if got, exp := c.CacheSnapshotMemorySize, uint64(100<<20); uint64(got) != exp {
		t.Errorf("unexpected cache-snapshot-memory-size:\n\nexp=%v\n\ngot=%v\n\n", exp, got)
	}
}
