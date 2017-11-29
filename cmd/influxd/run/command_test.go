package run_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/influxdata/influxdb/cmd/influxd/run"
)

func TestCommand_PIDFile(t *testing.T) {
	tmpdir, err := ioutil.TempDir(os.TempDir(), "influxd-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	pidFile := filepath.Join(tmpdir, "influxdb.pid")

	// Override the default data/wal dir so it doesn't look in ~/.influxdb which
	// might have junk not related to this test.
	os.Setenv("INFLUXDB_DATA_DIR", tmpdir)
	os.Setenv("INFLUXDB_DATA_WAL_DIR", tmpdir)

	cmd := run.NewCommand()
	cmd.Getenv = func(key string) string {
		switch key {
		case "INFLUXDB_DATA_DIR":
			return filepath.Join(tmpdir, "data")
		case "INFLUXDB_META_DIR":
			return filepath.Join(tmpdir, "meta")
		case "INFLUXDB_DATA_WAL_DIR":
			return filepath.Join(tmpdir, "wal")
		case "INFLUXDB_BIND_ADDRESS", "INFLUXDB_HTTP_BIND_ADDRESS":
			return "127.0.0.1:0"
		case "INFLUXDB_REPORTING_DISABLED":
			return "true"
		default:
			return os.Getenv(key)
		}
	}
	if err := cmd.Run("-pidfile", pidFile, "-config", os.DevNull); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if _, err := os.Stat(pidFile); err != nil {
		t.Fatalf("could not stat pid file: %s", err)
	}
	go cmd.Close()

	timeout := time.NewTimer(100 * time.Millisecond)
	select {
	case <-timeout.C:
		t.Fatal("unexpected timeout")
	case <-cmd.Closed:
		timeout.Stop()
	}

	if _, err := os.Stat(pidFile); err == nil {
		t.Fatal("expected pid file to be removed")
	}
}
