package run_test

import (
	"context"
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

	ctx, cancel := context.WithCancel(context.Background())
	errChan := make(chan error)
	go func() { errChan <- cmd.Run(ctx, "-pidfile", pidFile, "-config", os.DevNull) }()

	time.Sleep(1 * time.Second) // wait for pid file to be written.

	if _, err := os.Stat(pidFile); err != nil {
		t.Fatalf("could not stat pid file: %s", err)
	}
	cancel()

	timeout := time.NewTimer(100 * time.Millisecond)
	select {
	case <-timeout.C:
		t.Fatal("unexpected timeout")
	case <-errChan:
		timeout.Stop()
	}

	if _, err := os.Stat(pidFile); err == nil {
		t.Fatal("expected pid file to be removed")
	}
}
