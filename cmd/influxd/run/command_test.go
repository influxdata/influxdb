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

	cmd := run.NewCommand()
	if err := cmd.Run("-pidfile", pidFile); err != nil {
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
