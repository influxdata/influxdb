package main_test

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/influxdb/influxdb"
	main "github.com/influxdb/influxdb/cmd/influxd"
)

func newConfig(path string, port, snapshotPort int) main.Config {
	config := main.NewConfig()
	config.Port = port
	config.Broker.Enabled = true
	config.Broker.Dir = filepath.Join(path, "broker")

	config.Data.Enabled = true
	config.Data.Dir = filepath.Join(path, "data")
	config.Snapshot.Port = snapshotPort
	return *config
}

// Ensure the restore command can expand a snapshot and bootstrap a broker.
func TestRestoreCommand(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestRestoreCommand")
	}

	now := time.Now()

	// Create root path to server.
	path := tempfile()
	defer os.Remove(path)

	// Parse configuration.
	config := newConfig(path, 8900, 8901)

	// Start server.
	cmd := main.NewRunCommand()
	b, s, l := cmd.Open(&config, "")
	if b == nil {
		t.Fatal("cannot run broker")
	} else if s == nil {
		t.Fatal("cannot run server")
	}

	// Create data.
	if err := s.CreateDatabase("db"); err != nil {
		t.Fatalf("cannot create database: %s", err)
	}
	if index, err := s.WriteSeries("db", "default", []influxdb.Point{{Name: "cpu", Timestamp: now, Fields: map[string]interface{}{"value": float64(100)}}}); err != nil {
		t.Fatalf("cannot write series: %s", err)
	} else if err = s.Sync(1, index); err != nil {
		t.Fatalf("shard sync: %s", err)
	}

	// Create snapshot writer.
	sw, err := s.CreateSnapshotWriter()
	if err != nil {
		t.Fatalf("create snapshot writer: %s", err)
	}

	// Snapshot to file.
	sspath := tempfile()
	f, err := os.Create(sspath)
	if err != nil {
		t.Fatal(err)
	}
	sw.WriteTo(f)
	f.Close()

	// Stop server.
	l.Close()
	s.Close()
	b.Close()

	// Remove data & broker directories.
	if err := os.RemoveAll(path); err != nil {
		t.Fatalf("remove: %s", err)
	}

	// Execute the restore.
	if err := NewRestoreCommand().Restore(&config, sspath); err != nil {
		t.Fatal(err)
	}

	// Rewrite config to a new port and re-parse.
	config = newConfig(path, 8910, 8911)

	// Restart server.
	cmd = main.NewRunCommand()
	b, s, l = cmd.Open(&config, "")
	if b == nil {
		t.Fatal("cannot run broker")
	} else if s == nil {
		t.Fatal("cannot run server")
	}

	// Write new data.
	if err := s.CreateDatabase("newdb"); err != nil {
		t.Fatalf("cannot create new database: %s", err)
	}
	if index, err := s.WriteSeries("newdb", "default", []influxdb.Point{{Name: "mem", Timestamp: now, Fields: map[string]interface{}{"value": float64(1000)}}}); err != nil {
		t.Fatalf("cannot write new series: %s", err)
	} else if err = s.Sync(2, index); err != nil {
		t.Fatalf("shard sync: %s", err)
	}

	// Read series data.
	if v, err := s.ReadSeries("db", "default", "cpu", nil, now); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(v, map[string]interface{}{"value": float64(100)}) {
		t.Fatalf("read series(0) mismatch: %#v", v)
	}

	// Read new series data.
	if v, err := s.ReadSeries("newdb", "default", "mem", nil, now); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(v, map[string]interface{}{"value": float64(1000)}) {
		t.Fatalf("read series(1) mismatch: %#v", v)
	}

	// Stop server.
	l.Close()
	s.Close()
	b.Close()
}

// RestoreCommand is a test wrapper for main.RestoreCommand.
type RestoreCommand struct {
	*main.RestoreCommand
	Stderr bytes.Buffer
}

// NewRestoreCommand returns a new instance of RestoreCommand.
func NewRestoreCommand() *RestoreCommand {
	cmd := &RestoreCommand{RestoreCommand: main.NewRestoreCommand()}
	cmd.RestoreCommand.Stderr = &cmd.Stderr
	return cmd
}

// MustReadFile reads data from a file. Panic on error.
func MustReadFile(filename string) []byte {
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err.Error())
	}
	return b
}
