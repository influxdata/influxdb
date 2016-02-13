package run_test

import (
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/influxdata/influxdb/cmd/influxd/backup"
	"github.com/influxdata/influxdb/cmd/influxd/restore"
	"github.com/influxdata/influxdb/cmd/influxd/run"
	"github.com/influxdata/influxdb/services/meta"
)

func TestServer_BackupAndRestore(t *testing.T) {
	config := NewConfig()
	config.Data.Engine = "tsm1"
	config.Data.Dir, _ = ioutil.TempDir("", "data_backup")
	config.Meta.Dir, _ = ioutil.TempDir("", "meta_backup")
	config.Meta.BindAddress = freePort()
	config.Meta.HTTPBindAddress = freePort()

	backupDir, _ := ioutil.TempDir("", "backup")
	defer os.RemoveAll(backupDir)

	db := "mydb"
	rp := "forever"
	expected := `{"results":[{"series":[{"name":"myseries","columns":["time","host","value"],"values":[["1970-01-01T00:00:00.001Z","A",23]]}]}]}`

	// set the cache snapshot size low so that a single point will cause TSM file creation
	config.Data.CacheSnapshotMemorySize = 1

	func() {
		s := OpenServer(config, "")
		defer s.Close()

		if err := s.CreateDatabaseAndRetentionPolicy(db, newRetentionPolicyInfo(rp, 1, 0)); err != nil {
			t.Fatal(err)
		}
		if err := s.MetaClient.SetDefaultRetentionPolicy(db, rp); err != nil {
			t.Fatal(err)
		}

		s.MustWrite(db, rp, "myseries,host=A value=23 1000000", nil)

		// wait for the snapshot to write
		time.Sleep(time.Second)

		res, err := s.Query(`select * from "mydb"."forever"."myseries"`)
		if err != nil {
			t.Fatalf("error querying: %s", err.Error())
		}
		if res != expected {
			t.Fatalf("query results wrong:\n\texp: %s\n\tgot: %s", expected, res)
		}

		// now backup
		cmd := backup.NewCommand()
		hostAddress, _ := meta.DefaultHost(run.DefaultHostname, config.Meta.BindAddress)
		if err := cmd.Run("-host", hostAddress, "-database", "mydb", backupDir); err != nil {
			t.Fatalf("error backing up: %s", err.Error())
		}
	}()

	if _, err := os.Stat(config.Meta.Dir); err == nil || !os.IsNotExist(err) {
		t.Fatalf("meta dir should be deleted")
	}

	if _, err := os.Stat(config.Data.Dir); err == nil || !os.IsNotExist(err) {
		t.Fatalf("meta dir should be deleted")
	}

	// restore
	cmd := restore.NewCommand()
	cmd.MetaConfig.BindAddress = config.Meta.BindAddress

	if err := cmd.Run("-metadir", config.Meta.Dir, "-datadir", config.Data.Dir, "-database", "mydb", backupDir); err != nil {
		t.Fatalf("error restoring: %s", err.Error())
	}

	// Make sure node.json was restored
	nodePath := filepath.Join(config.Meta.Dir, "node.json")
	if _, err := os.Stat(nodePath); err != nil || os.IsNotExist(err) {
		t.Fatalf("node.json should exist")
	}

	// now open it up and verify we're good
	s := OpenServer(config, "")
	defer s.Close()

	res, err := s.Query(`select * from "mydb"."forever"."myseries"`)
	if err != nil {
		t.Fatalf("error querying: %s", err.Error())
	}
	if res != expected {
		t.Fatalf("query results wrong:\n\texp: %s\n\tgot: %s", expected, res)
	}
}

func freePort() string {
	l, _ := net.Listen("tcp", "")
	defer l.Close()
	return l.Addr().String()
}
