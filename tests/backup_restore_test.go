package tests

import (
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"fmt"

	"github.com/influxdata/influxdb/cmd/influxd/backup"
	"github.com/influxdata/influxdb/cmd/influxd/restore"
	"github.com/influxdata/influxdb/toml"
	"strings"
)

func TestServer_BackupAndRestore(t *testing.T) {
	config := NewConfig()
	config.Data.Engine = "tsm1"
	config.BindAddress = freePort()
	config.Monitor.StoreEnabled = true
	config.Monitor.StoreInterval = toml.Duration(time.Second)

	fullBackupDir, _ := ioutil.TempDir("", "backup")
	defer os.RemoveAll(fullBackupDir)

	partialBackupDir, _ := ioutil.TempDir("", "backup")
	defer os.RemoveAll(partialBackupDir)

	portableBackupDir, _ := ioutil.TempDir("", "backup")
	defer os.RemoveAll(portableBackupDir)

	db := "mydb"
	rp := "forever"

	expected := `{"results":[{"statement_id":0,"series":[{"name":"myseries","columns":["time","host","value"],"values":[["1970-01-01T00:00:00.001Z","A",23],["1970-01-01T00:00:00.005Z","B",24],["1970-01-01T00:00:00.006Z","C",22],["1970-01-01T00:00:00.007Z","C",23],["1970-01-01T00:00:00.008Z","C",24],["1970-01-01T00:00:00.009000001Z","D",24],["1970-01-01T00:00:00.009000002Z","D",25],["1970-01-01T00:00:00.009000003Z","D",26]]}]}]}`
	partialExpected := `{"results":[{"statement_id":0,"series":[{"name":"myseries","columns":["time","host","value"],"values":[["1970-01-01T00:00:00.001Z","A",23],["1970-01-01T00:00:00.005Z","B",24],["1970-01-01T00:00:00.006Z","C",22],["1970-01-01T00:00:00.007Z","C",23],["1970-01-01T00:00:00.008Z","C",24]]}]}]}`
	// set the cache snapshot size low so that a single point will cause TSM file creation
	config.Data.CacheSnapshotMemorySize = 1

	func() {
		s := OpenServer(config)
		defer s.Close()

		if _, ok := s.(*RemoteServer); ok {
			t.Skip("Skipping.  Cannot modify remote server config")
		}

		if err := s.CreateDatabaseAndRetentionPolicy(db, NewRetentionPolicySpec(rp, 1, 0), true); err != nil {
			t.Fatal(err)
		}

		if _, err := s.Write(db, rp, "myseries,host=A value=23 1000000", nil); err != nil {
			t.Fatalf("failed to write: %s", err)
		}

		// wait for the snapshot to write
		time.Sleep(time.Second)

		if _, err := s.Write(db, rp, "myseries,host=B value=24 5000000", nil); err != nil {
			t.Fatalf("failed to write: %s", err)
		}

		// wait for the snapshot to write
		time.Sleep(time.Second)

		if _, err := s.Write(db, rp, "myseries,host=C value=22 6000000", nil); err != nil {
			t.Fatalf("failed to write: %s", err)
		}

		if _, err := s.Write(db, rp, "myseries,host=C value=23 7000000", nil); err != nil {
			t.Fatalf("failed to write: %s", err)
		}

		if _, err := s.Write(db, rp, "myseries,host=C value=24 8000000", nil); err != nil {
			t.Fatalf("failed to write: %s", err)
		}

		if _, err := s.Write(db, rp, "myseries,host=D value=24 9000001", nil); err != nil {
			t.Fatalf("failed to write: %s", err)
		}

		if _, err := s.Write(db, rp, "myseries,host=D value=25 9000002", nil); err != nil {
			t.Fatalf("failed to write: %s", err)
		}

		if _, err := s.Write(db, rp, "myseries,host=D value=26 9000003", nil); err != nil {
			t.Fatalf("failed to write: %s", err)
		}

		// wait for the snapshot to write
		time.Sleep(time.Second)

		if _, err := s.Query(`show series on mydb; show retention policies on mydb`); err != nil {
			t.Fatalf("error querying: %s", err.Error())
		}

		res, err := s.Query(`select * from "mydb"."forever"."myseries"`)
		if err != nil {
			t.Fatalf("error querying: %s", err.Error())
		}
		if res != expected {
			t.Fatalf("query results wrong:\n\texp: %s\n\tgot: %s", expected, res)
		}

		i := 0
		for {
			res, err = s.Query(`SHOW DATABASES`)
			if err != nil {
				t.Fatalf("error querying: %s", err.Error())
			}

			if strings.Contains(res, "_internal") {
				break
			}
			i++
			if i > 90 {
				t.Fatal("_internal not created within 90 seconds")
			}
			// technically not necessary, but no reason to crush the CPU for polling
			time.Sleep(time.Second)
		}

		// now backup
		cmd := backup.NewCommand()
		_, port, err := net.SplitHostPort(config.BindAddress)
		if err != nil {
			t.Fatal(err)
		}
		hostAddress := net.JoinHostPort("localhost", port)
		if err := cmd.Run("-host", hostAddress, "-database", "mydb", fullBackupDir); err != nil {
			t.Fatalf("error backing up: %s, hostAddress: %s", err.Error(), hostAddress)
		}

		time.Sleep(time.Second)
		if err := cmd.Run("-host", hostAddress, "-database", "mydb", "-start", "1970-01-01T00:00:00.001Z", "-end", "1970-01-01T00:00:00.009Z", partialBackupDir); err != nil {
			t.Fatalf("error backing up: %s, hostAddress: %s", err.Error(), hostAddress)
		}

		// also testing short-form flag here
		if err := cmd.Run("-portable", "-host", hostAddress, "-db", "mydb", "-start", "1970-01-01T00:00:00.001Z", "-end", "1970-01-01T00:00:00.009Z", portableBackupDir); err != nil {
			t.Fatalf("error backing up: %s, hostAddress: %s", err.Error(), hostAddress)
		}

	}()

	if _, err := os.Stat(config.Meta.Dir); err == nil || !os.IsNotExist(err) {
		t.Fatalf("meta dir should be deleted")
	}

	if _, err := os.Stat(config.Data.Dir); err == nil || !os.IsNotExist(err) {
		t.Fatalf("meta dir should be deleted")
	}

	// if doing a real restore, these dirs should exist in the real DB.
	if err := os.MkdirAll(config.Data.Dir, 0777); err != nil {
		t.Fatalf("error making restore dir: %s", err.Error())
	}
	if err := os.MkdirAll(config.Meta.Dir, 0777); err != nil {
		t.Fatalf("error making restore dir: %s", err.Error())
	}

	// restore
	cmd := restore.NewCommand()

	if err := cmd.Run("-metadir", config.Meta.Dir, "-datadir", config.Data.Dir, "-database", "mydb", fullBackupDir); err != nil {
		t.Fatalf("error restoring: %s", err.Error())
	}

	// Make sure node.json was restored
	nodePath := filepath.Join(config.Meta.Dir, "node.json")
	if _, err := os.Stat(nodePath); err != nil || os.IsNotExist(err) {
		t.Fatalf("node.json should exist")
	}

	// now open it up and verify we're good
	s := OpenServer(config)
	defer s.Close()

	// 1.  offline restore is correct
	res, err := s.Query(`select * from "mydb"."forever"."myseries"`)
	if err != nil {
		t.Fatalf("error querying: %s", err.Error())
	}
	if res != expected {
		t.Fatalf("query results wrong:\n\texp: %s\n\tgot: %s", expected, res)
	}

	_, port, err := net.SplitHostPort(config.BindAddress)
	if err != nil {
		t.Fatal(err)
	}

	// 2.  online restore of a partial backup is correct.
	hostAddress := net.JoinHostPort("localhost", port)
	cmd.Run("-host", hostAddress, "-online", "-newdb", "mydbbak", "-db", "mydb", partialBackupDir)

	// wait for the import to finish, and unlock the shard engine.
	time.Sleep(time.Second)

	res, err = s.Query(`select * from "mydbbak"."forever"."myseries"`)
	if err != nil {
		t.Fatalf("error querying: %s", err.Error())
	}

	if res != partialExpected {
		t.Fatalf("query results wrong:\n\texp: %s\n\tgot: %s", partialExpected, res)
	}

	// 3. portable should be the same as the non-portable live restore
	cmd.Run("-host", hostAddress, "-portable", "-newdb", "mydbbak2", "-db", "mydb", portableBackupDir)

	// wait for the import to finish, and unlock the shard engine.
	time.Sleep(time.Second)

	res, err = s.Query(`select * from "mydbbak2"."forever"."myseries"`)
	if err != nil {
		t.Fatalf("error querying: %s", err.Error())
	}

	if res != partialExpected {
		t.Fatalf("query results wrong:\n\texp: %s\n\tgot: %s", partialExpected, res)
	}

	// 4.  backup all DB's, then drop them, then restore them and all 3 above tests should pass again.
	// now backup
	bCmd := backup.NewCommand()

	if err := bCmd.Run("-portable", "-host", hostAddress, portableBackupDir); err != nil {
		t.Fatalf("error backing up: %s, hostAddress: %s", err.Error(), hostAddress)
	}

	_, err = s.Query(`drop database mydb; drop database mydbbak; drop database mydbbak2;`)
	if err != nil {
		t.Fatalf("Error dropping databases %s", err.Error())
	}

	// 3. portable should be the same as the non-portable live restore
	cmd.Run("-host", hostAddress, "-portable", portableBackupDir)

	// wait for the import to finish, and unlock the shard engine.
	time.Sleep(3 * time.Second)

	res, err = s.Query(`show shards`)
	if err != nil {
		t.Fatalf("error querying: %s", err.Error())
	}
	fmt.Println(res)

	res, err = s.Query(`select * from "mydbbak"."forever"."myseries"`)
	if err != nil {
		t.Fatalf("error querying: %s", err.Error())
	}

	if res != partialExpected {
		t.Fatalf("query results wrong:\n\texp: %s\n\tgot: %s", partialExpected, res)
	}

	res, err = s.Query(`select * from "mydbbak2"."forever"."myseries"`)
	if err != nil {
		t.Fatalf("error querying: %s", err.Error())
	}

	if res != partialExpected {
		t.Fatalf("query results wrong:\n\texp: %s\n\tgot: %s", partialExpected, res)
	}

	res, err = s.Query(`select * from "mydb"."forever"."myseries"`)
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
