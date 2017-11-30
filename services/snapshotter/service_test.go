package snapshotter_test

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/influxdata/influxdb/internal"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/snapshotter"
	"github.com/influxdata/influxdb/tcp"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

var data = meta.Data{
	Databases: []meta.DatabaseInfo{
		{
			Name: "db0",
			DefaultRetentionPolicy: "autogen",
			RetentionPolicies: []meta.RetentionPolicyInfo{
				{
					Name:               "rp0",
					ReplicaN:           1,
					Duration:           24 * 7 * time.Hour,
					ShardGroupDuration: 24 * time.Hour,
					ShardGroups: []meta.ShardGroupInfo{
						{
							ID:        1,
							StartTime: time.Unix(0, 0).UTC(),
							EndTime:   time.Unix(0, 0).UTC().Add(24 * time.Hour),
							Shards: []meta.ShardInfo{
								{ID: 2},
							},
						},
					},
				},
				{
					Name:               "autogen",
					ReplicaN:           1,
					ShardGroupDuration: 24 * 7 * time.Hour,
					ShardGroups: []meta.ShardGroupInfo{
						{
							ID:        3,
							StartTime: time.Unix(0, 0).UTC(),
							EndTime:   time.Unix(0, 0).UTC().Add(24 * time.Hour),
							Shards: []meta.ShardInfo{
								{ID: 4},
							},
						},
					},
				},
			},
		},
	},
	Users: []meta.UserInfo{
		{
			Name:       "admin",
			Hash:       "abcxyz",
			Admin:      true,
			Privileges: map[string]influxql.Privilege{},
		},
	},
}

func init() {
	// Set the admin privilege on the user using this method so the meta.Data's check for
	// an admin user is set properly.
	data.SetAdminPrivilege("admin", true)
}

func TestSnapshotter_Open(t *testing.T) {
	s, l, err := NewTestService()
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	if err := s.Open(); err != nil {
		t.Fatalf("unexpected open error: %s", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("unexpected close error: %s", err)
	}
}

func TestSnapshotter_RequestShardBackup(t *testing.T) {
	s, l, err := NewTestService()
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	var tsdb internal.TSDBStoreMock
	tsdb.BackupShardFn = func(id uint64, since time.Time, w io.Writer) error {
		if id != 5 {
			t.Errorf("unexpected shard id: got=%#v want=%#v", id, 5)
		}
		if got, want := since, time.Unix(0, 0).UTC(); !got.Equal(want) {
			t.Errorf("unexpected time since: got=%#v want=%#v", got, want)
		}
		// Write some nonsense data so we can check that it gets returned.
		w.Write([]byte(`{"status":"ok"}`))
		return nil
	}
	s.TSDBStore = &tsdb

	if err := s.Open(); err != nil {
		t.Fatalf("unexpected open error: %s", err)
	}
	defer s.Close()

	conn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Errorf("unexpected error: %s", err)
		return
	}
	defer conn.Close()

	req := snapshotter.Request{
		Type:    snapshotter.RequestShardBackup,
		ShardID: 5,
		Since:   time.Unix(0, 0),
	}
	conn.Write([]byte{snapshotter.MuxHeader})
	_, err = conn.Write([]byte{byte(req.Type)})
	if err != nil {
		t.Errorf("could not encode request type to conn: %v", err)
	}
	enc := json.NewEncoder(conn)
	if err := enc.Encode(&req); err != nil {
		t.Errorf("unable to encode request: %s", err)
		return
	}

	// Read the result.
	out, err := ioutil.ReadAll(conn)
	if err != nil {
		t.Errorf("unexpected error reading shard backup: %s", err)
		return
	}

	if got, want := string(out), `{"status":"ok"}`; got != want {
		t.Errorf("unexpected shard data: got=%#v want=%#v", got, want)
		return
	}
}

func TestSnapshotter_RequestMetastoreBackup(t *testing.T) {
	s, l, err := NewTestService()
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	s.MetaClient = &MetaClient{Data: data}
	if err := s.Open(); err != nil {
		t.Fatalf("unexpected open error: %s", err)
	}
	defer s.Close()

	conn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Errorf("unexpected error: %s", err)
		return
	}
	defer conn.Close()

	c := snapshotter.NewClient(l.Addr().String())
	if got, err := c.MetastoreBackup(); err != nil {
		t.Errorf("unable to obtain metastore backup: %s", err)
		return
	} else if want := &data; !reflect.DeepEqual(got, want) {
		t.Errorf("unexpected data backup:\n\ngot=%s\nwant=%s", spew.Sdump(got), spew.Sdump(want))
		return
	}
}

func TestSnapshotter_RequestDatabaseInfo(t *testing.T) {
	s, l, err := NewTestService()
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	var tsdbStore internal.TSDBStoreMock
	tsdbStore.ShardFn = func(id uint64) *tsdb.Shard {
		if id != 2 && id != 4 {
			t.Errorf("unexpected shard id: %d", id)
			return nil
		} else if id == 4 {
			return nil
		}
		return &tsdb.Shard{}
	}
	tsdbStore.ShardRelativePathFn = func(id uint64) (string, error) {
		if id == 2 {
			return "db0/rp0", nil
		} else if id == 4 {
			t.Errorf("unexpected relative path request for shard id: %d", id)
		}
		return "", fmt.Errorf("no such shard id: %d", id)
	}

	s.MetaClient = &MetaClient{Data: data}
	s.TSDBStore = &tsdbStore
	if err := s.Open(); err != nil {
		t.Fatalf("unexpected open error: %s", err)
	}
	defer s.Close()

	conn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Errorf("unexpected error: %s", err)
		return
	}
	defer conn.Close()

	req := snapshotter.Request{
		Type:           snapshotter.RequestDatabaseInfo,
		BackupDatabase: "db0",
	}
	conn.Write([]byte{snapshotter.MuxHeader})
	_, err = conn.Write([]byte{byte(req.Type)})
	if err != nil {
		t.Errorf("could not encode request type to conn: %v", err)
	}
	enc := json.NewEncoder(conn)
	if err := enc.Encode(&req); err != nil {
		t.Errorf("unable to encode request: %s", err)
		return
	}

	// Read the result.
	out, err := ioutil.ReadAll(conn)
	if err != nil {
		t.Errorf("unexpected error reading database info: %s", err)
		return
	}

	// Unmarshal the response.
	var resp snapshotter.Response
	if err := json.Unmarshal(out, &resp); err != nil {
		t.Errorf("error unmarshaling response: %s", err)
		return
	}

	if got, want := resp.Paths, []string{"db0/rp0"}; !reflect.DeepEqual(got, want) {
		t.Errorf("unexpected paths: got=%#v want=%#v", got, want)
	}
}

func TestSnapshotter_RequestDatabaseInfo_ErrDatabaseNotFound(t *testing.T) {
	s, l, err := NewTestService()
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	s.MetaClient = &MetaClient{Data: data}
	if err := s.Open(); err != nil {
		t.Fatalf("unexpected open error: %s", err)
	}
	defer s.Close()

	conn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Errorf("unexpected error: %s", err)
		return
	}
	defer conn.Close()

	req := snapshotter.Request{
		Type:           snapshotter.RequestDatabaseInfo,
		BackupDatabase: "doesnotexist",
	}
	conn.Write([]byte{snapshotter.MuxHeader})
	_, err = conn.Write([]byte{byte(req.Type)})
	if err != nil {
		t.Errorf("could not encode request type to conn: %v", err)
	}
	enc := json.NewEncoder(conn)
	if err := enc.Encode(&req); err != nil {
		t.Errorf("unable to encode request: %s", err)
		return
	}

	// Read the result.
	out, err := ioutil.ReadAll(conn)
	if err != nil {
		t.Errorf("unexpected error reading database info: %s", err)
		return
	}

	// There should be no response.
	if got, want := string(out), ""; got != want {
		t.Errorf("expected no message, got: %s", got)
	}
}

func TestSnapshotter_RequestRetentionPolicyInfo(t *testing.T) {
	s, l, err := NewTestService()
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	var tsdbStore internal.TSDBStoreMock
	tsdbStore.ShardFn = func(id uint64) *tsdb.Shard {
		if id != 2 {
			t.Errorf("unexpected shard id: %d", id)
			return nil
		}
		return &tsdb.Shard{}
	}
	tsdbStore.ShardRelativePathFn = func(id uint64) (string, error) {
		if id == 2 {
			return "db0/rp0", nil
		}
		return "", fmt.Errorf("no such shard id: %d", id)
	}

	s.MetaClient = &MetaClient{Data: data}
	s.TSDBStore = &tsdbStore
	if err := s.Open(); err != nil {
		t.Fatalf("unexpected open error: %s", err)
	}
	defer s.Close()

	conn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Errorf("unexpected error: %s", err)
		return
	}
	defer conn.Close()

	req := snapshotter.Request{
		Type:                  snapshotter.RequestRetentionPolicyInfo,
		BackupDatabase:        "db0",
		BackupRetentionPolicy: "rp0",
	}
	conn.Write([]byte{snapshotter.MuxHeader})
	_, err = conn.Write([]byte{byte(req.Type)})
	if err != nil {
		t.Errorf("could not encode request type to conn: %v", err)
	}
	enc := json.NewEncoder(conn)
	if err := enc.Encode(&req); err != nil {
		t.Errorf("unable to encode request: %s", err)
		return
	}

	// Read the result.
	out, err := ioutil.ReadAll(conn)
	if err != nil {
		t.Errorf("unexpected error reading database info: %s", err)
		return
	}

	// Unmarshal the response.
	var resp snapshotter.Response
	if err := json.Unmarshal(out, &resp); err != nil {
		t.Errorf("error unmarshaling response: %s", err)
		return
	}

	if got, want := resp.Paths, []string{"db0/rp0"}; !reflect.DeepEqual(got, want) {
		t.Errorf("unexpected paths: got=%#v want=%#v", got, want)
	}
}

func TestSnapshotter_InvalidRequest(t *testing.T) {
	s, l, err := NewTestService()
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	if err := s.Open(); err != nil {
		t.Fatalf("unexpected open error: %s", err)
	}
	defer s.Close()

	conn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Errorf("unexpected error: %s", err)
		return
	}
	defer conn.Close()

	conn.Write([]byte{snapshotter.MuxHeader})
	conn.Write([]byte(`["invalid request"]`))

	// Read the result.
	out, err := ioutil.ReadAll(conn)
	if err != nil {
		t.Errorf("unexpected error reading database info: %s", err)
		return
	}

	// There should be no response.
	if got, want := string(out), ""; got != want {
		t.Errorf("expected no message, got: %s", got)
	}
}

func NewTestService() (*snapshotter.Service, net.Listener, error) {
	s := snapshotter.NewService()
	s.WithLogger(logger.New(os.Stderr))

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}

	// The snapshotter needs to be used with a tcp.Mux listener.
	mux := tcp.NewMux()
	go mux.Serve(l)

	s.Listener = mux.Listen(snapshotter.MuxHeader)
	return s, l, nil
}

type MetaClient struct {
	Data meta.Data
}

func (m *MetaClient) MarshalBinary() ([]byte, error) {
	return m.Data.MarshalBinary()
}

func (m *MetaClient) Database(name string) *meta.DatabaseInfo {
	for _, dbi := range m.Data.Databases {
		if dbi.Name == name {
			return &dbi
		}
	}
	return nil
}
