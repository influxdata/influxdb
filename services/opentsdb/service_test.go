package opentsdb_test

import (
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/opentsdb"
)

// Ensure a point can be written via the telnet protocol.
func TestService_Telnet(t *testing.T) {
	t.Parallel()

	s := NewService("db0")
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Mock points writer.
	var called int32
	s.PointsWriter.WritePointsFn = func(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error {
		atomic.StoreInt32(&called, 1)

		if database != "db0" {
			t.Fatalf("unexpected database: %s", database)
		} else if retentionPolicy != "" {
			t.Fatalf("unexpected retention policy: %s", retentionPolicy)
		} else if !reflect.DeepEqual(points, []models.Point{
			models.MustNewPoint(
				"sys.cpu.user",
				map[string]string{"host": "webserver01", "cpu": "0"},
				map[string]interface{}{"value": 42.5},
				time.Unix(1356998400, 0),
			),
		}) {
			t.Fatalf("unexpected points: %#v", points)
		}
		return nil
	}

	// Open connection to the service.
	conn, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Write telnet data and close.
	if _, err := conn.Write([]byte("put sys.cpu.user 1356998400 42.5 host=webserver01 cpu=0")); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}

	tick := time.Tick(10 * time.Millisecond)
	timeout := time.After(10 * time.Second)

	for {
		select {
		case <-tick:
			// Verify that the writer was called.
			if atomic.LoadInt32(&called) > 0 {
				return
			}
		case <-timeout:
			t.Fatal("points writer not called")
		}
	}
}

// Ensure a point can be written via the HTTP protocol.
func TestService_HTTP(t *testing.T) {
	t.Parallel()

	s := NewService("db0")
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Mock points writer.
	var called bool
	s.PointsWriter.WritePointsFn = func(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error {
		called = true
		if database != "db0" {
			t.Fatalf("unexpected database: %s", database)
		} else if retentionPolicy != "" {
			t.Fatalf("unexpected retention policy: %s", retentionPolicy)
		} else if !reflect.DeepEqual(points, []models.Point{
			models.MustNewPoint(
				"sys.cpu.nice",
				map[string]string{"dc": "lga", "host": "web01"},
				map[string]interface{}{"value": 18.0},
				time.Unix(1346846400, 0),
			),
		}) {
			spew.Dump(points)
			t.Fatalf("unexpected points: %#v", points)
		}
		return nil
	}

	// Write HTTP request to server.
	resp, err := http.Post("http://"+s.Addr().String()+"/api/put", "application/json", strings.NewReader(`{"metric":"sys.cpu.nice", "timestamp":1346846400, "value":18, "tags":{"host":"web01", "dc":"lga"}}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Verify status and body.
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("unexpected status code: %d", resp.StatusCode)
	}

	// Verify that the writer was called.
	if !called {
		t.Fatal("points writer not called")
	}
}

type Service struct {
	*opentsdb.Service
	PointsWriter PointsWriter
}

// NewService returns a new instance of Service.
func NewService(database string) *Service {
	srv, _ := opentsdb.NewService(opentsdb.Config{
		BindAddress:      "127.0.0.1:0",
		Database:         database,
		ConsistencyLevel: "one",
	})
	s := &Service{Service: srv}
	s.Service.PointsWriter = &s.PointsWriter
	s.Service.MetaClient = &DatabaseCreator{}

	if !testing.Verbose() {
		s.Logger = log.New(ioutil.Discard, "", log.LstdFlags)
	}

	return s
}

// PointsWriter represents a mock impl of PointsWriter.
type PointsWriter struct {
	WritePointsFn func(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
}

func (w *PointsWriter) WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error {
	return w.WritePointsFn(database, retentionPolicy, consistencyLevel, points)
}

type DatabaseCreator struct {
}

func (d *DatabaseCreator) CreateDatabase(name string) (*meta.DatabaseInfo, error) {
	return nil, nil
}
