package opentsdb_test

import (
	"fmt"
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
	"github.com/influxdata/influxdb/internal"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/opentsdb"
)

func Test_Service_OpenClose(t *testing.T) {
	service := NewService("db0", "127.0.0.1:45362")

	// Closing a closed service is fine.
	if err := service.Service.Close(); err != nil {
		t.Fatal(err)
	}

	// Closing a closed service again is fine.
	if err := service.Service.Close(); err != nil {
		t.Fatal(err)
	}

	if err := service.Service.Open(); err != nil {
		t.Fatal(err)
	}

	// Opening an already open service is fine.
	if err := service.Service.Open(); err != nil {
		t.Fatal(err)
	}

	// Reopening a previously opened service is fine.
	if err := service.Service.Close(); err != nil {
		t.Fatal(err)
	}

	if err := service.Service.Open(); err != nil {
		t.Fatal(err)
	}

	// Tidy up.
	if err := service.Service.Close(); err != nil {
		t.Fatal(err)
	}
}

// Ensure a point can be written via the telnet protocol.
func TestService_Telnet(t *testing.T) {
	t.Parallel()

	s := NewService("db0", "127.0.0.1:0")
	if err := s.Service.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Service.Close()

	// Mock points writer.
	var called int32
	s.WritePointsFn = func(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error {
		atomic.StoreInt32(&called, 1)

		if database != "db0" {
			t.Fatalf("unexpected database: %s", database)
		} else if retentionPolicy != "" {
			t.Fatalf("unexpected retention policy: %s", retentionPolicy)
		} else if !reflect.DeepEqual(points, []models.Point{
			models.MustNewPoint(
				"sys.cpu.user",
				models.NewTags(map[string]string{"host": "webserver01", "cpu": "0"}),
				map[string]interface{}{"value": 42.5},
				time.Unix(1356998400, 0),
			),
		}) {
			t.Fatalf("unexpected points: %#v", points)
		}
		return nil
	}

	// Open connection to the service.
	conn, err := net.Dial("tcp", s.Service.Addr().String())
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

	s := NewService("db0", "127.0.0.1:0")
	if err := s.Service.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Service.Close()

	// Mock points writer.
	var called bool
	s.WritePointsFn = func(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error {
		called = true
		if database != "db0" {
			t.Fatalf("unexpected database: %s", database)
		} else if retentionPolicy != "" {
			t.Fatalf("unexpected retention policy: %s", retentionPolicy)
		} else if !reflect.DeepEqual(points, []models.Point{
			models.MustNewPoint(
				"sys.cpu.nice",
				models.NewTags(map[string]string{"dc": "lga", "host": "web01"}),
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
	resp, err := http.Post("http://"+s.Service.Addr().String()+"/api/put", "application/json", strings.NewReader(`{"metric":"sys.cpu.nice", "timestamp":1346846400, "value":18, "tags":{"host":"web01", "dc":"lga"}}`))
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
	Service       *opentsdb.Service
	MetaClient    *internal.MetaClientMock
	WritePointsFn func(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
}

// NewService returns a new instance of Service.
func NewService(database string, bind string) *Service {
	s, err := opentsdb.NewService(opentsdb.Config{
		BindAddress:      bind,
		Database:         database,
		ConsistencyLevel: "one",
	})

	if err != nil {
		panic(err)
	}

	service := &Service{
		Service:    s,
		MetaClient: &internal.MetaClientMock{},
	}

	service.MetaClient.CreateDatabaseFn = func(db string) (*meta.DatabaseInfo, error) {
		if got, exp := db, database; got != exp {
			return nil, fmt.Errorf("got %v, expected %v", got, exp)
		}
		return nil, nil
	}

	if !testing.Verbose() {
		service.Service.Logger = log.New(ioutil.Discard, "", log.LstdFlags)
	}

	service.Service.MetaClient = service.MetaClient
	service.Service.PointsWriter = service
	return service
}

func (s *Service) WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error {
	return s.WritePointsFn(database, retentionPolicy, consistencyLevel, points)
}
