package graphite_test

import (
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/diagnostic"
	"github.com/influxdata/influxdb/internal"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/graphite"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/toml"
)

func Test_Service_OpenClose(t *testing.T) {
	// Let the OS assign a random port since we are only opening and closing the service,
	// not actually connecting to it.
	c := graphite.Config{BindAddress: "127.0.0.1:0"}
	service := NewTestService(&c)

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

func TestService_CreatesDatabase(t *testing.T) {
	t.Parallel()

	config := graphite.NewConfig()
	s := NewTestService(&config)
	s.WritePointsFn = func(string, string, models.ConsistencyLevel, []models.Point) error {
		return nil
	}

	called := make(chan struct{})
	s.MetaClient.CreateDatabaseWithRetentionPolicyFn = func(name string, _ *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error) {
		if name != config.Database {
			t.Errorf("\n\texp = %s\n\tgot = %s\n", config.Database, name)
		}
		// Allow some time for the caller to return and the ready status to
		// be set.
		time.AfterFunc(10*time.Millisecond, func() { called <- struct{}{} })
		return nil, errors.New("an error")
	}

	if err := s.Service.Open(); err != nil {
		t.Fatal(err)
	}

	points, err := models.ParsePointsString(`cpu value=1`)
	if err != nil {
		t.Fatal(err)
	}

	batcher := s.Service.Batcher()
	batcher.In() <- points[0] // Send a point.
	batcher.Flush()
	select {
	case <-called:
		// OK
	case <-time.NewTimer(5 * time.Second).C:
		t.Fatal("Service should have attempted to create database")
	}

	// ready status should not have been switched due to meta client error.
	ready := s.Service.Ready()

	if got, exp := ready, false; got != exp {
		t.Fatalf("got %v, expected %v", got, exp)
	}

	// This time MC won't cause an error.
	s.MetaClient.CreateDatabaseWithRetentionPolicyFn = func(name string, _ *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error) {
		// Allow some time for the caller to return and the ready status to
		// be set.
		time.AfterFunc(10*time.Millisecond, func() { called <- struct{}{} })
		return nil, nil
	}

	batcher.In() <- points[0] // Send a point.
	batcher.Flush()
	select {
	case <-called:
		// OK
	case <-time.NewTimer(5 * time.Second).C:
		t.Fatal("Service should have attempted to create database")
	}

	// ready status should now be true.
	ready = s.Service.Ready()

	if got, exp := ready, true; got != exp {
		t.Fatalf("got %v, expected %v", got, exp)
	}

	s.Service.Close()
}

func Test_Service_TCP(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Round(time.Second)

	config := graphite.Config{}
	config.Database = "graphitedb"
	config.BatchSize = 0 // No batching.
	config.BatchTimeout = toml.Duration(time.Second)
	config.BindAddress = ":0"

	service := NewTestService(&config)

	// Allow test to wait until points are written.
	var wg sync.WaitGroup
	wg.Add(1)

	service.WritePointsFn = func(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error {
		defer wg.Done()

		pt, _ := models.NewPoint(
			"cpu",
			models.NewTags(map[string]string{}),
			map[string]interface{}{"value": 23.456},
			time.Unix(now.Unix(), 0))

		if database != "graphitedb" {
			t.Fatalf("unexpected database: %s", database)
		} else if retentionPolicy != "" {
			t.Fatalf("unexpected retention policy: %s", retentionPolicy)
		} else if len(points) != 1 {
			t.Fatalf("expected 1 point, got %d", len(points))
		} else if points[0].String() != pt.String() {
			t.Fatalf("expected point %v, got %v", pt.String(), points[0].String())
		}
		return nil
	}

	if err := service.Service.Open(); err != nil {
		t.Fatalf("failed to open Graphite service: %s", err.Error())
	}

	// Connect to the graphite endpoint we just spun up
	_, port, _ := net.SplitHostPort(service.Service.Addr().String())
	conn, err := net.Dial("tcp", "127.0.0.1:"+port)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte(`cpu 23.456 `)
	data = append(data, []byte(fmt.Sprintf("%d", now.Unix()))...)
	data = append(data, '\n')
	data = append(data, []byte(`memory NaN `)...)
	data = append(data, []byte(fmt.Sprintf("%d", now.Unix()))...)
	data = append(data, '\n')
	_, err = conn.Write(data)
	conn.Close()
	if err != nil {
		t.Fatal(err)
	}

	wg.Wait()
}

func Test_Service_UDP(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Round(time.Second)

	config := graphite.Config{}
	config.Database = "graphitedb"
	config.BatchSize = 0 // No batching.
	config.BatchTimeout = toml.Duration(time.Second)
	config.BindAddress = ":10000"
	config.Protocol = "udp"

	service := NewTestService(&config)

	// Allow test to wait until points are written.
	var wg sync.WaitGroup
	wg.Add(1)

	service.WritePointsFn = func(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error {
		defer wg.Done()

		pt, _ := models.NewPoint(
			"cpu",
			models.NewTags(map[string]string{}),
			map[string]interface{}{"value": 23.456},
			time.Unix(now.Unix(), 0))
		if database != "graphitedb" {
			t.Fatalf("unexpected database: %s", database)
		} else if retentionPolicy != "" {
			t.Fatalf("unexpected retention policy: %s", retentionPolicy)
		} else if points[0].String() != pt.String() {
			t.Fatalf("unexpected points: %#v", points[0].String())
		}
		return nil
	}

	if err := service.Service.Open(); err != nil {
		t.Fatalf("failed to open Graphite service: %s", err.Error())
	}

	// Connect to the graphite endpoint we just spun up
	_, port, _ := net.SplitHostPort(service.Service.Addr().String())
	conn, err := net.Dial("udp", "127.0.0.1:"+port)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte(`cpu 23.456 `)
	data = append(data, []byte(fmt.Sprintf("%d", now.Unix()))...)
	data = append(data, '\n')
	_, err = conn.Write(data)
	if err != nil {
		t.Fatal(err)
	}

	wg.Wait()
	conn.Close()
}

type TestService struct {
	Service       *graphite.Service
	MetaClient    *internal.MetaClientMock
	WritePointsFn func(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
}

func NewTestService(c *graphite.Config) *TestService {
	if c == nil {
		defaultC := graphite.NewConfig()
		c = &defaultC
	}

	gservice, err := graphite.NewService(*c)
	if err != nil {
		panic(err)
	}

	service := &TestService{
		Service:    gservice,
		MetaClient: &internal.MetaClientMock{},
	}

	service.MetaClient.CreateRetentionPolicyFn = func(string, *meta.RetentionPolicySpec, bool) (*meta.RetentionPolicyInfo, error) {
		return nil, nil
	}

	service.MetaClient.CreateDatabaseWithRetentionPolicyFn = func(string, *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error) {
		return nil, nil
	}

	service.MetaClient.DatabaseFn = func(string) *meta.DatabaseInfo {
		return nil
	}

	service.MetaClient.RetentionPolicyFn = func(string, string) (*meta.RetentionPolicyInfo, error) {
		return nil, nil
	}

	if testing.Verbose() {
		diag := diagnostic.New(os.Stderr)
		service.Service.With(diag.GraphiteContext())
	}

	// Set the Meta Client and PointsWriter.
	service.Service.MetaClient = service.MetaClient
	service.Service.PointsWriter = service

	return service
}

func (s *TestService) WritePointsPrivileged(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error {
	return s.WritePointsFn(database, retentionPolicy, consistencyLevel, points)
}
