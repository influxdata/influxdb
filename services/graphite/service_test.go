package graphite_test

import (
	"fmt"
	"io/ioutil"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/internal"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/graphite"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/toml"
)

func Test_Service_OpenClose(t *testing.T) {
	c := graphite.Config{BindAddress: ":35422"}
	service := NewService(&c)

	// Closing a closed service is fine.
	if err := service.GraphiteService.Close(); err != nil {
		t.Fatal(err)
	}

	// Closing a closed service again is fine.
	if err := service.GraphiteService.Close(); err != nil {
		t.Fatal(err)
	}

	if err := service.GraphiteService.Open(); err != nil {
		t.Fatal(err)
	}

	// Opening an already open service is fine.
	if err := service.GraphiteService.Open(); err != nil {
		t.Fatal(err)
	}

	// Reopening a previously opened service is fine.
	if err := service.GraphiteService.Close(); err != nil {
		t.Fatal(err)
	}

	if err := service.GraphiteService.Open(); err != nil {
		t.Fatal(err)
	}

	// Tidy up.
	if err := service.GraphiteService.Close(); err != nil {
		t.Fatal(err)
	}
}

func Test_Service_TCP(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Round(time.Second)

	config := graphite.Config{}
	config.Database = "graphitedb"
	config.BatchSize = 0 // No batching.
	config.BatchTimeout = toml.Duration(time.Second)
	config.BindAddress = ":0"

	service := NewService(&config)

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

	var created bool
	service.MetaClient.CreateDatabaseWithRetentionPolicyFn = func(db string, _ *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error) {
		if db != config.Database {
			t.Fatalf("got %s, expected %s", db, config.Database)
		}
		created = true
		return nil, nil
	}

	if err := service.GraphiteService.Open(); err != nil {
		t.Fatalf("failed to open Graphite service: %s", err.Error())
	}

	if !created {
		t.Fatalf("failed to create target database")
	}

	// Connect to the graphite endpoint we just spun up
	_, port, _ := net.SplitHostPort(service.GraphiteService.Addr().String())
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

	service := NewService(&config)

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

	var created bool
	service.MetaClient.CreateDatabaseWithRetentionPolicyFn = func(db string, _ *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error) {
		if db != config.Database {
			t.Fatalf("got %s, expected %s", db, config.Database)
		}
		created = true
		return nil, nil
	}

	if err := service.GraphiteService.Open(); err != nil {
		t.Fatalf("failed to open Graphite service: %s", err.Error())
	}

	if !created {
		t.Fatalf("failed to create target database")
	}

	// Connect to the graphite endpoint we just spun up
	_, port, _ := net.SplitHostPort(service.GraphiteService.Addr().String())
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

type Service struct {
	GraphiteService *graphite.Service

	MetaClient    *internal.MetaClientMock
	WritePointsFn func(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
}

func NewService(c *graphite.Config) *Service {
	if c == nil {
		defaultC := graphite.NewConfig()
		c = &defaultC
	}

	gservice, err := graphite.NewService(*c)
	if err != nil {
		panic(err)
	}

	service := &Service{
		GraphiteService: gservice,
		MetaClient:      &internal.MetaClientMock{},
	}

	service.MetaClient.CreateRetentionPolicyFn = func(string, *meta.RetentionPolicySpec) (*meta.RetentionPolicyInfo, error) {
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

	// Set the Meta Client
	service.GraphiteService.MetaClient = service.MetaClient

	// Set the PointsWriter
	service.GraphiteService.PointsWriter = service

	if !testing.Verbose() {
		service.GraphiteService.SetLogOutput(ioutil.Discard)
	}
	return service
}

func (s *Service) WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error {
	return s.WritePointsFn(database, retentionPolicy, consistencyLevel, points)
}

// Test Helpers
func errstr(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}
