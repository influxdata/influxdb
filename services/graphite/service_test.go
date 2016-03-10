package graphite_test

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/graphite"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/toml"
)

func Test_ServerGraphiteTCP(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Round(time.Second)

	config := graphite.Config{}
	config.Database = "graphitedb"
	config.BatchSize = 0 // No batching.
	config.BatchTimeout = toml.Duration(time.Second)
	config.BindAddress = ":0"

	service, err := graphite.NewService(config)
	if err != nil {
		t.Fatalf("failed to create Graphite service: %s", err.Error())
	}

	// Allow test to wait until points are written.
	var wg sync.WaitGroup
	wg.Add(1)

	pointsWriter := PointsWriter{
		WritePointsFn: func(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error {
			defer wg.Done()

			pt, _ := models.NewPoint(
				"cpu",
				map[string]string{},
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
		},
	}
	service.PointsWriter = &pointsWriter
	dbCreator := DatabaseCreator{}
	service.MetaClient = &dbCreator

	if err := service.Open(); err != nil {
		t.Fatalf("failed to open Graphite service: %s", err.Error())
	}

	if !dbCreator.Created {
		t.Fatalf("failed to create target database")
	}

	// Connect to the graphite endpoint we just spun up
	_, port, _ := net.SplitHostPort(service.Addr().String())
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

func Test_ServerGraphiteUDP(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Round(time.Second)

	config := graphite.Config{}
	config.Database = "graphitedb"
	config.BatchSize = 0 // No batching.
	config.BatchTimeout = toml.Duration(time.Second)
	config.BindAddress = ":10000"
	config.Protocol = "udp"

	service, err := graphite.NewService(config)
	if err != nil {
		t.Fatalf("failed to create Graphite service: %s", err.Error())
	}

	// Allow test to wait until points are written.
	var wg sync.WaitGroup
	wg.Add(1)

	pointsWriter := PointsWriter{
		WritePointsFn: func(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error {
			defer wg.Done()

			pt, _ := models.NewPoint(
				"cpu",
				map[string]string{},
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
		},
	}
	service.PointsWriter = &pointsWriter
	dbCreator := DatabaseCreator{}
	service.MetaClient = &dbCreator

	if err := service.Open(); err != nil {
		t.Fatalf("failed to open Graphite service: %s", err.Error())
	}

	if !dbCreator.Created {
		t.Fatalf("failed to create target database")
	}

	// Connect to the graphite endpoint we just spun up
	_, port, _ := net.SplitHostPort(service.Addr().String())
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

// PointsWriter represents a mock impl of PointsWriter.
type PointsWriter struct {
	WritePointsFn func(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
}

func (w *PointsWriter) WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error {
	return w.WritePointsFn(database, retentionPolicy, consistencyLevel, points)
}

type DatabaseCreator struct {
	Created bool
}

func (d *DatabaseCreator) CreateDatabase(name string) (*meta.DatabaseInfo, error) {
	d.Created = true
	return nil, nil
}

// Test Helpers
func errstr(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}
