package graphite_test

import (
	"fmt"
	"net"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/services/graphite"
	"github.com/influxdb/influxdb/toml"
	"github.com/influxdb/influxdb/tsdb"
)

func Test_DecodeNameAndTags(t *testing.T) {
	var tests = []struct {
		test      string
		str       string
		name      string
		tags      map[string]string
		position  string
		separator string
		err       string
	}{
		{test: "metric only", str: "cpu", name: "cpu"},
		{test: "metric with single series", str: "cpu.hostname.server01", name: "cpu", tags: map[string]string{"hostname": "server01"}},
		{test: "metric with multiple series", str: "cpu.region.us-west.hostname.server01", name: "cpu", tags: map[string]string{"hostname": "server01", "region": "us-west"}},
		{test: "no metric", tags: make(map[string]string), err: `no name specified for metric. ""`},
		{test: "wrong metric format", str: "foo.cpu", tags: make(map[string]string), err: `received "foo.cpu" which doesn't conform to format of key.value.key.value.name or name`},
	}

	for _, test := range tests {
		t.Logf("testing %q...", test.test)

		p := graphite.NewParser()
		if test.separator != "" {
			p.Separator = test.separator
		}

		name, tags, err := p.DecodeNameAndTags(test.str)
		if errstr(err) != test.err {
			t.Fatalf("err does not match.  expected %v, got %v", test.err, err)
		}
		if name != test.name {
			t.Fatalf("name parse failer.  expected %v, got %v", test.name, name)
		}
		if len(tags) != len(test.tags) {
			t.Fatalf("unexpected number of tags.  expected %d, got %d", len(test.tags), len(tags))
		}
		for k, v := range test.tags {
			if tags[k] != v {
				t.Fatalf("unexpected tag value for tags[%s].  expected %q, got %q", k, v, tags[k])
			}
		}
	}
}

func Test_DecodeMetric(t *testing.T) {
	testTime := time.Now().Round(time.Second)
	epochTime := testTime.Unix()
	strTime := strconv.FormatInt(epochTime, 10)

	var tests = []struct {
		test                string
		line                string
		name                string
		tags                map[string]string
		value               float64
		time                time.Time
		position, separator string
		err                 string
	}{
		{
			test:  "position first by default",
			line:  `cpu.foo.bar 50 ` + strTime,
			name:  "cpu",
			tags:  map[string]string{"foo": "bar"},
			value: 50,
			time:  testTime,
		},
		{
			test:     "position first if unable to determine",
			position: "foo",
			line:     `cpu.foo.bar 50 ` + strTime,
			name:     "cpu",
			tags:     map[string]string{"foo": "bar"},
			value:    50,
			time:     testTime,
		},
		{
			test:     "position last if specified",
			position: "last",
			line:     `foo.bar.cpu 50 ` + strTime,
			name:     "cpu",
			tags:     map[string]string{"foo": "bar"},
			value:    50,
			time:     testTime,
		},
		{
			test:     "position first if specified with no series",
			position: "first",
			line:     `cpu 50 ` + strTime,
			name:     "cpu",
			tags:     map[string]string{},
			value:    50,
			time:     testTime,
		},
		{
			test:     "position last if specified with no series",
			position: "last",
			line:     `cpu 50 ` + strTime,
			name:     "cpu",
			tags:     map[string]string{},
			value:    50,
			time:     testTime,
		},
		{
			test:  "separator is . by default",
			line:  `cpu.foo.bar 50 ` + strTime,
			name:  "cpu",
			tags:  map[string]string{"foo": "bar"},
			value: 50,
			time:  testTime,
		},
		{
			test:      "separator is . if specified",
			separator: ".",
			line:      `cpu.foo.bar 50 ` + strTime,
			name:      "cpu",
			tags:      map[string]string{"foo": "bar"},
			value:     50,
			time:      testTime,
		},
		{
			test:      "separator is - if specified",
			separator: "-",
			line:      `cpu-foo-bar 50 ` + strTime,
			name:      "cpu",
			tags:      map[string]string{"foo": "bar"},
			value:     50,
			time:      testTime,
		},
		{
			test:      "separator is boo if specified",
			separator: "boo",
			line:      `cpuboofooboobar 50 ` + strTime,
			name:      "cpu",
			tags:      map[string]string{"foo": "bar"},
			value:     50,
			time:      testTime,
		},

		{
			test:  "series + metric + integer value",
			line:  `cpu.foo.bar 50 ` + strTime,
			name:  "cpu",
			tags:  map[string]string{"foo": "bar"},
			value: 50,
			time:  testTime,
		},
		{
			test:  "metric only with float value",
			line:  `cpu 50.554 ` + strTime,
			name:  "cpu",
			value: 50.554,
			time:  testTime,
		},
		{
			test: "missing metric",
			line: `50.554 1419972457825`,
			err:  `received "50.554 1419972457825" which doesn't have three fields`,
		},
		{
			test: "should error on invalid key",
			line: `foo.cpu 50.554 1419972457825`,
			err:  `received "foo.cpu" which doesn't conform to format of key.value.key.value.name or name`,
		},
		{
			test: "should error parsing invalid float",
			line: `cpu 50.554z 1419972457825`,
			err:  `field "cpu" value: strconv.ParseFloat: parsing "50.554z": invalid syntax`,
		},
		{
			test: "should error parsing invalid int",
			line: `cpu 50z 1419972457825`,
			err:  `field "cpu" value: strconv.ParseFloat: parsing "50z": invalid syntax`,
		},
		{
			test: "should error parsing invalid time",
			line: `cpu 50.554 14199724z57825`,
			err:  `field "cpu" time: strconv.ParseFloat: parsing "14199724z57825": invalid syntax`,
		},
	}

	for _, test := range tests {
		t.Logf("testing %q...", test.test)

		p := graphite.NewParser()
		if test.separator != "" {
			p.Separator = test.separator
		}
		p.LastEnabled = (test.position == "last")

		point, err := p.Parse(test.line)
		if errstr(err) != test.err {
			t.Fatalf("err does not match.  expected %v, got %v", test.err, err)
		}
		if err != nil {
			// If we erred out,it was intended and the following tests won't work
			continue
		}
		if point.Name() != test.name {
			t.Fatalf("name parse failer.  expected %v, got %v", test.name, point.Name())
		}
		if len(point.Tags()) != len(test.tags) {
			t.Fatalf("tags len mismatch.  expected %d, got %d", len(test.tags), len(point.Tags()))
		}
		f := point.Fields()["value"].(float64)
		if point.Fields()["value"] != f {
			t.Fatalf("floatValue value mismatch.  expected %v, got %v", test.value, f)
		}
		if point.Time().UnixNano()/1000000 != test.time.UnixNano()/1000000 {
			t.Fatalf("time value mismatch.  expected %v, got %v", test.time.UnixNano(), point.Time().UnixNano())
		}
	}
}

func Test_ServerGraphiteTCP(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Round(time.Second)

	config := graphite.NewConfig()
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
		WritePointsFn: func(req *cluster.WritePointsRequest) error {
			defer wg.Done()

			if req.Database != "graphitedb" {
				t.Fatalf("unexpected database: %s", req.Database)
			} else if req.RetentionPolicy != "" {
				t.Fatalf("unexpected retention policy: %s", req.RetentionPolicy)
			} else if !reflect.DeepEqual(req.Points, []tsdb.Point{
				tsdb.NewPoint(
					"cpu",
					map[string]string{},
					map[string]interface{}{"value": 23.456},
					time.Unix(now.Unix(), 0),
				),
			}) {
				spew.Dump(req.Points)
				t.Fatalf("unexpected points: %#v", req.Points)
			}
			return nil
		},
	}
	service.PointsWriter = &pointsWriter
	dbCreator := DatabaseCreator{}
	service.MetaStore = &dbCreator

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

	config := graphite.NewConfig()
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
		WritePointsFn: func(req *cluster.WritePointsRequest) error {
			defer wg.Done()

			if req.Database != "graphitedb" {
				t.Fatalf("unexpected database: %s", req.Database)
			} else if req.RetentionPolicy != "" {
				t.Fatalf("unexpected retention policy: %s", req.RetentionPolicy)
			} else if !reflect.DeepEqual(req.Points, []tsdb.Point{
				tsdb.NewPoint(
					"cpu",
					map[string]string{},
					map[string]interface{}{"value": 23.456},
					time.Unix(now.Unix(), 0),
				),
			}) {
				spew.Dump(req.Points)
				t.Fatalf("unexpected points: %#v", req.Points)
			}
			return nil
		},
	}
	service.PointsWriter = &pointsWriter
	dbCreator := DatabaseCreator{}
	service.MetaStore = &dbCreator

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
	WritePointsFn func(*cluster.WritePointsRequest) error
}

func (w *PointsWriter) WritePoints(p *cluster.WritePointsRequest) error {
	return w.WritePointsFn(p)
}

type DatabaseCreator struct {
	Created bool
}

func (d *DatabaseCreator) CreateDatabaseIfNotExists(name string) (*meta.DatabaseInfo, error) {
	d.Created = true
	return nil, nil
}

func (d *DatabaseCreator) WaitForLeader(t time.Duration) error {
	return nil
}

// Test Helpers
func errstr(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}
