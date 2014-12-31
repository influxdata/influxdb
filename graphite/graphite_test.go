package graphite_test

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/graphite"
)

func Test_DecodeNameAndTags(t *testing.T) {
	var tests = []struct {
		test string
		str  string
		name string
		tags map[string]string
		err  string
	}{
		{test: "metric only", str: "cpu", name: "cpu"},
		{test: "metric with single series", str: "hostname.server01.cpu", name: "cpu", tags: map[string]string{"hostname": "server01"}},
		{test: "metric with multiple series", str: "region.us-west.hostname.server01.cpu", name: "cpu", tags: map[string]string{"hostname": "server01", "region": "us-west"}},
		{test: "no metric", tags: make(map[string]string), err: `no name specified for metric. ""`},
		{test: "wrong metric format", str: "foo.cpu", tags: make(map[string]string), err: `received "foo.cpu" which doesn't conform to format of key.value.key.value.metric or metric`},
	}

	for _, test := range tests {
		t.Logf("testing %q...", test.test)
		name, tags, err := graphite.DecodeNameAndTags(test.str)
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
	testTime := time.Now()
	epochTime := testTime.UnixNano() / 1000000 // nanos to milliseconds
	strTime := strconv.FormatInt(epochTime, 10)

	var tests = []struct {
		test      string
		reader    *bufio.Reader
		name      string
		tags      map[string]string
		isInt     bool
		iv        int64
		fv        float64
		timestamp time.Time
		err       string
	}{
		{
			test:      "series + metric + integer value",
			reader:    bufio.NewReader(bytes.NewBuffer([]byte(`foo.bar.cpu 50 ` + strTime + `\n`))),
			name:      "cpu",
			tags:      map[string]string{"foo": "bar"},
			isInt:     true,
			iv:        50,
			timestamp: testTime,
		},
		{
			test:      "metric only with float value",
			reader:    bufio.NewReader(bytes.NewBuffer([]byte(`cpu 50.554 ` + strTime + `\n`))),
			name:      "cpu",
			isInt:     false,
			fv:        50.554,
			timestamp: testTime,
		},
		{
			test:   "missing metric",
			reader: bufio.NewReader(bytes.NewBuffer([]byte(`50.554 1419972457825\n`))),
			err:    `received "50.554 1419972457825" which doesn't have three fields`,
		},
		{
			test:   "should fail on eof",
			reader: bufio.NewReader(bytes.NewBuffer([]byte(``))),
			err:    `EOF`,
		},
		{
			test:   "should fail on invalid key",
			reader: bufio.NewReader(bytes.NewBuffer([]byte(`foo.cpu 50.554 1419972457825\n`))),
			err:    `received "foo.cpu" which doesn't conform to format of key.value.key.value.metric or metric`,
		},
		{
			test:   "should fail parsing invalid float",
			reader: bufio.NewReader(bytes.NewBuffer([]byte(`cpu 50.554z 1419972457825\n`))),
			err:    `strconv.ParseFloat: parsing "50.554z": invalid syntax`,
		},
		{
			test:   "should fail parsing invalid int",
			reader: bufio.NewReader(bytes.NewBuffer([]byte(`cpu 50z 1419972457825\n`))),
			err:    `strconv.ParseFloat: parsing "50z": invalid syntax`,
		},
		{
			test:   "should fail parsing invalid time",
			reader: bufio.NewReader(bytes.NewBuffer([]byte(`cpu 50.554 14199724z57825\n`))),
			err:    `strconv.ParseInt: parsing "14199724z57825": invalid syntax`,
		},
	}

	for _, test := range tests {
		t.Logf("testing %q...", test.test)
		g, err := graphite.DecodeMetric(test.reader)
		if errstr(err) != test.err {
			t.Fatalf("err does not match.  expected %v, got %v", test.err, err)
		}
		if err != nil {
			// If we erred out,it was intended and the following tests won't work
			continue
		}
		if g.Name != test.name {
			t.Fatalf("name parse failer.  expected %v, got %v", test.name, g.Name)
		}
		if len(g.Tags) != len(test.tags) {
			t.Fatalf("tags len mismatch.  expected %d, got %d", len(test.tags), len(g.Tags))
		}
		if g.IsInt != test.isInt {
			t.Fatalf("isInt value mismatch.  expected %v, got %v", test.isInt, g.IsInt)
		}
		if g.IsInt && g.IntegerValue != test.iv {
			t.Fatalf("integerValue value mismatch.  expected %v, got %v", test.iv, g.IntegerValue)
		}
		if g.IsInt != true && g.FloatValue != test.fv {
			t.Fatalf("floatValue value mismatch.  expected %v, got %v", test.fv, g.FloatValue)
		}
		if g.Timestamp.UnixNano()/1000000 != test.timestamp.UnixNano()/1000000 {
			t.Fatalf("timestamp value mismatch.  expected %v, got %v", test.timestamp.UnixNano(), g.Timestamp.UnixNano())
		}
	}
}

type testServer string

func (testServer) DefaultRetentionPolicy(database string) (*influxdb.RetentionPolicy, error) {
	return &influxdb.RetentionPolicy{}, nil
}
func (testServer) WriteSeries(database, retentionPolicy, name string, tags map[string]string, timestamp time.Time, values map[string]interface{}) error {
	return nil
}

type testServerFailedRetention string

func (testServerFailedRetention) DefaultRetentionPolicy(database string) (*influxdb.RetentionPolicy, error) {
	return &influxdb.RetentionPolicy{}, fmt.Errorf("no retention policy")
}
func (testServerFailedRetention) WriteSeries(database, retentionPolicy, name string, tags map[string]string, timestamp time.Time, values map[string]interface{}) error {
	return nil
}

type testServerFailedWrite string

func (testServerFailedWrite) DefaultRetentionPolicy(database string) (*influxdb.RetentionPolicy, error) {
	return &influxdb.RetentionPolicy{}, nil
}
func (testServerFailedWrite) WriteSeries(database, retentionPolicy, name string, tags map[string]string, timestamp time.Time, values map[string]interface{}) error {
	return fmt.Errorf("failed write")
}

func TestServer_HandleMessage(t *testing.T) {
	//TODO bring back testing as an actual client
	t.Skip()
	/*
		var ts testServer
		var tsfr testServerFailedRetention
		var tsfw testServerFailedWrite

		var tests = []struct {
			test   string
			server interface {
				WriteSeries(database, retentionPolicy, name string, tags map[string]string, timestamp time.Time, values map[string]interface{}) error
				DefaultRetentionPolicy(database string) (*influxdb.RetentionPolicy, error)
			}
			reader *bufio.Reader
			err    string
		}{
			{
				test:   "should successfuly write valid data",
				server: ts,
				reader: bufio.NewReader(bytes.NewBuffer([]byte(`cpu 50.554 1419972457825\n`))),
			},
			{
				test:   "should fail decoding invalid data",
				server: ts,
				reader: bufio.NewReader(bytes.NewBuffer([]byte(` 50.554 1419972457825\n`))),
				err:    `received "50.554 1419972457825" which doesn't have three fields`,
			},
			{
				test:   "should fail if we can't retrieve a valid retention policy",
				server: tsfr,
				reader: bufio.NewReader(bytes.NewBuffer([]byte(`cpu 50.554 1419972457825\n`))),
				err:    `error looking up default database retention policy: no retention policy`,
			},
			{
				test:   "should fail if we can't write to the database",
				server: tsfw,
				reader: bufio.NewReader(bytes.NewBuffer([]byte(`cpu 50.554 1419972457825\n`))),
				err:    `write series data: failed write`,
			},
		}

		for _, test := range tests {
			t.Logf("testing %q...", test.test)
			s := graphite.GraphiteServer{Server: test.server}
			err := s.handleMessage(test.reader)
			if err != nil && err.Error() != test.err {
				t.Fatalf("err does not match.  expected %v, got %v", test.err, err.Error())
			} else if err == nil && test.err != "" {
				t.Fatalf("err does not match.  expected %v, got %v", test.err, err.Error())
			}
		}
	*/
}

func TestServer_ListenAndServe_ErrBindAddressRequired(t *testing.T) {
	var (
		test = "should fail if no address specified"
		ts   testServer
		s    = graphite.Server{Server: ts}
		err  = graphite.ErrBindAddressRequired
	)

	t.Logf("testing %q...", test)
	e := s.ListenAndServe()
	defer s.Close()
	if e != err {
		t.Fatalf("err does not match.  expected %v, got %v", err, e)
	}
}

func TestServer_ListenAndServe_ErrDatabaseNotSpecified(t *testing.T) {
	var (
		test = "should fail if no database"
		ts   testServer
		s    = graphite.Server{Server: ts}
		err  = graphite.ErrDatabaseNotSpecified
	)

	s.TCPAddr = &net.TCPAddr{IP: net.ParseIP("127.0.0.1")}

	t.Logf("testing %q...", test)
	e := s.ListenAndServe()
	defer s.Close()
	if e != err {
		t.Fatalf("err does not match.  expected %v, got %v", err, e)
	}
}

func TestServer_ListenAndServe_ErrServerNotSpecified(t *testing.T) {
	var (
		test = "should fail if no server"
		s    = graphite.Server{}
		err  = graphite.ErrServerNotSpecified
	)

	s.Database = "foo"
	s.TCPAddr = &net.TCPAddr{IP: net.ParseIP("127.0.0.1")}

	t.Logf("testing %q...", test)
	e := s.ListenAndServe()
	defer s.Close()
	if e != err {
		t.Fatalf("err does not match.  expected %v, got %v", err, e)
	}
}

// TODO look at what it will take to test successful listen and serve.
func TestServer_ListenAndServe_ValidRequest(t *testing.T) {
	t.Skip()
	var (
		test = "should work for tcp"
		ts   testServer
		s    = graphite.Server{Server: ts}
		err  string
	)

	s.Database = "foo"
	s.TCPAddr = &net.TCPAddr{IP: net.ParseIP("127.0.0.1, Port: 2004")}

	t.Logf("testing %q...", test)
	e := s.ListenAndServe()
	defer s.Close()
	if e != nil && e.Error() != err {
		t.Fatalf("err does not match.  expected %v, got %v", err, e.Error())
	} else if e == nil && err != "" {
		t.Fatalf("err does not match.  expected %v, got %v", err, e.Error())
	}
}

// Test Helpers
func errstr(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}
