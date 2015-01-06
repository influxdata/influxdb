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
		test      string
		str       string
		name      string
		tags      map[string]string
		position  string
		seperator string
		err       string
	}{
		{test: "metric only", str: "cpu", name: "cpu"},
		{test: "metric with single series", str: "cpu.hostname.server01", name: "cpu", tags: map[string]string{"hostname": "server01"}},
		{test: "metric with multiple series", str: "cpu.region.us-west.hostname.server01", name: "cpu", tags: map[string]string{"hostname": "server01", "region": "us-west"}},
		{test: "no metric", tags: make(map[string]string), err: `no name specified for metric. ""`},
		{test: "wrong metric format", str: "foo.cpu", tags: make(map[string]string), err: `received "foo.cpu" which doesn't conform to format of key.value.key.value.metric or metric`},
	}

	for _, test := range tests {
		t.Logf("testing %q...", test.test)
		name, tags, err := graphite.DecodeNameAndTags(test.str, test.position, test.seperator)
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
		test                string
		reader              *bufio.Reader
		name                string
		tags                map[string]string
		isInt               bool
		iv                  int64
		fv                  float64
		timestamp           time.Time
		position, seperator string
		err                 string
	}{
		{
			test:      "position first by default",
			reader:    bufio.NewReader(bytes.NewBuffer([]byte(`cpu.foo.bar 50 ` + strTime + `\n`))),
			name:      "cpu",
			tags:      map[string]string{"foo": "bar"},
			isInt:     true,
			iv:        50,
			timestamp: testTime,
		},
		{
			test:      "position first if unable to determine",
			position:  "foo",
			reader:    bufio.NewReader(bytes.NewBuffer([]byte(`cpu.foo.bar 50 ` + strTime + `\n`))),
			name:      "cpu",
			tags:      map[string]string{"foo": "bar"},
			isInt:     true,
			iv:        50,
			timestamp: testTime,
		},
		{
			test:      "position last if specified",
			position:  "last",
			reader:    bufio.NewReader(bytes.NewBuffer([]byte(`foo.bar.cpu 50 ` + strTime + `\n`))),
			name:      "cpu",
			tags:      map[string]string{"foo": "bar"},
			isInt:     true,
			iv:        50,
			timestamp: testTime,
		},
		{
			test:      "position first if specified with no series",
			position:  "first",
			reader:    bufio.NewReader(bytes.NewBuffer([]byte(`cpu 50 ` + strTime + `\n`))),
			name:      "cpu",
			tags:      map[string]string{},
			isInt:     true,
			iv:        50,
			timestamp: testTime,
		},
		{
			test:      "position last if specified with no series",
			position:  "last",
			reader:    bufio.NewReader(bytes.NewBuffer([]byte(`cpu 50 ` + strTime + `\n`))),
			name:      "cpu",
			tags:      map[string]string{},
			isInt:     true,
			iv:        50,
			timestamp: testTime,
		},
		{
			test:      "sepeartor is . by default",
			reader:    bufio.NewReader(bytes.NewBuffer([]byte(`cpu.foo.bar 50 ` + strTime + `\n`))),
			name:      "cpu",
			tags:      map[string]string{"foo": "bar"},
			isInt:     true,
			iv:        50,
			timestamp: testTime,
		},
		{
			test:      "sepeartor is . if specified",
			seperator: ".",
			reader:    bufio.NewReader(bytes.NewBuffer([]byte(`cpu.foo.bar 50 ` + strTime + `\n`))),
			name:      "cpu",
			tags:      map[string]string{"foo": "bar"},
			isInt:     true,
			iv:        50,
			timestamp: testTime,
		},
		{
			test:      "sepeartor is - if specified",
			seperator: "-",
			reader:    bufio.NewReader(bytes.NewBuffer([]byte(`cpu-foo-bar 50 ` + strTime + `\n`))),
			name:      "cpu",
			tags:      map[string]string{"foo": "bar"},
			isInt:     true,
			iv:        50,
			timestamp: testTime,
		},
		{
			test:      "sepeartor is boo if specified",
			seperator: "boo",
			reader:    bufio.NewReader(bytes.NewBuffer([]byte(`cpuboofooboobar 50 ` + strTime + `\n`))),
			name:      "cpu",
			tags:      map[string]string{"foo": "bar"},
			isInt:     true,
			iv:        50,
			timestamp: testTime,
		},

		{
			test:      "series + metric + integer value",
			reader:    bufio.NewReader(bytes.NewBuffer([]byte(`cpu.foo.bar 50 ` + strTime + `\n`))),
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
		g, err := graphite.DecodeMetric(test.reader, test.position, test.seperator)
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
		if test.isInt {
			i := g.Value.(int64)
			if i != test.iv {
				t.Fatalf("integerValue value mismatch.  expected %v, got %v", test.iv, g.Value)
			}
		} else {
			f := g.Value.(float64)
			if g.Value != f {
				t.Fatalf("floatValue value mismatch.  expected %v, got %v", test.fv, f)
			}
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

func TestServer_ListenAndServeTCP_ErrBindAddressRequired(t *testing.T) {
	var (
		ts   testServer
		s    = graphite.Server{Server: ts}
		err  = graphite.ErrBindAddressRequired
		addr *net.TCPAddr
	)

	e := s.ListenAndServeTCP(addr)
	defer s.Close()
	if e != err {
		t.Fatalf("err does not match.  expected %v, got %v", err, e)
	}
}

func TestServer_ListenAndServeTCP_ErrDatabaseNotSpecified(t *testing.T) {
	var (
		ts  testServer
		s   = graphite.Server{Server: ts}
		err = graphite.ErrDatabaseNotSpecified
	)

	addr := &net.TCPAddr{IP: net.ParseIP("127.0.0.1")}

	e := s.ListenAndServeTCP(addr)
	defer s.Close()
	if e != err {
		t.Fatalf("err does not match.  expected %v, got %v", err, e)
	}
}

func TestServer_ListenAndServeTCP_ErrServerNotSpecified(t *testing.T) {
	var (
		s   = graphite.Server{}
		err = graphite.ErrServerNotSpecified
	)

	s.Database = "foo"
	addr := &net.TCPAddr{IP: net.ParseIP("127.0.0.1")}

	e := s.ListenAndServeTCP(addr)
	defer s.Close()
	if e != err {
		t.Fatalf("err does not match.  expected %v, got %v", err, e)
	}
}

func TestServer_ListenAndServeTCP_ValidRequest(t *testing.T) {
	var (
		ts   testServer
		s    = graphite.Server{Server: ts}
		addr = &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 2004}
	)
	s.Database = "foo"

	s.ListenAndServeTCP(addr)
	defer s.Close()

	// Connect to the graphite endpoint we just spun up
	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatal(err)
		return
	}
	_, err = conn.Write([]byte(`cpu 50.554 1419972457825\n`))
	defer conn.Close()
	if err != nil {
		t.Fatal(err)
		return
	}
}

func TestServer_ListenAndServeUDP_ErrBindAddressRequired(t *testing.T) {
	var (
		ts   testServer
		s    = graphite.Server{Server: ts}
		err  = graphite.ErrBindAddressRequired
		addr *net.UDPAddr
	)

	e := s.ListenAndServeUDP(addr)
	defer s.Close()
	if e != err {
		t.Fatalf("err does not match.  expected %v, got %v", err, e)
	}
}

func TestServer_ListenAndServeUDP_ErrDatabaseNotSpecified(t *testing.T) {
	var (
		ts  testServer
		s   = graphite.Server{Server: ts}
		err = graphite.ErrDatabaseNotSpecified
	)

	addr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1")}

	e := s.ListenAndServeUDP(addr)
	defer s.Close()
	if e != err {
		t.Fatalf("err does not match.  expected %v, got %v", err, e)
	}
}

func TestServer_ListenAndServeUDP_ErrServerNotSpecified(t *testing.T) {
	var (
		s   = graphite.Server{}
		err = graphite.ErrServerNotSpecified
	)

	s.Database = "foo"
	addr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1")}

	e := s.ListenAndServeUDP(addr)
	defer s.Close()
	if e != err {
		t.Fatalf("err does not match.  expected %v, got %v", err, e)
	}
}

// Test Helpers
func errstr(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}
