package influxdb

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"
)

func Test_decodeNameAndTags(t *testing.T) {
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
		name, tags, err := decodeGraphiteNameAndTags(test.str)
		if err != nil && err.Error() != test.err {
			t.Fatalf("err does not match.  expected %v, got %v", test.err, err.Error())
		} else if err == nil && test.err != "" {
			t.Fatalf("err does not match.  expected %v, got %v", test.err, err.Error())
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

func Test_decodeGraphiteMetric(t *testing.T) {
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
		g, err := decodeGraphiteMetric(test.reader)
		if err != nil && err.Error() != test.err {
			t.Fatalf("err does not match.  expected %v, got %v", test.err, err.Error())
		} else if err == nil && test.err != "" {
			t.Fatalf("err does not match.  expected %v, got %v", test.err, err.Error())
		}
		if err != nil {
			// If we erred out,it was intended and the following tests won't work
			continue
		}
		if g.name != test.name {
			t.Fatalf("name parse failer.  expected %v, got %v", test.name, g.name)
		}
		if len(g.tags) != len(test.tags) {
			t.Fatalf("tags len mismatch.  expected %d, got %d", len(test.tags), len(g.tags))
		}
		if g.isInt != test.isInt {
			t.Fatalf("isInt value mismatch.  expected %v, got %v", test.isInt, g.isInt)
		}
		if g.isInt && g.integerValue != test.iv {
			t.Fatalf("integerValue value mismatch.  expected %v, got %v", test.iv, g.integerValue)
		}
		if g.isInt != true && g.floatValue != test.fv {
			t.Fatalf("floatValue value mismatch.  expected %v, got %v", test.fv, g.floatValue)
		}
		if g.timestamp.UnixNano()/1000000 != test.timestamp.UnixNano()/1000000 {
			t.Fatalf("timestamp value mismatch.  expected %v, got %v", test.timestamp.UnixNano(), g.timestamp.UnixNano())
		}
	}
}

type testServer string
type testServerFailedRetention string
type testServerFailedWrite string

func (testServer) DefaultRetentionPolicy(database string) (*RetentionPolicy, error) {
	return &RetentionPolicy{}, nil
}
func (testServer) WriteSeries(database, retentionPolicy, name string, tags map[string]string, timestamp time.Time, values map[string]interface{}) error {
	return nil
}
func (testServerFailedRetention) DefaultRetentionPolicy(database string) (*RetentionPolicy, error) {
	return &RetentionPolicy{}, fmt.Errorf("no retention policy")
}
func (testServerFailedRetention) WriteSeries(database, retentionPolicy, name string, tags map[string]string, timestamp time.Time, values map[string]interface{}) error {
	return nil
}
func (testServerFailedWrite) DefaultRetentionPolicy(database string) (*RetentionPolicy, error) {
	return &RetentionPolicy{}, nil
}
func (testServerFailedWrite) WriteSeries(database, retentionPolicy, name string, tags map[string]string, timestamp time.Time, values map[string]interface{}) error {
	return fmt.Errorf("failed write")
}

func Test_graphiteHandleMessage(t *testing.T) {
	var ts testServer
	var tsfr testServerFailedRetention
	var tsfw testServerFailedWrite

	var tests = []struct {
		test   string
		server seriesWriter
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
		s := NewGraphiteServer(test.server)
		err := s.handleMessage(test.reader)
		if err != nil && err.Error() != test.err {
			t.Fatalf("err does not match.  expected %v, got %v", test.err, err.Error())
		} else if err == nil && test.err != "" {
			t.Fatalf("err does not match.  expected %v, got %v", test.err, err.Error())
		}
	}
}

func Test_graphiteListenAndServeNoBindAddress(t *testing.T) {
	var (
		test = "should fail if no address specified"
		ts   testServer
		s    = NewGraphiteServer(ts)
		err  = "bind address required"
	)

	t.Logf("testing %q...", test)
	e := s.ListenAndServe()
	defer s.Close()
	if e != nil && e.Error() != err {
		t.Fatalf("err does not match.  expected %v, got %v", err, e.Error())
	} else if e == nil && err != "" {
		t.Fatalf("err does not match.  expected %v, got %v", err, e.Error())
	}
}

func Test_graphiteListenAndServeNoDatabase(t *testing.T) {
	var (
		test = "should fail if no database"
		ts   testServer
		s    = NewGraphiteServer(ts)
		err  = "graphite database was not specified in config"
	)

	s.TCPAddr = &net.TCPAddr{IP: net.ParseIP("127.0.0.1")}

	t.Logf("testing %q...", test)
	e := s.ListenAndServe()
	defer s.Close()
	if e != nil && e.Error() != err {
		t.Fatalf("err does not match.  expected %v, got %v", err, e.Error())
	} else if e == nil && err != "" {
		t.Fatalf("err does not match.  expected %v, got %v", err, e.Error())
	}
}

func Test_graphiteListenAndServeNoServer(t *testing.T) {
	var (
		test = "should fail if no server"
		s    = GraphiteServer{}
		err  = "graphite server was not present"
	)

	s.Database = "foo"
	s.TCPAddr = &net.TCPAddr{IP: net.ParseIP("127.0.0.1")}

	t.Logf("testing %q...", test)
	e := s.ListenAndServe()
	defer s.Close()
	if e != nil && e.Error() != err {
		t.Fatalf("err does not match.  expected %v, got %v", err, e.Error())
	} else if e == nil && err != "" {
		t.Fatalf("err does not match.  expected %v, got %v", err, e.Error())
	}
}

// TODO look at what it will take to test successful listen and serve.
//func Test_graphiteListenAndServeTCP(t *testing.T) {
//var (
//test = "should work for tcp"
//ts   testServer
//s    = NewGraphiteServer(ts)
//err  string
//)

//s.Database = "foo"
//s.TCPAddr = &net.TCPAddr{IP: net.ParseIP("127.0.0.1, Port: 2004")}

//t.Logf("testing %q...", test)
//e := s.ListenAndServe()
//defer s.Close()
//if e != nil && e.Error() != err {
//t.Fatalf("err does not match.  expected %v, got %v", err, e.Error())
//} else if e == nil && err != "" {
//t.Fatalf("err does not match.  expected %v, got %v", err, e.Error())
//}
//}
