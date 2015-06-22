package graphite_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/influxdb/influxdb/services/graphite"
)

func TestDecodeNameAndTags(t *testing.T) {
	var tests = []struct {
		test        string
		str         string
		measurement string
		tags        map[string]string
		schema      string
		separator   string
		ignore      bool
		err         string
	}{
		{test: "metric only",
			str:         "cpu",
			measurement: "cpu",
			schema:      "measurement",
			ignore:      true,
		},
		{test: "metric with single series",
			str:         "cpu.server01",
			measurement: "cpu",
			ignore:      true,
			schema:      "measurement.hostname",
			tags:        map[string]string{"hostname": "server01"},
		},
		{test: "metric with multiple series",
			str:         "cpu.us-west.server01",
			measurement: "cpu",
			ignore:      true,
			schema:      "measurement.region.hostname",
			tags:        map[string]string{"hostname": "server01", "region": "us-west"},
		},
		{test: "no metric",
			tags:   make(map[string]string),
			ignore: true,
			err:    `no measurement specified for metric. ""`,
		},
		{test: "ignore unnamed",
			str:         "foo.cpu",
			ignore:      true,
			schema:      "measurement",
			tags:        make(map[string]string),
			measurement: "foo"},
		{test: "not ignore unnamed",
			str:    "foo.cpu",
			ignore: false,
			schema: "measurement",
			tags:   make(map[string]string),
			err:    `received "foo.cpu" which contains unnamed field`,
		},
		{test: "name shorter than schema",
			str:         "foo",
			schema:      "measurement.A.B.C",
			ignore:      true,
			tags:        make(map[string]string),
			measurement: "foo",
		},
	}

	for _, test := range tests {
		t.Logf("testing %q...", test.test)

		if test.separator == "" {
			test.separator = "."
		}
		p := graphite.NewParser(test.schema, test.separator, test.ignore)

		measurement, tags, err := p.DecodeNameAndTags(test.str)
		if errstr(err) != test.err {
			t.Fatalf("err does not match.  expected %v, got %v", test.err, err)
		}
		if err != nil {
			// If we erred out,it was intended and the following tests won't work
			continue
		}
		if measurement != test.measurement {
			t.Fatalf("name parse failer.  expected %v, got %v", test.measurement, measurement)
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

func TestParse(t *testing.T) {
	testTime := time.Now().Round(time.Second)
	epochTime := testTime.Unix()
	strTime := strconv.FormatInt(epochTime, 10)

	var tests = []struct {
		test      string
		line      string
		name      string
		tags      map[string]string
		value     float64
		time      time.Time
		separator string
		schema    string
		ignore    bool
		err       string
	}{
		{
			test:   "normal case",
			line:   `cpu.foo.bar 50 ` + strTime,
			schema: "measurement.foo.bar",
			name:   "cpu",
			tags: map[string]string{
				"foo": "foo",
				"bar": "bar",
			},
			value: 50,
			time:  testTime,
		},
		{
			test:   "DecodeNameAndTags returns error",
			line:   `cpu.foo.bar 50 ` + strTime,
			schema: "a.b.c",
			err:    `no measurement specified for metric. "cpu.foo.bar"`,
		},
		{
			test:   "separator is . by default",
			line:   `cpu.foo.bar 50 ` + strTime,
			name:   "cpu",
			schema: "measurement.foo.bar",
			tags: map[string]string{
				"foo": "foo",
				"bar": "bar",
			},
			value: 50,
			time:  testTime,
		},
		{
			test:      "separator is . if specified",
			separator: ".",
			line:      `cpu.foo.bar 50 ` + strTime,
			name:      "cpu",
			schema:    "measurement.foo.bar",
			tags: map[string]string{
				"foo": "foo",
				"bar": "bar",
			},
			value: 50,
			time:  testTime,
		},
		{
			test:      "separator is - if specified",
			separator: "-",
			line:      `cpu-foo-bar 50 ` + strTime,
			name:      "cpu",
			schema:    "measurement-foo-bar",
			tags: map[string]string{
				"foo": "foo",
				"bar": "bar",
			},
			value: 50,
			time:  testTime,
		},
		{
			test:      "separator is boo if specified",
			separator: "boo",
			line:      `cpuboofooboobar 50 ` + strTime,
			name:      "cpu",
			schema:    "measurementboofooboobar",
			tags: map[string]string{
				"foo": "foo",
				"bar": "bar",
			},
			value: 50,
			time:  testTime,
		},
		{
			test:   "metric only with float value",
			line:   `cpu 50.554 ` + strTime,
			name:   "cpu",
			schema: "measurement",
			value:  50.554,
			time:   testTime,
		},
		{
			test: "missing metric",
			line: `50.554 1419972457825`,
			err:  `received "50.554 1419972457825" which doesn't have three fields`,
		},
		{
			test:   "should error parsing invalid float",
			line:   `cpu 50.554z 1419972457825`,
			schema: "measurement",
			err:    `field "cpu" value: strconv.ParseFloat: parsing "50.554z": invalid syntax`,
		},
		{
			test:   "should error parsing invalid int",
			line:   `cpu 50z 1419972457825`,
			schema: "measurement",
			err:    `field "cpu" value: strconv.ParseFloat: parsing "50z": invalid syntax`,
		},
		{
			test:   "should error parsing invalid time",
			line:   `cpu 50.554 14199724z57825`,
			schema: "measurement",
			err:    `field "cpu" time: strconv.ParseFloat: parsing "14199724z57825": invalid syntax`,
		},
	}

	for _, test := range tests {
		t.Logf("testing %q...", test.test)

		if test.separator == "" {
			test.separator = "."
		}
		p := graphite.NewParser(test.schema, test.separator, test.ignore)

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
