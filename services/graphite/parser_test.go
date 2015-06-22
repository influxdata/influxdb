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
		template    string
		err         string
	}{
		{test: "metric only",
			str:         "cpu",
			measurement: "cpu",
			template:    "measurement",
		},
		{test: "metric with single series",
			str:         "cpu.server01",
			measurement: "cpu",
			template:    "measurement.hostname",
			tags:        map[string]string{"hostname": "server01"},
		},
		{test: "metric with multiple series",
			str:         "cpu.us-west.server01",
			measurement: "cpu",
			template:    "measurement.region.hostname",
			tags:        map[string]string{"hostname": "server01", "region": "us-west"},
		},
		{test: "no metric",
			tags: make(map[string]string),
			err:  `no measurement specified for template. ""`,
		},
		{test: "ignore unnamed",
			str:         "foo.cpu",
			template:    "measurement",
			tags:        make(map[string]string),
			measurement: "foo"},
		{test: "name shorter than template",
			str:         "foo",
			template:    "measurement.A.B.C",
			tags:        make(map[string]string),
			measurement: "foo",
		},
		{test: "wildcard measurement at end",
			str:         "prod.us-west.server01.cpu.load",
			template:    "env.zone.host.measurement*",
			tags:        map[string]string{"env": "prod", "zone": "us-west", "host": "server01"},
			measurement: "cpu.load",
		},
	}

	for _, test := range tests {
		p, err := graphite.NewParser(test.template)
		if errstr(err) != test.err {
			t.Fatalf("err does not match.  expected %v, got %v", test.err, err)
		}
		if err != nil {
			// If we erred out,it was intended and the following tests won't work
			continue
		}

		measurement, tags := p.DecodeNameAndTags(test.str)
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

func TestParseMissingMeasurement(t *testing.T) {
	_, err := graphite.NewParser("a.b.c")
	if err == nil {
		t.Fatalf("expected error creating parser, got nil")
	}
}

func TestParse(t *testing.T) {
	testTime := time.Now().Round(time.Second)
	epochTime := testTime.Unix()
	strTime := strconv.FormatInt(epochTime, 10)

	var tests = []struct {
		test        string
		line        string
		measurement string
		tags        map[string]string
		value       float64
		time        time.Time
		template    string
		err         string
	}{
		{
			test:        "normal case",
			line:        `cpu.foo.bar 50 ` + strTime,
			template:    "measurement.foo.bar",
			measurement: "cpu",
			tags: map[string]string{
				"foo": "foo",
				"bar": "bar",
			},
			value: 50,
			time:  testTime,
		},
		{
			test:        "metric only with float value",
			line:        `cpu 50.554 ` + strTime,
			measurement: "cpu",
			template:    "measurement",
			value:       50.554,
			time:        testTime,
		},
		{
			test:     "missing metric",
			line:     `50.554 1419972457825`,
			template: "measurement",
			err:      `received "50.554 1419972457825" which doesn't have three fields`,
		},
		{
			test:     "should error parsing invalid float",
			line:     `cpu 50.554z 1419972457825`,
			template: "measurement",
			err:      `field "cpu" value: strconv.ParseFloat: parsing "50.554z": invalid syntax`,
		},
		{
			test:     "should error parsing invalid int",
			line:     `cpu 50z 1419972457825`,
			template: "measurement",
			err:      `field "cpu" value: strconv.ParseFloat: parsing "50z": invalid syntax`,
		},
		{
			test:     "should error parsing invalid time",
			line:     `cpu 50.554 14199724z57825`,
			template: "measurement",
			err:      `field "cpu" time: strconv.ParseFloat: parsing "14199724z57825": invalid syntax`,
		},
	}

	for _, test := range tests {
		p, err := graphite.NewParser(test.template)
		if err != nil {
			t.Fatalf("unexpected error creating graphite parser: %v", err)
		}

		point, err := p.Parse(test.line)
		if errstr(err) != test.err {
			t.Fatalf("err does not match.  expected %v, got %v", test.err, err)
		}
		if err != nil {
			// If we erred out,it was intended and the following tests won't work
			continue
		}
		if point.Name() != test.measurement {
			t.Fatalf("name parse failer.  expected %v, got %v", test.measurement, point.Name())
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
