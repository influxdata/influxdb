package influxdb_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/client"
)

func TestNormalizeBatchPoints(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name string
		bp   client.BatchPoints
		json []byte // if set, generate bp from json. overrides bp
		p    []influxdb.Point
		err  string
	}{
		{
			name: "default",
			bp: client.BatchPoints{
				Points: []client.Point{
					{Measurement: "cpu", Tags: map[string]string{"region": "useast"}, Time: now, Fields: map[string]interface{}{"value": 1.0}},
				},
			},
			p: []influxdb.Point{
				{Measurement: "cpu", Tags: map[string]string{"region": "useast"}, Time: now, Fields: map[string]interface{}{"value": 1.0}},
			},
		},
		{
			name: "merge time",
			bp: client.BatchPoints{
				Time: now,
				Points: []client.Point{
					{Measurement: "cpu", Tags: map[string]string{"region": "useast"}, Fields: map[string]interface{}{"value": 1.0}},
				},
			},
			p: []influxdb.Point{
				{Measurement: "cpu", Tags: map[string]string{"region": "useast"}, Time: now, Fields: map[string]interface{}{"value": 1.0}},
			},
		},
		{
			name: "merge tags",
			bp: client.BatchPoints{
				Tags: map[string]string{"day": "monday"},
				Points: []client.Point{
					{Measurement: "cpu", Tags: map[string]string{"region": "useast"}, Time: now, Fields: map[string]interface{}{"value": 1.0}},
					{Measurement: "memory", Time: now, Fields: map[string]interface{}{"value": 2.0}},
				},
			},
			p: []influxdb.Point{
				{Measurement: "cpu", Tags: map[string]string{"day": "monday", "region": "useast"}, Time: now, Fields: map[string]interface{}{"value": 1.0}},
				{Measurement: "memory", Tags: map[string]string{"day": "monday"}, Time: now, Fields: map[string]interface{}{"value": 2.0}},
			},
		},
		{
			name: "merge precision",
			json: []byte(`{ "precision": "ms", "points": [
			{"name": "cpu", "timestamp": 1422921600000, "fields": { "value": 1.0 }},
			{"name": "memory", "timestamp": 1423008000000, "fields": { "value": 2.0 }}
			] }`),
			p: []influxdb.Point{
				{Name: "cpu", Timestamp: time.Unix(1422921600, 0), Fields: map[string]interface{}{"value": 1.0}},
				{Name: "memory", Timestamp: time.Unix(1423008000, 0), Fields: map[string]interface{}{"value": 2.0}},
			},
		},
	}

	for _, test := range tests {
		t.Logf("running test %q", test.name)
		bp := test.bp
		if test.json != nil {
			e := bp.UnmarshalJSON(test.json)
			if e != nil {
				t.Errorf("error unmarshalling batchpoints %v\n json: %s", e, test.json)
			}
		}
		p, e := influxdb.NormalizeBatchPoints(bp)
		if test.err == "" && e != nil {
			t.Errorf("unexpected error %v", e)
		} else if test.err != "" && e == nil {
			t.Errorf("expected error %s, got <nil>", test.err)
		} else if e != nil && test.err != e.Error() {
			t.Errorf("unexpected error. expected: %s, got %v", test.err, e)
		}
		if !reflect.DeepEqual(p, test.p) {
			t.Logf("expected: %+v", test.p)
			t.Logf("got:      %+v", p)
			t.Error("failed to normalize.")
		}
	}
}
