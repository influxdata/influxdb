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
		p    []influxdb.Point
		err  string
	}{
		{
			name: "default",
			bp: client.BatchPoints{
				Points: []client.Point{
					{Name: "cpu", Tags: map[string]string{"region": "useast"}, Timestamp: now, Fields: map[string]interface{}{"value": 1.0}},
				},
			},
			p: []influxdb.Point{
				{Name: "cpu", Tags: map[string]string{"region": "useast"}, Timestamp: now, Fields: map[string]interface{}{"value": 1.0}},
			},
		},
		{
			name: "merge timestamp",
			bp: client.BatchPoints{
				Timestamp: now,
				Points: []client.Point{
					{Name: "cpu", Tags: map[string]string{"region": "useast"}, Fields: map[string]interface{}{"value": 1.0}},
				},
			},
			p: []influxdb.Point{
				{Name: "cpu", Tags: map[string]string{"region": "useast"}, Timestamp: now, Fields: map[string]interface{}{"value": 1.0}},
			},
		},
		{
			name: "merge tags",
			bp: client.BatchPoints{
				Tags: map[string]string{"day": "monday"},
				Points: []client.Point{
					{Name: "cpu", Tags: map[string]string{"region": "useast"}, Timestamp: now, Fields: map[string]interface{}{"value": 1.0}},
					{Name: "memory", Timestamp: now, Fields: map[string]interface{}{"value": 2.0}},
				},
			},
			p: []influxdb.Point{
				{Name: "cpu", Tags: map[string]string{"day": "monday", "region": "useast"}, Timestamp: now, Fields: map[string]interface{}{"value": 1.0}},
				{Name: "memory", Tags: map[string]string{"day": "monday"}, Timestamp: now, Fields: map[string]interface{}{"value": 2.0}},
			},
		},
	}

	for _, test := range tests {
		t.Logf("running test %q", test.name)
		p, e := influxdb.NormalizeBatchPoints(test.bp)
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
