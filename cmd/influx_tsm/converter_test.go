package main

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/influxdb/influxdb/tsdb/engine/tsm1"
)

func Test_scrubValuesNoFilter(t *testing.T) {
	values := []tsm1.Value{tsm1.NewValue(time.Unix(0, 0), 1.0)}
	scrubbed := scrubValues(values)
	if !reflect.DeepEqual(values, scrubbed) {
		t.Fatalf("mismatch:\n\nexp=%+v\n\ngot=%+v\n\n", values, scrubbed)
	}
}

func Test_scrubValuesFilterNaN(t *testing.T) {
	intV := tsm1.NewValue(time.Unix(0, 0), 1.0)
	values := []tsm1.Value{intV, tsm1.NewValue(time.Unix(0, 0), math.NaN())}
	scrubbed := scrubValues(values)
	if !reflect.DeepEqual([]tsm1.Value{intV}, scrubbed) {
		t.Fatalf("mismatch:\n\nexp=%+v\n\ngot=%+v\n\n", []tsm1.Value{intV}, scrubbed)
	}
}

func Test_scrubValuesFilterInf(t *testing.T) {
	intV := tsm1.NewValue(time.Unix(0, 0), 1.0)
	values := []tsm1.Value{intV, tsm1.NewValue(time.Unix(0, 0), math.Inf(-1))}
	scrubbed := scrubValues(values)
	if !reflect.DeepEqual([]tsm1.Value{intV}, scrubbed) {
		t.Fatalf("mismatch:\n\nexp=%+v\n\ngot=%+v\n\n", []tsm1.Value{intV}, scrubbed)
	}
}

func Test_scrubValuesFilterBoth(t *testing.T) {
	intV := tsm1.NewValue(time.Unix(0, 0), 1.0)
	values := []tsm1.Value{intV, tsm1.NewValue(time.Unix(0, 0), math.Inf(-1)), tsm1.NewValue(time.Unix(0, 0), math.NaN())}
	scrubbed := scrubValues(values)
	if !reflect.DeepEqual([]tsm1.Value{intV}, scrubbed) {
		t.Fatalf("mismatch:\n\nexp=%+v\n\ngot=%+v\n\n", []tsm1.Value{intV}, scrubbed)
	}
	return
}
