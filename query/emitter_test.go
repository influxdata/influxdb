package query_test

import (
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/query"
)

// Ensure the emitter can group iterators together into rows.
func TestEmitter(t *testing.T) {
	// Build an emitter that pulls from two iterators.
	e := query.NewEmitter([]query.Iterator{
		&FloatIterator{Points: []query.FloatPoint{
			{Name: "cpu", Tags: ParseTags("region=west"), Time: 0, Value: 1},
			{Name: "cpu", Tags: ParseTags("region=west"), Time: 1, Value: 2},
		}},
		&FloatIterator{Points: []query.FloatPoint{
			{Name: "cpu", Tags: ParseTags("region=west"), Time: 1, Value: 4},
			{Name: "cpu", Tags: ParseTags("region=north"), Time: 0, Value: 4},
			{Name: "mem", Time: 4, Value: 5},
		}},
	}, true)

	// Verify the cpu region=west is emitted first.
	var values [2]interface{}
	if ts, name, tags, err := e.LoadBuf(); err != nil {
		t.Fatalf("unexpected error(0): %s", err)
	} else if have, want := ts, int64(0); have != want {
		t.Fatalf("unexpected time(0): have=%v want=%v", have, want)
	} else if have, want := name, "cpu"; have != want {
		t.Fatalf("unexpected name(0): have=%v want=%v", have, want)
	} else if have, want := tags.ID(), "region\x00west"; have != want {
		t.Fatalf("unexpected tags(0): have=%v want=%v", have, want)
	} else {
		e.ReadInto(ts, name, tags, values[:])
		if have, want := values[:], []interface{}{float64(1), nil}; !reflect.DeepEqual(have, want) {
			t.Fatalf("unexpected values(0): have=%v want=%v", have, want)
		}
	}

	if ts, name, tags, err := e.LoadBuf(); err != nil {
		t.Fatalf("unexpected error(1): %s", err)
	} else if have, want := ts, int64(1); have != want {
		t.Fatalf("unexpected time(1): have=%v want=%v", have, want)
	} else if have, want := name, "cpu"; have != want {
		t.Fatalf("unexpected name(1): have=%v want=%v", have, want)
	} else if have, want := tags.ID(), "region\x00west"; have != want {
		t.Fatalf("unexpected tags(1): have=%v want=%v", have, want)
	} else {
		e.ReadInto(ts, name, tags, values[:])
		if have, want := values[:], []interface{}{float64(2), float64(4)}; !reflect.DeepEqual(have, want) {
			t.Fatalf("unexpected values(1): have=%v want=%v", have, want)
		}
	}

	// Verify the cpu region=north is emitted next.
	if ts, name, tags, err := e.LoadBuf(); err != nil {
		t.Fatalf("unexpected error(2): %s", err)
	} else if have, want := ts, int64(0); have != want {
		t.Fatalf("unexpected time(2): have=%v want=%v", have, want)
	} else if have, want := name, "cpu"; have != want {
		t.Fatalf("unexpected name(2): have=%v want=%v", have, want)
	} else if have, want := tags.ID(), "region\x00north"; have != want {
		t.Fatalf("unexpected tags(2): have=%v want=%v", have, want)
	} else {
		e.ReadInto(ts, name, tags, values[:])
		if have, want := values[:], []interface{}{nil, float64(4)}; !reflect.DeepEqual(have, want) {
			t.Fatalf("unexpected values(2): have=%v want=%v", have, want)
		}
	}

	// Verify the mem series is emitted last.
	if ts, name, tags, err := e.LoadBuf(); err != nil {
		t.Fatalf("unexpected error(3): %s", err)
	} else if have, want := ts, int64(4); have != want {
		t.Fatalf("unexpected time(3): have=%v want=%v", have, want)
	} else if have, want := name, "mem"; have != want {
		t.Fatalf("unexpected name(3): have=%v want=%v", have, want)
	} else if have, want := tags.ID(), ""; have != want {
		t.Fatalf("unexpected tags(3): have=%v want=%v", have, want)
	} else {
		e.ReadInto(ts, name, tags, values[:])
		if have, want := values[:], []interface{}{nil, float64(5)}; !reflect.DeepEqual(have, want) {
			t.Fatalf("unexpected values(2): have=%v want=%v", have, want)
		}
	}

	// Verify EOF.
	if ts, _, _, err := e.LoadBuf(); err != nil {
		t.Fatalf("unexpected error(eof): %s", err)
	} else if ts != query.ZeroTime {
		t.Fatalf("unexpected time(eof): %v", ts)
	}
}
