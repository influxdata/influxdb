package execute_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/values"
)

var (
	cols = []query.ColMeta{
		{Label: "a", Type: query.TString},
		{Label: "b", Type: query.TString},
		{Label: "c", Type: query.TString},
	}
	key0 = execute.NewPartitionKey(
		cols,
		[]values.Value{
			values.NewStringValue("I"),
			values.NewStringValue("J"),
			values.NewStringValue("K"),
		},
	)
	key1 = execute.NewPartitionKey(
		cols,
		[]values.Value{
			values.NewStringValue("L"),
			values.NewStringValue("M"),
			values.NewStringValue("N"),
		},
	)
	key2 = execute.NewPartitionKey(
		cols,
		[]values.Value{
			values.NewStringValue("X"),
			values.NewStringValue("Y"),
			values.NewStringValue("Z"),
		},
	)
)

func TestPartitionLookup(t *testing.T) {
	l := execute.NewPartitionLookup()
	l.Set(key0, 0)
	if v, ok := l.Lookup(key0); !ok || v != 0 {
		t.Error("failed to lookup key0")
	}
	l.Set(key1, 1)
	if v, ok := l.Lookup(key1); !ok || v != 1 {
		t.Error("failed to lookup key1")
	}
	l.Set(key2, 2)
	if v, ok := l.Lookup(key2); !ok || v != 2 {
		t.Error("failed to lookup key2")
	}

	want := []entry{
		{Key: key0, Value: 0},
		{Key: key1, Value: 1},
		{Key: key2, Value: 2},
	}

	var got []entry
	l.Range(func(k query.PartitionKey, v interface{}) {
		got = append(got, entry{
			Key:   k,
			Value: v.(int),
		})
	})

	if !cmp.Equal(want, got) {
		t.Fatalf("unexpected range: -want/+got:\n%s", cmp.Diff(want, got))
	}

	l.Set(key0, -1)
	if v, ok := l.Lookup(key0); !ok || v != -1 {
		t.Error("failed to lookup key0 after set")
	}

	l.Delete(key1)
	if _, ok := l.Lookup(key1); ok {
		t.Error("failed to delete key1")
	}
	l.Delete(key0)
	if _, ok := l.Lookup(key0); ok {
		t.Error("failed to delete key0")
	}
	l.Delete(key2)
	if _, ok := l.Lookup(key2); ok {
		t.Error("failed to delete key2")
	}
}

// Test that the lookup supports Deletes while rangeing.
func TestPartitionLookup_RangeWithDelete(t *testing.T) {
	l := execute.NewPartitionLookup()
	l.Set(key0, 0)
	if v, ok := l.Lookup(key0); !ok || v != 0 {
		t.Error("failed to lookup key0")
	}
	l.Set(key1, 1)
	if v, ok := l.Lookup(key1); !ok || v != 1 {
		t.Error("failed to lookup key1")
	}
	l.Set(key2, 2)
	if v, ok := l.Lookup(key2); !ok || v != 2 {
		t.Error("failed to lookup key2")
	}

	want := []entry{
		{Key: key0, Value: 0},
		{Key: key1, Value: 1},
	}
	var got []entry
	l.Range(func(k query.PartitionKey, v interface{}) {
		// Delete the current key
		l.Delete(key0)
		// Delete a future key
		l.Delete(key2)

		got = append(got, entry{
			Key:   k,
			Value: v.(int),
		})
	})
	if !cmp.Equal(want, got) {
		t.Fatalf("unexpected range: -want/+got:\n%s", cmp.Diff(want, got))
	}
}

type entry struct {
	Key   query.PartitionKey
	Value int
}
