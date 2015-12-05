package tsm1

import (
	"reflect"
	"testing"
	"time"
)

func TestCache_NewCache(t *testing.T) {
	c := NewCache(100)
	if c == nil {
		t.Fatalf("failed to create new cache")
	}

	if c.MaxSize() != 100 {
		t.Fatalf("new cache max size not correct")
	}
	if c.Size() != 0 {
		t.Fatalf("new cache size not correct")
	}
	if len(c.Keys()) != 0 {
		t.Fatalf("new cache keys not correct: %v", c.Keys())
	}
}

func TestCache_CacheWrite(t *testing.T) {
	v0 := NewValue(time.Unix(1, 0).UTC(), 1.0)
	v1 := NewValue(time.Unix(2, 0).UTC(), 2.0)
	v2 := NewValue(time.Unix(3, 0).UTC(), 3.0)
	values := Values{v0, v1, v2}
	valuesSize := uint64(v0.Size() + v1.Size() + v2.Size())

	c := NewCache(3 * valuesSize)

	if err := c.Write("foo", values); err != nil {
		t.Fatalf("failed to write key foo to cache: %s", err.Error())
	}
	if err := c.Write("bar", values); err != nil {
		t.Fatalf("failed to write key foo to cache: %s", err.Error())
	}
	if n := c.Size(); n != 2*valuesSize {
		t.Fatalf("cache size incorrect after 2 writes, exp %d, got %d", 2*valuesSize, n)
	}

	if exp, keys := []string{"bar", "foo"}, c.Keys(); !reflect.DeepEqual(keys, exp) {
		t.Fatalf("cache keys incorrect after 2 writes, exp %v, got %v", exp, keys)
	}
}

func TestCache_CacheWriteMulti(t *testing.T) {
	v0 := NewValue(time.Unix(1, 0).UTC(), 1.0)
	v1 := NewValue(time.Unix(2, 0).UTC(), 2.0)
	v2 := NewValue(time.Unix(3, 0).UTC(), 3.0)
	values := Values{v0, v1, v2}
	valuesSize := uint64(v0.Size() + v1.Size() + v2.Size())

	c := NewCache(3 * valuesSize)

	if err := c.WriteMulti(map[string][]Value{"foo": values, "bar": values}); err != nil {
		t.Fatalf("failed to write key foo to cache: %s", err.Error())
	}
	if n := c.Size(); n != 2*valuesSize {
		t.Fatalf("cache size incorrect after 2 writes, exp %d, got %d", 2*valuesSize, n)
	}

	if exp, keys := []string{"bar", "foo"}, c.Keys(); !reflect.DeepEqual(keys, exp) {
		t.Fatalf("cache keys incorrect after 2 writes, exp %v, got %v", exp, keys)
	}
}

func TestCache_CacheValues(t *testing.T) {
	v0 := NewValue(time.Unix(1, 0).UTC(), 0.0)
	v1 := NewValue(time.Unix(2, 0).UTC(), 2.0)
	v2 := NewValue(time.Unix(3, 0).UTC(), 3.0)
	v3 := NewValue(time.Unix(1, 0).UTC(), 1.0)
	v4 := NewValue(time.Unix(4, 0).UTC(), 4.0)

	c := NewCache(512)
	if deduped := c.Values("no such key"); deduped != nil {
		t.Fatalf("Values returned for no such key")
	}

	if err := c.Write("foo", Values{v0, v1, v2, v3}); err != nil {
		t.Fatalf("failed to write 3 values, key foo to cache: %s", err.Error())
	}
	if err := c.Write("foo", Values{v4}); err != nil {
		t.Fatalf("failed to write 1 value, key foo to cache: %s", err.Error())
	}

	expAscValues := Values{v3, v1, v2, v4}
	if deduped := c.Values("foo"); !reflect.DeepEqual(expAscValues, deduped) {
		t.Fatalf("deduped ascending values for foo incorrect, exp: %v, got %v", expAscValues, deduped)
	}
}

func TestCache_CacheSnapshot(t *testing.T) {
	v0 := NewValue(time.Unix(2, 0).UTC(), 0.0)
	v1 := NewValue(time.Unix(3, 0).UTC(), 2.0)
	v2 := NewValue(time.Unix(4, 0).UTC(), 3.0)
	v3 := NewValue(time.Unix(5, 0).UTC(), 4.0)
	v4 := NewValue(time.Unix(6, 0).UTC(), 5.0)
	v5 := NewValue(time.Unix(1, 0).UTC(), 5.0)

	c := NewCache(512)
	if err := c.Write("foo", Values{v0, v1, v2, v3}); err != nil {
		t.Fatalf("failed to write 3 values, key foo to cache: %s", err.Error())
	}

	// Grab snapshot, and ensure it's as expected.
	snapshot := c.Snapshot()
	expValues := Values{v0, v1, v2, v3}
	if deduped := snapshot.values("foo"); !reflect.DeepEqual(expValues, deduped) {
		t.Fatalf("snapshotted values for foo incorrect, exp: %v, got %v", expValues, deduped)
	}

	// Ensure cache is still as expected.
	if deduped := c.Values("foo"); !reflect.DeepEqual(expValues, deduped) {
		t.Fatalf("post-snapshot values for foo incorrect, exp: %v, got %v", expValues, deduped)
	}

	// Write a new value to the cache.
	if err := c.Write("foo", Values{v4}); err != nil {
		t.Fatalf("failed to write post-snap value, key foo to cache: %s", err.Error())
	}
	expValues = Values{v0, v1, v2, v3, v4}
	if deduped := c.Values("foo"); !reflect.DeepEqual(expValues, deduped) {
		t.Fatalf("post-snapshot write values for foo incorrect, exp: %v, got %v", expValues, deduped)
	}

	// Write a new, out-of-order, value to the cache.
	if err := c.Write("foo", Values{v5}); err != nil {
		t.Fatalf("failed to write post-snap value, key foo to cache: %s", err.Error())
	}
	expValues = Values{v5, v0, v1, v2, v3, v4}
	if deduped := c.Values("foo"); !reflect.DeepEqual(expValues, deduped) {
		t.Fatalf("post-snapshot out-of-order write values for foo incorrect, exp: %v, got %v", expValues, deduped)
	}

	// Clear snapshot, ensuring non-snapshot data untouched.
	c.ClearSnapshot(snapshot)
	expValues = Values{v5, v4}
	if deduped := c.Values("foo"); !reflect.DeepEqual(expValues, deduped) {
		t.Fatalf("post-clear values for foo incorrect, exp: %v, got %v", expValues, deduped)
	}
}

func TestCache_CacheEmptySnapshot(t *testing.T) {
	c := NewCache(512)

	// Grab snapshot, and ensure it's as expected.
	snapshot := c.Snapshot()
	if deduped := snapshot.values("foo"); !reflect.DeepEqual(Values(nil), deduped) {
		t.Fatalf("snapshotted values for foo incorrect, exp: %v, got %v", nil, deduped)
	}

	// Ensure cache is still as expected.
	if deduped := c.Values("foo"); !reflect.DeepEqual(Values(nil), deduped) {
		t.Fatalf("post-snapshotted values for foo incorrect, exp: %v, got %v", Values(nil), deduped)
	}

	// Clear snapshot.
	c.ClearSnapshot(snapshot)
	if deduped := c.Values("foo"); !reflect.DeepEqual(Values(nil), deduped) {
		t.Fatalf("post-snapshot-clear values for foo incorrect, exp: %v, got %v", Values(nil), deduped)
	}
}

func TestCache_CacheWriteMemoryExceeded(t *testing.T) {
	v0 := NewValue(time.Unix(1, 0).UTC(), 1.0)
	v1 := NewValue(time.Unix(2, 0).UTC(), 2.0)

	c := NewCache(uint64(v1.Size()))

	if err := c.Write("foo", Values{v0}); err != nil {
		t.Fatalf("failed to write key foo to cache: %s", err.Error())
	}
	if exp, keys := []string{"foo"}, c.Keys(); !reflect.DeepEqual(keys, exp) {
		t.Fatalf("cache keys incorrect after writes, exp %v, got %v", exp, keys)
	}
	if err := c.Write("bar", Values{v1}); err != ErrCacheMemoryExceeded {
		t.Fatalf("wrong error writing key bar to cache")
	}

	// Grab snapshot, write should still fail since we're still using the memory.
	snapshot := c.Snapshot()
	if err := c.Write("bar", Values{v1}); err != ErrCacheMemoryExceeded {
		t.Fatalf("wrong error writing key bar to cache")
	}

	// Clear the snapshot and the write should now succeed.
	c.ClearSnapshot(snapshot)
	if err := c.Write("bar", Values{v1}); err != nil {
		t.Fatalf("failed to write key foo to cache: %s", err.Error())
	}
	expAscValues := Values{v1}
	if deduped := c.Values("bar"); !reflect.DeepEqual(expAscValues, deduped) {
		t.Fatalf("deduped ascending values for bar incorrect, exp: %v, got %v", expAscValues, deduped)
	}
}
