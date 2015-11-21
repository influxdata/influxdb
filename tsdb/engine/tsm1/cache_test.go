package tsm1

import (
	"reflect"
	"testing"
	"time"
)

func Test_EntriesAdd(t *testing.T) {
	e := newEntries()
	v1 := NewValue(time.Unix(2, 0).UTC(), 1.0)
	v2 := NewValue(time.Unix(3, 0).UTC(), 2.0)
	v3 := NewValue(time.Unix(1, 0).UTC(), 2.0)

	e.add(uint64(100), []Value{v1, v2})
	if e.size != uint64(v1.Size()+v2.Size()) {
		t.Fatal("adding points to entry, wrong size")
	}
	e.add(uint64(100), []Value{v3})
	if e.size != uint64(v1.Size()+v2.Size()+v3.Size()) {
		t.Fatal("adding point to entry, wrong size")
	}
}

func Test_EntriesDedupe(t *testing.T) {
	e := newEntries()
	v0 := NewValue(time.Unix(4, 0).UTC(), 1.0)
	v1 := NewValue(time.Unix(2, 0).UTC(), 2.0)
	v2 := NewValue(time.Unix(3, 0).UTC(), 3.0)
	v3 := NewValue(time.Unix(3, 0).UTC(), 4.0)

	e.add(uint64(100), []Value{v0, v1})
	e.add(uint64(200), []Value{v2})
	e.add(uint64(400), []Value{v3})

	values := e.dedupe()
	if len(values) != 3 {
		t.Fatalf("cloned values is wrong length, got %d", len(values))
	}
	if !reflect.DeepEqual(values[0], v1) {
		t.Fatal("0th point does not equal v1:", values[0], v1)
	}
	if !reflect.DeepEqual(values[1], v3) {
		t.Fatal("1st point does not equal v3:", values[0], v3)
	}
	if !reflect.DeepEqual(values[2], v0) {
		t.Fatal("2nd point does not equal v0:", values[0], v0)
	}
}

func Test_EntriesEvict(t *testing.T) {
	e := newEntries()
	v0 := NewValue(time.Unix(1, 0).UTC(), 1.0)
	v1 := NewValue(time.Unix(2, 0).UTC(), 2.0)
	v2 := NewValue(time.Unix(3, 0).UTC(), 3.0)

	e.add(uint64(100), []Value{v0, v1})
	e.add(uint64(200), []Value{v2})
	if e.size != uint64(v0.Size()+v1.Size()+v2.Size()) {
		t.Fatal("wrong size post eviction:", e.size)
	}

	values := e.dedupe()
	if len(values) != 3 {
		t.Fatalf("cloned values is wrong length, got %d", len(values))
	}
	if !reflect.DeepEqual(values[0], v0) {
		t.Fatal("0th point does not equal v0:", values[0], v0)
	}
	if !reflect.DeepEqual(values[1], v1) {
		t.Fatal("1st point does not equal v1:", values[0], v1)
	}
	if !reflect.DeepEqual(values[2], v2) {
		t.Fatal("2nd point does not equal v2:", values[0], v2)
	}

	e.evict(100)
	if e.size != uint64(v2.Size()) {
		t.Fatalf("wrong size post eviction, exp: %d, got %d:", v2.Size(), e.size)
	}

	values = e.dedupe()
	if len(values) != 1 {
		t.Fatalf("purged cloned values is wrong length, got %d", len(values))
	}
	if !reflect.DeepEqual(values[0], v2) {
		t.Fatal("0th point does not equal v1:", values[0], v2)
	}

	e.evict(200)
	if e.size != 0 {
		t.Fatal("wrong size post eviction of last point:", e.size)
	}

	values = e.dedupe()
	if len(values) != 0 {
		t.Fatalf("purged cloned values is wrong length, got %d", len(values))
	}
}

func Test_NewCache(t *testing.T) {
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
	if c.Checkpoint() != 0 {
		t.Fatalf("new checkpoint not correct")
	}
	if len(c.Keys()) != 0 {
		t.Fatalf("new cache keys not correct: %v", c.Keys())
	}
}

func Test_CacheWrite(t *testing.T) {
	v0 := NewValue(time.Unix(1, 0).UTC(), 1.0)
	v1 := NewValue(time.Unix(2, 0).UTC(), 2.0)
	v2 := NewValue(time.Unix(3, 0).UTC(), 3.0)
	values := Values{v0, v1, v2}
	valuesSize := uint64(v0.Size() + v1.Size() + v2.Size())

	c := MustNewCache(3 * valuesSize)

	if err := c.Write("foo", values, 100); err != nil {
		t.Fatalf("failed to write key foo to cache: %s", err.Error())
	}
	if err := c.Write("bar", values, 100); err != nil {
		t.Fatalf("failed to write key foo to cache: %s", err.Error())
	}
	if n := c.Size(); n != 2*valuesSize {
		t.Fatalf("cache size incorrect after 2 writes, exp %d, got %d", 2*valuesSize, n)
	}

	if exp, keys := []string{"bar", "foo"}, c.Keys(); !reflect.DeepEqual(keys, exp) {
		t.Fatalf("cache keys incorrect after 2 writes, exp %v, got %v", exp, keys)
	}
}

func Test_CacheValues(t *testing.T) {
	v0 := NewValue(time.Unix(1, 0).UTC(), 0.0)
	v1 := NewValue(time.Unix(2, 0).UTC(), 2.0)
	v2 := NewValue(time.Unix(3, 0).UTC(), 3.0)
	v3 := NewValue(time.Unix(1, 0).UTC(), 1.0)
	v4 := NewValue(time.Unix(4, 0).UTC(), 4.0)

	c := MustNewCache(512)
	if deduped := c.Values("no such key"); deduped != nil {
		t.Fatalf("Values returned for no such key")
	}

	if err := c.Write("foo", Values{v0, v1, v2, v3}, 100); err != nil {
		t.Fatalf("failed to write 3 values, key foo to cache: %s", err.Error())
	}
	if err := c.Write("foo", Values{v4}, 200); err != nil {
		t.Fatalf("failed to write 1 value, key foo to cache: %s", err.Error())
	}

	expValues := Values{v3, v1, v2, v4}
	if deduped := c.Values("foo"); !reflect.DeepEqual(expValues, deduped) {
		t.Fatalf("deduped values for foo incorrect, exp: %v, got %v", expValues, deduped)
	}
}

func Test_CacheCheckpoint(t *testing.T) {
	v0 := NewValue(time.Unix(1, 0).UTC(), 1.0)

	c := MustNewCache(1024)

	if err := c.SetCheckpoint(50); err != nil {
		t.Fatalf("failed to set checkpoint: %s", err.Error())
	}
	if err := c.Write("foo", Values{v0}, 100); err != nil {
		t.Fatalf("failed to write key foo to cache: %s", err.Error())
	}
	if err := c.SetCheckpoint(25); err != ErrCacheInvalidCheckpoint {
		t.Fatalf("unexpectedly set checkpoint")
	}
	if err := c.Write("foo", Values{v0}, 30); err != ErrCacheInvalidCheckpoint {
		t.Fatalf("unexpectedly wrote key foo to cache")
	}
}

func Test_CacheWriteMemoryExceeded(t *testing.T) {
	v0 := NewValue(time.Unix(1, 0).UTC(), 1.0)
	v1 := NewValue(time.Unix(2, 0).UTC(), 2.0)

	c := MustNewCache(uint64(v1.Size()))

	if err := c.Write("foo", Values{v0}, 100); err != nil {
		t.Fatalf("failed to write key foo to cache: %s", err.Error())
	}
	if exp, keys := []string{"foo"}, c.Keys(); !reflect.DeepEqual(keys, exp) {
		t.Fatalf("cache keys incorrect after writes, exp %v, got %v", exp, keys)
	}
	if err := c.Write("bar", Values{v1}, 100); err != ErrCacheMemoryExceeded {
		t.Fatalf("wrong error writing key bar to cache")
	}

	// Set too-early checkpoint, write should still fail.
	if err := c.SetCheckpoint(50); err != nil {
		t.Fatalf("failed to set checkpoint: %s", err.Error())
	}
	if err := c.Write("bar", Values{v1}, 100); err != ErrCacheMemoryExceeded {
		t.Fatalf("wrong error writing key bar to cache")
	}

	// Set later checkpoint, write should then succeed.
	if err := c.SetCheckpoint(100); err != nil {
		t.Fatalf("failed to set checkpoint: %s", err.Error())
	}
	if err := c.Write("bar", Values{v1}, 100); err != nil {
		t.Fatalf("failed to write key bar to checkpointed cache: %s", err.Error())
	}
	if exp, keys := []string{"bar"}, c.Keys(); !reflect.DeepEqual(keys, exp) {
		t.Fatalf("cache keys incorrect after writes, exp %v, got %v", exp, keys)
	}
}

func MustNewCache(size uint64) *Cache {
	c := NewCache(size)
	if c == nil {
		panic("failed to create cache")
	}
	return c
}
