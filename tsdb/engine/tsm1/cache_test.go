package tsm1

import (
	"reflect"
	"testing"
	"time"
)

func Test_LRU(t *testing.T) {
	lru := newLRU()
	if lru == nil {
		t.Fatalf("failed to create LRU")
	}

	// Test adding various elements to the LRU.

	lru.MoveToFront("A")
	if f := lru.Front(); f != "A" {
		t.Fatalf("first inserted key not at front, got: %s", f)
	}
	if f := lru.Back(); f != "A" {
		t.Fatalf("first inserted key not at back, got: %s", f)
	}

	lru.MoveToFront("B")
	if f := lru.Front(); f != "B" {
		t.Fatalf("second inserted key not at front, got: %s", f)
	}
	if f := lru.Back(); f != "A" {
		t.Fatalf("second inserted key not at back, got: %s", f)
	}

	lru.MoveToFront("C")
	if f := lru.Front(); f != "C" {
		t.Fatalf("second inserted key not at front, got: %s", f)
	}
	if f := lru.Back(); f != "A" {
		t.Fatalf("second inserted key not at back, got: %s", f)
	}

	lru.MoveToFront("A")
	if f := lru.Front(); f != "A" {
		t.Fatalf("second inserted key not at front, got: %s", f)
	}
	if f := lru.Back(); f != "B" {
		t.Fatalf("second inserted key not at back, got: %s", f)
	}

	// Ensure that LRU ordering is correct.
	expectedOrder, gotOrder := []string{"B", "C", "A"}, []string{}
	lru.DoFromLeast(func(key string) {
		gotOrder = append(gotOrder, key)
	})
	if !reflect.DeepEqual(expectedOrder, gotOrder) {
		t.Fatalf("expected LRU order not correct, got %v, exp %v", gotOrder, expectedOrder)
	}

	// Ensure ordering is still correct after various remove operations.
	lru.Remove("A")
	lru.Remove("X")
	expectedOrder, gotOrder = []string{"B", "C"}, []string{}
	lru.DoFromLeast(func(key string) {
		gotOrder = append(gotOrder, key)
	})
	if !reflect.DeepEqual(expectedOrder, gotOrder) {
		t.Fatalf("expected LRU order not correct post remove, got %v, exp %v", gotOrder, expectedOrder)
	}
}

func Test_EntryAdd(t *testing.T) {
	e := newEntry()
	v1 := NewValue(time.Unix(2, 0).UTC(), 1.0)
	v2 := NewValue(time.Unix(3, 0).UTC(), 2.0)
	v3 := NewValue(time.Unix(1, 0).UTC(), 2.0)

	e.add([]Value{v1, v2})
	if e.size != uint64(v1.Size()+v2.Size()) {
		t.Fatal("adding points to entry, wrong size")
	}
	if e.unsorted {
		t.Fatal("adding ordered points resulted in unordered entry")
	}
	e.add([]Value{v3})
	if e.size != uint64(v1.Size()+v2.Size()+v3.Size()) {
		t.Fatal("adding point to entry, wrong size")
	}
	if !e.unsorted {
		t.Fatal("adding unordered point resulted in ordered entry")
	}
}

func Test_EntryDedupe(t *testing.T) {
	e := newEntry()
	v1 := NewValue(time.Unix(1, 0).UTC(), 1.0)
	v2 := NewValue(time.Unix(2, 0).UTC(), 2.0)
	v3 := NewValue(time.Unix(1, 0).UTC(), 2.0)

	e.add([]Value{v1, v2})
	if e.size != uint64(v1.Size()+v2.Size()) {
		t.Fatal("adding points to entry, wrong size")
	}
	if !reflect.DeepEqual(e.values, Values{v1, v2}) {
		t.Fatal("entry values not as expected")
	}
	e.dedupe()
	if !reflect.DeepEqual(e.values, Values{v1, v2}) {
		t.Fatal("entry values not as expected after dedupe")
	}

	e.add([]Value{v3})
	if !reflect.DeepEqual(e.values, Values{v1, v2, v3}) {
		t.Fatal("entry values not as expected after v3")
	}
	if e.size != uint64(v1.Size()+v2.Size()+v3.Size()) {
		t.Fatal("adding points to entry, wrong size")
	}
	e.dedupe()
	if e.size != uint64(v3.Size()+v2.Size()) {
		t.Fatal("adding points to entry, wrong size")
	}
	if !reflect.DeepEqual(e.values, Values{v3, v2}) {
		t.Fatal("entry values not as expected dedupe of v3")
	}
}

func Test_EntriesAdd(t *testing.T) {
	e := newEntries()
	v1 := NewValue(time.Unix(2, 0).UTC(), 1.0)
	v2 := NewValue(time.Unix(3, 0).UTC(), 2.0)
	v3 := NewValue(time.Unix(1, 0).UTC(), 2.0)

	e.add([]Value{v1, v2}, uint64(100))
	if e.size() != uint64(v1.Size()+v2.Size()) {
		t.Fatal("adding points to entry, wrong size")
	}
	e.add([]Value{v3}, uint64(100))
	if e.size() != uint64(v1.Size()+v2.Size()+v3.Size()) {
		t.Fatal("adding point to entry, wrong size")
	}
}

func Test_EntriesClone(t *testing.T) {
	e := newEntries()
	v0 := NewValue(time.Unix(4, 0).UTC(), 1.0)
	v1 := NewValue(time.Unix(2, 0).UTC(), 2.0)
	v2 := NewValue(time.Unix(3, 0).UTC(), 3.0)
	v3 := NewValue(time.Unix(3, 0).UTC(), 4.0)

	e.add([]Value{v0, v1}, uint64(100))
	e.add([]Value{v2}, uint64(200))
	e.add([]Value{v3}, uint64(400))

	values := e.clone()
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

	if n := e.purge(100); n != uint64(v0.Size()+v1.Size()) {
		t.Fatal("wrong size of points purged:", n)
	}
}

func Test_EntriesPurge(t *testing.T) {
	e := newEntries()
	v0 := NewValue(time.Unix(1, 0).UTC(), 1.0)
	v1 := NewValue(time.Unix(2, 0).UTC(), 2.0)
	v2 := NewValue(time.Unix(3, 0).UTC(), 3.0)

	e.add([]Value{v0, v1}, uint64(100))
	e.add([]Value{v2}, uint64(200))

	values := e.clone()
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

	if n := e.purge(100); n != uint64(v0.Size()+v1.Size()) {
		t.Fatal("wrong size of points purged:", n)
	}

	values = e.clone()
	if len(values) != 1 {
		t.Fatalf("purged cloned values is wrong length, got %d", len(values))
	}
	if !reflect.DeepEqual(values[0], v2) {
		t.Fatal("0th point does not equal v1:", values[0], v2)
	}

	if n := e.purge(200); n != uint64(v2.Size()) {
		t.Fatal("wrong size of points purged:", n)
	}
	values = e.clone()
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
