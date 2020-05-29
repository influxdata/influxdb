package tsm1

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestIndirectIndexIterator(t *testing.T) {
	checkEqual := func(t *testing.T, got, exp interface{}) {
		t.Helper()
		if !reflect.DeepEqual(got, exp) {
			t.Fatalf("expected: %v but got: %v\n%v", exp, got, cmp.Diff(got, exp))
		}
	}

	index := NewIndexWriter()
	index.Add([]byte("cpu1"), BlockInteger, 0, 10, 10, 20)
	index.Add([]byte("cpu1"), BlockInteger, 10, 20, 10, 20)
	index.Add([]byte("cpu2"), BlockInteger, 0, 10, 10, 20)
	index.Add([]byte("cpu2"), BlockInteger, 10, 20, 10, 20)
	index.Add([]byte("mem"), BlockInteger, 0, 10, 10, 20)
	ind := loadIndex(t, index)

	// check that the iterator walks the whole index
	iter := ind.Iterator(nil)
	checkEqual(t, iter.Next(), true)
	checkEqual(t, iter.Peek(), []byte("cpu2"))
	checkEqual(t, iter.Key(), []byte("cpu1"))
	checkEqual(t, iter.Type(), BlockInteger)
	checkEqual(t, iter.Entries(), []IndexEntry{
		{0, 10, 10, 20},
		{10, 20, 10, 20},
	})
	checkEqual(t, iter.Next(), true)
	checkEqual(t, iter.Peek(), []byte("mem"))
	checkEqual(t, iter.Key(), []byte("cpu2"))
	checkEqual(t, iter.Type(), BlockInteger)
	checkEqual(t, iter.Entries(), []IndexEntry{
		{0, 10, 10, 20},
		{10, 20, 10, 20},
	})
	checkEqual(t, iter.Next(), true)
	checkEqual(t, iter.Peek(), []byte(nil))
	checkEqual(t, iter.Key(), []byte("mem"))
	checkEqual(t, iter.Type(), BlockInteger)
	checkEqual(t, iter.Entries(), []IndexEntry{
		{0, 10, 10, 20},
	})
	checkEqual(t, iter.Next(), false)
	checkEqual(t, iter.Err(), error(nil))

	// check can seek and iterate index
	iter = ind.Iterator(nil)
	exact, ok := iter.Seek([]byte("cpu2"))
	checkEqual(t, exact, true)
	checkEqual(t, ok, true)
	checkEqual(t, iter.Key(), []byte("cpu2"))
	checkEqual(t, iter.Type(), BlockInteger)
	checkEqual(t, iter.Entries(), []IndexEntry{
		{0, 10, 10, 20},
		{10, 20, 10, 20},
	})
	checkEqual(t, iter.Next(), true)
	checkEqual(t, iter.Key(), []byte("mem"))
	checkEqual(t, iter.Next(), false)
	exact, ok = iter.Seek([]byte("cpu1"))
	checkEqual(t, exact, true)
	checkEqual(t, ok, true)
	checkEqual(t, iter.Key(), []byte("cpu1"))
	exact, ok = iter.Seek([]byte("cpu3"))
	checkEqual(t, exact, false)
	checkEqual(t, ok, true)
	checkEqual(t, iter.Key(), []byte("mem"))
	exact, ok = iter.Seek([]byte("cpu0"))
	checkEqual(t, exact, false)
	checkEqual(t, ok, true)
	checkEqual(t, iter.Key(), []byte("cpu1"))
	exact, ok = iter.Seek([]byte("zzz"))
	checkEqual(t, exact, false)
	checkEqual(t, ok, false)
	checkEqual(t, iter.Next(), false)
	checkEqual(t, iter.Err(), error(nil))

	// delete the cpu2 key and make sure it's skipped
	ind.Delete([][]byte{[]byte("cpu2")})
	iter = ind.Iterator(nil)
	checkEqual(t, iter.Next(), true)
	checkEqual(t, iter.Peek(), []byte("mem"))
	checkEqual(t, iter.Key(), []byte("cpu1"))
	checkEqual(t, iter.Type(), BlockInteger)
	checkEqual(t, iter.Entries(), []IndexEntry{
		{0, 10, 10, 20},
		{10, 20, 10, 20},
	})
	checkEqual(t, iter.Next(), true)
	checkEqual(t, iter.Peek(), []byte(nil))
	checkEqual(t, iter.Key(), []byte("mem"))
	checkEqual(t, iter.Type(), BlockInteger)
	checkEqual(t, iter.Entries(), []IndexEntry{
		{0, 10, 10, 20},
	})
	checkEqual(t, iter.Next(), false)
	checkEqual(t, iter.Err(), error(nil))

	// check that seek works
	iter = ind.Iterator([]byte("d"))
	checkEqual(t, iter.Next(), true)
	checkEqual(t, iter.Peek(), []byte(nil))
	checkEqual(t, iter.Key(), []byte("mem"))
	checkEqual(t, iter.Type(), BlockInteger)
	checkEqual(t, iter.Entries(), []IndexEntry{
		{0, 10, 10, 20},
	})
	checkEqual(t, iter.Next(), false)
	checkEqual(t, iter.Err(), error(nil))
}
