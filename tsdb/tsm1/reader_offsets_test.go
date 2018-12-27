package tsm1

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestReaderOffsets(t *testing.T) {
	const numKeys = 100

	check := func(t *testing.T, what string, got, exp interface{}, extra ...interface{}) {
		t.Helper()
		if got != exp {
			args := []interface{}{"incorrect", what, "got:", got, "exp:", exp}
			args = append(args, extra...)
			t.Fatal(args...)
		}
	}

	makeKey := func(i int) string { return fmt.Sprintf("%09d", i) }

	makeRO := func() (readerOffsets, *faultBuffer) {
		var buf []byte
		var ro readerOffsets
		for i := 0; i < numKeys; i++ {
			ro.AddKey(addKey(&buf, makeKey(i)))
		}
		ro.Done()

		return ro, &faultBuffer{b: buf}
	}

	t.Run("Create_SingleKey", func(t *testing.T) {
		var buf []byte
		var ro readerOffsets
		ro.AddKey(addKey(&buf, makeKey(0)))
		ro.Done()

		check(t, "offsets", len(ro.offsets), 1)
		check(t, "prefixes", len(ro.prefixes), 1)
	})

	t.Run("Create", func(t *testing.T) {
		ro, _ := makeRO()

		check(t, "offsets", len(ro.offsets), numKeys)
		check(t, "prefixes", len(ro.prefixes), numKeys/10)
	})

	t.Run("Iterate", func(t *testing.T) {
		ro, fb := makeRO()

		iter := ro.Iterator()
		for i := 0; iter.Next(); i++ {
			check(t, "key", string(iter.Key(fb)), makeKey(i))
		}
	})

	t.Run("Seek", func(t *testing.T) {
		ro, fb := makeRO()
		exact, ok := false, false

		iter := ro.Iterator()
		for i := 0; i < numKeys-1; i++ {
			exact, ok = iter.Seek([]byte(makeKey(i)), fb)
			check(t, "exact", exact, true)
			check(t, "ok", ok, true)
			check(t, "key", string(iter.Key(fb)), makeKey(i))

			exact, ok = iter.Seek([]byte(makeKey(i)+"0"), fb)
			check(t, "exact", exact, false)
			check(t, "ok", ok, true)
			check(t, "key", string(iter.Key(fb)), makeKey(i+1))
		}

		exact, ok = iter.Seek([]byte(makeKey(numKeys-1)), fb)
		check(t, "exact", exact, true)
		check(t, "ok", ok, true)
		check(t, "key", string(iter.Key(fb)), makeKey(numKeys-1))

		exact, ok = iter.Seek([]byte(makeKey(numKeys-1)+"0"), fb)
		check(t, "exact", exact, false)
		check(t, "ok", ok, false)

		exact, ok = iter.Seek([]byte("1"), fb)
		check(t, "exact", exact, false)
		check(t, "ok", ok, false)

		exact, ok = iter.Seek(nil, fb)
		check(t, "exact", exact, false)
		check(t, "ok", ok, true)
		check(t, "key", string(iter.Key(fb)), makeKey(0))
	})

	t.Run("Delete", func(t *testing.T) {
		ro, fb := makeRO()

		iter := ro.Iterator()
		for i := 0; iter.Next(); i++ {
			if i%2 == 0 {
				continue
			}
			iter.Delete()
		}
		iter.Done()

		iter = ro.Iterator()
		for i := 0; iter.Next(); i++ {
			check(t, "key", string(iter.Key(fb)), makeKey(2*i))
		}
	})

	t.Run("Fuzz", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			ro, fb := makeRO()
			deleted := make(map[string]struct{})
			iter := ro.Iterator()

			for i := 0; i < numKeys; i++ {
				// delete a random key. if we seek past, delete the first key.
				_, ok := iter.Seek([]byte(makeKey(rand.Intn(numKeys))), fb)
				if !ok {
					iter.Seek(nil, fb)
				}
				key := string(iter.Key(fb))
				_, ok = deleted[key]
				check(t, "key deleted", ok, false, "for key", key)
				deleted[key] = struct{}{}
				iter.Delete()
				iter.Done()

				// seek to every key that isn't deleted.
				for i := 0; i < numKeys; i++ {
					key := makeKey(i)
					if _, ok := deleted[key]; ok {
						continue
					}

					exact, ok := iter.Seek([]byte(key), fb)
					check(t, "exact", exact, true, "for key", key)
					check(t, "ok", ok, true, "for key", key)
					check(t, "key", string(iter.Key(fb)), key)
				}
			}

			check(t, "amount deleted", len(deleted), numKeys)
			iter = ro.Iterator()
			check(t, "next", iter.Next(), false)
		}
	})
}

func addKey(buf *[]byte, key string) (uint32, []byte) {
	offset := len(*buf)
	*buf = append(*buf, byte(len(key)>>8), byte(len(key)))
	*buf = append(*buf, key...)
	*buf = append(*buf, 0)
	*buf = append(*buf, make([]byte, indexEntrySize)...)
	return uint32(offset), []byte(key)
}
