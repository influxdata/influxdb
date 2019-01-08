package rhh_test

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/influxdata/influxdb/pkg/rhh"
)

// Ensure hash map can perform basic get/put operations.
func TestHashMap(t *testing.T) {
	m := rhh.NewHashMap(rhh.DefaultOptions)
	m.Put([]byte("foo"), []byte("bar"))
	m.Put([]byte("baz"), []byte("bat"))

	// Verify values can be retrieved.
	if v := m.Get([]byte("foo")); !bytes.Equal(v.([]byte), []byte("bar")) {
		t.Fatalf("unexpected value: %s", v)
	}
	if v := m.Get([]byte("baz")); !bytes.Equal(v.([]byte), []byte("bat")) {
		t.Fatalf("unexpected value: %s", v)
	}

	// Overwrite field & verify.
	m.Put([]byte("foo"), []byte("XXX"))
	if v := m.Get([]byte("foo")); !bytes.Equal(v.([]byte), []byte("XXX")) {
		t.Fatalf("unexpected value: %s", v)
	}
}

// Ensure hash map can insert random data.
func TestHashMap_Quick(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode, skipping")
	}

	if err := quick.Check(func(keys, values [][]byte) bool {
		m := rhh.NewHashMap(rhh.Options{Capacity: 1000, LoadFactor: 90})
		h := make(map[string][]byte)

		// Insert all key/values into both maps.
		for i := range keys {
			key, value := keys[i], values[i]
			h[string(key)] = value
			m.Put(key, value)
		}

		// Verify the maps are equal.
		for k, v := range h {
			if mv := m.Get([]byte(k)); !bytes.Equal(mv.([]byte), v) {
				t.Fatalf("value mismatch:\nkey=%x\ngot=%x\nexp=%x\n\n", []byte(k), mv, v)
			}
		}

		return true
	}, &quick.Config{
		Values: func(values []reflect.Value, rand *rand.Rand) {
			n := rand.Intn(10000)
			values[0] = GenerateByteSlices(rand, n)
			values[1] = GenerateByteSlices(rand, n)
		},
	}); err != nil {
		t.Fatal(err)
	}
}

// GenerateByteSlices returns a random list of byte slices.
func GenerateByteSlices(rand *rand.Rand, n int) reflect.Value {
	var a [][]byte
	for i := 0; i < n; i++ {
		v, _ := quick.Value(reflect.TypeOf(([]byte)(nil)), rand)
		a = append(a, v.Interface().([]byte))
	}
	return reflect.ValueOf(a)
}
