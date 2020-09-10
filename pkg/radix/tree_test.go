package radix

import (
	"crypto/rand"
	"fmt"
	"reflect"
	"testing"
)

// generateUUID is used to generate a random UUID
func generateUUID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		panic(fmt.Errorf("failed to read random bytes: %v", err))
	}

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%12x",
		buf[0:4],
		buf[4:6],
		buf[6:8],
		buf[8:10],
		buf[10:16])
}

func TestRadix(t *testing.T) {
	var min, max string
	inp := make(map[string]int)
	for i := 0; i < 1000; i++ {
		gen := generateUUID()
		inp[gen] = i
		if gen < min || i == 0 {
			min = gen
		}
		if gen > max || i == 0 {
			max = gen
		}
	}

	r := NewFromMap(inp)
	if r.Len() != len(inp) {
		t.Fatalf("bad length: %v %v", r.Len(), len(inp))
	}

	// Check min and max
	outMin, _, _ := r.Minimum()
	if string(outMin) != min {
		t.Fatalf("bad minimum: %s %v", outMin, min)
	}
	outMax, _, _ := r.Maximum()
	if string(outMax) != max {
		t.Fatalf("bad maximum: %s %v", outMax, max)
	}

	for k, v := range inp {
		out, ok := r.Get([]byte(k))
		if !ok {
			t.Fatalf("missing key: %v", k)
		}
		if out != v {
			t.Fatalf("value mis-match: %v %v", out, v)
		}
	}

}

func TestDeletePrefix(t *testing.T) {
	type exp struct {
		inp        []string
		prefix     string
		out        []string
		numDeleted int
	}

	cases := []exp{
		{[]string{"", "A", "AB", "ABC", "R", "S"}, "A", []string{"", "R", "S"}, 3},
		{[]string{"", "A", "AB", "ABC", "R", "S"}, "ABC", []string{"", "A", "AB", "R", "S"}, 1},
		{[]string{"", "A", "AB", "ABC", "R", "S"}, "", []string{}, 6},
		{[]string{"", "A", "AB", "ABC", "R", "S"}, "S", []string{"", "A", "AB", "ABC", "R"}, 1},
		{[]string{"", "A", "AB", "ABC", "R", "S"}, "SS", []string{"", "A", "AB", "ABC", "R", "S"}, 0},
	}

	for _, test := range cases {
		r := New()
		for _, ss := range test.inp {
			r.Insert([]byte(ss), 1)
		}

		deleted := r.DeletePrefix([]byte(test.prefix))
		if deleted != test.numDeleted {
			t.Fatalf("Bad delete, expected %v to be deleted but got %v", test.numDeleted, deleted)
		}

		out := []string{}
		fn := func(s []byte, v int) bool {
			out = append(out, string(s))
			return false
		}
		recursiveWalk(r.root, fn)

		if !reflect.DeepEqual(out, test.out) {
			t.Fatalf("mis-match: %v %v", out, test.out)
		}
	}
}

func TestInsert_Duplicate(t *testing.T) {
	r := New()
	vv, ok := r.Insert([]byte("cpu"), 1)
	if vv != 1 {
		t.Fatalf("value mismatch: got %v, exp %v", vv, 1)
	}

	if !ok {
		t.Fatalf("value mismatch: got %v, exp %v", ok, true)
	}

	// Insert a dup with a different type should fail
	vv, ok = r.Insert([]byte("cpu"), 2)
	if vv != 1 {
		t.Fatalf("value mismatch: got %v, exp %v", vv, 1)
	}

	if ok {
		t.Fatalf("value mismatch: got %v, exp %v", ok, false)
	}
}

//
// benchmarks
//

func BenchmarkTree_Insert(b *testing.B) {
	t := New()

	keys := make([][]byte, 0, 10000)
	for i := 0; i < cap(keys); i++ {
		k := []byte(fmt.Sprintf("cpu,host=%d", i))
		if v, ok := t.Insert(k, 1); v != 1 || !ok {
			b.Fatalf("insert failed: %v != 1 || !%v", v, ok)
		}
		keys = append(keys, k)
	}

	b.SetBytes(int64(len(keys)))
	b.ReportAllocs()
	b.ResetTimer()

	for j := 0; j < b.N; j++ {
		for _, key := range keys {
			if v, ok := t.Insert(key, 1); v != 1 || ok {
				b.Fatalf("insert failed: %v != 1 || !%v", v, ok)
			}
		}
	}
}

func BenchmarkTree_InsertNew(b *testing.B) {
	keys := make([][]byte, 0, 10000)
	for i := 0; i < cap(keys); i++ {
		k := []byte(fmt.Sprintf("cpu,host=%d", i))
		keys = append(keys, k)
	}

	b.SetBytes(int64(len(keys)))
	b.ReportAllocs()
	b.ResetTimer()

	for j := 0; j < b.N; j++ {
		t := New()
		for _, key := range keys {
			t.Insert(key, 1)
		}
	}
}
