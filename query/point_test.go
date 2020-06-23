package query_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/pkg/deep"
	"github.com/influxdata/influxdb/query"
)

func TestPoint_Clone_Float(t *testing.T) {
	p := &query.FloatPoint{
		Name:  "cpu",
		Tags:  ParseTags("host=server01"),
		Time:  5,
		Value: 2,
		Aux:   []interface{}{float64(45)},
	}
	c := p.Clone()
	if p == c {
		t.Errorf("clone has the same address as the original: %v == %v", p, c)
	}
	if !deep.Equal(p, c) {
		t.Errorf("mismatched point: %s", spew.Sdump(c))
	}
	if &p.Aux[0] == &c.Aux[0] {
		t.Errorf("aux values share the same address: %v == %v", p.Aux, c.Aux)
	} else if !deep.Equal(p.Aux, c.Aux) {
		t.Errorf("mismatched aux fields: %v != %v", p.Aux, c.Aux)
	}
}

func TestPoint_Clone_Integer(t *testing.T) {
	p := &query.IntegerPoint{
		Name:  "cpu",
		Tags:  ParseTags("host=server01"),
		Time:  5,
		Value: 2,
		Aux:   []interface{}{float64(45)},
	}
	c := p.Clone()
	if p == c {
		t.Errorf("clone has the same address as the original: %v == %v", p, c)
	}
	if !deep.Equal(p, c) {
		t.Errorf("mismatched point: %s", spew.Sdump(c))
	}
	if &p.Aux[0] == &c.Aux[0] {
		t.Errorf("aux values share the same address: %v == %v", p.Aux, c.Aux)
	} else if !deep.Equal(p.Aux, c.Aux) {
		t.Errorf("mismatched aux fields: %v != %v", p.Aux, c.Aux)
	}
}

func TestPoint_Clone_String(t *testing.T) {
	p := &query.StringPoint{
		Name:  "cpu",
		Tags:  ParseTags("host=server01"),
		Time:  5,
		Value: "clone",
		Aux:   []interface{}{float64(45)},
	}
	c := p.Clone()
	if p == c {
		t.Errorf("clone has the same address as the original: %v == %v", p, c)
	}
	if !deep.Equal(p, c) {
		t.Errorf("mismatched point: %s", spew.Sdump(c))
	}
	if &p.Aux[0] == &c.Aux[0] {
		t.Errorf("aux values share the same address: %v == %v", p.Aux, c.Aux)
	} else if !deep.Equal(p.Aux, c.Aux) {
		t.Errorf("mismatched aux fields: %v != %v", p.Aux, c.Aux)
	}
}

func TestPoint_Clone_Boolean(t *testing.T) {
	p := &query.BooleanPoint{
		Name:  "cpu",
		Tags:  ParseTags("host=server01"),
		Time:  5,
		Value: true,
		Aux:   []interface{}{float64(45)},
	}
	c := p.Clone()
	if p == c {
		t.Errorf("clone has the same address as the original: %v == %v", p, c)
	}
	if !deep.Equal(p, c) {
		t.Errorf("mismatched point: %s", spew.Sdump(c))
	}
	if &p.Aux[0] == &c.Aux[0] {
		t.Errorf("aux values share the same address: %v == %v", p.Aux, c.Aux)
	} else if !deep.Equal(p.Aux, c.Aux) {
		t.Errorf("mismatched aux fields: %v != %v", p.Aux, c.Aux)
	}
}

func TestPoint_Clone_Nil(t *testing.T) {
	var fp *query.FloatPoint
	if p := fp.Clone(); p != nil {
		t.Errorf("expected nil, got %v", p)
	}

	var ip *query.IntegerPoint
	if p := ip.Clone(); p != nil {
		t.Errorf("expected nil, got %v", p)
	}

	var sp *query.StringPoint
	if p := sp.Clone(); p != nil {
		t.Errorf("expected nil, got %v", p)
	}

	var bp *query.BooleanPoint
	if p := bp.Clone(); p != nil {
		t.Errorf("expected nil, got %v", p)
	}
}

// TestPoint_Fields ensures that no additional fields are added to the point structs.
// This struct is very sensitive and can effect performance unless handled carefully.
// To avoid the struct becoming a dumping ground for every function that needs to store
// miscellaneous information, this test is meant to ensure that new fields don't slip
// into the struct.
func TestPoint_Fields(t *testing.T) {
	allowedFields := map[string]bool{
		"Name":       true,
		"Tags":       true,
		"Time":       true,
		"Nil":        true,
		"Value":      true,
		"Aux":        true,
		"Aggregated": true,
	}

	for _, typ := range []reflect.Type{
		reflect.TypeOf(query.FloatPoint{}),
		reflect.TypeOf(query.IntegerPoint{}),
		reflect.TypeOf(query.StringPoint{}),
		reflect.TypeOf(query.BooleanPoint{}),
	} {
		f, ok := typ.FieldByNameFunc(func(name string) bool {
			return !allowedFields[name]
		})
		if ok {
			t.Errorf("found an unallowed field in %s: %s %s", typ, f.Name, f.Type)
		}
	}
}

// Ensure that tags can return a unique id.
func TestTags_ID(t *testing.T) {
	tags := query.NewTags(map[string]string{"foo": "bar", "baz": "bat"})
	if id := tags.ID(); id != "baz\x00foo\x00bat\x00bar" {
		t.Fatalf("unexpected id: %q", id)
	}
}

// Ensure that a subset can be created from a tag set.
func TestTags_Subset(t *testing.T) {
	tags := query.NewTags(map[string]string{"a": "0", "b": "1", "c": "2"})
	subset := tags.Subset([]string{"b", "c", "d"})
	if keys := subset.Keys(); !reflect.DeepEqual(keys, []string{"b", "c", "d"}) {
		t.Fatalf("unexpected keys: %+v", keys)
	} else if v := subset.Value("a"); v != "" {
		t.Fatalf("unexpected 'a' value: %s", v)
	} else if v := subset.Value("b"); v != "1" {
		t.Fatalf("unexpected 'b' value: %s", v)
	} else if v := subset.Value("c"); v != "2" {
		t.Fatalf("unexpected 'c' value: %s", v)
	} else if v := subset.Value("d"); v != "" {
		t.Fatalf("unexpected 'd' value: %s", v)
	}
}

// enumerates all combinations of the set, where
// len(set) ≤ #bits of current architecture.
func combinations(set []string) [][]string {
	length := uint(len(set))
	subsets := make([][]string, 0, (1<<length)-1)

	// Go through all possible combinations of objects
	// from 1 (only first object in subset) to 2^length (all objects in subset)
	for subsetBits := 1; subsetBits < (1 << length); subsetBits++ {
		var subset []string
		for object := uint(0); object < length; object++ {
			// checks if object is contained in subset
			// by checking if bit 'object' is set in subsetBits
			if (subsetBits>>object)&1 == 1 {
				// add object to subset
				subset = append(subset, set[object])
			}
		}
		// add subset to subsets
		subsets = append(subsets, subset)
	}
	return subsets
}

// Compares Subset and CompareKeys produce the same behavior
// under specific preconditions.
// This is important for use in iterators.
func TestTags_SubsetEqualsCompareKeys(t *testing.T) {
	nt := func(kv ...string) *query.Tags {
		if len(kv)%2 != 0 {
			panic("expect even number of kv")
		}

		m := make(map[string]string, len(kv)/2)
		for i := 0; i < len(kv); i += 2 {
			m[kv[i]] = kv[i+1]
		}
		r := query.NewTags(m)
		return &r
	}

	// allKeys contains all combinations of the ordered set
	allKeys := combinations([]string{"a", "b", "c", "d", "e"})

	t.Run("when tag keys and values are equal", func(t *testing.T) {
		a, b := nt("a", "0", "b", "1", "c", "2"), nt("a", "0", "b", "1", "c", "2")

		t.Run("compare a with b", func(t *testing.T) {
			for _, keys := range allKeys {
				got := a.CompareKeys(keys, b)
				l := a.Subset(keys)
				r := b.Subset(keys)

				var exp int
				if l.ID() == r.ID() {
					exp = 0
				} else if l.ID() < r.ID() {
					exp = -1
				} else {
					exp = 1
				}

				if !cmp.Equal(got, exp) {
					t.Errorf("CompareKeys: -got/+exp\n%s", cmp.Diff(got, exp))
				}
			}
		})

		t.Run("compare b with a", func(t *testing.T) {
			for _, keys := range allKeys {
				got := b.CompareKeys(keys, a)
				l := b.Subset(keys)
				r := a.Subset(keys)

				var exp int
				if l.ID() == r.ID() {
					exp = 0
				} else if l.ID() < r.ID() {
					exp = -1
				} else {
					exp = 1
				}

				if !cmp.Equal(got, exp) {
					t.Errorf("CompareKeys: -got/+exp\n%s", cmp.Diff(got, exp))
				}
			}
		})

	})

	t.Run("when tag keys are equal and values are different", func(t *testing.T) {
		a, b := nt("a", "0", "b", "0", "c", "1"), nt("a", "0", "b", "1", "c", "2")

		t.Run("compare a with b", func(t *testing.T) {
			for _, keys := range allKeys {
				got := a.CompareKeys(keys, b)
				l := a.Subset(keys)
				r := b.Subset(keys)

				var exp int
				if l.ID() == r.ID() {
					exp = 0
				} else if l.ID() < r.ID() {
					exp = -1
				} else {
					exp = 1
				}

				if !cmp.Equal(got, exp) {
					t.Errorf("CompareKeys: -got/+exp\n%s", cmp.Diff(got, exp))
				}
			}
		})

		t.Run("compare b with a", func(t *testing.T) {
			for _, keys := range allKeys {
				got := b.CompareKeys(keys, a)
				l := b.Subset(keys)
				r := a.Subset(keys)

				var exp int
				if l.ID() == r.ID() {
					exp = 0
				} else if l.ID() < r.ID() {
					exp = -1
				} else {
					exp = 1
				}

				if !cmp.Equal(got, exp) {
					t.Errorf("CompareKeys: -got/+exp\n%s", cmp.Diff(got, exp))
				}
			}
		})
	})

	t.Run("when tag keys are partially equal and values are equal", func(t *testing.T) {
		a, b := nt("a", "0", "b", "1", "d", "2"), nt("a", "0", "b", "1", "c", "2", "e", "3")

		t.Run("compare a with b", func(t *testing.T) {
			for _, keys := range allKeys {
				got := a.CompareKeys(keys, b)
				l := a.Subset(keys)
				r := b.Subset(keys)

				var exp int
				if l.ID() == r.ID() {
					exp = 0
				} else if l.ID() < r.ID() {
					exp = -1
				} else {
					exp = 1
				}

				if !cmp.Equal(got, exp) {
					t.Errorf("CompareKeys: -got/+exp\n%s", cmp.Diff(got, exp))
				}
			}
		})

		t.Run("compare b with a", func(t *testing.T) {
			for _, keys := range allKeys {
				got := b.CompareKeys(keys, a)
				l := b.Subset(keys)
				r := a.Subset(keys)

				var exp int
				if l.ID() == r.ID() {
					exp = 0
				} else if l.ID() < r.ID() {
					exp = -1
				} else {
					exp = 1
				}

				if !cmp.Equal(got, exp) {
					t.Errorf("CompareKeys: -got/+exp\n%s", cmp.Diff(got, exp))
				}
			}
		})
	})
}

// ParseTags returns an instance of Tags for a comma-delimited list of key/values.
func ParseTags(s string) query.Tags {
	m := make(map[string]string)
	for _, kv := range strings.Split(s, ",") {
		a := strings.Split(kv, "=")
		m[a[0]] = a[1]
	}
	return query.NewTags(m)
}
