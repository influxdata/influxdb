package tsm1

import (
	"fmt"
	"testing"
	"testing/quick"
)

// TestCacheStoreProperty_IdenticalToUnsharded verifies that the sharded
// CacheStore type behaves identically to the simpler, unsharded
// map[string]*entry type.
func TestCacheStoreProperty_IdenticalToUnsharded(t *testing.T) {
	f := func(orig map[CompositeKey][]*EmptyValue) bool {
		// build the unsharded version to compare against:
		gold := map[string]*entry{}
		for ck, vv := range orig {
			key := ck.StringKey()

			for _, v := range vv {
				existing, ok := gold[key]
				if !ok {
					existing = newEntry()
					gold[key] = existing
				}
				existing.add([]Value{v})
			}
		}

		// build the sharded version:
		x := NewCacheStore()
		for ck, vv := range orig {
			for _, v := range vv {
				existing := x.Get(ck)
				if existing == nil {
					existing = newEntry()
					x.Put(ck, existing)
				}
				existing.add([]Value{v})
			}
		}

		// check that the lengths of the sharded and unsharded map
		// are the same:
		l := 0
		f := func(_ CompositeKey, _ *entry) error {
			l++
			return nil
		}
		x.Iter(f)
		if l != len(gold) {
			return false
		}

		// check the slice of Value objects associated with each
		// CompositeKey by verifying pointer equality with the objects
		// in the gold data:
		g := func(ck CompositeKey, e *entry) error {
			e2 := gold[ck.StringKey()]

			// check that we didn't make a mistake when writing
			// this test:
			if e == e2 {
				return fmt.Errorf("logic error: test is cheating by duplicating a []Value object")
			}

			// check the []Value slice has equal length:
			if e.count() != e2.count() {
				return fmt.Errorf("bad CacheStore storage len")
			}

			// check the pointer equality of each pair of Value
			// objects:
			for i := 0; i < e.count(); i++ {
				if e.values[i] != e2.values[i] {
					return fmt.Errorf("bad CacheStore pointer equality")
				}
			}

			return nil
		}
		err := x.Iter(g)
		if err != nil {
			fmt.Println(err.Error())
			return false
		}

		return true
	}
	cfg := &quick.Config{MaxCount: 1000}
	if err := quick.Check(f, cfg); err != nil {
		t.Error(err)
	}
}
