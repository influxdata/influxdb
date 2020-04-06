package kv

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
)

func Test_userResourceMappingPredicate(t *testing.T) {
	mk := func(rid, uid influxdb.ID) (urm *influxdb.UserResourceMapping, key []byte) {
		t.Helper()
		urm = &influxdb.UserResourceMapping{UserID: rid, ResourceID: uid}
		key, err := userResourceKey(urm)
		if err != nil {
			t.Fatal(err)
		}
		return urm, key
	}

	t.Run("match only ResourceID", func(t *testing.T) {
		u, k := mk(10, 20)
		f := influxdb.UserResourceMappingFilter{ResourceID: u.ResourceID}
		fn := userResourceMappingPredicate(f)
		if got, exp := fn(k, nil), true; got != exp {
			t.Errorf("unexpected result -got/+exp\n%s", cmp.Diff(got, exp))
		}

		_, k = mk(10, 21)
		if got, exp := fn(k, nil), false; got != exp {
			t.Errorf("unexpected result -got/+exp\n%s", cmp.Diff(got, exp))
		}
	})

	t.Run("match only UserID", func(t *testing.T) {
		u, k := mk(10, 20)
		f := influxdb.UserResourceMappingFilter{UserID: u.UserID}
		fn := userResourceMappingPredicate(f)
		if got, exp := fn(k, nil), true; got != exp {
			t.Errorf("unexpected result -got/+exp\n%s", cmp.Diff(got, exp))
		}

		_, k = mk(11, 20)
		if got, exp := fn(k, nil), false; got != exp {
			t.Errorf("unexpected result -got/+exp\n%s", cmp.Diff(got, exp))
		}
	})

	t.Run("match ResourceID and UserID", func(t *testing.T) {
		u, k := mk(10, 20)
		f := influxdb.UserResourceMappingFilter{ResourceID: u.ResourceID, UserID: u.UserID}
		fn := userResourceMappingPredicate(f)
		if got, exp := fn(k, nil), true; got != exp {
			t.Errorf("unexpected result -got/+exp\n%s", cmp.Diff(got, exp))
		}

		_, k = mk(11, 20)
		if got, exp := fn(k, nil), false; got != exp {
			t.Errorf("unexpected result -got/+exp\n%s", cmp.Diff(got, exp))
		}
	})

	t.Run("no match function", func(t *testing.T) {
		f := influxdb.UserResourceMappingFilter{}
		fn := userResourceMappingPredicate(f)
		if fn != nil {
			t.Errorf("expected nil")
		}
	})
}
