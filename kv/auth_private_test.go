package kv

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
)

func mustMarshal(t testing.TB, v interface{}) []byte {
	t.Helper()
	d, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func Test_authorizationsPredicateFn(t *testing.T) {
	t.Run("ID", func(t *testing.T) {
		val := influxdb.ID(1)
		f := influxdb.AuthorizationFilter{ID: &val}
		fn := authorizationsPredicateFn(f)

		t.Run("does match", func(t *testing.T) {
			a := &influxdb.Authorization{ID: val, OrgID: 2}
			if got, exp := fn(nil, mustMarshal(t, a)), true; got != exp {
				t.Errorf("unexpected result -got/+exp\n%s", cmp.Diff(got, exp))
			}
		})

		t.Run("does not match", func(t *testing.T) {
			a := &influxdb.Authorization{ID: 10, OrgID: 2}
			if got, exp := fn(nil, mustMarshal(t, a)), false; got != exp {
				t.Errorf("unexpected result -got/+exp\n%s", cmp.Diff(got, exp))
			}
		})
	})

	t.Run("token", func(t *testing.T) {
		val := "token_token"
		f := influxdb.AuthorizationFilter{Token: &val}
		fn := authorizationsPredicateFn(f)

		t.Run("does match", func(t *testing.T) {
			a := &influxdb.Authorization{ID: 10, OrgID: 2, Token: val}
			if got, exp := fn(nil, mustMarshal(t, a)), true; got != exp {
				t.Errorf("unexpected result -got/+exp\n%s", cmp.Diff(got, exp))
			}
		})

		t.Run("does not match", func(t *testing.T) {
			a := &influxdb.Authorization{ID: 10, OrgID: 2, Token: "no_no_no"}
			if got, exp := fn(nil, mustMarshal(t, a)), false; got != exp {
				t.Errorf("unexpected result -got/+exp\n%s", cmp.Diff(got, exp))
			}
		})
	})

	t.Run("orgID", func(t *testing.T) {
		val := influxdb.ID(1)
		f := influxdb.AuthorizationFilter{OrgID: &val}
		fn := authorizationsPredicateFn(f)

		t.Run("does match", func(t *testing.T) {
			a := &influxdb.Authorization{ID: 10, OrgID: val}
			if got, exp := fn(nil, mustMarshal(t, a)), true; got != exp {
				t.Errorf("unexpected result -got/+exp\n%s", cmp.Diff(got, exp))
			}
		})

		t.Run("does not match", func(t *testing.T) {
			a := &influxdb.Authorization{ID: 10, OrgID: 2}
			if got, exp := fn(nil, mustMarshal(t, a)), false; got != exp {
				t.Errorf("unexpected result -got/+exp\n%s", cmp.Diff(got, exp))
			}
		})
	})

	t.Run("userID", func(t *testing.T) {
		val := influxdb.ID(1)
		f := influxdb.AuthorizationFilter{UserID: &val}
		fn := authorizationsPredicateFn(f)

		t.Run("does match", func(t *testing.T) {
			a := &influxdb.Authorization{ID: 10, OrgID: 5, UserID: val}
			if got, exp := fn(nil, mustMarshal(t, a)), true; got != exp {
				t.Errorf("unexpected result -got/+exp\n%s", cmp.Diff(got, exp))
			}
		})

		t.Run("does not match", func(t *testing.T) {
			a := &influxdb.Authorization{ID: 10, OrgID: 5, UserID: 2}
			if got, exp := fn(nil, mustMarshal(t, a)), false; got != exp {
				t.Errorf("unexpected result -got/+exp\n%s", cmp.Diff(got, exp))
			}
		})

		t.Run("missing userID", func(t *testing.T) {
			a := &influxdb.Authorization{ID: 10, OrgID: 5}
			if got, exp := fn(nil, mustMarshal(t, a)), false; got != exp {
				t.Errorf("unexpected result -got/+exp\n%s", cmp.Diff(got, exp))
			}
		})
	})

	t.Run("orgID and userID", func(t *testing.T) {
		orgID := influxdb.ID(1)
		userID := influxdb.ID(10)
		f := influxdb.AuthorizationFilter{OrgID: &orgID, UserID: &userID}
		fn := authorizationsPredicateFn(f)

		t.Run("does match", func(t *testing.T) {
			a := &influxdb.Authorization{ID: 10, OrgID: orgID, UserID: userID}
			if got, exp := fn(nil, mustMarshal(t, a)), true; got != exp {
				t.Errorf("unexpected result -got/+exp\n%s", cmp.Diff(got, exp))
			}
		})

		t.Run("org match user not match", func(t *testing.T) {
			a := &influxdb.Authorization{ID: 10, OrgID: orgID, UserID: 11}
			if got, exp := fn(nil, mustMarshal(t, a)), false; got != exp {
				t.Errorf("unexpected result -got/+exp\n%s", cmp.Diff(got, exp))
			}
		})

		t.Run("org not match user match", func(t *testing.T) {
			a := &influxdb.Authorization{ID: 10, OrgID: 2, UserID: userID}
			if got, exp := fn(nil, mustMarshal(t, a)), false; got != exp {
				t.Errorf("unexpected result -got/+exp\n%s", cmp.Diff(got, exp))
			}
		})

		t.Run("org and user not match", func(t *testing.T) {
			a := &influxdb.Authorization{ID: 10, OrgID: 2, UserID: 11}
			if got, exp := fn(nil, mustMarshal(t, a)), false; got != exp {
				t.Errorf("unexpected result -got/+exp\n%s", cmp.Diff(got, exp))
			}
		})

		t.Run("org match user missing", func(t *testing.T) {
			a := &influxdb.Authorization{ID: 10, OrgID: orgID}
			if got, exp := fn(nil, mustMarshal(t, a)), false; got != exp {
				t.Errorf("unexpected result -got/+exp\n%s", cmp.Diff(got, exp))
			}
		})
	})
}
