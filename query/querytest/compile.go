package querytest

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/semantic/semantictest"
)

type NewQueryTestCase struct {
	Name    string
	Raw     string
	Want    *query.Spec
	WantErr bool
}

var opts = append(
	semantictest.CmpOptions,
	cmp.AllowUnexported(query.Spec{}),
	cmp.AllowUnexported(functions.JoinOpSpec{}),
	cmpopts.IgnoreUnexported(query.Spec{}),
	cmpopts.IgnoreUnexported(functions.JoinOpSpec{}),
)

func NewQueryTestHelper(t *testing.T, tc NewQueryTestCase) {
	t.Helper()

	now := time.Now().UTC()
	got, err := query.Compile(context.Background(), tc.Raw, now)
	if (err != nil) != tc.WantErr {
		t.Errorf("query.NewQuery() error = %v, wantErr %v", err, tc.WantErr)
		return
	}
	if tc.WantErr {
		return
	}
	if tc.Want != nil {
		tc.Want.Now = now
	}
	if !cmp.Equal(tc.Want, got, opts...) {
		t.Errorf("query.NewQuery() = -want/+got %s", cmp.Diff(tc.Want, got, opts...))
	}
}
