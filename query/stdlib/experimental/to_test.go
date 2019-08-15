package experimental_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/querytest"
	"github.com/influxdata/flux/stdlib/universe"
	platform "github.com/influxdata/influxdb"
	_ "github.com/influxdata/influxdb/query/builtin"
	pquerytest "github.com/influxdata/influxdb/query/querytest"
	"github.com/influxdata/influxdb/query/stdlib/experimental"
	"github.com/influxdata/influxdb/query/stdlib/influxdata/influxdb"
)

func TestTo_Query(t *testing.T) {
	tests := []querytest.NewQueryTestCase{
		{
			Name: "from range pivot experimental to",
			Raw: `import "experimental"
import "influxdata/influxdb/v1"
from(bucket:"mydb")
  |> range(start: -1h)
  |> v1.fieldsAsCols()
  |> experimental.to(bucket:"series1", org:"fred", host:"localhost", token:"auth-token")`,
			Want: &flux.Spec{
				Operations: []*flux.Operation{
					{
						ID: "influxDBFrom0",
						Spec: &influxdb.FromOpSpec{
							Bucket: "mydb",
						},
					},
					{
						ID: "range1",
						Spec: &universe.RangeOpSpec{
							Start:       flux.Time{IsRelative: true, Relative: -time.Hour},
							Stop:        flux.Time{IsRelative: true},
							TimeColumn:  "_time",
							StartColumn: "_start",
							StopColumn:  "_stop",
						},
					},
					{
						ID: "pivot2",
						Spec: &universe.PivotOpSpec{
							RowKey:      []string{"_time"},
							ColumnKey:   []string{"_field"},
							ValueColumn: "_value"},
					},
					{
						ID: "experimental-to3",
						Spec: &experimental.ToOpSpec{
							Bucket: "series1",
							Org:    "fred",
							Host:   "localhost",
							Token:  "auth-token",
						},
					},
				},
				Edges: []flux.Edge{
					{Parent: "influxDBFrom0", Child: "range1"},
					{Parent: "range1", Child: "pivot2"},
					{Parent: "pivot2", Child: "experimental-to3"},
				},
			},
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			querytest.NewQueryTestHelper(t, tc)
		})
	}
}

func TestToOpSpec_BucketsAccessed(t *testing.T) {
	bucketName := "my_bucket"
	bucketIDString := "ddddccccbbbbaaaa"
	bucketID, err := platform.IDFromString(bucketIDString)
	if err != nil {
		t.Fatal(err)
	}
	orgName := "my_org"
	orgIDString := "aaaabbbbccccdddd"
	orgID, err := platform.IDFromString(orgIDString)
	if err != nil {
		t.Fatal(err)
	}
	tests := []pquerytest.BucketsAccessedTestCase{
		{
			Name: "from() with bucket and to with org and bucket",
			Raw: fmt.Sprintf(`import "experimental"
from(bucket:"%s")
  |> experimental.to(bucket:"%s", org:"%s")`, bucketName, bucketName, orgName),
			WantReadBuckets:  &[]platform.BucketFilter{{Name: &bucketName}},
			WantWriteBuckets: &[]platform.BucketFilter{{Name: &bucketName, Org: &orgName}},
		},
		{
			Name: "from() with bucket and to with orgID and bucket",
			Raw: fmt.Sprintf(`import "experimental"
from(bucket:"%s") |> experimental.to(bucket:"%s", orgID:"%s")`, bucketName, bucketName, orgIDString),
			WantReadBuckets:  &[]platform.BucketFilter{{Name: &bucketName}},
			WantWriteBuckets: &[]platform.BucketFilter{{Name: &bucketName, OrganizationID: orgID}},
		},
		{
			Name: "from() with bucket and to with orgID and bucketID",
			Raw: fmt.Sprintf(`import "experimental"
from(bucket:"%s") |> experimental.to(bucketID:"%s", orgID:"%s")`, bucketName, bucketIDString, orgIDString),
			WantReadBuckets:  &[]platform.BucketFilter{{Name: &bucketName}},
			WantWriteBuckets: &[]platform.BucketFilter{{ID: bucketID, OrganizationID: orgID}},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			pquerytest.BucketsAccessedTestHelper(t, tc)
		})
	}
}
