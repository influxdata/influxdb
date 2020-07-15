package tenant_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/tenant"
)

// type Bucket struct {
// 	ID                  ID            `json:"id,omitempty"`
// 	OrgID               ID            `json:"bucketID,omitempty"`
// 	Type                BucketType    `json:"type"`
// 	Name                string        `json:"name"`
// 	Description         string        `json:"description"`
// 	RetentionPolicyName string        `json:"rp,omitempty"` // This to support v1 sources
// 	RetentionPeriod     time.Duration `json:"retentionPeriod"`
// 	CRUDLog
// }

func TestBucket(t *testing.T) {
	simpleSetup := func(t *testing.T, store *tenant.Store, tx kv.Tx) {
		for i := 1; i <= 10; i++ {
			err := store.CreateBucket(context.Background(), tx, &influxdb.Bucket{
				ID:                  influxdb.ID(i),
				OrgID:               influxdb.ID(i%2 + 1),
				Name:                fmt.Sprintf("bucket%d", i),
				Description:         "words",
				RetentionPolicyName: "name",
				RetentionPeriod:     time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	st := []struct {
		name    string
		setup   func(*testing.T, *tenant.Store, kv.Tx)
		update  func(*testing.T, *tenant.Store, kv.Tx)
		results func(*testing.T, *tenant.Store, kv.Tx)
	}{
		{
			name:  "create",
			setup: simpleSetup,
			results: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
				buckets, err := store.ListBuckets(context.Background(), tx, tenant.BucketFilter{})
				if err != nil {
					t.Fatal(err)
				}

				if len(buckets) != 10 {
					t.Fatalf("expected 10 buckets got: %d", len(buckets))
				}

				expected := []*influxdb.Bucket{}
				for i := 1; i <= 10; i++ {
					expected = append(expected, &influxdb.Bucket{
						ID:                  influxdb.ID(i),
						OrgID:               influxdb.ID(i%2 + 1),
						Name:                fmt.Sprintf("bucket%d", i),
						Description:         "words",
						RetentionPolicyName: "name",
						RetentionPeriod:     time.Second,
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: buckets[i-1].CreatedAt,
							UpdatedAt: buckets[i-1].UpdatedAt,
						},
					})
				}
				if !cmp.Equal(buckets, expected) {
					t.Fatalf("expected identical buckets: \n%+v", cmp.Diff(buckets, expected))
				}
			},
		},
		{
			name:  "get",
			setup: simpleSetup,
			results: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
				bucket, err := store.GetBucket(context.Background(), tx, 5)
				if err != nil {
					t.Fatal(err)
				}

				expected := &influxdb.Bucket{
					ID:                  5,
					OrgID:               2,
					Name:                "bucket5",
					Description:         "words",
					RetentionPolicyName: "name",
					RetentionPeriod:     time.Second,
					CRUDLog: influxdb.CRUDLog{
						CreatedAt: bucket.CreatedAt,
						UpdatedAt: bucket.UpdatedAt,
					},
				}

				if !reflect.DeepEqual(bucket, expected) {
					t.Fatalf("expected identical bucket: \n%+v\n%+v", bucket, expected)
				}

				bucket, err = store.GetBucketByName(context.Background(), tx, influxdb.ID(2), "bucket5")
				if err != nil {
					t.Fatal(err)
				}

				if !reflect.DeepEqual(bucket, expected) {
					t.Fatalf("expected identical bucket: \n%+v\n%+v", bucket, expected)
				}

				if _, err := store.GetBucket(context.Background(), tx, 500); err != tenant.ErrBucketNotFound {
					t.Fatal("failed to get correct error when looking for invalid bucket by id")
				}

				if _, err := store.GetBucketByName(context.Background(), tx, 3, "notabucket"); err.Error() != tenant.ErrBucketNotFoundByName("notabucket").Error() {
					t.Fatal("failed to get correct error when looking for invalid bucket by name")
				}

			},
		},
		{
			name:  "list",
			setup: simpleSetup,
			results: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
				buckets, err := store.ListBuckets(context.Background(), tx, tenant.BucketFilter{})
				if err != nil {
					t.Fatal(err)
				}

				if len(buckets) != 10 {
					t.Fatalf("expected 10 buckets got: %d", len(buckets))
				}

				expected := []*influxdb.Bucket{}
				for i := 1; i <= 10; i++ {
					expected = append(expected, &influxdb.Bucket{
						ID:                  influxdb.ID(i),
						OrgID:               influxdb.ID(i%2 + 1),
						Name:                fmt.Sprintf("bucket%d", i),
						Description:         "words",
						RetentionPolicyName: "name",
						RetentionPeriod:     time.Second,
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: buckets[i-1].CreatedAt,
							UpdatedAt: buckets[i-1].UpdatedAt,
						},
					})
				}
				if !reflect.DeepEqual(buckets, expected) {
					t.Fatalf("expected identical buckets: \n%+v\n%+v", buckets, expected)
				}

				orgid := influxdb.ID(1)
				buckets, err = store.ListBuckets(context.Background(), tx, tenant.BucketFilter{OrganizationID: &orgid})
				if err != nil {
					t.Fatal(err)
				}

				if len(buckets) != 5 {
					t.Fatalf("expected 5 buckets got: %d", len(buckets))
				}

				orgExpected := []*influxdb.Bucket{
					expected[9], // id 10 => 000a which is alphabetically first
					expected[1],
					expected[3],
					expected[5],
					expected[7],
				}
				if !cmp.Equal(buckets, orgExpected) {
					t.Fatalf("expected identical buckets with limit: \n%+v", cmp.Diff(buckets, orgExpected))
				}

				buckets, err = store.ListBuckets(context.Background(), tx, tenant.BucketFilter{}, influxdb.FindOptions{Limit: 4})
				if err != nil {
					t.Fatal(err)
				}

				if len(buckets) != 4 {
					t.Fatalf("expected 4 buckets got: %d", len(buckets))
				}
				if !reflect.DeepEqual(buckets, expected[:4]) {
					t.Fatalf("expected identical buckets with limit: \n%+v\n%+v", buckets, expected[:4])
				}

				buckets, err = store.ListBuckets(context.Background(), tx, tenant.BucketFilter{}, influxdb.FindOptions{Offset: 3})
				if err != nil {
					t.Fatal(err)
				}

				if len(buckets) != 7 {
					t.Fatalf("expected 7 buckets got: %d", len(buckets))
				}
				if !reflect.DeepEqual(buckets, expected[3:]) {
					t.Fatalf("expected identical buckets with limit: \n%+v\n%+v", buckets, expected[3:])
				}
			},
		},
		{
			name:  "update",
			setup: simpleSetup,
			update: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
				bucket5 := "bucket5"
				_, err := store.UpdateBucket(context.Background(), tx, influxdb.ID(3), influxdb.BucketUpdate{Name: &bucket5})
				if err != tenant.ErrBucketNameNotUnique {
					t.Fatal("failed to error on duplicate bucketname")
				}

				bucket30 := "bucket30"
				_, err = store.UpdateBucket(context.Background(), tx, influxdb.ID(3), influxdb.BucketUpdate{Name: &bucket30})
				if err != nil {
					t.Fatal(err)
				}

				description := "notWords"
				_, err = store.UpdateBucket(context.Background(), tx, influxdb.ID(3), influxdb.BucketUpdate{Description: &description})
				if err != nil {
					t.Fatal(err)
				}
			},
			results: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
				buckets, err := store.ListBuckets(context.Background(), tx, tenant.BucketFilter{})
				if err != nil {
					t.Fatal(err)
				}

				if len(buckets) != 10 {
					t.Fatalf("expected 10 buckets got: %d", len(buckets))
				}

				expected := []*influxdb.Bucket{}
				for i := 1; i <= 10; i++ {
					expected = append(expected, &influxdb.Bucket{
						ID:                  influxdb.ID(i),
						OrgID:               influxdb.ID(i%2 + 1),
						Name:                fmt.Sprintf("bucket%d", i),
						Description:         "words",
						RetentionPolicyName: "name",
						RetentionPeriod:     time.Second,
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: buckets[i-1].CreatedAt,
							UpdatedAt: buckets[i-1].UpdatedAt,
						},
					})
				}
				expected[2].Name = "bucket30"
				expected[2].Description = "notWords"
				if !reflect.DeepEqual(buckets, expected) {
					t.Fatalf("expected identical buckets: \n%+v\n%+v", buckets, expected)
				}
			},
		},
		{
			name:  "delete",
			setup: simpleSetup,
			update: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
				err := store.DeleteBucket(context.Background(), tx, 1)
				if err != nil {
					t.Fatal(err)
				}

				err = store.DeleteBucket(context.Background(), tx, 1)
				if err != tenant.ErrBucketNotFound {
					t.Fatal("invalid error when deleting bucket that has already been deleted", err)
				}

				err = store.DeleteBucket(context.Background(), tx, 3)
				if err != nil {
					t.Fatal(err)
				}
			},
			results: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
				buckets, err := store.ListBuckets(context.Background(), tx, tenant.BucketFilter{})
				if err != nil {
					t.Fatal(err)
				}

				if len(buckets) != 8 {
					t.Fatalf("expected 10 buckets got: %d", len(buckets))
				}

				expected := []*influxdb.Bucket{}
				for i := 1; i <= 10; i++ {
					if i != 1 && i != 3 {
						expected = append(expected, &influxdb.Bucket{
							ID:                  influxdb.ID(i),
							OrgID:               influxdb.ID(i%2 + 1),
							Name:                fmt.Sprintf("bucket%d", i),
							Description:         "words",
							RetentionPolicyName: "name",
							RetentionPeriod:     time.Second,
						})
					}
				}
				for i, exp := range expected {
					exp.CRUDLog.CreatedAt = buckets[i].CreatedAt
					exp.CRUDLog.UpdatedAt = buckets[i].UpdatedAt
				}

				if !reflect.DeepEqual(buckets, expected) {
					t.Fatalf("expected identical buckets: \n%+v\n%+v", buckets, expected)
				}
			},
		},
	}
	for _, testScenario := range st {
		t.Run(testScenario.name, func(t *testing.T) {
			s, closeS, err := NewTestInmemStore(t)
			if err != nil {
				t.Fatal(err)
			}
			defer closeS()

			ts := tenant.NewStore(s)

			// setup
			if testScenario.setup != nil {
				err := ts.Update(context.Background(), func(tx kv.Tx) error {
					testScenario.setup(t, ts, tx)
					return nil
				})

				if err != nil {
					t.Fatal(err)
				}
			}

			// update
			if testScenario.update != nil {
				err := ts.Update(context.Background(), func(tx kv.Tx) error {
					testScenario.update(t, ts, tx)
					return nil
				})

				if err != nil {
					t.Fatal(err)
				}
			}

			// results
			if testScenario.results != nil {
				err := ts.View(context.Background(), func(tx kv.Tx) error {
					testScenario.results(t, ts, tx)
					return nil
				})

				if err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
