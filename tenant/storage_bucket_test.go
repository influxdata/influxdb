package tenant_test

import (
	"context"
	"fmt"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

const (
	firstBucketID platform.ID = (iota + 1)
	secondBucketID
	thirdBucketID
	fourthBucketID
	fifthBucketID
)

var orgIDs = []platform.ID{firstOrgID, secondOrgID}

func TestBucket(t *testing.T) {
	var (
		aTime = time.Date(2020, 7, 23, 10, 0, 0, 0, time.UTC)
		// generate 10 buckets to test with
		// optionally provide a visit function to manipulate
		// the generated slice (for convenience)
		testBuckets = func(count int, visit ...func(*influxdb.Bucket)) (buckets []*influxdb.Bucket) {
			buckets = make([]*influxdb.Bucket, count)
			for i := range buckets {
				id := firstBucketID + platform.ID(i)
				// flip-flop between (reserved_id + reserved_id+1)
				orgID := orgIDs[i%2]
				buckets[i] = &influxdb.Bucket{
					ID:                  id,
					OrgID:               orgID,
					Name:                fmt.Sprintf("bucket%d", int(id)),
					Description:         "words",
					RetentionPolicyName: "name",
					RetentionPeriod:     time.Second,
				}

				for _, fn := range visit {
					fn(buckets[i])
				}
			}
			return
		}
		withCrudLog = func(bkt *influxdb.Bucket) {
			bkt.CRUDLog = influxdb.CRUDLog{
				CreatedAt: aTime,
				UpdatedAt: aTime,
			}
		}
	)

	simpleSetup := func(t *testing.T, store *tenant.Store, tx kv.Tx) {
		store.BucketIDGen = mock.NewIncrementingIDGenerator(1)
		for _, bucket := range testBuckets(10) {
			err := store.CreateBucket(context.Background(), tx, bucket)
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

				expected := testBuckets(10, withCrudLog)
				assert.Equal(t, expected, buckets)
			},
		},
		{
			name:  "get",
			setup: simpleSetup,
			results: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
				bucket, err := store.GetBucket(context.Background(), tx, fifthBucketID)
				assert.NoError(t, err)

				expected := &influxdb.Bucket{
					ID:                  fifthBucketID,
					OrgID:               firstOrgID,
					Name:                "bucket5",
					Description:         "words",
					RetentionPolicyName: "name",
					RetentionPeriod:     time.Second,
					CRUDLog: influxdb.CRUDLog{
						CreatedAt: aTime,
						UpdatedAt: aTime,
					},
				}

				assert.Equal(t, expected, bucket)

				bucket, err = store.GetBucketByName(context.Background(), tx, firstOrgID, "bucket5")
				require.NoError(t, err)
				assert.Equal(t, expected, bucket)

				if _, err := store.GetBucket(context.Background(), tx, 11); err != tenant.ErrBucketNotFound {
					t.Fatal("failed to get correct error when looking for non present bucket by id")
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
				expected := testBuckets(10, withCrudLog)
				orgID := firstOrgID
				buckets, err := store.ListBuckets(context.Background(), tx, tenant.BucketFilter{OrganizationID: &orgID})
				require.NoError(t, err)
				assert.Len(t, buckets, 5)

				orgExpected := []*influxdb.Bucket{
					expected[0], // id 10 => 000a which is alphabetically first
					expected[2],
					expected[4],
					expected[6],
					expected[8],
				}
				assert.Equal(t, orgExpected, buckets)

				buckets, err = store.ListBuckets(context.Background(), tx, tenant.BucketFilter{}, influxdb.FindOptions{Limit: 4})
				require.NoError(t, err)

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
			name:  "list all with limit 3 using after to paginate",
			setup: simpleSetup,
			results: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
				var (
					expected  = testBuckets(10, withCrudLog)
					found     []*influxdb.Bucket
					lastID    *platform.ID
					limit     = 3
					listAfter = func(after *platform.ID) ([]*influxdb.Bucket, error) {
						return store.ListBuckets(context.Background(), tx, tenant.BucketFilter{}, influxdb.FindOptions{
							After: after,
							Limit: limit,
						})
					}
				)

				var (
					b   []*influxdb.Bucket
					err error
				)

				for b, err = listAfter(lastID); err == nil; b, err = listAfter(lastID) {
					lastID = &b[len(b)-1].ID
					found = append(found, b...)

					// given we've seen the last page
					if len(b) < limit {
						break
					}
				}

				require.NoError(t, err)

				assert.Equal(t, expected, found)
			},
		},
		{
			name:  "update",
			setup: simpleSetup,
			update: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
				bucket5 := "bucket5"
				_, err := store.UpdateBucket(context.Background(), tx, thirdBucketID, influxdb.BucketUpdate{Name: &bucket5})
				if err != tenant.ErrBucketNameNotUnique {
					t.Fatal("failed to error on duplicate bucketname")
				}

				bucket30 := "bucket30"
				_, err = store.UpdateBucket(context.Background(), tx, thirdBucketID, influxdb.BucketUpdate{Name: &bucket30})
				require.NoError(t, err)

				description := "notWords"
				_, err = store.UpdateBucket(context.Background(), tx, thirdBucketID, influxdb.BucketUpdate{Description: &description})
				require.NoError(t, err)
			},
			results: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
				buckets, err := store.ListBuckets(context.Background(), tx, tenant.BucketFilter{})
				if err != nil {
					t.Fatal(err)
				}

				expected := testBuckets(10, withCrudLog)
				expected[2].Name = "bucket30"
				expected[2].Description = "notWords"
				assert.Equal(t, expected, buckets)
			},
		},
		{
			name:  "delete",
			setup: simpleSetup,
			update: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
				err := store.DeleteBucket(context.Background(), tx, firstBucketID)
				require.NoError(t, err)

				err = store.DeleteBucket(context.Background(), tx, firstBucketID)
				if err != tenant.ErrBucketNotFound {
					t.Fatal("invalid error when deleting bucket that has already been deleted", err)
				}

				err = store.DeleteBucket(context.Background(), tx, secondBucketID)
				require.NoError(t, err)
			},
			results: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
				buckets, err := store.ListBuckets(context.Background(), tx, tenant.BucketFilter{})
				if err != nil {
					t.Fatal(err)
				}

				expected := testBuckets(10, withCrudLog)[2:]
				assert.Equal(t, expected, buckets)
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

			ts := tenant.NewStore(s, tenant.WithNow(func() time.Time {
				return aTime
			}))

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
