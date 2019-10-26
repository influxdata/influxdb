package pkger

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestService(t *testing.T) {
	t.Run("Apply", func(t *testing.T) {
		t.Run("buckets", func(t *testing.T) {
			t.Run("successfully creates pkg of buckets", func(t *testing.T) {
				testfileRunner(t, "testdata/bucket", func(t *testing.T, pkg *Pkg) {
					fakeBucketSVC := mock.NewBucketService()
					fakeBucketSVC.CreateBucketFn = func(_ context.Context, b *influxdb.Bucket) error {
						b.ID = influxdb.ID(b.RetentionPeriod)
						return nil
					}

					svc := NewService(zap.NewNop(), fakeBucketSVC, nil)

					orgID := influxdb.ID(9000)

					sum, err := svc.Apply(context.TODO(), orgID, pkg)
					require.NoError(t, err)

					require.Len(t, sum.Buckets, 1)
					buck1 := sum.Buckets[0]
					assert.Equal(t, influxdb.ID(time.Hour), buck1.ID)
					assert.Equal(t, orgID, buck1.OrgID)
					assert.Equal(t, "rucket_11", buck1.Name)
					assert.Equal(t, time.Hour, buck1.RetentionPeriod)
					assert.Equal(t, "bucket 1 description", buck1.Description)
				})
			})

			t.Run("rolls back all created buckets on an error", func(t *testing.T) {
				testfileRunner(t, "testdata/bucket", func(t *testing.T, pkg *Pkg) {
					fakeBucketSVC := mock.NewBucketService()
					var c int
					fakeBucketSVC.CreateBucketFn = func(_ context.Context, b *influxdb.Bucket) error {
						if c == 2 {
							return errors.New("blowed up ")
						}
						c++
						return nil
					}
					var count int
					fakeBucketSVC.DeleteBucketFn = func(_ context.Context, id influxdb.ID) error {
						count++
						return nil
					}

					pkg.mBuckets["copybuck1"] = pkg.mBuckets["rucket_11"]
					pkg.mBuckets["copybuck2"] = pkg.mBuckets["rucket_11"]

					svc := NewService(zap.NewNop(), fakeBucketSVC, nil)

					orgID := influxdb.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, pkg)
					require.Error(t, err)

					assert.Equal(t, 2, count)
				})
			})
		})

		t.Run("labels", func(t *testing.T) {
			t.Run("successfully creates pkg of labels", func(t *testing.T) {
				testfileRunner(t, "testdata/label", func(t *testing.T, pkg *Pkg) {
					fakeLabelSVC := mock.NewLabelService()
					id := 1
					fakeLabelSVC.CreateLabelFn = func(_ context.Context, l *influxdb.Label) error {
						l.ID = influxdb.ID(id)
						id++
						return nil
					}

					svc := NewService(zap.NewNop(), nil, fakeLabelSVC)

					orgID := influxdb.ID(9000)

					sum, err := svc.Apply(context.TODO(), orgID, pkg)
					require.NoError(t, err)

					require.Len(t, sum.Labels, 2)
					label1 := sum.Labels[0]
					assert.Equal(t, influxdb.ID(1), label1.ID)
					assert.Equal(t, orgID, label1.OrgID)
					assert.Equal(t, "label_1", label1.Name)
					assert.Equal(t, "#FFFFFF", label1.Properties["color"])
					assert.Equal(t, "label 1 description", label1.Properties["description"])

					label2 := sum.Labels[1]
					assert.Equal(t, influxdb.ID(2), label2.ID)
					assert.Equal(t, orgID, label2.OrgID)
					assert.Equal(t, "label_2", label2.Name)
					assert.Equal(t, "#000000", label2.Properties["color"])
					assert.Equal(t, "label 2 description", label2.Properties["description"])
				})
			})

			t.Run("rolls back all created labels on an error", func(t *testing.T) {
				testfileRunner(t, "testdata/label", func(t *testing.T, pkg *Pkg) {
					fakeLabelSVC := mock.NewLabelService()
					var c int
					fakeLabelSVC.CreateLabelFn = func(_ context.Context, l *influxdb.Label) error {
						// 4th label will return the error here, and 3 before should be rolled back
						if c == 3 {
							return errors.New("blowed up ")
						}
						c++
						return nil
					}
					var count int
					fakeLabelSVC.DeleteLabelFn = func(_ context.Context, id influxdb.ID) error {
						count++
						return nil
					}

					pkg.mLabels["copy1"] = pkg.mLabels["label_1"]
					pkg.mLabels["copy2"] = pkg.mLabels["label_2"]

					svc := NewService(zap.NewNop(), nil, fakeLabelSVC)

					orgID := influxdb.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, pkg)
					require.Error(t, err)

					assert.Equal(t, 3, count)
				})
			})
		})

		t.Run("label mapping", func(t *testing.T) {
			t.Run("successfully creates pkg of labels", func(t *testing.T) {
				testfileRunner(t, "testdata/bucket_associates_label", func(t *testing.T, pkg *Pkg) {
					fakeBktSVC := mock.NewBucketService()
					id := 1
					fakeBktSVC.CreateBucketFn = func(_ context.Context, b *influxdb.Bucket) error {
						b.ID = influxdb.ID(id)
						id++
						return nil
					}

					fakeLabelSVC := mock.NewLabelService()
					id = 1
					fakeLabelSVC.CreateLabelFn = func(_ context.Context, l *influxdb.Label) error {
						l.ID = influxdb.ID(id)
						id++
						return nil
					}
					numLabelMappings := 0
					fakeLabelSVC.CreateLabelMappingFn = func(_ context.Context, mapping *influxdb.LabelMapping) error {
						numLabelMappings++
						return nil
					}

					svc := NewService(zap.NewNop(), fakeBktSVC, fakeLabelSVC)

					orgID := influxdb.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, pkg)
					require.NoError(t, err)

					assert.Equal(t, 4, numLabelMappings)
				})
			})

			t.Run("rolls back all created resources on an error", func(t *testing.T) {
				testfileRunner(t, "testdata/bucket_associates_label", func(t *testing.T, pkg *Pkg) {
					var deleteCount struct {
						bkts, labels, mappings int
					}
					fakeBktSVC := mock.NewBucketService()
					fakeBktSVC.DeleteBucketFn = func(_ context.Context, _ influxdb.ID) error {
						deleteCount.bkts++
						return nil
					}
					fakeLabelSVC := mock.NewLabelService()
					fakeLabelSVC.DeleteLabelFn = func(_ context.Context, id influxdb.ID) error {
						deleteCount.labels++
						return nil
					}
					var createdLabelMappings int
					fakeLabelSVC.CreateLabelMappingFn = func(_ context.Context, _ *influxdb.LabelMapping) error {
						if createdLabelMappings == 3 {
							return errors.New("error")
						}
						createdLabelMappings++
						return nil
					}
					fakeLabelSVC.DeleteLabelMappingFn = func(_ context.Context, _ *influxdb.LabelMapping) error {
						deleteCount.mappings++
						return nil
					}

					svc := NewService(zap.NewNop(), fakeBktSVC, fakeLabelSVC)

					orgID := influxdb.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, pkg)
					require.Error(t, err)

					assert.Equal(t, 3, deleteCount.bkts)
					assert.Equal(t, 2, deleteCount.labels)
					assert.Equal(t, 3, deleteCount.mappings)
				})
			})
		})
	})
}

func TestPkg(t *testing.T) {
	t.Run("Summary", func(t *testing.T) {
		t.Run("buckets returned in asc order by name", func(t *testing.T) {
			pkg := Pkg{
				mBuckets: map[string]*bucket{
					"buck_2": {
						ID:              influxdb.ID(2),
						OrgID:           influxdb.ID(100),
						Description:     "desc2",
						Name:            "name2",
						RetentionPeriod: 2 * time.Hour,
					},
					"buck_1": {
						ID:              influxdb.ID(1),
						OrgID:           influxdb.ID(100),
						Name:            "name1",
						Description:     "desc1",
						RetentionPeriod: time.Hour,
					},
				},
			}

			summary := pkg.Summary()

			require.Len(t, summary.Buckets, len(pkg.mBuckets))
			for i := 1; i <= len(summary.Buckets); i++ {
				buck := summary.Buckets[i-1]
				assert.Equal(t, influxdb.ID(i), buck.ID)
				assert.Equal(t, influxdb.ID(100), buck.OrgID)
				assert.Equal(t, "desc"+strconv.Itoa(i), buck.Description)
				assert.Equal(t, "name"+strconv.Itoa(i), buck.Name)
				assert.Equal(t, time.Duration(i)*time.Hour, buck.RetentionPeriod)
			}
		})

		t.Run("labels returned in asc order by name", func(t *testing.T) {
			pkg := Pkg{
				mLabels: map[string]*label{
					"2": {
						ID:          influxdb.ID(2),
						OrgID:       influxdb.ID(100),
						Name:        "name2",
						Description: "desc2",
						Color:       "blurple",
					},
					"1": {
						ID:          influxdb.ID(1),
						OrgID:       influxdb.ID(100),
						Name:        "name1",
						Description: "desc1",
						Color:       "peru",
					},
				},
			}

			summary := pkg.Summary()

			require.Len(t, summary.Labels, len(pkg.mLabels))
			label1 := summary.Labels[0]
			assert.Equal(t, influxdb.ID(1), label1.ID)
			assert.Equal(t, influxdb.ID(100), label1.OrgID)
			assert.Equal(t, "desc1", label1.Properties["description"])
			assert.Equal(t, "name1", label1.Name)
			assert.Equal(t, "peru", label1.Properties["color"])

			label2 := summary.Labels[1]
			assert.Equal(t, influxdb.ID(2), label2.ID)
			assert.Equal(t, influxdb.ID(100), label2.OrgID)
			assert.Equal(t, "desc2", label2.Properties["description"])
			assert.Equal(t, "name2", label2.Name)
			assert.Equal(t, "blurple", label2.Properties["color"])
		})
	})
}
