package pkger

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestService(t *testing.T) {
	t.Run("DryRun", func(t *testing.T) {
		t.Run("buckets", func(t *testing.T) {
			t.Run("single bucket updated", func(t *testing.T) {
				testfileRunner(t, "testdata/bucket", func(t *testing.T, pkg *Pkg) {
					fakeBktSVC := mock.NewBucketService()
					fakeBktSVC.FindBucketByNameFn = func(_ context.Context, orgID influxdb.ID, name string) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{
							ID:              influxdb.ID(1),
							OrgID:           orgID,
							Name:            name,
							Description:     "old desc",
							RetentionPeriod: 30 * time.Hour,
						}, nil
					}
					fakeLabelSVC := mock.NewLabelService()
					fakeDashSVC := mock.NewDashboardService()
					svc := NewService(zap.NewNop(), fakeBktSVC, fakeLabelSVC, fakeDashSVC)

					_, diff, err := svc.DryRun(context.TODO(), influxdb.ID(100), pkg)
					require.NoError(t, err)

					require.Len(t, diff.Buckets, 1)

					expected := DiffBucket{
						ID:           influxdb.ID(1),
						Name:         "rucket_11",
						OldDesc:      "old desc",
						NewDesc:      "bucket 1 description",
						OldRetention: 30 * time.Hour,
						NewRetention: time.Hour,
					}
					assert.Equal(t, expected, diff.Buckets[0])
				})
			})

			t.Run("single bucket new", func(t *testing.T) {
				testfileRunner(t, "testdata/bucket", func(t *testing.T, pkg *Pkg) {
					fakeBktSVC := mock.NewBucketService()
					fakeBktSVC.FindBucketByNameFn = func(_ context.Context, orgID influxdb.ID, name string) (*influxdb.Bucket, error) {
						return nil, errors.New("not found")
					}
					fakeLabelSVC := mock.NewLabelService()
					fakeDashSVC := mock.NewDashboardService()
					svc := NewService(zap.NewNop(), fakeBktSVC, fakeLabelSVC, fakeDashSVC)

					_, diff, err := svc.DryRun(context.TODO(), influxdb.ID(100), pkg)
					require.NoError(t, err)

					require.Len(t, diff.Buckets, 1)

					expected := DiffBucket{
						Name:         "rucket_11",
						NewDesc:      "bucket 1 description",
						NewRetention: time.Hour,
					}
					assert.Equal(t, expected, diff.Buckets[0])
				})
			})
		})

		t.Run("labels", func(t *testing.T) {
			t.Run("two labels updated", func(t *testing.T) {
				testfileRunner(t, "testdata/label", func(t *testing.T, pkg *Pkg) {
					fakeBktSVC := mock.NewBucketService()
					fakeLabelSVC := mock.NewLabelService()
					fakeLabelSVC.FindLabelsFn = func(_ context.Context, filter influxdb.LabelFilter) ([]*influxdb.Label, error) {
						return []*influxdb.Label{
							{
								ID:   influxdb.ID(1),
								Name: filter.Name,
								Properties: map[string]string{
									"color":       "old color",
									"description": "old description",
								},
							},
						}, nil
					}
					fakeDashSVC := mock.NewDashboardService()
					svc := NewService(zap.NewNop(), fakeBktSVC, fakeLabelSVC, fakeDashSVC)

					_, diff, err := svc.DryRun(context.TODO(), influxdb.ID(100), pkg)
					require.NoError(t, err)

					require.Len(t, diff.Labels, 2)

					expected := DiffLabel{
						ID:       influxdb.ID(1),
						Name:     "label_1",
						OldColor: "old color",
						NewColor: "#FFFFFF",
						OldDesc:  "old description",
						NewDesc:  "label 1 description",
					}
					assert.Equal(t, expected, diff.Labels[0])

					expected.Name = "label_2"
					expected.NewColor = "#000000"
					expected.NewDesc = "label 2 description"
					assert.Equal(t, expected, diff.Labels[1])
				})
			})

			t.Run("two labels created", func(t *testing.T) {
				testfileRunner(t, "testdata/label", func(t *testing.T, pkg *Pkg) {
					fakeBktSVC := mock.NewBucketService()
					fakeLabelSVC := mock.NewLabelService()
					fakeLabelSVC.FindLabelsFn = func(_ context.Context, filter influxdb.LabelFilter) ([]*influxdb.Label, error) {
						return nil, errors.New("no labels found")
					}
					fakeDashSVC := mock.NewDashboardService()
					svc := NewService(zap.NewNop(), fakeBktSVC, fakeLabelSVC, fakeDashSVC)

					_, diff, err := svc.DryRun(context.TODO(), influxdb.ID(100), pkg)
					require.NoError(t, err)

					require.Len(t, diff.Labels, 2)

					expected := DiffLabel{
						Name:     "label_1",
						NewColor: "#FFFFFF",
						NewDesc:  "label 1 description",
					}
					assert.Equal(t, expected, diff.Labels[0])

					expected.Name = "label_2"
					expected.NewColor = "#000000"
					expected.NewDesc = "label 2 description"
					assert.Equal(t, expected, diff.Labels[1])
				})
			})
		})
	})

	t.Run("Apply", func(t *testing.T) {
		t.Run("buckets", func(t *testing.T) {
			t.Run("successfully creates pkg of buckets", func(t *testing.T) {
				testfileRunner(t, "testdata/bucket", func(t *testing.T, pkg *Pkg) {
					fakeBucketSVC := mock.NewBucketService()
					fakeBucketSVC.CreateBucketFn = func(_ context.Context, b *influxdb.Bucket) error {
						b.ID = influxdb.ID(b.RetentionPeriod)
						return nil
					}
					fakeBucketSVC.FindBucketByNameFn = func(_ context.Context, id influxdb.ID, s string) (*influxdb.Bucket, error) {
						// forces the bucket to be created a new
						return nil, errors.New("an error")
					}
					fakeBucketSVC.UpdateBucketFn = func(_ context.Context, id influxdb.ID, upd influxdb.BucketUpdate) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{ID: id}, nil
					}

					svc := NewService(zap.NewNop(), fakeBucketSVC, nil, nil)

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

			t.Run("will not apply bucket if no changes to be applied", func(t *testing.T) {
				testfileRunner(t, "testdata/bucket", func(t *testing.T, pkg *Pkg) {
					orgID := influxdb.ID(9000)

					pkg.isVerified = true
					pkgBkt := pkg.mBuckets["rucket_11"]
					pkgBkt.existing = &influxdb.Bucket{
						// makes all pkg changes same as they are on thes existing bucket
						ID:              influxdb.ID(3),
						OrgID:           orgID,
						Name:            pkgBkt.Name,
						Description:     pkgBkt.Description,
						RetentionPeriod: pkgBkt.RetentionPeriod,
					}

					fakeBucketSVC := mock.NewBucketService()
					var createCallCount int
					fakeBucketSVC.CreateBucketFn = func(_ context.Context, b *influxdb.Bucket) error {
						createCallCount++
						return nil
					}
					var updateCallCount int
					fakeBucketSVC.UpdateBucketFn = func(_ context.Context, id influxdb.ID, upd influxdb.BucketUpdate) (*influxdb.Bucket, error) {
						updateCallCount++
						return &influxdb.Bucket{ID: id}, nil
					}

					svc := NewService(zap.NewNop(), fakeBucketSVC, nil, nil)

					sum, err := svc.Apply(context.TODO(), orgID, pkg)
					require.NoError(t, err)

					require.Len(t, sum.Buckets, 1)
					buck1 := sum.Buckets[0]
					assert.Equal(t, influxdb.ID(3), buck1.ID)
					assert.Equal(t, orgID, buck1.OrgID)
					assert.Equal(t, "rucket_11", buck1.Name)
					assert.Equal(t, time.Hour, buck1.RetentionPeriod)
					assert.Equal(t, "bucket 1 description", buck1.Description)
					assert.Zero(t, createCallCount)
					assert.Zero(t, updateCallCount)
				})
			})

			t.Run("rolls back all created buckets on an error", func(t *testing.T) {
				testfileRunner(t, "testdata/bucket", func(t *testing.T, pkg *Pkg) {
					fakeBucketSVC := mock.NewBucketService()
					fakeBucketSVC.FindBucketByNameFn = func(_ context.Context, id influxdb.ID, s string) (*influxdb.Bucket, error) {
						// forces the bucket to be created a new
						return nil, errors.New("an error")
					}
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

					svc := NewService(zap.NewNop(), fakeBucketSVC, nil, nil)

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

					svc := NewService(zap.NewNop(), nil, fakeLabelSVC, nil)

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
					fakeDashSVC := mock.NewDashboardService()

					pkg.mLabels["copy1"] = pkg.mLabels["label_1"]
					pkg.mLabels["copy2"] = pkg.mLabels["label_2"]

					svc := NewService(zap.NewNop(), nil, fakeLabelSVC, fakeDashSVC)

					orgID := influxdb.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, pkg)
					require.Error(t, err)

					assert.Equal(t, 3, count)
				})
			})

			t.Run("will not apply label if no changes to be applied", func(t *testing.T) {
				testfileRunner(t, "testdata/label", func(t *testing.T, pkg *Pkg) {
					orgID := influxdb.ID(9000)

					pkg.isVerified = true
					pkgLabel := pkg.mLabels["label_1"]
					pkgLabel.existing = &influxdb.Label{
						// makes all pkg changes same as they are on the existing
						ID:    influxdb.ID(1),
						OrgID: orgID,
						Name:  pkgLabel.Name,
						Properties: map[string]string{
							"color":       pkgLabel.Color,
							"description": pkgLabel.Description,
						},
					}

					fakeLabelSVC := mock.NewLabelService()
					var createCallCount int
					fakeLabelSVC.CreateLabelFn = func(_ context.Context, l *influxdb.Label) error {
						createCallCount++
						if l.Name == "label_2" {
							l.ID = influxdb.ID(2)
							return nil
						}
						return nil
					}
					fakeLabelSVC.UpdateLabelFn = func(_ context.Context, id influxdb.ID, l influxdb.LabelUpdate) (*influxdb.Label, error) {
						if id == influxdb.ID(3) {
							return nil, errors.New("invalid id provided")
						}
						return &influxdb.Label{ID: id}, nil
					}

					svc := NewService(zap.NewNop(), nil, fakeLabelSVC, nil)

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

					assert.Equal(t, 1, createCallCount) // only called for second label
				})
			})

		})

		t.Run("dashboards", func(t *testing.T) {
			t.Run("successfully creates a dashboard", func(t *testing.T) {
				testfileRunner(t, "testdata/dashboard", func(t *testing.T, pkg *Pkg) {
					fakeDashSVC := mock.NewDashboardService()
					id := 1
					fakeDashSVC.CreateDashboardF = func(_ context.Context, d *influxdb.Dashboard) error {
						d.ID = influxdb.ID(id)
						id++
						return nil
					}
					viewCalls := 0
					fakeDashSVC.UpdateDashboardCellViewF = func(ctx context.Context, dID influxdb.ID, cID influxdb.ID, upd influxdb.ViewUpdate) (*influxdb.View, error) {
						viewCalls++
						return &influxdb.View{}, nil
					}

					svc := NewService(zap.NewNop(), nil, nil, fakeDashSVC)

					orgID := influxdb.ID(9000)

					sum, err := svc.Apply(context.TODO(), orgID, pkg)
					require.NoError(t, err)

					require.Len(t, sum.Dashboards, 1)
					dash1 := sum.Dashboards[0]
					assert.Equal(t, influxdb.ID(1), dash1.ID)
					assert.Equal(t, orgID, dash1.OrgID)
					assert.Equal(t, "dash_1", dash1.Name)
					require.Len(t, dash1.Charts, 1)
				})
			})

			t.Run("rolls back created dashboard on an error", func(t *testing.T) {
				testfileRunner(t, "testdata/dashboard", func(t *testing.T, pkg *Pkg) {
					fakeDashSVC := mock.NewDashboardService()
					var c int
					fakeDashSVC.CreateDashboardF = func(_ context.Context, d *influxdb.Dashboard) error {
						// error out on second dashboard attempted
						if c == 1 {
							return errors.New("blowed up ")
						}
						c++
						d.ID = influxdb.ID(c)
						return nil
					}
					deletedDashs := make(map[influxdb.ID]bool)
					fakeDashSVC.DeleteDashboardF = func(_ context.Context, id influxdb.ID) error {
						deletedDashs[id] = true
						return nil
					}

					pkg.mDashboards["copy1"] = pkg.mDashboards["dash_1"]

					svc := NewService(zap.NewNop(), nil, nil, fakeDashSVC)

					orgID := influxdb.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, pkg)
					require.Error(t, err)

					assert.True(t, deletedDashs[influxdb.ID(c)])
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
					fakeBktSVC.FindBucketByNameFn = func(_ context.Context, id influxdb.ID, s string) (*influxdb.Bucket, error) {
						// forces the bucket to be created a new
						return nil, errors.New("an error")
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
					fakeDashSVC := mock.NewDashboardService()
					svc := NewService(zap.NewNop(), fakeBktSVC, fakeLabelSVC, fakeDashSVC)

					orgID := influxdb.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, pkg)
					require.NoError(t, err)

					assert.Equal(t, 4, numLabelMappings)
				})
			})
		})
	})

	t.Run("CreatePkg", func(t *testing.T) {
		t.Run("with metadata sets the new pkgs metadata", func(t *testing.T) {
			svc := new(Service)

			expectedMeta := Metadata{
				Description: "desc",
				Name:        "name",
				Version:     "v1",
			}
			pkg, err := svc.CreatePkg(context.TODO(), WithMetadata(expectedMeta))
			require.NoError(t, err)

			assert.Equal(t, APIVersion, pkg.APIVersion)
			assert.Equal(t, expectedMeta, pkg.Metadata)
			assert.NotNil(t, pkg.Spec.Resources)
		})
	})
}
