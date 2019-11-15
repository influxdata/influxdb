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
					svc := NewService(WithBucketSVC(fakeBktSVC), WithLabelSVC(mock.NewLabelService()))

					_, diff, err := svc.DryRun(context.TODO(), influxdb.ID(100), pkg)
					require.NoError(t, err)

					require.Len(t, diff.Buckets, 1)

					expected := DiffBucket{
						ID:           SafeID(1),
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
					svc := NewService(WithBucketSVC(fakeBktSVC), WithLabelSVC(mock.NewLabelService()))

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
					svc := NewService(WithLabelSVC(fakeLabelSVC))

					_, diff, err := svc.DryRun(context.TODO(), influxdb.ID(100), pkg)
					require.NoError(t, err)

					require.Len(t, diff.Labels, 2)

					expected := DiffLabel{
						ID:       SafeID(1),
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
					fakeLabelSVC := mock.NewLabelService()
					fakeLabelSVC.FindLabelsFn = func(_ context.Context, filter influxdb.LabelFilter) ([]*influxdb.Label, error) {
						return nil, errors.New("no labels found")
					}
					svc := NewService(WithLabelSVC(fakeLabelSVC))

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

		t.Run("variables", func(t *testing.T) {
			testfileRunner(t, "testdata/variables", func(t *testing.T, pkg *Pkg) {
				fakeVarSVC := mock.NewVariableService()
				fakeVarSVC.FindVariablesF = func(_ context.Context, filter influxdb.VariableFilter, opts ...influxdb.FindOptions) ([]*influxdb.Variable, error) {
					return []*influxdb.Variable{
						{
							ID:          influxdb.ID(1),
							Name:        "var_const",
							Description: "old desc",
						},
					}, nil
				}
				fakeLabelSVC := mock.NewLabelService() // ignore mappings for now
				svc := NewService(
					WithLabelSVC(fakeLabelSVC),
					WithVariableSVC(fakeVarSVC),
				)

				_, diff, err := svc.DryRun(context.TODO(), influxdb.ID(100), pkg)
				require.NoError(t, err)

				require.Len(t, diff.Variables, 4)

				expected := DiffVariable{
					ID:      SafeID(1),
					Name:    "var_const",
					OldDesc: "old desc",
					NewDesc: "var_const desc",
					NewArgs: &influxdb.VariableArguments{
						Type:   "constant",
						Values: influxdb.VariableConstantValues{"first val"},
					},
				}
				assert.Equal(t, expected, diff.Variables[0])

				expected = DiffVariable{
					// no ID here since this one would be new
					Name:    "var_map",
					OldDesc: "",
					NewDesc: "var_map desc",
					NewArgs: &influxdb.VariableArguments{
						Type:   "map",
						Values: influxdb.VariableMapValues{"k1": "v1"},
					},
				}
				assert.Equal(t, expected, diff.Variables[1])
			})
		})
	})

	t.Run("Apply", func(t *testing.T) {
		t.Run("buckets", func(t *testing.T) {
			t.Run("successfully creates pkg of buckets", func(t *testing.T) {
				testfileRunner(t, "testdata/bucket.yml", func(t *testing.T, pkg *Pkg) {
					fakeBktSVC := mock.NewBucketService()
					fakeBktSVC.CreateBucketFn = func(_ context.Context, b *influxdb.Bucket) error {
						b.ID = influxdb.ID(b.RetentionPeriod)
						return nil
					}
					fakeBktSVC.FindBucketByNameFn = func(_ context.Context, id influxdb.ID, s string) (*influxdb.Bucket, error) {
						// forces the bucket to be created a new
						return nil, errors.New("an error")
					}
					fakeBktSVC.UpdateBucketFn = func(_ context.Context, id influxdb.ID, upd influxdb.BucketUpdate) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{ID: id}, nil
					}

					svc := NewService(WithBucketSVC(fakeBktSVC))

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

					fakeBktSVC := mock.NewBucketService()
					var createCallCount int
					fakeBktSVC.CreateBucketFn = func(_ context.Context, b *influxdb.Bucket) error {
						createCallCount++
						return nil
					}
					var updateCallCount int
					fakeBktSVC.UpdateBucketFn = func(_ context.Context, id influxdb.ID, upd influxdb.BucketUpdate) (*influxdb.Bucket, error) {
						updateCallCount++
						return &influxdb.Bucket{ID: id}, nil
					}

					svc := NewService(WithBucketSVC(fakeBktSVC))

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
					fakeBktSVC := mock.NewBucketService()
					fakeBktSVC.FindBucketByNameFn = func(_ context.Context, id influxdb.ID, s string) (*influxdb.Bucket, error) {
						// forces the bucket to be created a new
						return nil, errors.New("an error")
					}
					var c int
					fakeBktSVC.CreateBucketFn = func(_ context.Context, b *influxdb.Bucket) error {
						if c == 2 {
							return errors.New("blowed up ")
						}
						c++
						return nil
					}
					var count int
					fakeBktSVC.DeleteBucketFn = func(_ context.Context, id influxdb.ID) error {
						count++
						return nil
					}

					pkg.mBuckets["copybuck1"] = pkg.mBuckets["rucket_11"]
					pkg.mBuckets["copybuck2"] = pkg.mBuckets["rucket_11"]

					svc := NewService(WithBucketSVC(fakeBktSVC))

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

					svc := NewService(WithLabelSVC(fakeLabelSVC))

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

					svc := NewService(WithLabelSVC(fakeLabelSVC))

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

					svc := NewService(WithLabelSVC(fakeLabelSVC))

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
				testfileRunner(t, "testdata/dashboard.yml", func(t *testing.T, pkg *Pkg) {
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

					svc := NewService(WithDashboardSVC(fakeDashSVC))

					orgID := influxdb.ID(9000)

					sum, err := svc.Apply(context.TODO(), orgID, pkg)
					require.NoError(t, err)

					require.Len(t, sum.Dashboards, 1)
					dash1 := sum.Dashboards[0]
					assert.Equal(t, SafeID(1), dash1.ID)
					assert.Equal(t, SafeID(orgID), dash1.OrgID)
					assert.Equal(t, "dash_1", dash1.Name)
					require.Len(t, dash1.Charts, 1)
				})
			})

			t.Run("rolls back created dashboard on an error", func(t *testing.T) {
				testfileRunner(t, "testdata/dashboard.yml", func(t *testing.T, pkg *Pkg) {
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

					svc := NewService(WithDashboardSVC(fakeDashSVC))

					orgID := influxdb.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, pkg)
					require.Error(t, err)

					assert.True(t, deletedDashs[influxdb.ID(c)])
				})
			})
		})

		t.Run("label mapping", func(t *testing.T) {
			t.Run("successfully creates pkg of labels", func(t *testing.T) {
				testfileRunner(t, "testdata/bucket_associates_label.yml", func(t *testing.T, pkg *Pkg) {
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
					svc := NewService(
						WithBucketSVC(fakeBktSVC),
						WithLabelSVC(fakeLabelSVC),
						WithDashboardSVC(fakeDashSVC),
					)

					orgID := influxdb.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, pkg)
					require.NoError(t, err)

					assert.Equal(t, 4, numLabelMappings)
				})
			})
		})

		t.Run("variables", func(t *testing.T) {
			t.Run("successfully creates pkg of variables", func(t *testing.T) {
				testfileRunner(t, "testdata/variables.yml", func(t *testing.T, pkg *Pkg) {
					fakeVarSVC := mock.NewVariableService()
					id := 1
					fakeVarSVC.CreateVariableF = func(_ context.Context, v *influxdb.Variable) error {
						v.ID = influxdb.ID(id)
						id++
						return nil
					}

					svc := NewService(
						WithLabelSVC(mock.NewLabelService()),
						WithVariableSVC(fakeVarSVC),
					)

					orgID := influxdb.ID(9000)

					sum, err := svc.Apply(context.TODO(), orgID, pkg)
					require.NoError(t, err)

					require.Len(t, sum.Variables, 4)
					expected := sum.Variables[0]
					assert.Equal(t, influxdb.ID(1), expected.ID)
					assert.Equal(t, orgID, expected.OrganizationID)
					assert.Equal(t, "var_const", expected.Name)
					assert.Equal(t, "var_const desc", expected.Description)
					require.NotNil(t, expected.Arguments)
					assert.Equal(t, influxdb.VariableConstantValues{"first val"}, expected.Arguments.Values)

					for i := 1; i < 3; i++ {
						expected = sum.Variables[i]
						assert.Equal(t, influxdb.ID(i+1), expected.ID)
					}
				})
			})

			t.Run("rolls back all created variables on an error", func(t *testing.T) {
				testfileRunner(t, "testdata/variables.yml", func(t *testing.T, pkg *Pkg) {
					fakeVarSVC := mock.NewVariableService()
					var c int
					fakeVarSVC.CreateVariableF = func(_ context.Context, l *influxdb.Variable) error {
						// 4th variable will return the error here, and 3 before should be rolled back
						if c == 3 {
							return errors.New("blowed up ")
						}
						c++
						return nil
					}
					var count int
					fakeVarSVC.DeleteVariableF = func(_ context.Context, id influxdb.ID) error {
						count++
						return nil
					}

					svc := NewService(
						WithLabelSVC(mock.NewLabelService()),
						WithVariableSVC(fakeVarSVC),
					)

					orgID := influxdb.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, pkg)
					require.Error(t, err)

					assert.Equal(t, 3, count)
				})
			})

			t.Run("will not apply variable if no changes to be applied", func(t *testing.T) {
				testfileRunner(t, "testdata/variables.yml", func(t *testing.T, pkg *Pkg) {
					orgID := influxdb.ID(9000)

					pkg.isVerified = true
					pkgLabel := pkg.mVariables["var_const"]
					pkgLabel.existing = &influxdb.Variable{
						// makes all pkg changes same as they are on the existing
						ID:             influxdb.ID(1),
						OrganizationID: orgID,
						Name:           pkgLabel.Name,
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{"first val"},
						},
					}

					fakeVarSVC := mock.NewVariableService()
					var createCallCount int
					fakeVarSVC.CreateVariableF = func(_ context.Context, l *influxdb.Variable) error {
						createCallCount++
						if l.Name == "var_const" {
							return errors.New("shouldn't get here")
						}
						return nil
					}
					fakeVarSVC.UpdateVariableF = func(_ context.Context, id influxdb.ID, v *influxdb.VariableUpdate) (*influxdb.Variable, error) {
						if id > influxdb.ID(1) {
							return nil, errors.New("this id should not be updated")
						}
						return &influxdb.Variable{ID: id}, nil
					}

					svc := NewService(
						WithLabelSVC(mock.NewLabelService()),
						WithVariableSVC(fakeVarSVC),
					)

					sum, err := svc.Apply(context.TODO(), orgID, pkg)
					require.NoError(t, err)

					require.Len(t, sum.Variables, 4)
					expected := sum.Variables[0]
					assert.Equal(t, influxdb.ID(1), expected.ID)
					assert.Equal(t, "var_const", expected.Name)

					assert.Equal(t, 3, createCallCount) // only called for last 3 labels
				})
			})
		})
	})

	t.Run("CreatePkg", func(t *testing.T) {
		t.Run("with metadata sets the new pkgs metadata", func(t *testing.T) {
			svc := NewService()

			expectedMeta := Metadata{
				Description: "desc",
				Name:        "name",
				Version:     "v1",
			}
			pkg, err := svc.CreatePkg(context.TODO(), CreateWithMetadata(expectedMeta))
			require.NoError(t, err)

			assert.Equal(t, APIVersion, pkg.APIVersion)
			assert.Equal(t, expectedMeta, pkg.Metadata)
			assert.NotNil(t, pkg.Spec.Resources)
		})

		t.Run("with existing resources", func(t *testing.T) {
			t.Run("bucket", func(t *testing.T) {
				tests := []struct {
					name    string
					newName string
				}{
					{
						name: "without new name",
					},
					{
						name:    "with new name",
						newName: "new name",
					},
				}

				for _, tt := range tests {
					fn := func(t *testing.T) {
						expected := &influxdb.Bucket{
							ID:              3,
							Name:            "bucket name",
							Description:     "desc",
							RetentionPeriod: time.Hour,
						}

						bktSVC := mock.NewBucketService()
						bktSVC.FindBucketByIDFn = func(_ context.Context, id influxdb.ID) (*influxdb.Bucket, error) {
							if id != expected.ID {
								return nil, errors.New("uh ohhh, wrong id here: " + id.String())
							}
							return expected, nil
						}

						svc := NewService(WithBucketSVC(bktSVC), WithLabelSVC(mock.NewLabelService()))

						resToClone := ResourceToClone{
							Kind: KindBucket,
							ID:   expected.ID,
							Name: tt.newName,
						}
						pkg, err := svc.CreatePkg(context.TODO(), CreateWithExistingResources(resToClone))
						require.NoError(t, err)

						bkts := pkg.Summary().Buckets
						require.Len(t, bkts, 1)

						actual := bkts[0]
						expectedName := expected.Name
						if tt.newName != "" {
							expectedName = tt.newName
						}
						assert.Equal(t, expectedName, actual.Name)
						assert.Equal(t, expected.Description, actual.Description)
						assert.Equal(t, expected.RetentionPeriod, actual.RetentionPeriod)
					}
					t.Run(tt.name, fn)
				}
			})

			newQuery := func() influxdb.DashboardQuery {
				q := influxdb.DashboardQuery{
					Text:     "from(v.bucket) |> count()",
					EditMode: "advanced",
				}
				// TODO: remove this when issue that forced the builder tag to be here to render in UI.
				q.BuilderConfig.Tags = append(q.BuilderConfig.Tags, influxdb.NewBuilderTag("_measurement"))
				return q
			}

			newAxes := func() map[string]influxdb.Axis {
				return map[string]influxdb.Axis{
					"x": {
						Bounds: []string{},
						Label:  "labx",
						Prefix: "pre",
						Suffix: "suf",
						Base:   "base",
						Scale:  "linear",
					},
					"y": {
						Bounds: []string{},
						Label:  "laby",
						Prefix: "pre",
						Suffix: "suf",
						Base:   "base",
						Scale:  "linear",
					},
				}
			}

			newColors := func(types ...string) []influxdb.ViewColor {
				var out []influxdb.ViewColor
				for _, t := range types {
					out = append(out, influxdb.ViewColor{
						Type:  t,
						Hex:   time.Now().Format(time.RFC3339),
						Name:  time.Now().Format(time.RFC3339),
						Value: float64(time.Now().Unix()),
					})
				}
				return out
			}

			t.Run("dashboard", func(t *testing.T) {
				tests := []struct {
					name         string
					newName      string
					expectedView influxdb.View
				}{
					{
						name:    "gauge",
						newName: "new name",
						expectedView: influxdb.View{
							ViewContents: influxdb.ViewContents{
								Name: "view name",
							},
							Properties: influxdb.GaugeViewProperties{
								Type:              influxdb.ViewPropertyTypeGauge,
								DecimalPlaces:     influxdb.DecimalPlaces{IsEnforced: true, Digits: 1},
								Note:              "a note",
								Prefix:            "pre",
								Suffix:            "suf",
								Queries:           []influxdb.DashboardQuery{newQuery()},
								ShowNoteWhenEmpty: true,
								ViewColors:        newColors("min", "max", "threshold"),
							},
						},
					},
					{
						name:    "heatmap",
						newName: "new name",
						expectedView: influxdb.View{
							ViewContents: influxdb.ViewContents{
								Name: "view name",
							},
							Properties: influxdb.HeatmapViewProperties{
								Type:              influxdb.ViewPropertyTypeHeatMap,
								Note:              "a note",
								Queries:           []influxdb.DashboardQuery{newQuery()},
								ShowNoteWhenEmpty: true,
								ViewColors:        []string{"#8F8AF4", "#8F8AF4", "#8F8AF4"},
								XColumn:           "x",
								YColumn:           "y",
								XDomain:           []float64{0, 10},
								YDomain:           []float64{0, 100},
								XAxisLabel:        "x_label",
								XPrefix:           "x_prefix",
								XSuffix:           "x_suffix",
								YAxisLabel:        "y_label",
								YPrefix:           "y_prefix",
								YSuffix:           "y_suffix",
								BinSize:           10,
							},
						},
					},
					{
						name:    "scatter",
						newName: "new name",
						expectedView: influxdb.View{
							ViewContents: influxdb.ViewContents{
								Name: "view name",
							},
							Properties: influxdb.ScatterViewProperties{
								Type:              influxdb.ViewPropertyTypeScatter,
								Note:              "a note",
								Queries:           []influxdb.DashboardQuery{newQuery()},
								ShowNoteWhenEmpty: true,
								ViewColors:        []string{"#8F8AF4", "#8F8AF4", "#8F8AF4"},
								XColumn:           "x",
								YColumn:           "y",
								XDomain:           []float64{0, 10},
								YDomain:           []float64{0, 100},
								XAxisLabel:        "x_label",
								XPrefix:           "x_prefix",
								XSuffix:           "x_suffix",
								YAxisLabel:        "y_label",
								YPrefix:           "y_prefix",
								YSuffix:           "y_suffix",
							},
						},
					},
					{
						name: "without new name single stat",
						expectedView: influxdb.View{
							ViewContents: influxdb.ViewContents{
								Name: "view name",
							},
							Properties: influxdb.SingleStatViewProperties{
								Type:              influxdb.ViewPropertyTypeSingleStat,
								DecimalPlaces:     influxdb.DecimalPlaces{IsEnforced: true, Digits: 1},
								Note:              "a note",
								Queries:           []influxdb.DashboardQuery{newQuery()},
								Prefix:            "pre",
								ShowNoteWhenEmpty: true,
								Suffix:            "suf",
								ViewColors:        []influxdb.ViewColor{{Type: "text", Hex: "red"}},
							},
						},
					},
					{
						name:    "with new name single stat",
						newName: "new name",
						expectedView: influxdb.View{
							ViewContents: influxdb.ViewContents{
								Name: "view name",
							},
							Properties: influxdb.SingleStatViewProperties{
								Type:              influxdb.ViewPropertyTypeSingleStat,
								DecimalPlaces:     influxdb.DecimalPlaces{IsEnforced: true, Digits: 1},
								Note:              "a note",
								Queries:           []influxdb.DashboardQuery{newQuery()},
								Prefix:            "pre",
								ShowNoteWhenEmpty: true,
								Suffix:            "suf",
								ViewColors:        []influxdb.ViewColor{{Type: "text", Hex: "red"}},
							},
						},
					},
					{
						name:    "single stat plus line",
						newName: "new name",
						expectedView: influxdb.View{
							ViewContents: influxdb.ViewContents{
								Name: "view name",
							},
							Properties: influxdb.LinePlusSingleStatProperties{
								Type:              influxdb.ViewPropertyTypeSingleStatPlusLine,
								Axes:              newAxes(),
								DecimalPlaces:     influxdb.DecimalPlaces{IsEnforced: true, Digits: 1},
								Legend:            influxdb.Legend{Type: "type", Orientation: "horizontal"},
								Note:              "a note",
								Prefix:            "pre",
								Suffix:            "suf",
								Queries:           []influxdb.DashboardQuery{newQuery()},
								ShadeBelow:        true,
								ShowNoteWhenEmpty: true,
								ViewColors:        []influxdb.ViewColor{{Type: "text", Hex: "red"}},
								XColumn:           "x",
								YColumn:           "y",
							},
						},
					},
					{
						name:    "xy",
						newName: "new name",
						expectedView: influxdb.View{
							ViewContents: influxdb.ViewContents{
								Name: "view name",
							},
							Properties: influxdb.XYViewProperties{
								Type:              influxdb.ViewPropertyTypeXY,
								Axes:              newAxes(),
								Geom:              "step",
								Legend:            influxdb.Legend{Type: "type", Orientation: "horizontal"},
								Note:              "a note",
								Queries:           []influxdb.DashboardQuery{newQuery()},
								ShadeBelow:        true,
								ShowNoteWhenEmpty: true,
								ViewColors:        []influxdb.ViewColor{{Type: "text", Hex: "red"}},
								XColumn:           "x",
								YColumn:           "y",
							},
						},
					},
					{
						name:    "markdown",
						newName: "new name",
						expectedView: influxdb.View{
							ViewContents: influxdb.ViewContents{
								Name: "view name",
							},
							Properties: influxdb.MarkdownViewProperties{
								Type: influxdb.ViewPropertyTypeMarkdown,
								Note: "a note",
							},
						},
					},
				}

				for _, tt := range tests {
					fn := func(t *testing.T) {
						expectedCell := &influxdb.Cell{
							ID:           5,
							CellProperty: influxdb.CellProperty{X: 1, Y: 2, W: 3, H: 4},
						}
						expected := &influxdb.Dashboard{
							ID:          3,
							Name:        "bucket name",
							Description: "desc",
							Cells:       []*influxdb.Cell{expectedCell},
						}

						dashSVC := mock.NewDashboardService()
						dashSVC.FindDashboardByIDF = func(_ context.Context, id influxdb.ID) (*influxdb.Dashboard, error) {
							if id != expected.ID {
								return nil, errors.New("uh ohhh, wrong id here: " + id.String())
							}
							return expected, nil
						}
						dashSVC.GetDashboardCellViewF = func(_ context.Context, id influxdb.ID, cID influxdb.ID) (*influxdb.View, error) {
							if id == expected.ID && cID == expectedCell.ID {
								return &tt.expectedView, nil
							}
							return nil, errors.New("wrongo ids")
						}

						svc := NewService(WithDashboardSVC(dashSVC), WithLabelSVC(mock.NewLabelService()))

						resToClone := ResourceToClone{
							Kind: KindDashboard,
							ID:   expected.ID,
							Name: tt.newName,
						}
						pkg, err := svc.CreatePkg(context.TODO(), CreateWithExistingResources(resToClone))
						require.NoError(t, err)

						dashs := pkg.Summary().Dashboards
						require.Len(t, dashs, 1)

						actual := dashs[0]
						expectedName := expected.Name
						if tt.newName != "" {
							expectedName = tt.newName
						}
						assert.Equal(t, expectedName, actual.Name)
						assert.Equal(t, expected.Description, actual.Description)

						require.Len(t, actual.Charts, 1)
						ch := actual.Charts[0]
						assert.Equal(t, int(expectedCell.X), ch.XPosition)
						assert.Equal(t, int(expectedCell.Y), ch.YPosition)
						assert.Equal(t, int(expectedCell.H), ch.Height)
						assert.Equal(t, int(expectedCell.W), ch.Width)
						assert.Equal(t, tt.expectedView.Properties, ch.Properties)
					}
					t.Run(tt.name, fn)
				}
			})

			t.Run("label", func(t *testing.T) {
				tests := []struct {
					name    string
					newName string
				}{
					{
						name: "without new name",
					},
					{
						name:    "with new name",
						newName: "new name",
					},
				}

				for _, tt := range tests {
					fn := func(t *testing.T) {
						expectedLabel := &influxdb.Label{
							ID:   3,
							Name: "bucket name",
							Properties: map[string]string{
								"description": "desc",
								"color":       "red",
							},
						}

						labelSVC := mock.NewLabelService()
						labelSVC.FindLabelByIDFn = func(_ context.Context, id influxdb.ID) (*influxdb.Label, error) {
							if id != expectedLabel.ID {
								return nil, errors.New("uh ohhh, wrong id here: " + id.String())
							}
							return expectedLabel, nil
						}

						svc := NewService(WithLabelSVC(labelSVC))

						resToClone := ResourceToClone{
							Kind: KindLabel,
							ID:   expectedLabel.ID,
							Name: tt.newName,
						}
						pkg, err := svc.CreatePkg(context.TODO(), CreateWithExistingResources(resToClone))
						require.NoError(t, err)

						newLabels := pkg.Summary().Labels
						require.Len(t, newLabels, 1)

						actual := newLabels[0]
						expectedName := expectedLabel.Name
						if tt.newName != "" {
							expectedName = tt.newName
						}
						assert.Equal(t, expectedName, actual.Name)
						assert.Equal(t, expectedLabel.Properties, actual.Properties)
					}
					t.Run(tt.name, fn)
				}
			})

			t.Run("variable", func(t *testing.T) {
				tests := []struct {
					name        string
					newName     string
					expectedVar influxdb.Variable
				}{
					{
						name: "without new name",
						expectedVar: influxdb.Variable{
							ID:          1,
							Name:        "old name",
							Description: "desc",
							Arguments: &influxdb.VariableArguments{
								Type:   "constant",
								Values: influxdb.VariableConstantValues{"val"},
							},
						},
					},
					{
						name:    "with new name",
						newName: "new name",
						expectedVar: influxdb.Variable{
							ID:   1,
							Name: "old name",
							Arguments: &influxdb.VariableArguments{
								Type:   "constant",
								Values: influxdb.VariableConstantValues{"val"},
							},
						},
					},
					{
						name: "with map arg",
						expectedVar: influxdb.Variable{
							ID:   1,
							Name: "old name",
							Arguments: &influxdb.VariableArguments{
								Type:   "map",
								Values: influxdb.VariableMapValues{"k": "v"},
							},
						},
					},
					{
						name: "with query arg",
						expectedVar: influxdb.Variable{
							ID:   1,
							Name: "old name",
							Arguments: &influxdb.VariableArguments{
								Type: "query",
								Values: influxdb.VariableQueryValues{
									Query:    "query",
									Language: "flux",
								},
							},
						},
					},
				}

				for _, tt := range tests {
					fn := func(t *testing.T) {
						varSVC := mock.NewVariableService()
						varSVC.FindVariableByIDF = func(_ context.Context, id influxdb.ID) (*influxdb.Variable, error) {
							if id != tt.expectedVar.ID {
								return nil, errors.New("uh ohhh, wrong id here: " + id.String())
							}
							return &tt.expectedVar, nil
						}

						svc := NewService(WithVariableSVC(varSVC), WithLabelSVC(mock.NewLabelService()))

						resToClone := ResourceToClone{
							Kind: KindVariable,
							ID:   tt.expectedVar.ID,
							Name: tt.newName,
						}
						pkg, err := svc.CreatePkg(context.TODO(), CreateWithExistingResources(resToClone))
						require.NoError(t, err)

						newVars := pkg.Summary().Variables
						require.Len(t, newVars, 1)

						actual := newVars[0]
						expectedName := tt.expectedVar.Name
						if tt.newName != "" {
							expectedName = tt.newName
						}
						assert.Equal(t, expectedName, actual.Name)
						assert.Equal(t, tt.expectedVar.Description, actual.Description)
						assert.Equal(t, tt.expectedVar.Arguments, actual.Arguments)
					}
					t.Run(tt.name, fn)
				}
			})

			t.Run("includes resource associations", func(t *testing.T) {
				t.Run("single resource with single association", func(t *testing.T) {
					expected := &influxdb.Bucket{
						ID:              3,
						Name:            "bucket name",
						Description:     "desc",
						RetentionPeriod: time.Hour,
					}

					bktSVC := mock.NewBucketService()
					bktSVC.FindBucketByIDFn = func(_ context.Context, id influxdb.ID) (*influxdb.Bucket, error) {
						if id != expected.ID {
							return nil, errors.New("uh ohhh, wrong id here: " + id.String())
						}
						return expected, nil
					}

					labelSVC := mock.NewLabelService()
					labelSVC.FindResourceLabelsFn = func(_ context.Context, f influxdb.LabelMappingFilter) ([]*influxdb.Label, error) {
						if f.ResourceID != expected.ID {
							return nil, errors.New("uh ohs wrong id: " + f.ResourceID.String())
						}
						return []*influxdb.Label{
							{Name: "label_1"},
						}, nil
					}

					svc := NewService(WithBucketSVC(bktSVC), WithLabelSVC(labelSVC))

					resToClone := ResourceToClone{
						Kind: KindBucket,
						ID:   expected.ID,
					}
					pkg, err := svc.CreatePkg(context.TODO(), CreateWithExistingResources(resToClone))
					require.NoError(t, err)

					bkts := pkg.Summary().Buckets
					require.Len(t, bkts, 1)

					actual := bkts[0]
					expectedName := expected.Name
					assert.Equal(t, expectedName, actual.Name)
					assert.Equal(t, expected.Description, actual.Description)
					assert.Equal(t, expected.RetentionPeriod, actual.RetentionPeriod)
					require.Len(t, actual.LabelAssociations, 1)
					assert.Equal(t, "label_1", actual.LabelAssociations[0].Name)

					labels := pkg.Summary().Labels
					require.Len(t, labels, 1)
					assert.Equal(t, "label_1", labels[0].Name)
				})

				t.Run("multiple resources with same associations", func(t *testing.T) {
					bktSVC := mock.NewBucketService()
					bktSVC.FindBucketByIDFn = func(_ context.Context, id influxdb.ID) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{ID: id, Name: strconv.Itoa(int(id))}, nil
					}

					labelSVC := mock.NewLabelService()
					labelSVC.FindResourceLabelsFn = func(_ context.Context, f influxdb.LabelMappingFilter) ([]*influxdb.Label, error) {
						return []*influxdb.Label{
							{Name: "label_1"},
							{Name: "label_2"},
						}, nil
					}

					svc := NewService(WithBucketSVC(bktSVC), WithLabelSVC(labelSVC))

					resourcesToClone := []ResourceToClone{
						{
							Kind: KindBucket,
							ID:   1,
						},
						{
							Kind: KindBucket,
							ID:   2,
						},
					}
					pkg, err := svc.CreatePkg(context.TODO(), CreateWithExistingResources(resourcesToClone...))
					require.NoError(t, err)

					bkts := pkg.Summary().Buckets
					require.Len(t, bkts, 2)

					for i, actual := range bkts {
						assert.Equal(t, strconv.Itoa(i+1), actual.Name)
						require.Len(t, actual.LabelAssociations, 2)
						assert.Equal(t, "label_1", actual.LabelAssociations[0].Name)
						assert.Equal(t, "label_2", actual.LabelAssociations[1].Name)
					}

					labels := pkg.Summary().Labels
					require.Len(t, labels, 2)
					assert.Equal(t, "label_1", labels[0].Name)
					assert.Equal(t, "label_2", labels[1].Name)
				})

				t.Run("labels do not fetch associations", func(t *testing.T) {
					labelSVC := mock.NewLabelService()
					labelSVC.FindLabelByIDFn = func(_ context.Context, id influxdb.ID) (*influxdb.Label, error) {
						return &influxdb.Label{ID: id, Name: "label_1"}, nil
					}
					labelSVC.FindResourceLabelsFn = func(_ context.Context, f influxdb.LabelMappingFilter) ([]*influxdb.Label, error) {
						return nil, errors.New("should not get here")
					}

					svc := NewService(WithLabelSVC(labelSVC))

					resToClone := ResourceToClone{
						Kind: KindLabel,
						ID:   1,
					}
					pkg, err := svc.CreatePkg(context.TODO(), CreateWithExistingResources(resToClone))
					require.NoError(t, err)

					labels := pkg.Summary().Labels
					require.Len(t, labels, 1)
					assert.Equal(t, "label_1", labels[0].Name)
				})
			})
		})
	})
}
