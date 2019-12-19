package pkger

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/notification"
	icheck "github.com/influxdata/influxdb/notification/check"
	"github.com/influxdata/influxdb/notification/endpoint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestService(t *testing.T) {
	newTestService := func(opts ...ServiceSetterFn) *Service {
		opt := serviceOpt{
			bucketSVC:   mock.NewBucketService(),
			checkSVC:    mock.NewCheckService(),
			dashSVC:     mock.NewDashboardService(),
			labelSVC:    mock.NewLabelService(),
			endpointSVC: mock.NewNotificationEndpointService(),
			teleSVC:     mock.NewTelegrafConfigStore(),
			varSVC:      mock.NewVariableService(),
		}
		for _, o := range opts {
			o(&opt)
		}

		return NewService(
			WithBucketSVC(opt.bucketSVC),
			WithCheckSVC(opt.checkSVC),
			WithDashboardSVC(opt.dashSVC),
			WithLabelSVC(opt.labelSVC),
			WithNoticationEndpointSVC(opt.endpointSVC),
			WithSecretSVC(opt.secretSVC),
			WithTelegrafSVC(opt.teleSVC),
			WithVariableSVC(opt.varSVC),
		)
	}

	t.Run("DryRun", func(t *testing.T) {
		t.Run("buckets", func(t *testing.T) {
			t.Run("single bucket updated", func(t *testing.T) {
				testfileRunner(t, "testdata/bucket.yml", func(t *testing.T, pkg *Pkg) {
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
					svc := newTestService(WithBucketSVC(fakeBktSVC))

					_, diff, err := svc.DryRun(context.TODO(), influxdb.ID(100), 0, pkg)
					require.NoError(t, err)

					require.Len(t, diff.Buckets, 1)

					expected := DiffBucket{
						ID:   SafeID(1),
						Name: "rucket_11",
						Old: &DiffBucketValues{
							Description:    "old desc",
							RetentionRules: retentionRules{newRetentionRule(30 * time.Hour)},
						},
						New: DiffBucketValues{
							Description:    "bucket 1 description",
							RetentionRules: retentionRules{newRetentionRule(time.Hour)},
						},
					}
					assert.Equal(t, expected, diff.Buckets[0])
				})
			})

			t.Run("single bucket new", func(t *testing.T) {
				testfileRunner(t, "testdata/bucket.json", func(t *testing.T, pkg *Pkg) {
					fakeBktSVC := mock.NewBucketService()
					fakeBktSVC.FindBucketByNameFn = func(_ context.Context, orgID influxdb.ID, name string) (*influxdb.Bucket, error) {
						return nil, errors.New("not found")
					}
					svc := newTestService(WithBucketSVC(fakeBktSVC))

					_, diff, err := svc.DryRun(context.TODO(), influxdb.ID(100), 0, pkg)
					require.NoError(t, err)

					require.Len(t, diff.Buckets, 1)

					expected := DiffBucket{
						Name: "rucket_11",
						New: DiffBucketValues{
							Description:    "bucket 1 description",
							RetentionRules: retentionRules{newRetentionRule(time.Hour)},
						},
					}
					assert.Equal(t, expected, diff.Buckets[0])
				})
			})
		})

		t.Run("checks", func(t *testing.T) {
			testfileRunner(t, "testdata/checks.yml", func(t *testing.T, pkg *Pkg) {
				fakeCheckSVC := mock.NewCheckService()
				id := influxdb.ID(1)
				existing := &icheck.Deadman{
					Base: icheck.Base{
						ID:          id,
						Name:        "check_1",
						Description: "old desc",
					},
				}
				fakeCheckSVC.FindCheckFn = func(ctx context.Context, f influxdb.CheckFilter) (influxdb.Check, error) {
					if f.Name != nil && *f.Name == "check_1" {
						return existing, nil
					}
					return nil, errors.New("not found")
				}

				svc := newTestService(WithCheckSVC(fakeCheckSVC))

				_, diff, err := svc.DryRun(context.TODO(), influxdb.ID(100), 0, pkg)
				require.NoError(t, err)

				checks := diff.Checks
				require.Len(t, checks, 2)
				check0 := checks[0]
				assert.True(t, check0.IsNew())
				assert.Equal(t, "check_0", check0.Name)
				assert.Zero(t, check0.ID)
				assert.Nil(t, check0.Old)

				check1 := checks[1]
				assert.False(t, check1.IsNew())
				assert.Equal(t, "check_1", check1.Name)
				assert.NotZero(t, check1.ID)
				assert.Equal(t, existing, check1.Old.Check)
			})
		})

		t.Run("labels", func(t *testing.T) {
			t.Run("two labels updated", func(t *testing.T) {
				testfileRunner(t, "testdata/label.json", func(t *testing.T, pkg *Pkg) {
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
					svc := newTestService(WithLabelSVC(fakeLabelSVC))

					_, diff, err := svc.DryRun(context.TODO(), influxdb.ID(100), 0, pkg)
					require.NoError(t, err)

					require.Len(t, diff.Labels, 2)

					expected := DiffLabel{
						ID:   SafeID(1),
						Name: "label_1",
						Old: &DiffLabelValues{
							Color:       "old color",
							Description: "old description",
						},
						New: DiffLabelValues{
							Color:       "#FFFFFF",
							Description: "label 1 description",
						},
					}
					assert.Equal(t, expected, diff.Labels[0])

					expected.Name = "label_2"
					expected.New.Color = "#000000"
					expected.New.Description = "label 2 description"
					assert.Equal(t, expected, diff.Labels[1])
				})
			})

			t.Run("two labels created", func(t *testing.T) {
				testfileRunner(t, "testdata/label.yml", func(t *testing.T, pkg *Pkg) {
					fakeLabelSVC := mock.NewLabelService()
					fakeLabelSVC.FindLabelsFn = func(_ context.Context, filter influxdb.LabelFilter) ([]*influxdb.Label, error) {
						return nil, errors.New("no labels found")
					}
					svc := newTestService(WithLabelSVC(fakeLabelSVC))

					_, diff, err := svc.DryRun(context.TODO(), influxdb.ID(100), 0, pkg)
					require.NoError(t, err)

					require.Len(t, diff.Labels, 2)

					expected := DiffLabel{
						Name: "label_1",
						New: DiffLabelValues{
							Color:       "#FFFFFF",
							Description: "label 1 description",
						},
					}
					assert.Equal(t, expected, diff.Labels[0])

					expected.Name = "label_2"
					expected.New.Color = "#000000"
					expected.New.Description = "label 2 description"
					assert.Equal(t, expected, diff.Labels[1])
				})
			})
		})

		t.Run("notification endpoints", func(t *testing.T) {
			testfileRunner(t, "testdata/notification_endpoint.yml", func(t *testing.T, pkg *Pkg) {
				fakeEndpointSVC := mock.NewNotificationEndpointService()
				id := influxdb.ID(1)
				existing := &endpoint.HTTP{
					Base: endpoint.Base{
						ID:          &id,
						Name:        "http_none_auth_notification_endpoint",
						Description: "old desc",
						Status:      influxdb.TaskStatusInactive,
					},
					Method:     "POST",
					AuthMethod: "none",
					URL:        "https://www.example.com/endpoint/old",
				}
				fakeEndpointSVC.FindNotificationEndpointsF = func(ctx context.Context, f influxdb.NotificationEndpointFilter, opt ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, int, error) {
					return []influxdb.NotificationEndpoint{existing}, 1, nil
				}

				svc := newTestService(WithNoticationEndpointSVC(fakeEndpointSVC))

				_, diff, err := svc.DryRun(context.TODO(), influxdb.ID(100), 0, pkg)
				require.NoError(t, err)

				require.Len(t, diff.NotificationEndpoints, 5)

				var (
					newEndpoints      []DiffNotificationEndpoint
					existingEndpoints []DiffNotificationEndpoint
				)
				for _, e := range diff.NotificationEndpoints {
					if e.Old != nil {
						existingEndpoints = append(existingEndpoints, e)
						continue
					}
					newEndpoints = append(newEndpoints, e)
				}
				require.Len(t, newEndpoints, 4)
				require.Len(t, existingEndpoints, 1)

				expected := DiffNotificationEndpoint{
					ID:   SafeID(1),
					Name: "http_none_auth_notification_endpoint",
					Old: &DiffNotificationEndpointValues{
						NotificationEndpoint: existing,
					},
					New: DiffNotificationEndpointValues{
						NotificationEndpoint: &endpoint.HTTP{
							Base: endpoint.Base{
								ID:          &id,
								Name:        "http_none_auth_notification_endpoint",
								Description: "http none auth desc",
								Status:      influxdb.TaskStatusActive,
							},
							AuthMethod: "none",
							Method:     "GET",
							URL:        "https://www.example.com/endpoint/noneauth",
						},
					},
				}
				assert.Equal(t, expected, existingEndpoints[0])
			})
		})

		t.Run("secrets not found returns error", func(t *testing.T) {
			testfileRunner(t, "testdata/notification_endpoint_secrets.yml", func(t *testing.T, pkg *Pkg) {
				fakeSecretSVC := mock.NewSecretService()
				fakeSecretSVC.GetSecretKeysFn = func(ctx context.Context, orgID influxdb.ID) ([]string, error) {
					return []string{"rando-1", "rando-2"}, nil
				}
				svc := newTestService(WithSecretSVC(fakeSecretSVC))

				_, _, err := svc.DryRun(context.TODO(), influxdb.ID(100), 0, pkg)
				require.Error(t, err)
			})
		})

		t.Run("variables", func(t *testing.T) {
			testfileRunner(t, "testdata/variables", func(t *testing.T, pkg *Pkg) {
				fakeVarSVC := mock.NewVariableService()
				fakeVarSVC.FindVariablesF = func(_ context.Context, filter influxdb.VariableFilter, opts ...influxdb.FindOptions) ([]*influxdb.Variable, error) {
					return []*influxdb.Variable{
						{
							ID:          influxdb.ID(1),
							Name:        "var_const_3",
							Description: "old desc",
						},
					}, nil
				}
				svc := newTestService(WithVariableSVC(fakeVarSVC))

				_, diff, err := svc.DryRun(context.TODO(), influxdb.ID(100), 0, pkg)
				require.NoError(t, err)

				require.Len(t, diff.Variables, 4)

				expected := DiffVariable{
					ID:   SafeID(1),
					Name: "var_const_3",
					Old: &DiffVariableValues{
						Description: "old desc",
					},
					New: DiffVariableValues{
						Description: "var_const_3 desc",
						Args: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{"first val"},
						},
					},
				}
				assert.Equal(t, expected, diff.Variables[0])

				expected = DiffVariable{
					// no ID here since this one would be new
					Name: "var_map_4",
					New: DiffVariableValues{
						Description: "var_map_4 desc",
						Args: &influxdb.VariableArguments{
							Type:   "map",
							Values: influxdb.VariableMapValues{"k1": "v1"},
						},
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

					svc := newTestService(WithBucketSVC(fakeBktSVC))

					orgID := influxdb.ID(9000)

					sum, err := svc.Apply(context.TODO(), orgID, 0, pkg)
					require.NoError(t, err)

					require.Len(t, sum.Buckets, 1)
					buck1 := sum.Buckets[0]
					assert.Equal(t, SafeID(time.Hour), buck1.ID)
					assert.Equal(t, SafeID(orgID), buck1.OrgID)
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
						Name:            pkgBkt.Name(),
						Description:     pkgBkt.Description,
						RetentionPeriod: pkgBkt.RetentionRules.RP(),
					}

					fakeBktSVC := mock.NewBucketService()
					fakeBktSVC.UpdateBucketFn = func(_ context.Context, id influxdb.ID, upd influxdb.BucketUpdate) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{ID: id}, nil
					}

					svc := newTestService(WithBucketSVC(fakeBktSVC))

					sum, err := svc.Apply(context.TODO(), orgID, 0, pkg)
					require.NoError(t, err)

					require.Len(t, sum.Buckets, 1)
					buck1 := sum.Buckets[0]
					assert.Equal(t, SafeID(3), buck1.ID)
					assert.Equal(t, SafeID(orgID), buck1.OrgID)
					assert.Equal(t, "rucket_11", buck1.Name)
					assert.Equal(t, time.Hour, buck1.RetentionPeriod)
					assert.Equal(t, "bucket 1 description", buck1.Description)
					assert.Zero(t, fakeBktSVC.CreateBucketCalls.Count())
					assert.Zero(t, fakeBktSVC.UpdateBucketCalls.Count())
				})
			})

			t.Run("rolls back all created buckets on an error", func(t *testing.T) {
				testfileRunner(t, "testdata/bucket", func(t *testing.T, pkg *Pkg) {
					fakeBktSVC := mock.NewBucketService()
					fakeBktSVC.FindBucketByNameFn = func(_ context.Context, id influxdb.ID, s string) (*influxdb.Bucket, error) {
						// forces the bucket to be created a new
						return nil, errors.New("an error")
					}
					fakeBktSVC.CreateBucketFn = func(_ context.Context, b *influxdb.Bucket) error {
						if fakeBktSVC.CreateBucketCalls.Count() == 1 {
							return errors.New("blowed up ")
						}
						return nil
					}

					pkg.mBuckets["copybuck1"] = pkg.mBuckets["rucket_11"]
					pkg.mBuckets["copybuck2"] = pkg.mBuckets["rucket_11"]

					svc := newTestService(WithBucketSVC(fakeBktSVC))

					orgID := influxdb.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, 0, pkg)
					require.Error(t, err)

					assert.GreaterOrEqual(t, fakeBktSVC.DeleteBucketCalls.Count(), 1)
				})
			})
		})

		t.Run("checks", func(t *testing.T) {
			t.Run("successfully creates pkg of checks", func(t *testing.T) {
				testfileRunner(t, "testdata/checks.yml", func(t *testing.T, pkg *Pkg) {
					fakeCheckSVC := mock.NewCheckService()
					fakeCheckSVC.CreateCheckFn = func(ctx context.Context, c influxdb.CheckCreate, id influxdb.ID) error {
						c.SetID(influxdb.ID(fakeCheckSVC.CreateCheckCalls.Count() + 1))
						return nil
					}

					svc := newTestService(WithCheckSVC(fakeCheckSVC))

					orgID := influxdb.ID(9000)

					sum, err := svc.Apply(context.TODO(), orgID, 0, pkg)
					require.NoError(t, err)

					require.Len(t, sum.Checks, 2)

					containsWithID := func(t *testing.T, name string) {
						for _, actualNotification := range sum.Checks {
							actual := actualNotification.Check
							if actual.GetID() == 0 {
								assert.NotZero(t, actual.GetID())
							}
							if actual.GetName() == name {
								return
							}
						}
						assert.Fail(t, "did not find notification by name: "+name)
					}

					for _, expectedName := range []string{"check_0", "check_1"} {
						containsWithID(t, expectedName)
					}
				})
			})

			t.Run("rolls back all created checks on an error", func(t *testing.T) {
				testfileRunner(t, "testdata/checks.yml", func(t *testing.T, pkg *Pkg) {
					fakeCheckSVC := mock.NewCheckService()
					fakeCheckSVC.CreateCheckFn = func(ctx context.Context, c influxdb.CheckCreate, id influxdb.ID) error {
						c.SetID(influxdb.ID(fakeCheckSVC.CreateCheckCalls.Count() + 1))
						if fakeCheckSVC.CreateCheckCalls.Count() == 1 {
							return errors.New("hit that kill count")
						}
						return nil
					}

					// create some dupes
					for name, c := range pkg.mChecks {
						pkg.mChecks["copy"+name] = c
					}

					svc := newTestService(WithCheckSVC(fakeCheckSVC))

					orgID := influxdb.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, 0, pkg)
					require.Error(t, err)

					assert.GreaterOrEqual(t, fakeCheckSVC.DeleteCheckCalls.Count(), 1)
				})
			})
		})

		t.Run("labels", func(t *testing.T) {
			t.Run("successfully creates pkg of labels", func(t *testing.T) {
				testfileRunner(t, "testdata/label", func(t *testing.T, pkg *Pkg) {
					fakeLabelSVC := mock.NewLabelService()
					fakeLabelSVC.CreateLabelFn = func(_ context.Context, l *influxdb.Label) error {
						i, err := strconv.Atoi(l.Name[len(l.Name)-1:])
						if err != nil {
							return err
						}
						l.ID = influxdb.ID(i)
						return nil
					}

					svc := newTestService(WithLabelSVC(fakeLabelSVC))

					orgID := influxdb.ID(9000)

					sum, err := svc.Apply(context.TODO(), orgID, 0, pkg)
					require.NoError(t, err)

					require.Len(t, sum.Labels, 2)
					label1 := sum.Labels[0]
					assert.Equal(t, SafeID(1), label1.ID)
					assert.Equal(t, SafeID(orgID), label1.OrgID)
					assert.Equal(t, "label_1", label1.Name)
					assert.Equal(t, "#FFFFFF", label1.Properties.Color)
					assert.Equal(t, "label 1 description", label1.Properties.Description)

					label2 := sum.Labels[1]
					assert.Equal(t, SafeID(2), label2.ID)
					assert.Equal(t, SafeID(orgID), label2.OrgID)
					assert.Equal(t, "label_2", label2.Name)
					assert.Equal(t, "#000000", label2.Properties.Color)
					assert.Equal(t, "label 2 description", label2.Properties.Description)
				})
			})

			t.Run("rolls back all created labels on an error", func(t *testing.T) {
				testfileRunner(t, "testdata/label", func(t *testing.T, pkg *Pkg) {
					fakeLabelSVC := mock.NewLabelService()
					fakeLabelSVC.CreateLabelFn = func(_ context.Context, l *influxdb.Label) error {
						// 3rd/4th label will return the error here, and 2 before should be rolled back
						if fakeLabelSVC.CreateLabelCalls.Count() == 2 {
							return errors.New("blowed up ")
						}
						return nil
					}

					pkg.mLabels["copy1"] = pkg.mLabels["label_1"]
					pkg.mLabels["copy2"] = pkg.mLabels["label_2"]

					svc := newTestService(WithLabelSVC(fakeLabelSVC))

					orgID := influxdb.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, 0, pkg)
					require.Error(t, err)

					assert.GreaterOrEqual(t, fakeLabelSVC.DeleteLabelCalls.Count(), 1)
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
						Name:  pkgLabel.Name(),
						Properties: map[string]string{
							"color":       pkgLabel.Color,
							"description": pkgLabel.Description,
						},
					}

					fakeLabelSVC := mock.NewLabelService()
					fakeLabelSVC.CreateLabelFn = func(_ context.Context, l *influxdb.Label) error {
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

					svc := newTestService(WithLabelSVC(fakeLabelSVC))

					sum, err := svc.Apply(context.TODO(), orgID, 0, pkg)
					require.NoError(t, err)

					require.Len(t, sum.Labels, 2)
					label1 := sum.Labels[0]
					assert.Equal(t, SafeID(1), label1.ID)
					assert.Equal(t, SafeID(orgID), label1.OrgID)
					assert.Equal(t, "label_1", label1.Name)
					assert.Equal(t, "#FFFFFF", label1.Properties.Color)
					assert.Equal(t, "label 1 description", label1.Properties.Description)

					label2 := sum.Labels[1]
					assert.Equal(t, SafeID(2), label2.ID)
					assert.Equal(t, SafeID(orgID), label2.OrgID)
					assert.Equal(t, "label_2", label2.Name)
					assert.Equal(t, "#000000", label2.Properties.Color)
					assert.Equal(t, "label 2 description", label2.Properties.Description)

					assert.Equal(t, 1, fakeLabelSVC.CreateLabelCalls.Count()) // only called for second label
				})
			})
		})

		t.Run("dashboards", func(t *testing.T) {
			t.Run("successfully creates a dashboard", func(t *testing.T) {
				testfileRunner(t, "testdata/dashboard.yml", func(t *testing.T, pkg *Pkg) {
					fakeDashSVC := mock.NewDashboardService()
					fakeDashSVC.CreateDashboardF = func(_ context.Context, d *influxdb.Dashboard) error {
						d.ID = influxdb.ID(1)
						return nil
					}
					fakeDashSVC.UpdateDashboardCellViewF = func(ctx context.Context, dID influxdb.ID, cID influxdb.ID, upd influxdb.ViewUpdate) (*influxdb.View, error) {
						return &influxdb.View{}, nil
					}

					svc := newTestService(WithDashboardSVC(fakeDashSVC))

					orgID := influxdb.ID(9000)

					sum, err := svc.Apply(context.TODO(), orgID, 0, pkg)
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
					fakeDashSVC.CreateDashboardF = func(_ context.Context, d *influxdb.Dashboard) error {
						// error out on second dashboard attempted
						if fakeDashSVC.CreateDashboardCalls.Count() == 1 {
							return errors.New("blowed up ")
						}
						d.ID = influxdb.ID(1)
						return nil
					}
					deletedDashs := make(map[influxdb.ID]bool)
					fakeDashSVC.DeleteDashboardF = func(_ context.Context, id influxdb.ID) error {
						deletedDashs[id] = true
						return nil
					}

					pkg.mDashboards = append(pkg.mDashboards, pkg.mDashboards[0])

					svc := newTestService(WithDashboardSVC(fakeDashSVC))

					orgID := influxdb.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, 0, pkg)
					require.Error(t, err)

					assert.True(t, deletedDashs[1])
				})
			})
		})

		t.Run("label mapping", func(t *testing.T) {
			testLabelMappingFn := func(t *testing.T, filename string, numExpected int, settersFn func() []ServiceSetterFn) {
				t.Run("applies successfully", func(t *testing.T) {
					testfileRunner(t, filename, func(t *testing.T, pkg *Pkg) {
						fakeLabelSVC := mock.NewLabelService()
						fakeLabelSVC.CreateLabelFn = func(_ context.Context, l *influxdb.Label) error {
							l.ID = influxdb.ID(rand.Int())
							return nil
						}
						fakeLabelSVC.CreateLabelMappingFn = func(_ context.Context, mapping *influxdb.LabelMapping) error {
							if mapping.ResourceID == 0 {
								return errors.New("did not get a resource ID")
							}
							if mapping.ResourceType == "" {
								return errors.New("did not get a resource type")
							}
							return nil
						}
						svc := newTestService(append(settersFn(),
							WithLabelSVC(fakeLabelSVC),
							WithLogger(zaptest.NewLogger(t)),
						)...)

						orgID := influxdb.ID(9000)

						_, err := svc.Apply(context.TODO(), orgID, 0, pkg)
						require.NoError(t, err)

						assert.Equal(t, numExpected, fakeLabelSVC.CreateLabelMappingCalls.Count())
					})
				})

				t.Run("deletes new label mappings on error", func(t *testing.T) {
					testfileRunner(t, filename, func(t *testing.T, pkg *Pkg) {
						for _, l := range pkg.mLabels {
							for resource, vals := range l.mappings {
								// create extra label mappings, enough for delete to ahve head room
								l.mappings[resource] = append(l.mappings[resource], vals...)
								l.mappings[resource] = append(l.mappings[resource], vals...)
								l.mappings[resource] = append(l.mappings[resource], vals...)
							}
						}

						fakeLabelSVC := mock.NewLabelService()
						fakeLabelSVC.CreateLabelFn = func(_ context.Context, l *influxdb.Label) error {
							l.ID = influxdb.ID(fakeLabelSVC.CreateLabelCalls.Count() + 1)
							return nil
						}
						fakeLabelSVC.CreateLabelMappingFn = func(_ context.Context, mapping *influxdb.LabelMapping) error {
							if mapping.ResourceID == 0 {
								return errors.New("did not get a resource ID")
							}
							if mapping.ResourceType == "" {
								return errors.New("did not get a resource type")
							}
							if fakeLabelSVC.CreateLabelMappingCalls.Count() > numExpected {
								return errors.New("hit last label")
							}
							return nil
						}
						svc := newTestService(append(settersFn(),
							WithLabelSVC(fakeLabelSVC),
							WithLogger(zaptest.NewLogger(t)),
						)...)

						orgID := influxdb.ID(9000)

						_, err := svc.Apply(context.TODO(), orgID, 0, pkg)
						require.Error(t, err)

						assert.GreaterOrEqual(t, fakeLabelSVC.DeleteLabelMappingCalls.Count(), numExpected)
					})
				})
			}

			t.Run("maps buckets with labels", func(t *testing.T) {
				testLabelMappingFn(
					t,
					"testdata/bucket_associates_label.yml",
					4,
					func() []ServiceSetterFn {
						fakeBktSVC := mock.NewBucketService()
						fakeBktSVC.CreateBucketFn = func(_ context.Context, b *influxdb.Bucket) error {
							b.ID = influxdb.ID(rand.Int())
							return nil
						}
						fakeBktSVC.FindBucketByNameFn = func(_ context.Context, id influxdb.ID, s string) (*influxdb.Bucket, error) {
							// forces the bucket to be created a new
							return nil, errors.New("an error")
						}
						return []ServiceSetterFn{WithBucketSVC(fakeBktSVC)}
					},
				)
			})

			t.Run("maps checks with labels", func(t *testing.T) {
				testLabelMappingFn(
					t,
					"testdata/checks.yml",
					2, // 1 for each check
					func() []ServiceSetterFn {
						fakeCheckSVC := mock.NewCheckService()
						fakeCheckSVC.CreateCheckFn = func(ctx context.Context, c influxdb.CheckCreate, id influxdb.ID) error {
							c.Check.SetID(influxdb.ID(rand.Int()))
							return nil
						}
						fakeCheckSVC.FindCheckFn = func(ctx context.Context, f influxdb.CheckFilter) (influxdb.Check, error) {
							return nil, errors.New("check not found")
						}

						return []ServiceSetterFn{WithCheckSVC(fakeCheckSVC)}
					},
				)
			})

			t.Run("maps dashboards with labels", func(t *testing.T) {
				testLabelMappingFn(
					t,
					"testdata/dashboard_associates_label.yml",
					1,
					func() []ServiceSetterFn {
						fakeDashSVC := mock.NewDashboardService()
						fakeDashSVC.CreateDashboardF = func(_ context.Context, d *influxdb.Dashboard) error {
							d.ID = influxdb.ID(rand.Int())
							return nil
						}
						return []ServiceSetterFn{WithDashboardSVC(fakeDashSVC)}
					},
				)
			})

			t.Run("maps notificaton endpoints with labels", func(t *testing.T) {
				testLabelMappingFn(
					t,
					"testdata/notification_endpoint.yml",
					5, // 1 for each check
					func() []ServiceSetterFn {
						fakeEndpointSVC := mock.NewNotificationEndpointService()
						fakeEndpointSVC.CreateNotificationEndpointF = func(ctx context.Context, nr influxdb.NotificationEndpoint, userID influxdb.ID) error {
							nr.SetID(influxdb.ID(rand.Int()))
							return nil
						}
						return []ServiceSetterFn{WithNoticationEndpointSVC(fakeEndpointSVC)}
					},
				)
			})

			t.Run("maps telegrafs with labels", func(t *testing.T) {
				testLabelMappingFn(
					t,
					"testdata/telegraf.yml",
					1,
					func() []ServiceSetterFn {
						fakeTeleSVC := mock.NewTelegrafConfigStore()
						fakeTeleSVC.CreateTelegrafConfigF = func(_ context.Context, cfg *influxdb.TelegrafConfig, _ influxdb.ID) error {
							cfg.ID = influxdb.ID(rand.Int())
							return nil
						}
						return []ServiceSetterFn{WithTelegrafSVC(fakeTeleSVC)}
					},
				)
			})

			t.Run("maps variables with labels", func(t *testing.T) {
				testLabelMappingFn(
					t,
					"testdata/variable_associates_label.yml",
					1,
					func() []ServiceSetterFn {
						fakeVarSVC := mock.NewVariableService()
						fakeVarSVC.CreateVariableF = func(_ context.Context, v *influxdb.Variable) error {
							v.ID = influxdb.ID(rand.Int())
							return nil
						}
						return []ServiceSetterFn{WithVariableSVC(fakeVarSVC)}
					},
				)
			})

		})

		t.Run("notification endpoints", func(t *testing.T) {
			t.Run("successfully creates pkg of endpoints", func(t *testing.T) {
				testfileRunner(t, "testdata/notification_endpoint.yml", func(t *testing.T, pkg *Pkg) {
					fakeEndpointSVC := mock.NewNotificationEndpointService()
					fakeEndpointSVC.CreateNotificationEndpointF = func(ctx context.Context, nr influxdb.NotificationEndpoint, userID influxdb.ID) error {
						nr.SetID(influxdb.ID(fakeEndpointSVC.CreateNotificationEndpointCalls.Count() + 1))
						return nil
					}

					svc := newTestService(WithNoticationEndpointSVC(fakeEndpointSVC))

					orgID := influxdb.ID(9000)

					sum, err := svc.Apply(context.TODO(), orgID, 0, pkg)
					require.NoError(t, err)

					require.Len(t, sum.NotificationEndpoints, 5)

					containsWithID := func(t *testing.T, name string) {
						for _, actualNotification := range sum.NotificationEndpoints {
							actual := actualNotification.NotificationEndpoint
							if actual.GetID() == 0 {
								assert.NotZero(t, actual.GetID())
							}
							if actual.GetName() == name {
								return
							}
						}
						assert.Fail(t, "did not find notification by name: "+name)
					}

					expectedNames := []string{
						"http_basic_auth_notification_endpoint",
						"http_bearer_auth_notification_endpoint",
						"http_none_auth_notification_endpoint",
						"pager_duty_notification_endpoint",
						"slack_notification_endpoint",
					}
					for _, expectedName := range expectedNames {
						containsWithID(t, expectedName)
					}
				})
			})

			t.Run("rolls back all created notifications on an error", func(t *testing.T) {
				testfileRunner(t, "testdata/notification_endpoint.yml", func(t *testing.T, pkg *Pkg) {
					fakeEndpointSVC := mock.NewNotificationEndpointService()
					fakeEndpointSVC.CreateNotificationEndpointF = func(ctx context.Context, nr influxdb.NotificationEndpoint, userID influxdb.ID) error {
						nr.SetID(influxdb.ID(fakeEndpointSVC.CreateNotificationEndpointCalls.Count() + 1))
						if fakeEndpointSVC.CreateNotificationEndpointCalls.Count() == 5 {
							return errors.New("hit that kill count")
						}
						return nil
					}

					// create some dupes
					for name, endpoint := range pkg.mNotificationEndpoints {
						pkg.mNotificationEndpoints["copy"+name] = endpoint
					}

					svc := newTestService(WithNoticationEndpointSVC(fakeEndpointSVC))

					orgID := influxdb.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, 0, pkg)
					require.Error(t, err)

					assert.GreaterOrEqual(t, fakeEndpointSVC.DeleteNotificationEndpointCalls.Count(), 5)
				})
			})
		})

		t.Run("telegrafs", func(t *testing.T) {
			t.Run("successfuly creates", func(t *testing.T) {
				testfileRunner(t, "testdata/telegraf.yml", func(t *testing.T, pkg *Pkg) {
					orgID := influxdb.ID(9000)

					fakeTeleSVC := mock.NewTelegrafConfigStore()
					fakeTeleSVC.CreateTelegrafConfigF = func(_ context.Context, tc *influxdb.TelegrafConfig, userID influxdb.ID) error {
						tc.ID = 1
						return nil
					}

					svc := newTestService(WithTelegrafSVC(fakeTeleSVC))

					sum, err := svc.Apply(context.TODO(), orgID, 0, pkg)
					require.NoError(t, err)

					require.Len(t, sum.TelegrafConfigs, 1)
					assert.Equal(t, "first_tele_config", sum.TelegrafConfigs[0].TelegrafConfig.Name)
					assert.Equal(t, "desc", sum.TelegrafConfigs[0].TelegrafConfig.Description)
				})
			})

			t.Run("rolls back all created telegrafs on an error", func(t *testing.T) {
				testfileRunner(t, "testdata/telegraf.yml", func(t *testing.T, pkg *Pkg) {
					fakeTeleSVC := mock.NewTelegrafConfigStore()
					fakeTeleSVC.CreateTelegrafConfigF = func(_ context.Context, tc *influxdb.TelegrafConfig, userID influxdb.ID) error {
						if fakeTeleSVC.CreateTelegrafConfigCalls.Count() == 1 {
							return errors.New("limit hit")
						}
						tc.ID = influxdb.ID(1)
						return nil
					}
					fakeTeleSVC.DeleteTelegrafConfigF = func(_ context.Context, id influxdb.ID) error {
						if id != 1 {
							return errors.New("wrong id here")
						}
						return nil
					}

					pkg.mTelegrafs = append(pkg.mTelegrafs, pkg.mTelegrafs[0])

					svc := newTestService(WithTelegrafSVC(fakeTeleSVC))

					orgID := influxdb.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, 0, pkg)
					require.Error(t, err)

					assert.Equal(t, 1, fakeTeleSVC.DeleteTelegrafConfigCalls.Count())
				})
			})
		})

		t.Run("variables", func(t *testing.T) {
			t.Run("successfully creates pkg of variables", func(t *testing.T) {
				testfileRunner(t, "testdata/variables.yml", func(t *testing.T, pkg *Pkg) {
					fakeVarSVC := mock.NewVariableService()
					fakeVarSVC.CreateVariableF = func(_ context.Context, v *influxdb.Variable) error {
						id, err := strconv.Atoi(v.Name[len(v.Name)-1:])
						if err != nil {
							return err
						}
						v.ID = influxdb.ID(id)
						return nil
					}

					svc := newTestService(WithVariableSVC(fakeVarSVC))

					orgID := influxdb.ID(9000)

					sum, err := svc.Apply(context.TODO(), orgID, 0, pkg)
					require.NoError(t, err)

					require.Len(t, sum.Variables, 4)
					expected := sum.Variables[0]
					assert.Equal(t, SafeID(3), expected.ID)
					assert.Equal(t, SafeID(orgID), expected.OrgID)
					assert.Equal(t, "var_const_3", expected.Name)
					assert.Equal(t, "var_const_3 desc", expected.Description)
					require.NotNil(t, expected.Arguments)
					assert.Equal(t, influxdb.VariableConstantValues{"first val"}, expected.Arguments.Values)

					for _, actual := range sum.Variables {
						assert.Contains(t, []SafeID{1, 2, 3, 4}, actual.ID)
					}
				})
			})

			t.Run("rolls back all created variables on an error", func(t *testing.T) {
				testfileRunner(t, "testdata/variables.yml", func(t *testing.T, pkg *Pkg) {
					fakeVarSVC := mock.NewVariableService()
					fakeVarSVC.CreateVariableF = func(_ context.Context, l *influxdb.Variable) error {
						// 4th variable will return the error here, and 3 before should be rolled back
						if fakeVarSVC.CreateVariableCalls.Count() == 2 {
							return errors.New("blowed up ")
						}
						return nil
					}

					svc := newTestService(WithVariableSVC(fakeVarSVC))

					orgID := influxdb.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, 0, pkg)
					require.Error(t, err)

					assert.GreaterOrEqual(t, fakeVarSVC.DeleteVariableCalls.Count(), 1)
				})
			})

			t.Run("will not apply variable if no changes to be applied", func(t *testing.T) {
				testfileRunner(t, "testdata/variables.yml", func(t *testing.T, pkg *Pkg) {
					orgID := influxdb.ID(9000)

					pkg.isVerified = true
					pkgLabel := pkg.mVariables["var_const_3"]
					pkgLabel.existing = &influxdb.Variable{
						// makes all pkg changes same as they are on the existing
						ID:             influxdb.ID(1),
						OrganizationID: orgID,
						Name:           pkgLabel.Name(),
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{"first val"},
						},
					}

					fakeVarSVC := mock.NewVariableService()
					fakeVarSVC.CreateVariableF = func(_ context.Context, l *influxdb.Variable) error {
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

					svc := newTestService(WithVariableSVC(fakeVarSVC))

					sum, err := svc.Apply(context.TODO(), orgID, 0, pkg)
					require.NoError(t, err)

					require.Len(t, sum.Variables, 4)
					expected := sum.Variables[0]
					assert.Equal(t, SafeID(1), expected.ID)
					assert.Equal(t, "var_const_3", expected.Name)

					assert.Equal(t, 3, fakeVarSVC.CreateVariableCalls.Count()) // only called for last 3 labels
				})
			})
		})
	})

	t.Run("CreatePkg", func(t *testing.T) {
		newThresholdBase := func(i int) icheck.Base {
			return icheck.Base{
				ID:          influxdb.ID(i),
				Name:        fmt.Sprintf("check_%d", i),
				Description: fmt.Sprintf("desc_%d", i),
				Every:       mustDuration(t, time.Minute),
				Offset:      mustDuration(t, 15*time.Second),
				Query: influxdb.DashboardQuery{
					Text: `from(bucket: "telegraf") |> range(start: -1m) |> filter(fn: (r) => r._field == "usage_user")`,
				},
				StatusMessageTemplate: "Check: ${ r._check_name } is: ${ r._level }",
				Tags: []influxdb.Tag{
					{Key: "key_1", Value: "val_1"},
					{Key: "key_2", Value: "val_2"},
				},
			}
		}

		t.Run("with metadata sets the new pkgs metadata", func(t *testing.T) {
			svc := newTestService(WithLogger(zaptest.NewLogger(t)))

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

						svc := newTestService(WithBucketSVC(bktSVC), WithLabelSVC(mock.NewLabelService()))

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

			t.Run("checks", func(t *testing.T) {
				tests := []struct {
					name     string
					newName  string
					expected influxdb.Check
				}{
					{
						name: "threshold",
						expected: &icheck.Threshold{
							Base: newThresholdBase(0),
							Thresholds: []icheck.ThresholdConfig{
								icheck.Lesser{
									ThresholdConfigBase: icheck.ThresholdConfigBase{
										AllValues: true,
										Level:     notification.Critical,
									},
									Value: 20,
								},
								icheck.Greater{
									ThresholdConfigBase: icheck.ThresholdConfigBase{
										AllValues: true,
										Level:     notification.Warn,
									},
									Value: 30,
								},
								icheck.Range{
									ThresholdConfigBase: icheck.ThresholdConfigBase{
										AllValues: true,
										Level:     notification.Info,
									},
									Within: false, // outside_range
									Min:    10,
									Max:    25,
								},
								icheck.Range{
									ThresholdConfigBase: icheck.ThresholdConfigBase{
										AllValues: true,
										Level:     notification.Ok,
									},
									Within: true, // inside_range
									Min:    21,
									Max:    24,
								},
							},
						},
					},
					{
						name:    "deadman",
						newName: "new name",
						expected: &icheck.Deadman{
							Base:       newThresholdBase(1),
							TimeSince:  mustDuration(t, time.Hour),
							StaleTime:  mustDuration(t, 5*time.Hour),
							ReportZero: true,
							Level:      notification.Critical,
						},
					},
				}

				for _, tt := range tests {
					fn := func(t *testing.T) {
						id := influxdb.ID(1)
						tt.expected.SetID(id)

						checkSVC := mock.NewCheckService()
						checkSVC.FindCheckByIDFn = func(ctx context.Context, id influxdb.ID) (influxdb.Check, error) {
							if id != tt.expected.GetID() {
								return nil, errors.New("uh ohhh, wrong id here: " + id.String())
							}
							return tt.expected, nil
						}

						svc := newTestService(WithCheckSVC(checkSVC))

						resToClone := ResourceToClone{
							Kind: KindCheck,
							ID:   tt.expected.GetID(),
							Name: tt.newName,
						}
						pkg, err := svc.CreatePkg(context.TODO(), CreateWithExistingResources(resToClone))
						require.NoError(t, err)

						checks := pkg.Summary().Checks
						require.Len(t, checks, 1)

						actual := checks[0].Check
						expectedName := tt.expected.GetName()
						if tt.newName != "" {
							expectedName = tt.newName
						}
						assert.Equal(t, expectedName, actual.GetName())
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
								TimeFormat:        "",
							},
						},
					},
					{
						name:    "histogram",
						newName: "new name",
						expectedView: influxdb.View{
							ViewContents: influxdb.ViewContents{
								Name: "view name",
							},
							Properties: influxdb.HistogramViewProperties{
								Type:              influxdb.ViewPropertyTypeHistogram,
								Note:              "a note",
								Queries:           []influxdb.DashboardQuery{newQuery()},
								ShowNoteWhenEmpty: true,
								ViewColors:        []influxdb.ViewColor{{Type: "scale", Hex: "#8F8AF4", Value: 0}, {Type: "scale", Hex: "#8F8AF4", Value: 0}, {Type: "scale", Hex: "#8F8AF4", Value: 0}},
								FillColumns:       []string{},
								XColumn:           "_value",
								XDomain:           []float64{0, 10},
								XAxisLabel:        "x_label",
								BinCount:          30,
								Position:          "stacked",
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
								TimeFormat:        "",
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
								Position:          "stacked",
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
								Position:          "overlaid",
								TimeFormat:        "",
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
							View:         &tt.expectedView,
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

						svc := newTestService(WithDashboardSVC(dashSVC), WithLabelSVC(mock.NewLabelService()))

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

						svc := newTestService(WithLabelSVC(labelSVC))

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
						assert.Equal(t, expectedLabel.Properties["color"], actual.Properties.Color)
						assert.Equal(t, expectedLabel.Properties["description"], actual.Properties.Description)
					}
					t.Run(tt.name, fn)
				}
			})

			t.Run("notification endpoints", func(t *testing.T) {
				tests := []struct {
					name     string
					newName  string
					expected influxdb.NotificationEndpoint
				}{
					{
						name: "pager duty",
						expected: &endpoint.PagerDuty{
							Base: endpoint.Base{
								Name:        "pd-endpoint",
								Description: "desc",
								Status:      influxdb.TaskStatusActive,
							},
							ClientURL:  "http://example.com",
							RoutingKey: influxdb.SecretField{Key: "-routing-key"},
						},
					},
					{
						name:    "pager duty with new name",
						newName: "new name",
						expected: &endpoint.PagerDuty{
							Base: endpoint.Base{
								Name:        "pd-endpoint",
								Description: "desc",
								Status:      influxdb.TaskStatusActive,
							},
							ClientURL:  "http://example.com",
							RoutingKey: influxdb.SecretField{Key: "-routing-key"},
						},
					},
					{
						name: "slack",
						expected: &endpoint.Slack{
							Base: endpoint.Base{
								Name:        "pd-endpoint",
								Description: "desc",
								Status:      influxdb.TaskStatusInactive,
							},
							URL:   "http://example.com",
							Token: influxdb.SecretField{Key: "tokne"},
						},
					},
					{
						name: "http basic",
						expected: &endpoint.HTTP{
							Base: endpoint.Base{
								Name:        "pd-endpoint",
								Description: "desc",
								Status:      influxdb.TaskStatusInactive,
							},
							AuthMethod: "basic",
							Method:     "POST",
							URL:        "http://example.com",
							Password:   influxdb.SecretField{Key: "password"},
							Username:   influxdb.SecretField{Key: "username"},
						},
					},
					{
						name: "http bearer",
						expected: &endpoint.HTTP{
							Base: endpoint.Base{
								Name:        "pd-endpoint",
								Description: "desc",
								Status:      influxdb.TaskStatusInactive,
							},
							AuthMethod: "bearer",
							Method:     "GET",
							URL:        "http://example.com",
							Token:      influxdb.SecretField{Key: "token"},
						},
					},
					{
						name: "http none",
						expected: &endpoint.HTTP{
							Base: endpoint.Base{
								Name:        "pd-endpoint",
								Description: "desc",
								Status:      influxdb.TaskStatusInactive,
							},
							AuthMethod: "none",
							Method:     "GET",
							URL:        "http://example.com",
						},
					},
				}

				for _, tt := range tests {
					fn := func(t *testing.T) {
						id := influxdb.ID(1)
						tt.expected.SetID(id)

						endpointSVC := mock.NewNotificationEndpointService()
						endpointSVC.FindNotificationEndpointByIDF = func(ctx context.Context, id influxdb.ID) (influxdb.NotificationEndpoint, error) {
							if id != tt.expected.GetID() {
								return nil, errors.New("uh ohhh, wrong id here: " + id.String())
							}
							return tt.expected, nil
						}

						svc := newTestService(WithNoticationEndpointSVC(endpointSVC))

						resToClone := ResourceToClone{
							Kind: KindNotificationEndpoint,
							ID:   tt.expected.GetID(),
							Name: tt.newName,
						}
						pkg, err := svc.CreatePkg(context.TODO(), CreateWithExistingResources(resToClone))
						require.NoError(t, err)

						endpoints := pkg.Summary().NotificationEndpoints
						require.Len(t, endpoints, 1)

						actual := endpoints[0].NotificationEndpoint
						expectedName := tt.expected.GetName()
						if tt.newName != "" {
							expectedName = tt.newName
						}
						assert.Equal(t, expectedName, actual.GetName())
						assert.Equal(t, tt.expected.GetDescription(), actual.GetDescription())
						assert.Equal(t, tt.expected.GetStatus(), actual.GetStatus())
						assert.Equal(t, tt.expected.SecretFields(), actual.SecretFields())
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

						svc := newTestService(WithVariableSVC(varSVC), WithLabelSVC(mock.NewLabelService()))

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

					svc := newTestService(WithBucketSVC(bktSVC), WithLabelSVC(labelSVC))

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

					svc := newTestService(WithBucketSVC(bktSVC), WithLabelSVC(labelSVC))

					resourcesToClone := []ResourceToClone{
						{
							Kind: KindBucket,
							ID:   10,
						},
						{
							Kind: KindBucket,
							ID:   20,
						},
					}
					pkg, err := svc.CreatePkg(context.TODO(), CreateWithExistingResources(resourcesToClone...))
					require.NoError(t, err)

					bkts := pkg.Summary().Buckets
					require.Len(t, bkts, 2)

					for i, actual := range bkts {
						assert.Equal(t, strconv.Itoa((i+1)*10), actual.Name)
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

					svc := newTestService(WithLabelSVC(labelSVC))

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

		t.Run("with org id", func(t *testing.T) {
			orgID := influxdb.ID(9000)

			bktSVC := mock.NewBucketService()
			bktSVC.FindBucketsFn = func(_ context.Context, f influxdb.BucketFilter, opts ...influxdb.FindOptions) ([]*influxdb.Bucket, int, error) {
				if f.OrganizationID == nil || *f.OrganizationID != orgID {
					return nil, 0, errors.New("not suppose to get here")
				}
				return []*influxdb.Bucket{{ID: 1, Name: "bucket"}}, 1, nil
			}
			bktSVC.FindBucketByIDFn = func(_ context.Context, id influxdb.ID) (*influxdb.Bucket, error) {
				if id != 1 {
					return nil, errors.New("wrong id")
				}
				return &influxdb.Bucket{ID: 1, Name: "bucket"}, nil
			}

			checkSVC := mock.NewCheckService()
			expectedCheck := &icheck.Deadman{
				Base:       newThresholdBase(1),
				TimeSince:  mustDuration(t, time.Hour),
				StaleTime:  mustDuration(t, 5*time.Hour),
				ReportZero: true,
				Level:      notification.Critical,
			}
			checkSVC.FindChecksFn = func(ctx context.Context, f influxdb.CheckFilter, _ ...influxdb.FindOptions) ([]influxdb.Check, int, error) {
				if f.OrgID == nil || *f.OrgID != orgID {
					return nil, 0, errors.New("not suppose to get here")
				}
				return []influxdb.Check{expectedCheck}, 1, nil
			}
			checkSVC.FindCheckByIDFn = func(ctx context.Context, id influxdb.ID) (influxdb.Check, error) {
				return expectedCheck, nil
			}

			dashSVC := mock.NewDashboardService()
			dashSVC.FindDashboardsF = func(_ context.Context, f influxdb.DashboardFilter, _ influxdb.FindOptions) ([]*influxdb.Dashboard, int, error) {
				if f.OrganizationID == nil || *f.OrganizationID != orgID {
					return nil, 0, errors.New("not suppose to get here")
				}
				return []*influxdb.Dashboard{{
					ID:    2,
					Name:  "dashboard",
					Cells: []*influxdb.Cell{},
				}}, 1, nil
			}
			dashSVC.FindDashboardByIDF = func(_ context.Context, id influxdb.ID) (*influxdb.Dashboard, error) {
				if id != 2 {
					return nil, errors.New("wrong id")
				}
				return &influxdb.Dashboard{
					ID:    2,
					Name:  "dashboard",
					Cells: []*influxdb.Cell{},
				}, nil
			}

			endpointSVC := mock.NewNotificationEndpointService()
			endpointSVC.FindNotificationEndpointsF = func(ctx context.Context, f influxdb.NotificationEndpointFilter, _ ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, int, error) {
				id := influxdb.ID(2)
				endpoints := []influxdb.NotificationEndpoint{
					&endpoint.HTTP{Base: endpoint.Base{ID: &id}},
				}
				return endpoints, len(endpoints), nil
			}
			endpointSVC.FindNotificationEndpointByIDF = func(ctx context.Context, id influxdb.ID) (influxdb.NotificationEndpoint, error) {
				return &endpoint.HTTP{
					Base: endpoint.Base{
						ID:   &id,
						Name: "http",
					},
					URL:        "http://example.com",
					Username:   influxdb.SecretField{Key: id.String() + "-username"},
					Password:   influxdb.SecretField{Key: id.String() + "-password"},
					AuthMethod: "basic",
					Method:     "POST",
				}, nil
			}

			labelSVC := mock.NewLabelService()
			labelSVC.FindLabelsFn = func(_ context.Context, f influxdb.LabelFilter) ([]*influxdb.Label, error) {
				if f.OrgID == nil || *f.OrgID != orgID {
					return nil, errors.New("not suppose to get here")
				}
				return []*influxdb.Label{{ID: 3, Name: "label"}}, nil
			}
			labelSVC.FindLabelByIDFn = func(_ context.Context, id influxdb.ID) (*influxdb.Label, error) {
				if id != 3 {
					return nil, errors.New("wrong id")
				}
				return &influxdb.Label{ID: 3, Name: "label"}, nil
			}

			varSVC := mock.NewVariableService()
			varSVC.FindVariablesF = func(_ context.Context, f influxdb.VariableFilter, _ ...influxdb.FindOptions) ([]*influxdb.Variable, error) {
				if f.OrganizationID == nil || *f.OrganizationID != orgID {
					return nil, errors.New("not suppose to get here")
				}
				return []*influxdb.Variable{{ID: 4, Name: "variable"}}, nil
			}
			varSVC.FindVariableByIDF = func(_ context.Context, id influxdb.ID) (*influxdb.Variable, error) {
				if id != 4 {
					return nil, errors.New("wrong id")
				}
				return &influxdb.Variable{ID: 4, Name: "variable"}, nil
			}

			svc := newTestService(
				WithBucketSVC(bktSVC),
				WithCheckSVC(checkSVC),
				WithDashboardSVC(dashSVC),
				WithLabelSVC(labelSVC),
				WithNoticationEndpointSVC(endpointSVC),
				WithVariableSVC(varSVC),
			)

			pkg, err := svc.CreatePkg(context.TODO(), CreateWithAllOrgResources(orgID))
			require.NoError(t, err)

			summary := pkg.Summary()
			bkts := summary.Buckets
			require.Len(t, bkts, 1)
			assert.Equal(t, "bucket", bkts[0].Name)

			checks := summary.Checks
			require.Len(t, checks, 1)
			assert.Equal(t, "check_1", checks[0].Check.GetName())

			dashs := summary.Dashboards
			require.Len(t, dashs, 1)
			assert.Equal(t, "dashboard", dashs[0].Name)

			labels := summary.Labels
			require.Len(t, labels, 1)
			assert.Equal(t, "label", labels[0].Name)

			endpoints := summary.NotificationEndpoints
			require.Len(t, endpoints, 1)
			assert.Equal(t, "http", endpoints[0].NotificationEndpoint.GetName())

			vars := summary.Variables
			require.Len(t, vars, 1)
			assert.Equal(t, "variable", vars[0].Name)
		})
	})
}
