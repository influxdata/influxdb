package pkger

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/notification"
	icheck "github.com/influxdata/influxdb/notification/check"
	"github.com/influxdata/influxdb/notification/endpoint"
	"github.com/influxdata/influxdb/notification/rule"
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
			orgSVC:      mock.NewOrganizationService(),
			ruleSVC:     mock.NewNotificationRuleStore(),
			taskSVC:     mock.NewTaskService(),
			teleSVC:     mock.NewTelegrafConfigStore(),
			varSVC:      mock.NewVariableService(),
		}
		for _, o := range opts {
			o(&opt)
		}

		return NewService(
			WithIDGenerator(opt.idGen),
			WithTimeGenerator(opt.timeGen),
			WithStore(opt.store),
			WithBucketSVC(opt.bucketSVC),
			WithCheckSVC(opt.checkSVC),
			WithDashboardSVC(opt.dashSVC),
			WithLabelSVC(opt.labelSVC),
			WithNotificationEndpointSVC(opt.endpointSVC),
			WithNotificationRuleSVC(opt.ruleSVC),
			WithOrganizationService(opt.orgSVC),
			WithSecretSVC(opt.secretSVC),
			WithTaskSVC(opt.taskSVC),
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
						if name != "rucket_11" {
							return nil, errors.New("not found")
						}
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

					require.Len(t, diff.Buckets, 2)

					expected := DiffBucket{
						ID:      SafeID(1),
						PkgName: "rucket_11",
						Old: &DiffBucketValues{
							Name:           "rucket_11",
							Description:    "old desc",
							RetentionRules: retentionRules{newRetentionRule(30 * time.Hour)},
						},
						New: DiffBucketValues{
							Name:           "rucket_11",
							Description:    "bucket 1 description",
							RetentionRules: retentionRules{newRetentionRule(time.Hour)},
						},
					}
					assert.Contains(t, diff.Buckets, expected)
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

					require.Len(t, diff.Buckets, 2)

					expected := DiffBucket{
						PkgName: "rucket_11",
						New: DiffBucketValues{
							Name:           "rucket_11",
							Description:    "bucket 1 description",
							RetentionRules: retentionRules{newRetentionRule(time.Hour)},
						},
					}
					assert.Contains(t, diff.Buckets, expected)
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
						Name:        "display name",
						Description: "old desc",
					},
				}
				fakeCheckSVC.FindCheckFn = func(ctx context.Context, f influxdb.CheckFilter) (influxdb.Check, error) {
					if f.Name != nil && *f.Name == "display name" {
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
				assert.Equal(t, "display name", check1.Name)
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

					require.Len(t, diff.Labels, 3)

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
					assert.Contains(t, diff.Labels, expected)

					expected.Name = "label_2"
					expected.New.Color = "#000000"
					expected.New.Description = "label 2 description"
					assert.Contains(t, diff.Labels, expected)
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

					require.Len(t, diff.Labels, 3)

					expected := DiffLabel{
						Name: "label_1",
						New: DiffLabelValues{
							Color:       "#FFFFFF",
							Description: "label 1 description",
						},
					}
					assert.Contains(t, diff.Labels, expected)

					expected.Name = "label_2"
					expected.New.Color = "#000000"
					expected.New.Description = "label 2 description"
					assert.Contains(t, diff.Labels, expected)
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

				svc := newTestService(WithNotificationEndpointSVC(fakeEndpointSVC))

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

		t.Run("notification rules", func(t *testing.T) {
			testfileRunner(t, "testdata/notification_rule.yml", func(t *testing.T, pkg *Pkg) {
				fakeEndpointSVC := mock.NewNotificationEndpointService()
				id := influxdb.ID(1)
				existing := &endpoint.HTTP{
					Base: endpoint.Base{
						ID: &id,
						// This name here matches the endpoint identified in the pkg notification rule
						Name:        "endpoint_0",
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

				svc := newTestService(WithNotificationEndpointSVC(fakeEndpointSVC))

				_, diff, err := svc.DryRun(context.TODO(), influxdb.ID(100), 0, pkg)
				require.NoError(t, err)

				require.Len(t, diff.NotificationRules, 1)

				actual := diff.NotificationRules[0]
				assert.Equal(t, "rule_0", actual.Name)
				assert.Equal(t, "desc_0", actual.Description)
				assert.Equal(t, "http", actual.EndpointType)
				assert.Equal(t, existing.Name, actual.EndpointName)
				assert.Equal(t, SafeID(*existing.ID), actual.EndpointID)
				assert.Equal(t, influxdb.Active, actual.Status)
				assert.Equal(t, (10 * time.Minute).String(), actual.Every)
				assert.Equal(t, (30 * time.Second).String(), actual.Offset)

				expectedStatusRules := []SummaryStatusRule{
					{CurrentLevel: "CRIT", PreviousLevel: "OK"},
					{CurrentLevel: "WARN"},
				}
				assert.Equal(t, expectedStatusRules, actual.StatusRules)

				expectedTagRules := []SummaryTagRule{
					{Key: "k1", Value: "v1", Operator: "equal"},
					{Key: "k1", Value: "v2", Operator: "equal"},
				}
				assert.Equal(t, expectedTagRules, actual.TagRules)
			})

			t.Run("should error if endpoint name is not in pkg or in platform", func(t *testing.T) {
				testfileRunner(t, "testdata/notification_rule.yml", func(t *testing.T, pkg *Pkg) {
					svc := newTestService()

					_, _, err := svc.DryRun(context.TODO(), influxdb.ID(100), 0, pkg)
					require.Error(t, err)
				})
			})
		})

		t.Run("secrets not returns missing secrets", func(t *testing.T) {
			testfileRunner(t, "testdata/notification_endpoint_secrets.yml", func(t *testing.T, pkg *Pkg) {
				fakeSecretSVC := mock.NewSecretService()
				fakeSecretSVC.GetSecretKeysFn = func(ctx context.Context, orgID influxdb.ID) ([]string, error) {
					return []string{"rando-1", "rando-2"}, nil
				}
				svc := newTestService(WithSecretSVC(fakeSecretSVC))

				sum, _, err := svc.DryRun(context.TODO(), influxdb.ID(100), 0, pkg)
				require.NoError(t, err)

				assert.Equal(t, []string{"routing-key"}, sum.MissingSecrets)
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
				assert.Equal(t, expected, diff.Variables[1])

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
				assert.Equal(t, expected, diff.Variables[2])
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

					require.Len(t, sum.Buckets, 2)

					expected := SummaryBucket{
						ID:                SafeID(time.Hour),
						OrgID:             SafeID(orgID),
						PkgName:           "rucket_11",
						Name:              "rucket_11",
						Description:       "bucket 1 description",
						RetentionPeriod:   time.Hour,
						LabelAssociations: []SummaryLabel{},
					}
					assert.Contains(t, sum.Buckets, expected)
				})
			})

			t.Run("will not apply bucket if no changes to be applied", func(t *testing.T) {
				testfileRunner(t, "testdata/bucket.yml", func(t *testing.T, pkg *Pkg) {
					orgID := influxdb.ID(9000)

					pkg.isVerified = true
					stubExisting := func(name string, id influxdb.ID) {
						pkgBkt := pkg.mBuckets[name]
						pkgBkt.existing = &influxdb.Bucket{
							// makes all pkg changes same as they are on thes existing bucket
							ID:              id,
							OrgID:           orgID,
							Name:            pkgBkt.Name(),
							Description:     pkgBkt.Description,
							RetentionPeriod: pkgBkt.RetentionRules.RP(),
						}
					}
					stubExisting("rucket_11", 3)
					stubExisting("rucket_22", 4)

					fakeBktSVC := mock.NewBucketService()
					fakeBktSVC.UpdateBucketFn = func(_ context.Context, id influxdb.ID, upd influxdb.BucketUpdate) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{ID: id}, nil
					}

					svc := newTestService(WithBucketSVC(fakeBktSVC))

					sum, err := svc.Apply(context.TODO(), orgID, 0, pkg)
					require.NoError(t, err)

					require.Len(t, sum.Buckets, 2)

					expected := SummaryBucket{
						ID:                SafeID(3),
						OrgID:             SafeID(orgID),
						PkgName:           "rucket_11",
						Name:              "rucket_11",
						Description:       "bucket 1 description",
						RetentionPeriod:   time.Hour,
						LabelAssociations: []SummaryLabel{},
					}
					assert.Contains(t, sum.Buckets, expected)
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
						t.Helper()

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

					for _, expectedName := range []string{"check_0", "display name"} {
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
				testfileRunner(t, "testdata/label.json", func(t *testing.T, pkg *Pkg) {
					fakeLabelSVC := mock.NewLabelService()
					fakeLabelSVC.CreateLabelFn = func(_ context.Context, l *influxdb.Label) error {
						i, err := strconv.Atoi(l.Name[len(l.Name)-1:])
						if err != nil {
							return nil
						}
						l.ID = influxdb.ID(i)
						return nil
					}

					svc := newTestService(WithLabelSVC(fakeLabelSVC))

					orgID := influxdb.ID(9000)

					sum, err := svc.Apply(context.TODO(), orgID, 0, pkg)
					require.NoError(t, err)

					require.Len(t, sum.Labels, 3)

					assert.Contains(t, sum.Labels, SummaryLabel{
						ID:    1,
						OrgID: SafeID(orgID),
						Name:  "label_1",
						Properties: struct {
							Color       string `json:"color"`
							Description string `json:"description"`
						}{
							Color:       "#FFFFFF",
							Description: "label 1 description",
						},
					})

					assert.Contains(t, sum.Labels, SummaryLabel{
						ID:    2,
						OrgID: SafeID(orgID),
						Name:  "label_2",
						Properties: struct {
							Color       string `json:"color"`
							Description string `json:"description"`
						}{
							Color:       "#000000",
							Description: "label 2 description",
						},
					})
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
					stubExisting := func(name string, id influxdb.ID) {
						pkgLabel := pkg.mLabels[name]
						pkgLabel.existing = &influxdb.Label{
							// makes all pkg changes same as they are on the existing
							ID:    id,
							OrgID: orgID,
							Name:  pkgLabel.Name(),
							Properties: map[string]string{
								"color":       pkgLabel.Color,
								"description": pkgLabel.Description,
							},
						}
					}
					stubExisting("label_1", 1)
					stubExisting("label_3", 3)

					fakeLabelSVC := mock.NewLabelService()
					fakeLabelSVC.CreateLabelFn = func(_ context.Context, l *influxdb.Label) error {
						if l.Name == "label_2" {
							l.ID = 2
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

					require.Len(t, sum.Labels, 3)

					assert.Contains(t, sum.Labels, SummaryLabel{
						ID:    1,
						OrgID: SafeID(orgID),
						Name:  "label_1",
						Properties: struct {
							Color       string `json:"color"`
							Description string `json:"description"`
						}{
							Color:       "#FFFFFF",
							Description: "label 1 description",
						},
					})

					assert.Contains(t, sum.Labels, SummaryLabel{
						ID:    2,
						OrgID: SafeID(orgID),
						Name:  "label_2",
						Properties: struct {
							Color       string `json:"color"`
							Description string `json:"description"`
						}{
							Color:       "#000000",
							Description: "label 2 description",
						},
					})

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
					assert.Equal(t, "display name", dash1.Name)
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

					pkg.mDashboards["dash_1_copy"] = pkg.mDashboards["dash_1"]

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

			t.Run("maps notification endpoints with labels", func(t *testing.T) {
				testLabelMappingFn(
					t,
					"testdata/notification_endpoint.yml",
					5,
					func() []ServiceSetterFn {
						fakeEndpointSVC := mock.NewNotificationEndpointService()
						fakeEndpointSVC.CreateNotificationEndpointF = func(ctx context.Context, nr influxdb.NotificationEndpoint, userID influxdb.ID) error {
							nr.SetID(influxdb.ID(rand.Int()))
							return nil
						}
						return []ServiceSetterFn{WithNotificationEndpointSVC(fakeEndpointSVC)}
					},
				)
			})

			t.Run("maps notification rules with labels", func(t *testing.T) {
				testLabelMappingFn(
					t,
					"testdata/notification_rule.yml",
					1,
					func() []ServiceSetterFn {
						fakeEndpointSVC := mock.NewNotificationEndpointService()
						fakeEndpointSVC.FindNotificationEndpointsF = func(ctx context.Context, f influxdb.NotificationEndpointFilter, _ ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, int, error) {
							id := influxdb.ID(9)
							return []influxdb.NotificationEndpoint{
								&endpoint.HTTP{
									Base: endpoint.Base{
										ID:   &id,
										Name: "endpoint_0",
									},
									AuthMethod: "none",
								},
							}, 1, nil
						}
						fakeRuleStore := mock.NewNotificationRuleStore()
						fakeRuleStore.CreateNotificationRuleF = func(ctx context.Context, nr influxdb.NotificationRuleCreate, userID influxdb.ID) error {
							nr.SetID(influxdb.ID(fakeRuleStore.CreateNotificationRuleCalls.Count() + 1))
							return nil
						}
						return []ServiceSetterFn{
							WithNotificationEndpointSVC(fakeEndpointSVC),
							WithNotificationRuleSVC(fakeRuleStore),
						}
					},
				)
			})

			t.Run("maps tasks with labels", func(t *testing.T) {
				testLabelMappingFn(
					t,
					"testdata/tasks.yml",
					2,
					func() []ServiceSetterFn {
						fakeTaskSVC := mock.NewTaskService()
						fakeTaskSVC.CreateTaskFn = func(ctx context.Context, tc influxdb.TaskCreate) (*influxdb.Task, error) {
							reg := regexp.MustCompile(`name: "(.+)",`)
							names := reg.FindStringSubmatch(tc.Flux)
							if len(names) < 2 {
								return nil, errors.New("bad flux query provided: " + tc.Flux)
							}
							return &influxdb.Task{
								ID:             influxdb.ID(rand.Int()),
								Type:           tc.Type,
								OrganizationID: tc.OrganizationID,
								OwnerID:        tc.OwnerID,
								Name:           names[1],
								Description:    tc.Description,
								Status:         tc.Status,
								Flux:           tc.Flux,
							}, nil
						}
						return []ServiceSetterFn{WithTaskSVC(fakeTaskSVC)}
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

					svc := newTestService(WithNotificationEndpointSVC(fakeEndpointSVC))

					orgID := influxdb.ID(9000)

					sum, err := svc.Apply(context.TODO(), orgID, 0, pkg)
					require.NoError(t, err)

					require.Len(t, sum.NotificationEndpoints, 5)

					containsWithID := func(t *testing.T, name string) {
						var endpoints []string
						for _, actualNotification := range sum.NotificationEndpoints {
							actual := actualNotification.NotificationEndpoint
							if actual.GetID() == 0 {
								assert.NotZero(t, actual.GetID())
							}
							if actual.GetName() == name {
								return
							}
							endpoints = append(endpoints, fmt.Sprintf("%+v", actual))
						}
						assert.Failf(t, "did not find notification by name: "+name, "endpoints received: %s", endpoints)
					}

					expectedNames := []string{
						"basic endpoint name",
						"http_bearer_auth_notification_endpoint",
						"http_none_auth_notification_endpoint",
						"pager duty name",
						"slack name",
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

					svc := newTestService(WithNotificationEndpointSVC(fakeEndpointSVC))

					orgID := influxdb.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, 0, pkg)
					require.Error(t, err)

					assert.GreaterOrEqual(t, fakeEndpointSVC.DeleteNotificationEndpointCalls.Count(), 5)
				})
			})
		})

		t.Run("notification rules", func(t *testing.T) {
			t.Run("successfuly creates", func(t *testing.T) {
				testfileRunner(t, "testdata/notification_rule.yml", func(t *testing.T, pkg *Pkg) {
					fakeEndpointSVC := mock.NewNotificationEndpointService()
					fakeEndpointSVC.FindNotificationEndpointsF = func(ctx context.Context, f influxdb.NotificationEndpointFilter, _ ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, int, error) {
						id := influxdb.ID(9)
						return []influxdb.NotificationEndpoint{
							&endpoint.HTTP{
								Base: endpoint.Base{
									ID:   &id,
									Name: "endpoint_0",
								},
							},
						}, 1, nil
					}
					fakeRuleStore := mock.NewNotificationRuleStore()
					fakeRuleStore.CreateNotificationRuleF = func(ctx context.Context, nr influxdb.NotificationRuleCreate, userID influxdb.ID) error {
						nr.SetID(influxdb.ID(fakeRuleStore.CreateNotificationRuleCalls.Count() + 1))
						return nil
					}

					svc := newTestService(
						WithNotificationEndpointSVC(fakeEndpointSVC),
						WithNotificationRuleSVC(fakeRuleStore),
					)

					orgID := influxdb.ID(9000)

					sum, err := svc.Apply(context.TODO(), orgID, 0, pkg)
					require.NoError(t, err)

					require.Len(t, sum.NotificationRules, 1)
					assert.Equal(t, "rule_0", sum.NotificationRules[0].Name)
					assert.Equal(t, "desc_0", sum.NotificationRules[0].Description)
					assert.Equal(t, SafeID(9), sum.NotificationRules[0].EndpointID)
					assert.Equal(t, "endpoint_0", sum.NotificationRules[0].EndpointName)
					assert.Equal(t, "http", sum.NotificationRules[0].EndpointType)
				})
			})

			t.Run("rolls back all created notification rules on an error", func(t *testing.T) {
				testfileRunner(t, "testdata/notification_rule.yml", func(t *testing.T, pkg *Pkg) {
					fakeEndpointSVC := mock.NewNotificationEndpointService()
					fakeEndpointSVC.FindNotificationEndpointsF = func(ctx context.Context, f influxdb.NotificationEndpointFilter, _ ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, int, error) {
						id := influxdb.ID(9)
						return []influxdb.NotificationEndpoint{
							&endpoint.HTTP{
								Base: endpoint.Base{
									ID:   &id,
									Name: "endpoint_0",
								},
								AuthMethod: "none",
							},
						}, 1, nil
					}
					fakeRuleStore := mock.NewNotificationRuleStore()
					fakeRuleStore.CreateNotificationRuleF = func(ctx context.Context, nr influxdb.NotificationRuleCreate, userID influxdb.ID) error {
						if fakeRuleStore.CreateNotificationRuleCalls.Count() == 1 {
							return errors.New("limit hit")
						}
						nr.SetID(1)
						return nil
					}
					fakeRuleStore.DeleteNotificationRuleF = func(ctx context.Context, id influxdb.ID) error {
						if id != 1 {
							return errors.New("wrong id here")
						}
						return nil
					}

					pkg.mNotificationRules["rule_UUID_copy"] = pkg.mNotificationRules["rule_UUID"]

					svc := newTestService(
						WithNotificationEndpointSVC(fakeEndpointSVC),
						WithNotificationRuleSVC(fakeRuleStore),
					)

					orgID := influxdb.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, 0, pkg)
					require.Error(t, err)

					assert.Equal(t, 1, fakeRuleStore.DeleteNotificationRuleCalls.Count())
				})
			})
		})

		t.Run("tasks", func(t *testing.T) {
			t.Run("successfuly creates", func(t *testing.T) {
				testfileRunner(t, "testdata/tasks.yml", func(t *testing.T, pkg *Pkg) {
					orgID := influxdb.ID(9000)

					fakeTaskSVC := mock.NewTaskService()
					fakeTaskSVC.CreateTaskFn = func(ctx context.Context, tc influxdb.TaskCreate) (*influxdb.Task, error) {
						reg := regexp.MustCompile(`name: "(.+)",`)
						names := reg.FindStringSubmatch(tc.Flux)
						if len(names) < 2 {
							return nil, errors.New("bad flux query provided: " + tc.Flux)
						}
						return &influxdb.Task{
							ID:             influxdb.ID(fakeTaskSVC.CreateTaskCalls.Count() + 1),
							Type:           tc.Type,
							OrganizationID: tc.OrganizationID,
							OwnerID:        tc.OwnerID,
							Name:           names[1],
							Description:    tc.Description,
							Status:         tc.Status,
							Flux:           tc.Flux,
						}, nil
					}

					svc := newTestService(WithTaskSVC(fakeTaskSVC))

					sum, err := svc.Apply(context.TODO(), orgID, 0, pkg)
					require.NoError(t, err)

					require.Len(t, sum.Tasks, 2)
					for i, actual := range sum.Tasks {
						assert.NotZero(t, actual.ID)
						assert.Equal(t, "task_"+strconv.Itoa(i), actual.Name)
						assert.Equal(t, "desc_"+strconv.Itoa(i), actual.Description)
					}
				})
			})

			t.Run("rolls back all created tasks on an error", func(t *testing.T) {
				testfileRunner(t, "testdata/tasks.yml", func(t *testing.T, pkg *Pkg) {
					fakeTaskSVC := mock.NewTaskService()
					fakeTaskSVC.CreateTaskFn = func(ctx context.Context, tc influxdb.TaskCreate) (*influxdb.Task, error) {
						if fakeTaskSVC.CreateTaskCalls.Count() == 1 {
							return nil, errors.New("expected error")
						}
						return &influxdb.Task{
							ID: influxdb.ID(fakeTaskSVC.CreateTaskCalls.Count() + 1),
						}, nil
					}

					svc := newTestService(WithTaskSVC(fakeTaskSVC))

					orgID := influxdb.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, 0, pkg)
					require.Error(t, err)

					assert.Equal(t, 1, fakeTaskSVC.DeleteTaskCalls.Count())
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
					assert.Equal(t, "display name", sum.TelegrafConfigs[0].TelegrafConfig.Name)
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

					pkg.mTelegrafs["first_tele_config_copy"] = pkg.mTelegrafs["first_tele_config"]

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
						v.ID = influxdb.ID(fakeVarSVC.CreateVariableCalls.Count() + 1)
						return nil
					}

					svc := newTestService(WithVariableSVC(fakeVarSVC))

					orgID := influxdb.ID(9000)

					sum, err := svc.Apply(context.TODO(), orgID, 0, pkg)
					require.NoError(t, err)

					require.Len(t, sum.Variables, 4)
					expected := sum.Variables[1]
					assert.True(t, expected.ID > 0 && expected.ID < 5)
					assert.Equal(t, SafeID(orgID), expected.OrgID)
					assert.Equal(t, "var_const_3", expected.Name)
					assert.Equal(t, "var_const_3 desc", expected.Description)
					require.NotNil(t, expected.Arguments)
					assert.Equal(t, influxdb.VariableConstantValues{"first val"}, expected.Arguments.Values)

					for _, actual := range sum.Variables {
						assert.Containsf(t, []SafeID{1, 2, 3, 4}, actual.ID, "actual var: %+v", actual)
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
					expected := sum.Variables[1]
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
				TaskID:      300,
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

		t.Run("with existing resources", func(t *testing.T) {
			encodeAndDecode := func(t *testing.T, pkg *Pkg) *Pkg {
				t.Helper()

				b, err := pkg.Encode(EncodingJSON)
				require.NoError(t, err)

				newPkg, err := Parse(EncodingJSON, FromReader(bytes.NewReader(b)))
				require.NoError(t, err)

				return newPkg
			}

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

						newPkg := encodeAndDecode(t, pkg)

						bkts := newPkg.Summary().Buckets
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

						newPkg := encodeAndDecode(t, pkg)

						checks := newPkg.Summary().Checks
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
				q.BuilderConfig.Tags = append(q.BuilderConfig.Tags, influxdb.NewBuilderTag("_measurement", "filter", ""))
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
				t.Run("with single chart", func(t *testing.T) {
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
									TickPrefix:        "true",
									Suffix:            "suf",
									TickSuffix:        "false",
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
									TickPrefix:        "false",
									ShowNoteWhenEmpty: true,
									Suffix:            "suf",
									TickSuffix:        "true",
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
									TickPrefix:        "false",
									ShowNoteWhenEmpty: true,
									Suffix:            "suf",
									TickSuffix:        "true",
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
						{
							name:    "table",
							newName: "new name",
							expectedView: influxdb.View{
								ViewContents: influxdb.ViewContents{
									Name: "view name",
								},
								Properties: influxdb.TableViewProperties{
									Type:              influxdb.ViewPropertyTypeTable,
									Note:              "a note",
									ShowNoteWhenEmpty: true,
									Queries:           []influxdb.DashboardQuery{newQuery()},
									ViewColors:        []influxdb.ViewColor{{Type: "scale", Hex: "#8F8AF4", Value: 0}, {Type: "scale", Hex: "#8F8AF4", Value: 0}, {Type: "scale", Hex: "#8F8AF4", Value: 0}},
									TableOptions: influxdb.TableOptions{
										VerticalTimeAxis: true,
										SortBy: influxdb.RenamableField{
											InternalName: "_time",
										},
										Wrapping:       "truncate",
										FixFirstColumn: true,
									},
									FieldOptions: []influxdb.RenamableField{
										{
											InternalName: "_time",
											DisplayName:  "time (ms)",
											Visible:      true,
										},
									},
									TimeFormat: "YYYY:MM:DD",
									DecimalPlaces: influxdb.DecimalPlaces{
										IsEnforced: true,
										Digits:     1,
									},
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

							newPkg := encodeAndDecode(t, pkg)

							dashs := newPkg.Summary().Dashboards
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

				t.Run("handles duplicate dashboard names", func(t *testing.T) {
					dashSVC := mock.NewDashboardService()
					dashSVC.FindDashboardByIDF = func(_ context.Context, id influxdb.ID) (*influxdb.Dashboard, error) {
						return &influxdb.Dashboard{
							ID:          id,
							Name:        "dash name",
							Description: "desc",
						}, nil
					}

					svc := newTestService(WithDashboardSVC(dashSVC), WithLabelSVC(mock.NewLabelService()))

					resourcesToClone := []ResourceToClone{
						{
							Kind: KindDashboard,
							ID:   1,
						},
						{
							Kind: KindDashboard,
							ID:   2,
						},
					}
					pkg, err := svc.CreatePkg(context.TODO(), CreateWithExistingResources(resourcesToClone...))
					require.NoError(t, err)

					newPkg := encodeAndDecode(t, pkg)

					dashs := newPkg.Summary().Dashboards
					require.Len(t, dashs, len(resourcesToClone))

					for i := range resourcesToClone {
						actual := dashs[i]
						assert.Equal(t, "dash name", actual.Name)
						assert.Equal(t, "desc", actual.Description)
					}
				})
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

						newPkg := encodeAndDecode(t, pkg)

						newLabels := newPkg.Summary().Labels
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

						svc := newTestService(WithNotificationEndpointSVC(endpointSVC))

						resToClone := ResourceToClone{
							Kind: KindNotificationEndpoint,
							ID:   tt.expected.GetID(),
							Name: tt.newName,
						}
						pkg, err := svc.CreatePkg(context.TODO(), CreateWithExistingResources(resToClone))
						require.NoError(t, err)

						newPkg := encodeAndDecode(t, pkg)

						endpoints := newPkg.Summary().NotificationEndpoints
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

			t.Run("notification rules", func(t *testing.T) {
				newRuleBase := func(id int) rule.Base {
					return rule.Base{
						ID:          influxdb.ID(id),
						Name:        "old_name",
						Description: "desc",
						EndpointID:  influxdb.ID(id),
						Every:       mustDuration(t, time.Hour),
						Offset:      mustDuration(t, time.Minute),
						TagRules: []notification.TagRule{
							{Tag: influxdb.Tag{Key: "k1", Value: "v1"}},
						},
						StatusRules: []notification.StatusRule{
							{CurrentLevel: notification.Ok, PreviousLevel: levelPtr(notification.Warn)},
							{CurrentLevel: notification.Critical},
						},
					}
				}

				t.Run("single rule export", func(t *testing.T) {
					tests := []struct {
						name     string
						newName  string
						endpoint influxdb.NotificationEndpoint
						rule     influxdb.NotificationRule
					}{
						{
							name:    "pager duty",
							newName: "pager_duty_name",
							endpoint: &endpoint.PagerDuty{
								Base: endpoint.Base{
									ID:          newTestIDPtr(13),
									Name:        "endpoint_0",
									Description: "desc",
									Status:      influxdb.TaskStatusActive,
								},
								ClientURL:  "http://example.com",
								RoutingKey: influxdb.SecretField{Key: "-routing-key"},
							},
							rule: &rule.PagerDuty{
								Base:            newRuleBase(13),
								MessageTemplate: "Template",
							},
						},
						{
							name: "slack",
							endpoint: &endpoint.Slack{
								Base: endpoint.Base{
									ID:          newTestIDPtr(13),
									Name:        "endpoint_0",
									Description: "desc",
									Status:      influxdb.TaskStatusInactive,
								},
								URL:   "http://example.com",
								Token: influxdb.SecretField{Key: "tokne"},
							},
							rule: &rule.Slack{
								Base:            newRuleBase(13),
								Channel:         "abc",
								MessageTemplate: "SLACK TEMPlate",
							},
						},
						{
							name: "http none",
							endpoint: &endpoint.HTTP{
								Base: endpoint.Base{
									ID:          newTestIDPtr(13),
									Name:        "endpoint_0",
									Description: "desc",
									Status:      influxdb.TaskStatusInactive,
								},
								AuthMethod: "none",
								Method:     "GET",
								URL:        "http://example.com",
							},
							rule: &rule.HTTP{
								Base: newRuleBase(13),
							},
						},
					}

					for _, tt := range tests {
						fn := func(t *testing.T) {
							endpointSVC := mock.NewNotificationEndpointService()
							endpointSVC.FindNotificationEndpointByIDF = func(ctx context.Context, id influxdb.ID) (influxdb.NotificationEndpoint, error) {
								if id != tt.endpoint.GetID() {
									return nil, errors.New("uh ohhh, wrong id here: " + id.String())
								}
								return tt.endpoint, nil
							}
							ruleSVC := mock.NewNotificationRuleStore()
							ruleSVC.FindNotificationRuleByIDF = func(ctx context.Context, id influxdb.ID) (influxdb.NotificationRule, error) {
								return tt.rule, nil
							}

							svc := newTestService(
								WithNotificationEndpointSVC(endpointSVC),
								WithNotificationRuleSVC(ruleSVC),
							)

							resToClone := ResourceToClone{
								Kind: KindNotificationRule,
								ID:   tt.rule.GetID(),
								Name: tt.newName,
							}
							pkg, err := svc.CreatePkg(context.TODO(), CreateWithExistingResources(resToClone))
							require.NoError(t, err)

							newPkg := encodeAndDecode(t, pkg)

							sum := newPkg.Summary()
							require.Len(t, sum.NotificationRules, 1)

							actualRule := sum.NotificationRules[0]
							assert.Zero(t, actualRule.ID)
							assert.Zero(t, actualRule.EndpointID)
							assert.Zero(t, actualRule.EndpointType)
							assert.NotEmpty(t, actualRule.EndpointName)

							baseEqual := func(t *testing.T, base rule.Base) {
								t.Helper()
								expectedName := base.Name
								if tt.newName != "" {
									expectedName = tt.newName
								}
								assert.Equal(t, expectedName, actualRule.Name)
								assert.Equal(t, base.Description, actualRule.Description)
								assert.Equal(t, base.Every.TimeDuration().String(), actualRule.Every)
								assert.Equal(t, base.Offset.TimeDuration().String(), actualRule.Offset)

								for _, sRule := range base.StatusRules {
									expected := SummaryStatusRule{CurrentLevel: sRule.CurrentLevel.String()}
									if sRule.PreviousLevel != nil {
										expected.PreviousLevel = sRule.PreviousLevel.String()
									}
									assert.Contains(t, actualRule.StatusRules, expected)
								}
								for _, tRule := range base.TagRules {
									expected := SummaryTagRule{
										Key:      tRule.Key,
										Value:    tRule.Value,
										Operator: tRule.Operator.String(),
									}
									assert.Contains(t, actualRule.TagRules, expected)
								}
							}

							switch p := tt.rule.(type) {
							case *rule.HTTP:
								baseEqual(t, p.Base)
							case *rule.PagerDuty:
								baseEqual(t, p.Base)
								assert.Equal(t, p.MessageTemplate, actualRule.MessageTemplate)
							case *rule.Slack:
								baseEqual(t, p.Base)
								assert.Equal(t, p.MessageTemplate, actualRule.MessageTemplate)
							}

							require.Len(t, pkg.Summary().NotificationEndpoints, 1)

							actualEndpoint := pkg.Summary().NotificationEndpoints[0].NotificationEndpoint
							assert.Equal(t, tt.endpoint.GetName(), actualEndpoint.GetName())
							assert.Equal(t, tt.endpoint.GetDescription(), actualEndpoint.GetDescription())
							assert.Equal(t, tt.endpoint.GetStatus(), actualEndpoint.GetStatus())
						}
						t.Run(tt.name, fn)
					}
				})

				t.Run("handles rules duplicate names", func(t *testing.T) {
					endpointSVC := mock.NewNotificationEndpointService()
					endpointSVC.FindNotificationEndpointByIDF = func(ctx context.Context, id influxdb.ID) (influxdb.NotificationEndpoint, error) {
						return &endpoint.HTTP{
							Base: endpoint.Base{
								ID:          &id,
								Name:        "endpoint_0",
								Description: "desc",
								Status:      influxdb.TaskStatusInactive,
							},
							AuthMethod: "none",
							Method:     "GET",
							URL:        "http://example.com",
						}, nil
					}
					ruleSVC := mock.NewNotificationRuleStore()
					ruleSVC.FindNotificationRuleByIDF = func(ctx context.Context, id influxdb.ID) (influxdb.NotificationRule, error) {
						return &rule.HTTP{
							Base: newRuleBase(int(id)),
						}, nil
					}

					svc := newTestService(
						WithNotificationEndpointSVC(endpointSVC),
						WithNotificationRuleSVC(ruleSVC),
					)

					resourcesToClone := []ResourceToClone{
						{
							Kind: KindNotificationRule,
							ID:   1,
						},
						{
							Kind: KindNotificationRule,
							ID:   2,
						},
					}
					pkg, err := svc.CreatePkg(context.TODO(), CreateWithExistingResources(resourcesToClone...))
					require.NoError(t, err)

					newPkg := encodeAndDecode(t, pkg)

					sum := newPkg.Summary()
					require.Len(t, sum.NotificationRules, len(resourcesToClone))

					expectedSameEndpointName := sum.NotificationRules[0].EndpointName
					assert.NotZero(t, expectedSameEndpointName)
					assert.NotEqual(t, "endpoint_0", expectedSameEndpointName)

					for i := range resourcesToClone {
						actual := sum.NotificationRules[i]
						assert.Equal(t, "old_name", actual.Name)
						assert.Equal(t, "desc", actual.Description)
						assert.Equal(t, expectedSameEndpointName, actual.EndpointName)
					}

					require.Len(t, sum.NotificationEndpoints, 1)
					assert.Equal(t, "endpoint_0", sum.NotificationEndpoints[0].NotificationEndpoint.GetName())
				})
			})

			t.Run("tasks", func(t *testing.T) {
				t.Run("single task exports", func(t *testing.T) {
					tests := []struct {
						name    string
						newName string
						task    influxdb.Task
					}{
						{
							name:    "every offset is set",
							newName: "new name",
							task: influxdb.Task{
								ID:     1,
								Name:   "name_9000",
								Every:  time.Minute.String(),
								Offset: 10 * time.Second,
								Type:   influxdb.TaskSystemType,
								Flux:   `option task = { name: "larry" } from(bucket: "rucket") |> yield()`,
							},
						},
						{
							name: "cron is set",
							task: influxdb.Task{
								ID:   1,
								Name: "name_0",
								Cron: "2 * * * *",
								Type: influxdb.TaskSystemType,
								Flux: `option task = { name: "larry" } from(bucket: "rucket") |> yield()`,
							},
						},
					}

					for _, tt := range tests {
						fn := func(t *testing.T) {
							taskSVC := mock.NewTaskService()
							taskSVC.FindTaskByIDFn = func(ctx context.Context, id influxdb.ID) (*influxdb.Task, error) {
								if id != tt.task.ID {
									return nil, errors.New("wrong id provided: " + id.String())
								}
								return &tt.task, nil
							}

							svc := newTestService(WithTaskSVC(taskSVC))

							resToClone := ResourceToClone{
								Kind: KindTask,
								ID:   tt.task.ID,
								Name: tt.newName,
							}
							pkg, err := svc.CreatePkg(context.TODO(), CreateWithExistingResources(resToClone))
							require.NoError(t, err)

							newPkg := encodeAndDecode(t, pkg)

							sum := newPkg.Summary()

							tasks := sum.Tasks
							require.Len(t, tasks, 1)

							expectedName := tt.task.Name
							if tt.newName != "" {
								expectedName = tt.newName
							}
							actual := tasks[0]
							assert.Equal(t, expectedName, actual.Name)
							assert.Equal(t, tt.task.Cron, actual.Cron)
							assert.Equal(t, tt.task.Description, actual.Description)
							assert.Equal(t, tt.task.Every, actual.Every)
							assert.Equal(t, durToStr(tt.task.Offset), actual.Offset)

							expectedQuery := `from(bucket: "rucket") |> yield()`
							assert.Equal(t, expectedQuery, actual.Query)
						}
						t.Run(tt.name, fn)
					}
				})

				t.Run("handles multiple tasks of same name", func(t *testing.T) {
					taskSVC := mock.NewTaskService()
					taskSVC.FindTaskByIDFn = func(ctx context.Context, id influxdb.ID) (*influxdb.Task, error) {
						return &influxdb.Task{
							ID:          id,
							Type:        influxdb.TaskSystemType,
							Name:        "same name",
							Description: "desc",
							Status:      influxdb.TaskStatusActive,
							Flux:        `from(bucket: "foo")`,
							Every:       "5m0s",
						}, nil
					}

					svc := newTestService(WithTaskSVC(taskSVC))

					resourcesToClone := []ResourceToClone{
						{
							Kind: KindTask,
							ID:   1,
						},
						{
							Kind: KindTask,
							ID:   2,
						},
					}
					pkg, err := svc.CreatePkg(context.TODO(), CreateWithExistingResources(resourcesToClone...))
					require.NoError(t, err)

					newPkg := encodeAndDecode(t, pkg)

					sum := newPkg.Summary()

					tasks := sum.Tasks
					require.Len(t, tasks, len(resourcesToClone))

					for _, actual := range sum.Tasks {
						assert.Equal(t, "same name", actual.Name)
						assert.Equal(t, "desc", actual.Description)
						assert.Equal(t, influxdb.Active, actual.Status)
						assert.Equal(t, `from(bucket: "foo")`, actual.Query)
						assert.Equal(t, "5m0s", actual.Every)
					}
				})
			})

			t.Run("telegraf configs", func(t *testing.T) {
				t.Run("allows for duplicate telegraf names to be exported", func(t *testing.T) {
					teleStore := mock.NewTelegrafConfigStore()
					teleStore.FindTelegrafConfigByIDF = func(ctx context.Context, id influxdb.ID) (*influxdb.TelegrafConfig, error) {
						return &influxdb.TelegrafConfig{
							ID:          id,
							OrgID:       9000,
							Name:        "same name",
							Description: "desc",
							Config:      "some config string",
						}, nil
					}

					svc := newTestService(WithTelegrafSVC(teleStore))

					resourcesToClone := []ResourceToClone{
						{
							Kind: KindTelegraf,
							ID:   1,
						},
						{
							Kind: KindTelegraf,
							ID:   2,
						},
					}
					pkg, err := svc.CreatePkg(context.TODO(), CreateWithExistingResources(resourcesToClone...))
					require.NoError(t, err)

					newPkg := encodeAndDecode(t, pkg)

					sum := newPkg.Summary()

					teles := sum.TelegrafConfigs
					require.Len(t, teles, len(resourcesToClone))

					for i := range resourcesToClone {
						actual := teles[i]
						assert.Equal(t, "same name", actual.TelegrafConfig.Name)
						assert.Equal(t, "desc", actual.TelegrafConfig.Description)
						assert.Equal(t, "some config string", actual.TelegrafConfig.Config)
					}
				})
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

						newPkg := encodeAndDecode(t, pkg)

						newVars := newPkg.Summary().Variables
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

					newPkg := encodeAndDecode(t, pkg)
					sum := newPkg.Summary()

					bkts := sum.Buckets
					require.Len(t, bkts, 1)

					actual := bkts[0]
					expectedName := expected.Name
					assert.Equal(t, expectedName, actual.Name)
					assert.Equal(t, expected.Description, actual.Description)
					assert.Equal(t, expected.RetentionPeriod, actual.RetentionPeriod)
					require.Len(t, actual.LabelAssociations, 1)
					assert.Equal(t, "label_1", actual.LabelAssociations[0].Name)

					labels := sum.Labels
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

					newPkg := encodeAndDecode(t, pkg)
					sum := newPkg.Summary()

					bkts := sum.Buckets
					require.Len(t, bkts, 2)

					for i, actual := range bkts {
						assert.Equal(t, strconv.Itoa((i+1)*10), actual.Name)
						require.Len(t, actual.LabelAssociations, 2)
						assert.Equal(t, "label_1", actual.LabelAssociations[0].Name)
						assert.Equal(t, "label_2", actual.LabelAssociations[1].Name)
					}

					labels := sum.Labels
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

					newPkg := encodeAndDecode(t, pkg)

					labels := newPkg.Summary().Labels
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
					&endpoint.HTTP{
						Base: endpoint.Base{
							ID:   &id,
							Name: "http",
						},
						URL:        "http://example.com",
						Username:   influxdb.SecretField{Key: id.String() + "-username"},
						Password:   influxdb.SecretField{Key: id.String() + "-password"},
						AuthMethod: "basic",
						Method:     "POST",
					},
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

			expectedRule := &rule.HTTP{
				Base: rule.Base{
					ID:          12,
					Name:        "rule_0",
					EndpointID:  2,
					Every:       mustDuration(t, time.Minute),
					StatusRules: []notification.StatusRule{{CurrentLevel: notification.Critical}},
				},
			}
			ruleSVC := mock.NewNotificationRuleStore()
			ruleSVC.FindNotificationRulesF = func(ctx context.Context, f influxdb.NotificationRuleFilter, _ ...influxdb.FindOptions) ([]influxdb.NotificationRule, int, error) {
				out := []influxdb.NotificationRule{expectedRule}
				return out, len(out), nil
			}
			ruleSVC.FindNotificationRuleByIDF = func(ctx context.Context, id influxdb.ID) (influxdb.NotificationRule, error) {
				return expectedRule, nil
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

			taskSVC := mock.NewTaskService()
			taskSVC.FindTasksFn = func(ctx context.Context, f influxdb.TaskFilter) ([]*influxdb.Task, int, error) {
				return []*influxdb.Task{
					{ID: 31, Type: influxdb.TaskSystemType},
					{ID: expectedCheck.TaskID, Type: influxdb.TaskSystemType}, // this one should be ignored in the return
					{ID: expectedRule.TaskID, Type: influxdb.TaskSystemType},  // this one should be ignored in the return as well
					{ID: 99}, // this one should be skipped since it is not a system task
				}, 3, nil
			}
			taskSVC.FindTaskByIDFn = func(ctx context.Context, id influxdb.ID) (*influxdb.Task, error) {
				if id != 31 {
					return nil, errors.New("wrong id: " + id.String())
				}
				return &influxdb.Task{
					ID:     id,
					Name:   "task_0",
					Every:  time.Minute.String(),
					Offset: 10 * time.Second,
					Type:   influxdb.TaskSystemType,
					Flux:   `option task = { name: "larry" } from(bucket: "rucket") |> yield()`,
				}, nil
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
				WithNotificationEndpointSVC(endpointSVC),
				WithNotificationRuleSVC(ruleSVC),
				WithTaskSVC(taskSVC),
				WithVariableSVC(varSVC),
			)

			pkg, err := svc.CreatePkg(
				context.TODO(),
				CreateWithAllOrgResources(CreateByOrgIDOpt{
					OrgID: orgID,
				}),
			)
			require.NoError(t, err)

			summary := pkg.Summary()
			bkts := summary.Buckets
			require.Len(t, bkts, 1)
			assert.Equal(t, "bucket", bkts[0].Name)

			checks := summary.Checks
			require.Len(t, checks, 1)
			assert.Equal(t, expectedCheck.Name, checks[0].Check.GetName())

			dashs := summary.Dashboards
			require.Len(t, dashs, 1)
			assert.Equal(t, "dashboard", dashs[0].Name)

			labels := summary.Labels
			require.Len(t, labels, 1)
			assert.Equal(t, "label", labels[0].Name)

			endpoints := summary.NotificationEndpoints
			require.Len(t, endpoints, 1)
			assert.Equal(t, "http", endpoints[0].NotificationEndpoint.GetName())

			rules := summary.NotificationRules
			require.Len(t, rules, 1)
			assert.Equal(t, expectedRule.Name, rules[0].Name)
			assert.NotEmpty(t, rules[0].EndpointName)

			require.Len(t, summary.Tasks, 1)
			task1 := summary.Tasks[0]
			assert.Equal(t, "task_0", task1.Name)

			vars := summary.Variables
			require.Len(t, vars, 1)
			assert.Equal(t, "variable", vars[0].Name)
		})
	})

	t.Run("InitStack", func(t *testing.T) {
		safeCreateFn := func(ctx context.Context, stack Stack) error {
			return nil
		}

		type createFn func(ctx context.Context, stack Stack) error

		newFakeStore := func(fn createFn) *fakeStore {
			return &fakeStore{
				createFn: fn,
			}
		}

		now := time.Time{}.Add(10 * 24 * time.Hour)

		t.Run("when store call is successful", func(t *testing.T) {
			svc := newTestService(
				WithIDGenerator(newFakeIDGen(3)),
				WithTimeGenerator(newTimeGen(now)),
				WithStore(newFakeStore(safeCreateFn)),
			)

			stack, err := svc.InitStack(context.Background(), 9000, Stack{OrgID: 3333})
			require.NoError(t, err)

			assert.Equal(t, influxdb.ID(3), stack.ID)
			assert.Equal(t, now, stack.CreatedAt)
			assert.Equal(t, now, stack.UpdatedAt)
		})

		t.Run("handles unexpected error paths", func(t *testing.T) {
			tests := []struct {
				name            string
				expectedErrCode string
				store           func() *fakeStore
				orgSVC          func() influxdb.OrganizationService
			}{
				{
					name:            "unexpected store err",
					expectedErrCode: influxdb.EInternal,
					store: func() *fakeStore {
						return newFakeStore(func(ctx context.Context, stack Stack) error {
							return errors.New("unexpected error")
						})
					},
				},
				{
					name:            "unexpected conflict store err",
					expectedErrCode: influxdb.EInternal,
					store: func() *fakeStore {
						return newFakeStore(func(ctx context.Context, stack Stack) error {
							return &influxdb.Error{Code: influxdb.EConflict}
						})
					},
				},
				{
					name:            "org does not exist produces conflict error",
					expectedErrCode: influxdb.EConflict,
					store: func() *fakeStore {
						return newFakeStore(safeCreateFn)
					},
					orgSVC: func() influxdb.OrganizationService {
						orgSVC := mock.NewOrganizationService()
						orgSVC.FindOrganizationByIDF = func(ctx context.Context, id influxdb.ID) (*influxdb.Organization, error) {
							return nil, &influxdb.Error{Code: influxdb.ENotFound}
						}
						return orgSVC
					},
				},
			}

			for _, tt := range tests {
				fn := func(t *testing.T) {
					var orgSVC influxdb.OrganizationService = mock.NewOrganizationService()
					if tt.orgSVC != nil {
						orgSVC = tt.orgSVC()
					}

					svc := newTestService(
						WithIDGenerator(newFakeIDGen(3)),
						WithTimeGenerator(newTimeGen(now)),
						WithStore(tt.store()),
						WithOrganizationService(orgSVC),
					)

					_, err := svc.InitStack(context.Background(), 9000, Stack{OrgID: 3333})
					require.Error(t, err)
					assert.Equal(t, tt.expectedErrCode, influxdb.ErrorCode(err))
				}
				t.Run(tt.name, fn)
			}
		})
	})
}

func newTestIDPtr(i int) *influxdb.ID {
	id := influxdb.ID(i)
	return &id
}

func levelPtr(l notification.CheckLevel) *notification.CheckLevel {
	return &l
}

type fakeStore struct {
	createFn func(ctx context.Context, stack Stack) error
}

var _ Store = (*fakeStore)(nil)

func (s *fakeStore) CreateStack(ctx context.Context, stack Stack) error {
	if s.createFn != nil {
		return s.createFn(ctx, stack)
	}
	panic("not implemented")
}

func (s *fakeStore) ReadStackByID(ctx context.Context, id influxdb.ID) (Stack, error) {
	panic("not implemented")
}

func (s *fakeStore) UpdateStack(ctx context.Context, stack Stack) error {
	panic("not implemented")
}

func (s *fakeStore) DeleteStack(ctx context.Context, id influxdb.ID) error {
	panic("not implemented")
}

type fakeIDGen func() influxdb.ID

func newFakeIDGen(id influxdb.ID) fakeIDGen {
	return func() influxdb.ID {
		return id
	}
}

func (f fakeIDGen) ID() influxdb.ID {
	return f()
}

type fakeTimeGen func() time.Time

func newTimeGen(t time.Time) fakeTimeGen {
	return func() time.Time {
		return t
	}
}

func (t fakeTimeGen) Now() time.Time {
	return t()
}
