package pkger

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	errors2 "github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/notification"
	icheck "github.com/influxdata/influxdb/v2/notification/check"
	"github.com/influxdata/influxdb/v2/notification/endpoint"
	"github.com/influxdata/influxdb/v2/notification/rule"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
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
			store: &fakeStore{
				createFn: func(ctx context.Context, stack Stack) error {
					return nil
				},
				deleteFn: func(ctx context.Context, id platform.ID) error {
					return nil
				},
				readFn: func(ctx context.Context, id platform.ID) (Stack, error) {
					return Stack{ID: id}, nil
				},
				updateFn: func(ctx context.Context, stack Stack) error {
					return nil
				},
			},
			taskSVC: mock.NewTaskService(),
			teleSVC: mock.NewTelegrafConfigStore(),
			varSVC:  mock.NewVariableService(),
		}
		for _, o := range opts {
			o(&opt)
		}

		applyOpts := []ServiceSetterFn{
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
		}
		if opt.idGen != nil {
			applyOpts = append(applyOpts, WithIDGenerator(opt.idGen))
		}
		if opt.timeGen != nil {
			applyOpts = append(applyOpts, WithTimeGenerator(opt.timeGen))
		}
		if opt.nameGen != nil {
			applyOpts = append(applyOpts, withNameGen(opt.nameGen))
		}

		return NewService(applyOpts...)
	}

	t.Run("DryRun", func(t *testing.T) {
		type dryRunTestFields struct {
			path          string
			kinds         []Kind
			skipResources []ActionSkipResource
			assertFn      func(*testing.T, ImpactSummary)
		}

		testDryRunActions := func(t *testing.T, fields dryRunTestFields) {
			t.Helper()

			var skipResOpts []ApplyOptFn
			for _, asr := range fields.skipResources {
				skipResOpts = append(skipResOpts, ApplyWithResourceSkip(asr))
			}

			testfileRunner(t, fields.path, func(t *testing.T, template *Template) {
				t.Helper()

				tests := []struct {
					name      string
					applyOpts []ApplyOptFn
				}{
					{
						name:      "skip resources",
						applyOpts: skipResOpts,
					},
				}

				for _, k := range fields.kinds {
					tests = append(tests, struct {
						name      string
						applyOpts []ApplyOptFn
					}{
						name: "skip kind " + k.String(),
						applyOpts: []ApplyOptFn{
							ApplyWithKindSkip(ActionSkipKind{
								Kind: k,
							}),
						},
					})
				}

				for _, tt := range tests {
					fn := func(t *testing.T) {
						t.Helper()

						svc := newTestService()

						impact, err := svc.DryRun(
							context.TODO(),
							platform.ID(100),
							0,
							append(tt.applyOpts, ApplyWithTemplate(template))...,
						)
						require.NoError(t, err)

						fields.assertFn(t, impact)
					}
					t.Run(tt.name, fn)
				}
			})
		}

		t.Run("buckets", func(t *testing.T) {
			t.Run("single bucket updated", func(t *testing.T) {
				testfileRunner(t, "testdata/bucket.yml", func(t *testing.T, template *Template) {
					fakeBktSVC := mock.NewBucketService()
					fakeBktSVC.FindBucketByNameFn = func(_ context.Context, orgID platform.ID, name string) (*influxdb.Bucket, error) {
						if name != "rucket-11" {
							return nil, errors.New("not found")
						}
						return &influxdb.Bucket{
							ID:              platform.ID(1),
							OrgID:           orgID,
							Name:            name,
							Description:     "old desc",
							RetentionPeriod: 30 * time.Hour,
						}, nil
					}
					svc := newTestService(WithBucketSVC(fakeBktSVC))

					impact, err := svc.DryRun(context.TODO(), platform.ID(100), 0, ApplyWithTemplate(template))
					require.NoError(t, err)

					require.Len(t, impact.Diff.Buckets, 2)

					expected := DiffBucket{
						DiffIdentifier: DiffIdentifier{
							ID:          SafeID(1),
							StateStatus: StateStatusExists,
							MetaName:    "rucket-11",
							Kind:        KindBucket,
						},

						Old: &DiffBucketValues{
							Name:           "rucket-11",
							Description:    "old desc",
							RetentionRules: retentionRules{newRetentionRule(30 * time.Hour)},
						},
						New: DiffBucketValues{
							Name:           "rucket-11",
							Description:    "bucket 1 description",
							RetentionRules: retentionRules{newRetentionRule(time.Hour)},
						},
					}
					assert.Contains(t, impact.Diff.Buckets, expected)
				})
			})

			t.Run("single bucket new", func(t *testing.T) {
				testfileRunner(t, "testdata/bucket.json", func(t *testing.T, template *Template) {
					fakeBktSVC := mock.NewBucketService()
					fakeBktSVC.FindBucketByNameFn = func(_ context.Context, orgID platform.ID, name string) (*influxdb.Bucket, error) {
						return nil, errors.New("not found")
					}
					svc := newTestService(WithBucketSVC(fakeBktSVC))

					impact, err := svc.DryRun(context.TODO(), platform.ID(100), 0, ApplyWithTemplate(template))
					require.NoError(t, err)

					require.Len(t, impact.Diff.Buckets, 2)

					expected := DiffBucket{
						DiffIdentifier: DiffIdentifier{
							MetaName:    "rucket-11",
							StateStatus: StateStatusNew,
							Kind:        KindBucket,
						},
						New: DiffBucketValues{
							Name:           "rucket-11",
							Description:    "bucket 1 description",
							RetentionRules: retentionRules{newRetentionRule(time.Hour)},
						},
					}
					assert.Contains(t, impact.Diff.Buckets, expected)
				})
			})

			t.Run("with actions applied", func(t *testing.T) {
				testDryRunActions(t, dryRunTestFields{
					path:  "testdata/bucket.yml",
					kinds: []Kind{KindBucket},
					skipResources: []ActionSkipResource{
						{
							Kind:     KindBucket,
							MetaName: "rucket-22",
						},
						{
							Kind:     KindBucket,
							MetaName: "rucket-11",
						},
					},
					assertFn: func(t *testing.T, impact ImpactSummary) {
						require.Empty(t, impact.Diff.Buckets)
					},
				})
			})
		})

		t.Run("checks", func(t *testing.T) {
			t.Run("mixed update and creates", func(t *testing.T) {
				testfileRunner(t, "testdata/checks.yml", func(t *testing.T, template *Template) {
					fakeCheckSVC := mock.NewCheckService()
					id := platform.ID(1)
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

					impact, err := svc.DryRun(context.TODO(), platform.ID(100), 0, ApplyWithTemplate(template))
					require.NoError(t, err)

					checks := impact.Diff.Checks
					require.Len(t, checks, 2)
					check0 := checks[0]
					assert.True(t, check0.IsNew())
					assert.Equal(t, "check-0", check0.MetaName)
					assert.Zero(t, check0.ID)
					assert.Nil(t, check0.Old)

					check1 := checks[1]
					assert.False(t, check1.IsNew())
					assert.Equal(t, "check-1", check1.MetaName)
					assert.Equal(t, "display name", check1.New.GetName())
					assert.NotZero(t, check1.ID)
					assert.Equal(t, existing, check1.Old.Check)
				})
			})

			t.Run("with actions applied", func(t *testing.T) {
				testDryRunActions(t, dryRunTestFields{
					path:  "testdata/checks.yml",
					kinds: []Kind{KindCheck, KindCheckDeadman, KindCheckThreshold},
					skipResources: []ActionSkipResource{
						{
							Kind:     KindCheck,
							MetaName: "check-0",
						},
						{
							Kind:     KindCheck,
							MetaName: "check-1",
						},
					},
					assertFn: func(t *testing.T, impact ImpactSummary) {
						require.Empty(t, impact.Diff.Checks)
					},
				})
			})
		})

		t.Run("dashboards", func(t *testing.T) {
			t.Run("with actions applied", func(t *testing.T) {
				testDryRunActions(t, dryRunTestFields{
					path:  "testdata/dashboard.yml",
					kinds: []Kind{KindDashboard},
					skipResources: []ActionSkipResource{
						{
							Kind:     KindDashboard,
							MetaName: "dash-1",
						},
						{
							Kind:     KindDashboard,
							MetaName: "dash-2",
						},
					},
					assertFn: func(t *testing.T, impact ImpactSummary) {
						require.Empty(t, impact.Diff.Dashboards)
					},
				})
			})
		})

		t.Run("labels", func(t *testing.T) {
			t.Run("two labels updated", func(t *testing.T) {
				testfileRunner(t, "testdata/label.json", func(t *testing.T, template *Template) {
					fakeLabelSVC := mock.NewLabelService()
					fakeLabelSVC.FindLabelsFn = func(_ context.Context, filter influxdb.LabelFilter) ([]*influxdb.Label, error) {
						return []*influxdb.Label{
							{
								ID:   platform.ID(1),
								Name: filter.Name,
								Properties: map[string]string{
									"color":       "old color",
									"description": "old description",
								},
							},
						}, nil
					}
					svc := newTestService(WithLabelSVC(fakeLabelSVC))

					impact, err := svc.DryRun(context.TODO(), platform.ID(100), 0, ApplyWithTemplate(template))
					require.NoError(t, err)

					require.Len(t, impact.Diff.Labels, 3)

					expected := DiffLabel{
						DiffIdentifier: DiffIdentifier{
							ID:          SafeID(1),
							StateStatus: StateStatusExists,
							MetaName:    "label-1",
							Kind:        KindLabel,
						},
						Old: &DiffLabelValues{
							Name:        "label-1",
							Color:       "old color",
							Description: "old description",
						},
						New: DiffLabelValues{
							Name:        "label-1",
							Color:       "#FFFFFF",
							Description: "label 1 description",
						},
					}
					assert.Contains(t, impact.Diff.Labels, expected)

					expected.MetaName = "label-2"
					expected.New.Name = "label-2"
					expected.New.Color = "#000000"
					expected.New.Description = "label 2 description"
					expected.Old.Name = "label-2"
					assert.Contains(t, impact.Diff.Labels, expected)
				})
			})

			t.Run("two labels created", func(t *testing.T) {
				testfileRunner(t, "testdata/label.yml", func(t *testing.T, template *Template) {
					fakeLabelSVC := mock.NewLabelService()
					fakeLabelSVC.FindLabelsFn = func(_ context.Context, filter influxdb.LabelFilter) ([]*influxdb.Label, error) {
						return nil, errors.New("no labels found")
					}
					svc := newTestService(WithLabelSVC(fakeLabelSVC))

					impact, err := svc.DryRun(context.TODO(), platform.ID(100), 0, ApplyWithTemplate(template))
					require.NoError(t, err)

					labels := impact.Diff.Labels
					require.Len(t, labels, 3)

					expected := DiffLabel{
						DiffIdentifier: DiffIdentifier{
							MetaName:    "label-1",
							StateStatus: StateStatusNew,
							Kind:        KindLabel,
						},
						New: DiffLabelValues{
							Name:        "label-1",
							Color:       "#FFFFFF",
							Description: "label 1 description",
						},
					}
					assert.Contains(t, labels, expected)

					expected.MetaName = "label-2"
					expected.New.Name = "label-2"
					expected.New.Color = "#000000"
					expected.New.Description = "label 2 description"
					assert.Contains(t, labels, expected)
				})
			})

			t.Run("with actions applied", func(t *testing.T) {
				testDryRunActions(t, dryRunTestFields{
					path:  "testdata/label.yml",
					kinds: []Kind{KindLabel},
					skipResources: []ActionSkipResource{
						{
							Kind:     KindLabel,
							MetaName: "label-1",
						},
						{
							Kind:     KindLabel,
							MetaName: "label-2",
						},
						{
							Kind:     KindLabel,
							MetaName: "label-3",
						},
					},
					assertFn: func(t *testing.T, impact ImpactSummary) {
						require.Empty(t, impact.Diff.Labels)
					},
				})
			})
		})

		t.Run("notification endpoints", func(t *testing.T) {
			t.Run("mixed update and created", func(t *testing.T) {
				testfileRunner(t, "testdata/notification_endpoint.yml", func(t *testing.T, template *Template) {
					fakeEndpointSVC := mock.NewNotificationEndpointService()
					id := platform.ID(1)
					existing := &endpoint.HTTP{
						Base: endpoint.Base{
							ID:          &id,
							Name:        "http-none-auth-notification-endpoint",
							Description: "old desc",
							Status:      taskmodel.TaskStatusInactive,
						},
						Method:     "POST",
						AuthMethod: "none",
						URL:        "https://www.example.com/endpoint/old",
					}
					fakeEndpointSVC.FindNotificationEndpointsF = func(ctx context.Context, f influxdb.NotificationEndpointFilter, opt ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, int, error) {
						return []influxdb.NotificationEndpoint{existing}, 1, nil
					}

					svc := newTestService(WithNotificationEndpointSVC(fakeEndpointSVC))

					impact, err := svc.DryRun(context.TODO(), platform.ID(100), 0, ApplyWithTemplate(template))
					require.NoError(t, err)

					require.Len(t, impact.Diff.NotificationEndpoints, 5)

					var (
						newEndpoints      []DiffNotificationEndpoint
						existingEndpoints []DiffNotificationEndpoint
					)
					for _, e := range impact.Diff.NotificationEndpoints {
						if e.Old != nil {
							existingEndpoints = append(existingEndpoints, e)
							continue
						}
						newEndpoints = append(newEndpoints, e)
					}
					require.Len(t, newEndpoints, 4)
					require.Len(t, existingEndpoints, 1)

					expected := DiffNotificationEndpoint{
						DiffIdentifier: DiffIdentifier{
							ID:          1,
							MetaName:    "http-none-auth-notification-endpoint",
							StateStatus: StateStatusExists,
							Kind:        KindNotificationEndpointHTTP,
						},
						Old: &DiffNotificationEndpointValues{
							NotificationEndpoint: existing,
						},
						New: DiffNotificationEndpointValues{
							NotificationEndpoint: &endpoint.HTTP{
								Base: endpoint.Base{
									ID:          &id,
									Name:        "http-none-auth-notification-endpoint",
									Description: "http none auth desc",
									Status:      taskmodel.TaskStatusActive,
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

			t.Run("with actions applied", func(t *testing.T) {
				testDryRunActions(t, dryRunTestFields{
					path: "testdata/notification_endpoint.yml",
					kinds: []Kind{
						KindNotificationEndpoint,
						KindNotificationEndpointHTTP,
						KindNotificationEndpointPagerDuty,
						KindNotificationEndpointSlack,
					},
					skipResources: []ActionSkipResource{
						{
							Kind:     KindNotificationEndpoint,
							MetaName: "http-none-auth-notification-endpoint",
						},
						{
							Kind:     KindNotificationEndpoint,
							MetaName: "http-bearer-auth-notification-endpoint",
						},
						{
							Kind:     KindNotificationEndpointHTTP,
							MetaName: "http-basic-auth-notification-endpoint",
						},
						{
							Kind:     KindNotificationEndpointSlack,
							MetaName: "slack-notification-endpoint",
						},
						{
							Kind:     KindNotificationEndpointPagerDuty,
							MetaName: "pager-duty-notification-endpoint",
						},
					},
					assertFn: func(t *testing.T, impact ImpactSummary) {
						require.Empty(t, impact.Diff.NotificationEndpoints)
					},
				})
			})
		})

		t.Run("notification rules", func(t *testing.T) {
			t.Run("mixed update and created", func(t *testing.T) {
				testfileRunner(t, "testdata/notification_rule.yml", func(t *testing.T, template *Template) {
					fakeEndpointSVC := mock.NewNotificationEndpointService()
					id := platform.ID(1)
					existing := &endpoint.HTTP{
						Base: endpoint.Base{
							ID: &id,
							// This name here matches the endpoint identified in the template notification rule
							Name:        "endpoint-0",
							Description: "old desc",
							Status:      taskmodel.TaskStatusInactive,
						},
						Method:     "POST",
						AuthMethod: "none",
						URL:        "https://www.example.com/endpoint/old",
					}
					fakeEndpointSVC.FindNotificationEndpointsF = func(ctx context.Context, f influxdb.NotificationEndpointFilter, opt ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, int, error) {
						return []influxdb.NotificationEndpoint{existing}, 1, nil
					}

					svc := newTestService(WithNotificationEndpointSVC(fakeEndpointSVC))

					impact, err := svc.DryRun(context.TODO(), platform.ID(100), 0, ApplyWithTemplate(template))
					require.NoError(t, err)

					require.Len(t, impact.Diff.NotificationRules, 1)

					actual := impact.Diff.NotificationRules[0].New
					assert.Equal(t, "rule_0", actual.Name)
					assert.Equal(t, "desc_0", actual.Description)
					assert.Equal(t, "slack", actual.EndpointType)
					assert.Equal(t, existing.Name, actual.EndpointName)
					assert.Equal(t, SafeID(*existing.ID), actual.EndpointID)
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
			})

			t.Run("with actions applied", func(t *testing.T) {
				testDryRunActions(t, dryRunTestFields{
					path:  "testdata/notification_rule.yml",
					kinds: []Kind{KindNotificationRule},
					skipResources: []ActionSkipResource{
						{
							Kind:     KindNotificationRule,
							MetaName: "rule-uuid",
						},
					},
					assertFn: func(t *testing.T, impact ImpactSummary) {
						require.Empty(t, impact.Diff.NotificationRules)
					},
				})
			})
		})

		t.Run("secrets not returns missing secrets", func(t *testing.T) {
			testfileRunner(t, "testdata/notification_endpoint_secrets.yml", func(t *testing.T, template *Template) {
				fakeSecretSVC := mock.NewSecretService()
				fakeSecretSVC.GetSecretKeysFn = func(ctx context.Context, orgID platform.ID) ([]string, error) {
					return []string{"rando-1", "rando-2"}, nil
				}
				svc := newTestService(WithSecretSVC(fakeSecretSVC))

				impact, err := svc.DryRun(context.TODO(), platform.ID(100), 0, ApplyWithTemplate(template))
				require.NoError(t, err)

				assert.Equal(t, []string{"routing-key"}, impact.Summary.MissingSecrets)
			})
		})

		t.Run("tasks", func(t *testing.T) {
			t.Run("with actions applied", func(t *testing.T) {
				testDryRunActions(t, dryRunTestFields{
					path:  "testdata/tasks.yml",
					kinds: []Kind{KindTask},
					skipResources: []ActionSkipResource{
						{
							Kind:     KindTask,
							MetaName: "task-uuid",
						},
						{
							Kind:     KindTask,
							MetaName: "task-1",
						},
					},
					assertFn: func(t *testing.T, impact ImpactSummary) {
						require.Empty(t, impact.Diff.Tasks)
					},
				})
			})
		})

		t.Run("telegraf configs", func(t *testing.T) {
			t.Run("with actions applied", func(t *testing.T) {
				testDryRunActions(t, dryRunTestFields{
					path:  "testdata/telegraf.yml",
					kinds: []Kind{KindTelegraf},
					skipResources: []ActionSkipResource{
						{
							Kind:     KindTelegraf,
							MetaName: "first-tele-config",
						},
						{
							Kind:     KindTelegraf,
							MetaName: "tele-2",
						},
					},
					assertFn: func(t *testing.T, impact ImpactSummary) {
						require.Empty(t, impact.Diff.Telegrafs)
					},
				})
			})
		})

		t.Run("variables", func(t *testing.T) {
			t.Run("mixed update and created", func(t *testing.T) {
				testfileRunner(t, "testdata/variables.json", func(t *testing.T, template *Template) {
					fakeVarSVC := mock.NewVariableService()
					fakeVarSVC.FindVariablesF = func(_ context.Context, filter influxdb.VariableFilter, opts ...influxdb.FindOptions) ([]*influxdb.Variable, error) {
						return []*influxdb.Variable{
							{
								ID:          platform.ID(1),
								Name:        "var-const-3",
								Description: "old desc",
							},
						}, nil
					}
					svc := newTestService(WithVariableSVC(fakeVarSVC))

					impact, err := svc.DryRun(context.TODO(), platform.ID(100), 0, ApplyWithTemplate(template))
					require.NoError(t, err)

					variables := impact.Diff.Variables
					require.Len(t, variables, 4)

					expected := DiffVariable{
						DiffIdentifier: DiffIdentifier{
							ID:          1,
							MetaName:    "var-const-3",
							StateStatus: StateStatusExists,
							Kind:        KindVariable,
						},
						Old: &DiffVariableValues{
							Name:        "var-const-3",
							Description: "old desc",
						},
						New: DiffVariableValues{
							Name:        "var-const-3",
							Description: "var-const-3 desc",
							Args: &influxdb.VariableArguments{
								Type:   "constant",
								Values: influxdb.VariableConstantValues{"first val"},
							},
						},
					}
					assert.Equal(t, expected, variables[0])

					expected = DiffVariable{
						DiffIdentifier: DiffIdentifier{
							// no ID here since this one would be new
							MetaName:    "var-map-4",
							StateStatus: StateStatusNew,
							Kind:        KindVariable,
						},
						New: DiffVariableValues{
							Name:        "var-map-4",
							Description: "var-map-4 desc",
							Args: &influxdb.VariableArguments{
								Type:   "map",
								Values: influxdb.VariableMapValues{"k1": "v1"},
							},
						},
					}
					assert.Equal(t, expected, variables[1])
				})
			})

			t.Run("with actions applied", func(t *testing.T) {
				testDryRunActions(t, dryRunTestFields{
					path:  "testdata/variables.yml",
					kinds: []Kind{KindVariable},
					skipResources: []ActionSkipResource{
						{
							Kind:     KindVariable,
							MetaName: "var-query-1",
						},
						{
							Kind:     KindVariable,
							MetaName: "var-query-2",
						},
						{
							Kind:     KindVariable,
							MetaName: "var-const-3",
						},
						{
							Kind:     KindVariable,
							MetaName: "var-map-4",
						},
					},
					assertFn: func(t *testing.T, impact ImpactSummary) {
						require.Empty(t, impact.Diff.Variables)
					},
				})
			})
		})
	})

	t.Run("Apply", func(t *testing.T) {
		t.Run("buckets", func(t *testing.T) {
			t.Run("successfully creates template of buckets", func(t *testing.T) {
				testfileRunner(t, "testdata/bucket.yml", func(t *testing.T, template *Template) {
					fakeBktSVC := mock.NewBucketService()
					fakeBktSVC.CreateBucketFn = func(_ context.Context, b *influxdb.Bucket) error {
						b.ID = platform.ID(b.RetentionPeriod)
						return nil
					}
					fakeBktSVC.FindBucketByNameFn = func(_ context.Context, id platform.ID, s string) (*influxdb.Bucket, error) {
						// forces the bucket to be created a new
						return nil, errors.New("an error")
					}
					fakeBktSVC.UpdateBucketFn = func(_ context.Context, id platform.ID, upd influxdb.BucketUpdate) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{ID: id}, nil
					}

					svc := newTestService(WithBucketSVC(fakeBktSVC))

					orgID := platform.ID(9000)

					impact, err := svc.Apply(context.TODO(), orgID, 0, ApplyWithTemplate(template))
					require.NoError(t, err)

					sum := impact.Summary
					require.Len(t, sum.Buckets, 2)

					expected := SummaryBucket{
						SummaryIdentifier: SummaryIdentifier{
							Kind:          KindBucket,
							MetaName:      "rucket-11",
							EnvReferences: []SummaryReference{},
						},
						ID:                SafeID(time.Hour),
						OrgID:             SafeID(orgID),
						Name:              "rucket-11",
						Description:       "bucket 1 description",
						RetentionPeriod:   time.Hour,
						LabelAssociations: []SummaryLabel{},
					}
					assert.Contains(t, sum.Buckets, expected)
				})
			})

			t.Run("will not apply bucket if no changes to be applied", func(t *testing.T) {
				testfileRunner(t, "testdata/bucket.yml", func(t *testing.T, template *Template) {
					orgID := platform.ID(9000)

					fakeBktSVC := mock.NewBucketService()
					fakeBktSVC.FindBucketByNameFn = func(ctx context.Context, oid platform.ID, name string) (*influxdb.Bucket, error) {
						if orgID != oid {
							return nil, errors.New("invalid org id")
						}

						id := platform.ID(3)
						if name == "display name" {
							id = 4
							name = "rucket-22"
						}
						if bkt, ok := template.mBuckets[name]; ok {
							return &influxdb.Bucket{
								ID:              id,
								OrgID:           oid,
								Name:            bkt.Name(),
								Description:     bkt.Description,
								RetentionPeriod: bkt.RetentionRules.RP(),
							}, nil
						}
						return nil, errors.New("not found")
					}
					fakeBktSVC.UpdateBucketFn = func(_ context.Context, id platform.ID, upd influxdb.BucketUpdate) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{ID: id}, nil
					}

					svc := newTestService(WithBucketSVC(fakeBktSVC))

					impact, err := svc.Apply(context.TODO(), orgID, 0, ApplyWithTemplate(template))
					require.NoError(t, err)

					sum := impact.Summary
					require.Len(t, sum.Buckets, 2)

					expected := SummaryBucket{
						SummaryIdentifier: SummaryIdentifier{
							Kind:          KindBucket,
							MetaName:      "rucket-11",
							EnvReferences: []SummaryReference{},
						},
						ID:                SafeID(3),
						OrgID:             SafeID(orgID),
						Name:              "rucket-11",
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
				testfileRunner(t, "testdata/bucket.yml", func(t *testing.T, template *Template) {
					fakeBktSVC := mock.NewBucketService()
					fakeBktSVC.FindBucketByNameFn = func(_ context.Context, id platform.ID, s string) (*influxdb.Bucket, error) {
						// forces the bucket to be created a new
						return nil, errors.New("an error")
					}
					fakeBktSVC.CreateBucketFn = func(_ context.Context, b *influxdb.Bucket) error {
						if fakeBktSVC.CreateBucketCalls.Count() == 1 {
							return errors.New("blowed up ")
						}
						return nil
					}

					svc := newTestService(WithBucketSVC(fakeBktSVC))

					orgID := platform.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, 0, ApplyWithTemplate(template))
					require.Error(t, err)

					assert.GreaterOrEqual(t, fakeBktSVC.DeleteBucketCalls.Count(), 1)
				})
			})
		})

		t.Run("checks", func(t *testing.T) {
			t.Run("successfully creates template of checks", func(t *testing.T) {
				testfileRunner(t, "testdata/checks.yml", func(t *testing.T, template *Template) {
					fakeCheckSVC := mock.NewCheckService()
					fakeCheckSVC.CreateCheckFn = func(ctx context.Context, c influxdb.CheckCreate, id platform.ID) error {
						c.SetID(platform.ID(fakeCheckSVC.CreateCheckCalls.Count() + 1))
						return nil
					}

					svc := newTestService(WithCheckSVC(fakeCheckSVC))

					orgID := platform.ID(9000)

					impact, err := svc.Apply(context.TODO(), orgID, 0, ApplyWithTemplate(template))
					require.NoError(t, err)

					sum := impact.Summary
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

					for _, expectedName := range []string{"check-0", "display name"} {
						containsWithID(t, expectedName)
					}
				})
			})

			t.Run("rolls back all created checks on an error", func(t *testing.T) {
				testfileRunner(t, "testdata/checks.yml", func(t *testing.T, template *Template) {
					fakeCheckSVC := mock.NewCheckService()
					fakeCheckSVC.CreateCheckFn = func(ctx context.Context, c influxdb.CheckCreate, id platform.ID) error {
						c.SetID(platform.ID(fakeCheckSVC.CreateCheckCalls.Count() + 1))
						if fakeCheckSVC.CreateCheckCalls.Count() == 1 {
							return errors.New("hit that kill count")
						}
						return nil
					}

					svc := newTestService(WithCheckSVC(fakeCheckSVC))

					orgID := platform.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, 0, ApplyWithTemplate(template))
					require.Error(t, err)

					assert.GreaterOrEqual(t, fakeCheckSVC.DeleteCheckCalls.Count(), 1)
				})
			})
		})

		t.Run("labels", func(t *testing.T) {
			t.Run("successfully creates template of labels", func(t *testing.T) {
				testfileRunner(t, "testdata/label.json", func(t *testing.T, template *Template) {
					fakeLabelSVC := mock.NewLabelService()
					fakeLabelSVC.CreateLabelFn = func(_ context.Context, l *influxdb.Label) error {
						i, err := strconv.Atoi(l.Name[len(l.Name)-1:])
						if err != nil {
							return nil
						}
						l.ID = platform.ID(i)
						return nil
					}

					svc := newTestService(WithLabelSVC(fakeLabelSVC))

					orgID := platform.ID(9000)

					impact, err := svc.Apply(context.TODO(), orgID, 0, ApplyWithTemplate(template))
					require.NoError(t, err)

					sum := impact.Summary
					require.Len(t, sum.Labels, 3)

					expectedLabel := sumLabelGen("label-1", "label-1", "#FFFFFF", "label 1 description")
					expectedLabel.ID = 1
					expectedLabel.OrgID = SafeID(orgID)
					assert.Contains(t, sum.Labels, expectedLabel)

					expectedLabel = sumLabelGen("label-2", "label-2", "#000000", "label 2 description")
					expectedLabel.ID = 2
					expectedLabel.OrgID = SafeID(orgID)
					assert.Contains(t, sum.Labels, expectedLabel)
				})
			})

			t.Run("rolls back all created labels on an error", func(t *testing.T) {
				testfileRunner(t, "testdata/label", func(t *testing.T, template *Template) {
					fakeLabelSVC := mock.NewLabelService()
					fakeLabelSVC.CreateLabelFn = func(_ context.Context, l *influxdb.Label) error {
						// 3rd/4th label will return the error here, and 2 before should be rolled back
						if fakeLabelSVC.CreateLabelCalls.Count() == 2 {
							return errors.New("blowed up ")
						}
						return nil
					}

					svc := newTestService(WithLabelSVC(fakeLabelSVC))

					orgID := platform.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, 0, ApplyWithTemplate(template))
					require.Error(t, err)

					assert.GreaterOrEqual(t, fakeLabelSVC.DeleteLabelCalls.Count(), 1)
				})
			})

			t.Run("will not apply label if no changes to be applied", func(t *testing.T) {
				testfileRunner(t, "testdata/label.yml", func(t *testing.T, template *Template) {
					orgID := platform.ID(9000)

					stubExisting := func(name string, id platform.ID) *influxdb.Label {
						templateLabel := template.mLabels[name]
						return &influxdb.Label{
							// makes all template changes same as they are on the existing
							ID:    id,
							OrgID: orgID,
							Name:  templateLabel.Name(),
							Properties: map[string]string{
								"color":       templateLabel.Color,
								"description": templateLabel.Description,
							},
						}
					}
					stubExisting("label-1", 1)
					stubExisting("label-3", 3)

					fakeLabelSVC := mock.NewLabelService()
					fakeLabelSVC.FindLabelsFn = func(ctx context.Context, f influxdb.LabelFilter) ([]*influxdb.Label, error) {
						if f.Name != "label-1" && f.Name != "display name" {
							return nil, nil
						}
						id := platform.ID(1)
						name := f.Name
						if f.Name == "display name" {
							id = 3
							name = "label-3"
						}
						return []*influxdb.Label{stubExisting(name, id)}, nil
					}
					fakeLabelSVC.CreateLabelFn = func(_ context.Context, l *influxdb.Label) error {
						if l.Name == "label-2" {
							l.ID = 2
						}
						return nil
					}
					fakeLabelSVC.UpdateLabelFn = func(_ context.Context, id platform.ID, l influxdb.LabelUpdate) (*influxdb.Label, error) {
						if id == platform.ID(3) {
							return nil, errors.New("invalid id provided")
						}
						return &influxdb.Label{ID: id}, nil
					}

					svc := newTestService(WithLabelSVC(fakeLabelSVC))

					impact, err := svc.Apply(context.TODO(), orgID, 0, ApplyWithTemplate(template))
					require.NoError(t, err)

					sum := impact.Summary
					require.Len(t, sum.Labels, 3)

					expectedLabel := sumLabelGen("label-1", "label-1", "#FFFFFF", "label 1 description")
					expectedLabel.ID = 1
					expectedLabel.OrgID = SafeID(orgID)
					assert.Contains(t, sum.Labels, expectedLabel)

					expectedLabel = sumLabelGen("label-2", "label-2", "#000000", "label 2 description")
					expectedLabel.ID = 2
					expectedLabel.OrgID = SafeID(orgID)
					assert.Contains(t, sum.Labels, expectedLabel)

					assert.Equal(t, 1, fakeLabelSVC.CreateLabelCalls.Count()) // only called for second label
				})
			})
		})

		t.Run("dashboards", func(t *testing.T) {
			t.Run("successfully creates a dashboard", func(t *testing.T) {
				testfileRunner(t, "testdata/dashboard.yml", func(t *testing.T, template *Template) {
					fakeDashSVC := mock.NewDashboardService()
					fakeDashSVC.CreateDashboardF = func(_ context.Context, d *influxdb.Dashboard) error {
						d.ID = platform.ID(1)
						return nil
					}
					fakeDashSVC.UpdateDashboardCellViewF = func(ctx context.Context, dID platform.ID, cID platform.ID, upd influxdb.ViewUpdate) (*influxdb.View, error) {
						return &influxdb.View{}, nil
					}

					svc := newTestService(WithDashboardSVC(fakeDashSVC))

					orgID := platform.ID(9000)

					impact, err := svc.Apply(context.TODO(), orgID, 0, ApplyWithTemplate(template))
					require.NoError(t, err)

					sum := impact.Summary

					require.Len(t, sum.Dashboards, 2)
					dash1 := sum.Dashboards[0]
					assert.NotZero(t, dash1.ID)
					assert.NotZero(t, dash1.OrgID)
					assert.Equal(t, "dash-1", dash1.MetaName)
					assert.Equal(t, "display name", dash1.Name)
					require.Len(t, dash1.Charts, 1)

					dash2 := sum.Dashboards[1]
					assert.NotZero(t, dash2.ID)
					assert.Equal(t, "dash-2", dash2.MetaName)
					assert.Equal(t, "dash-2", dash2.Name)
					require.Empty(t, dash2.Charts)
				})
			})

			t.Run("rolls back created dashboard on an error", func(t *testing.T) {
				testfileRunner(t, "testdata/dashboard.yml", func(t *testing.T, template *Template) {
					fakeDashSVC := mock.NewDashboardService()
					fakeDashSVC.CreateDashboardF = func(_ context.Context, d *influxdb.Dashboard) error {
						// error out on second dashboard attempted
						if fakeDashSVC.CreateDashboardCalls.Count() == 1 {
							return errors.New("blowed up ")
						}
						d.ID = platform.ID(1)
						return nil
					}
					deletedDashs := make(map[platform.ID]bool)
					fakeDashSVC.DeleteDashboardF = func(_ context.Context, id platform.ID) error {
						deletedDashs[id] = true
						return nil
					}

					svc := newTestService(WithDashboardSVC(fakeDashSVC))

					orgID := platform.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, 0, ApplyWithTemplate(template))
					require.Error(t, err)

					assert.True(t, deletedDashs[1])
				})
			})
		})

		t.Run("label mapping", func(t *testing.T) {
			testLabelMappingApplyFn := func(t *testing.T, filename string, numExpected int, settersFn func() []ServiceSetterFn) {
				t.Helper()
				testfileRunner(t, filename, func(t *testing.T, template *Template) {
					t.Helper()

					fakeLabelSVC := mock.NewLabelService()
					fakeLabelSVC.CreateLabelFn = func(_ context.Context, l *influxdb.Label) error {
						l.ID = platform.ID(rand.Int())
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

					orgID := platform.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, 0, ApplyWithTemplate(template))
					require.NoError(t, err)

					assert.Equal(t, numExpected, fakeLabelSVC.CreateLabelMappingCalls.Count())
				})
			}

			testLabelMappingRollbackFn := func(t *testing.T, filename string, killCount int, settersFn func() []ServiceSetterFn) {
				t.Helper()
				testfileRunner(t, filename, func(t *testing.T, template *Template) {
					t.Helper()

					fakeLabelSVC := mock.NewLabelService()
					fakeLabelSVC.CreateLabelFn = func(_ context.Context, l *influxdb.Label) error {
						l.ID = platform.ID(fakeLabelSVC.CreateLabelCalls.Count() + 1)
						return nil
					}
					fakeLabelSVC.CreateLabelMappingFn = func(_ context.Context, mapping *influxdb.LabelMapping) error {
						if mapping.ResourceID == 0 {
							return errors.New("did not get a resource ID")
						}
						if mapping.ResourceType == "" {
							return errors.New("did not get a resource type")
						}
						if fakeLabelSVC.CreateLabelMappingCalls.Count() == killCount {
							return errors.New("hit last label")
						}
						return nil
					}
					svc := newTestService(append(settersFn(),
						WithLabelSVC(fakeLabelSVC),
						WithLogger(zaptest.NewLogger(t)),
					)...)

					orgID := platform.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, 0, ApplyWithTemplate(template))
					require.Error(t, err)

					assert.GreaterOrEqual(t, fakeLabelSVC.DeleteLabelMappingCalls.Count(), killCount)
				})
			}

			t.Run("maps buckets with labels", func(t *testing.T) {
				bktOpt := func() []ServiceSetterFn {
					fakeBktSVC := mock.NewBucketService()
					fakeBktSVC.CreateBucketFn = func(_ context.Context, b *influxdb.Bucket) error {
						b.ID = platform.ID(rand.Int())
						return nil
					}
					fakeBktSVC.FindBucketByNameFn = func(_ context.Context, id platform.ID, s string) (*influxdb.Bucket, error) {
						// forces the bucket to be created a new
						return nil, errors.New("an error")
					}
					return []ServiceSetterFn{WithBucketSVC(fakeBktSVC)}
				}

				t.Run("applies successfully", func(t *testing.T) {
					testLabelMappingApplyFn(t, "testdata/bucket_associates_label.yml", 4, bktOpt)
				})

				t.Run("deletes new label mappings on error", func(t *testing.T) {
					testLabelMappingRollbackFn(t, "testdata/bucket_associates_label.yml", 2, bktOpt)
				})
			})

			t.Run("maps checks with labels", func(t *testing.T) {
				opts := func() []ServiceSetterFn {
					fakeCheckSVC := mock.NewCheckService()
					fakeCheckSVC.CreateCheckFn = func(ctx context.Context, c influxdb.CheckCreate, id platform.ID) error {
						c.Check.SetID(platform.ID(rand.Int()))
						return nil
					}
					fakeCheckSVC.FindCheckFn = func(ctx context.Context, f influxdb.CheckFilter) (influxdb.Check, error) {
						return nil, errors.New("check not found")
					}

					return []ServiceSetterFn{WithCheckSVC(fakeCheckSVC)}
				}

				t.Run("applies successfully", func(t *testing.T) {
					testLabelMappingApplyFn(t, "testdata/checks.yml", 2, opts)
				})

				t.Run("deletes new label mappings on error", func(t *testing.T) {
					testLabelMappingRollbackFn(t, "testdata/checks.yml", 1, opts)
				})
			})

			t.Run("maps dashboards with labels", func(t *testing.T) {
				opts := func() []ServiceSetterFn {
					fakeDashSVC := mock.NewDashboardService()
					fakeDashSVC.CreateDashboardF = func(_ context.Context, d *influxdb.Dashboard) error {
						d.ID = platform.ID(rand.Int())
						return nil
					}
					return []ServiceSetterFn{WithDashboardSVC(fakeDashSVC)}
				}

				t.Run("applies successfully", func(t *testing.T) {
					testLabelMappingApplyFn(t, "testdata/dashboard_associates_label.yml", 2, opts)
				})

				t.Run("deletes new label mappings on error", func(t *testing.T) {
					testLabelMappingRollbackFn(t, "testdata/dashboard_associates_label.yml", 1, opts)
				})
			})

			t.Run("maps notification endpoints with labels", func(t *testing.T) {
				opts := func() []ServiceSetterFn {
					fakeEndpointSVC := mock.NewNotificationEndpointService()
					fakeEndpointSVC.CreateNotificationEndpointF = func(ctx context.Context, nr influxdb.NotificationEndpoint, userID platform.ID) error {
						nr.SetID(platform.ID(rand.Int()))
						return nil
					}
					return []ServiceSetterFn{WithNotificationEndpointSVC(fakeEndpointSVC)}
				}

				t.Run("applies successfully", func(t *testing.T) {
					testLabelMappingApplyFn(t, "testdata/notification_endpoint.yml", 5, opts)
				})

				t.Run("deletes new label mappings on error", func(t *testing.T) {
					testLabelMappingRollbackFn(t, "testdata/notification_endpoint.yml", 3, opts)
				})
			})

			t.Run("maps notification rules with labels", func(t *testing.T) {
				opts := func() []ServiceSetterFn {
					fakeRuleStore := mock.NewNotificationRuleStore()
					fakeRuleStore.CreateNotificationRuleF = func(ctx context.Context, nr influxdb.NotificationRuleCreate, userID platform.ID) error {
						nr.SetID(platform.ID(fakeRuleStore.CreateNotificationRuleCalls.Count() + 1))
						return nil
					}
					return []ServiceSetterFn{
						WithNotificationRuleSVC(fakeRuleStore),
					}
				}

				t.Run("applies successfully", func(t *testing.T) {
					testLabelMappingApplyFn(t, "testdata/notification_rule.yml", 2, opts)
				})

				t.Run("deletes new label mappings on error", func(t *testing.T) {
					testLabelMappingRollbackFn(t, "testdata/notification_rule.yml", 1, opts)
				})
			})

			t.Run("maps tasks with labels", func(t *testing.T) {
				opts := func() []ServiceSetterFn {
					fakeTaskSVC := mock.NewTaskService()
					fakeTaskSVC.CreateTaskFn = func(ctx context.Context, tc taskmodel.TaskCreate) (*taskmodel.Task, error) {
						reg := regexp.MustCompile(`name: "(.+)",`)
						names := reg.FindStringSubmatch(tc.Flux)
						if len(names) < 2 {
							return nil, errors.New("bad flux query provided: " + tc.Flux)
						}
						return &taskmodel.Task{
							ID:             platform.ID(rand.Int()),
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
				}

				t.Run("applies successfully", func(t *testing.T) {
					testLabelMappingApplyFn(t, "testdata/tasks.yml", 2, opts)
				})

				t.Run("deletes new label mappings on error", func(t *testing.T) {
					testLabelMappingRollbackFn(t, "testdata/tasks.yml", 1, opts)
				})
			})

			t.Run("maps telegrafs with labels", func(t *testing.T) {
				opts := func() []ServiceSetterFn {
					fakeTeleSVC := mock.NewTelegrafConfigStore()
					fakeTeleSVC.CreateTelegrafConfigF = func(_ context.Context, cfg *influxdb.TelegrafConfig, _ platform.ID) error {
						cfg.ID = platform.ID(rand.Int())
						return nil
					}
					return []ServiceSetterFn{WithTelegrafSVC(fakeTeleSVC)}
				}

				t.Run("applies successfully", func(t *testing.T) {
					testLabelMappingApplyFn(t, "testdata/telegraf.yml", 2, opts)
				})

				t.Run("deletes new label mappings on error", func(t *testing.T) {
					testLabelMappingRollbackFn(t, "testdata/telegraf.yml", 1, opts)
				})
			})

			t.Run("maps variables with labels", func(t *testing.T) {
				opt := func() []ServiceSetterFn {
					fakeVarSVC := mock.NewVariableService()
					fakeVarSVC.CreateVariableF = func(_ context.Context, v *influxdb.Variable) error {
						v.ID = platform.ID(rand.Int())
						return nil
					}
					return []ServiceSetterFn{WithVariableSVC(fakeVarSVC)}
				}

				t.Run("applies successfully", func(t *testing.T) {
					testLabelMappingApplyFn(t, "testdata/variable_associates_label.yml", 1, opt)
				})

				t.Run("deletes new label mappings on error", func(t *testing.T) {
					testLabelMappingRollbackFn(t, "testdata/variable_associates_label.yml", 0, opt)
				})
			})
		})

		t.Run("notification endpoints", func(t *testing.T) {
			t.Run("successfully creates template of endpoints", func(t *testing.T) {
				testfileRunner(t, "testdata/notification_endpoint.yml", func(t *testing.T, template *Template) {
					fakeEndpointSVC := mock.NewNotificationEndpointService()
					fakeEndpointSVC.CreateNotificationEndpointF = func(ctx context.Context, nr influxdb.NotificationEndpoint, userID platform.ID) error {
						nr.SetID(platform.ID(fakeEndpointSVC.CreateNotificationEndpointCalls.Count() + 1))
						return nil
					}

					svc := newTestService(WithNotificationEndpointSVC(fakeEndpointSVC))

					orgID := platform.ID(9000)

					impact, err := svc.Apply(context.TODO(), orgID, 0, ApplyWithTemplate(template))
					require.NoError(t, err)

					sum := impact.Summary
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
						"http-bearer-auth-notification-endpoint",
						"http-none-auth-notification-endpoint",
						"pager duty name",
						"slack name",
					}
					for _, expectedName := range expectedNames {
						containsWithID(t, expectedName)
					}
				})
			})

			t.Run("rolls back all created notifications on an error", func(t *testing.T) {
				testfileRunner(t, "testdata/notification_endpoint.yml", func(t *testing.T, template *Template) {
					fakeEndpointSVC := mock.NewNotificationEndpointService()
					fakeEndpointSVC.CreateNotificationEndpointF = func(ctx context.Context, nr influxdb.NotificationEndpoint, userID platform.ID) error {
						nr.SetID(platform.ID(fakeEndpointSVC.CreateNotificationEndpointCalls.Count() + 1))
						if fakeEndpointSVC.CreateNotificationEndpointCalls.Count() == 3 {
							return errors.New("hit that kill count")
						}
						return nil
					}

					svc := newTestService(WithNotificationEndpointSVC(fakeEndpointSVC))

					orgID := platform.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, 0, ApplyWithTemplate(template))
					require.Error(t, err)

					assert.GreaterOrEqual(t, fakeEndpointSVC.DeleteNotificationEndpointCalls.Count(), 3)
				})
			})
		})

		t.Run("notification rules", func(t *testing.T) {
			t.Run("successfully creates", func(t *testing.T) {
				testfileRunner(t, "testdata/notification_rule.yml", func(t *testing.T, template *Template) {
					fakeEndpointSVC := mock.NewNotificationEndpointService()
					fakeEndpointSVC.CreateNotificationEndpointF = func(ctx context.Context, nr influxdb.NotificationEndpoint, userID platform.ID) error {
						nr.SetID(platform.ID(fakeEndpointSVC.CreateNotificationEndpointCalls.Count() + 1))
						return nil
					}
					fakeRuleStore := mock.NewNotificationRuleStore()
					fakeRuleStore.CreateNotificationRuleF = func(ctx context.Context, nr influxdb.NotificationRuleCreate, userID platform.ID) error {
						nr.SetID(platform.ID(fakeRuleStore.CreateNotificationRuleCalls.Count() + 1))
						return nil
					}

					svc := newTestService(
						WithNotificationEndpointSVC(fakeEndpointSVC),
						WithNotificationRuleSVC(fakeRuleStore),
					)

					orgID := platform.ID(9000)

					impact, err := svc.Apply(context.TODO(), orgID, 0, ApplyWithTemplate(template))
					require.NoError(t, err)

					sum := impact.Summary
					require.Len(t, sum.NotificationRules, 1)
					assert.Equal(t, "rule-uuid", sum.NotificationRules[0].MetaName)
					assert.Equal(t, "rule_0", sum.NotificationRules[0].Name)
					assert.Equal(t, "desc_0", sum.NotificationRules[0].Description)
					assert.Equal(t, SafeID(1), sum.NotificationRules[0].EndpointID)
					assert.Equal(t, "endpoint-0", sum.NotificationRules[0].EndpointMetaName)
					assert.Equal(t, "slack", sum.NotificationRules[0].EndpointType)
				})
			})

			t.Run("rolls back all created notification rules on an error", func(t *testing.T) {
				testfileRunner(t, "testdata/notification_rule.yml", func(t *testing.T, template *Template) {
					fakeRuleStore := mock.NewNotificationRuleStore()
					fakeRuleStore.CreateNotificationRuleF = func(ctx context.Context, nr influxdb.NotificationRuleCreate, userID platform.ID) error {
						nr.SetID(platform.ID(fakeRuleStore.CreateNotificationRuleCalls.Count() + 1))
						return nil
					}
					fakeRuleStore.DeleteNotificationRuleF = func(ctx context.Context, id platform.ID) error {
						if id != 1 {
							return errors.New("wrong id here")
						}
						return nil
					}
					fakeLabelSVC := mock.NewLabelService()
					fakeLabelSVC.CreateLabelFn = func(ctx context.Context, l *influxdb.Label) error {
						l.ID = platform.ID(fakeLabelSVC.CreateLabelCalls.Count() + 1)
						return nil
					}
					fakeLabelSVC.CreateLabelMappingFn = func(ctx context.Context, m *influxdb.LabelMapping) error {
						return errors.New("start the rollack")
					}

					svc := newTestService(
						WithLabelSVC(fakeLabelSVC),
						WithNotificationRuleSVC(fakeRuleStore),
					)

					orgID := platform.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, 0, ApplyWithTemplate(template))
					require.Error(t, err)

					assert.Equal(t, 1, fakeRuleStore.DeleteNotificationRuleCalls.Count())
				})
			})
		})

		t.Run("tasks", func(t *testing.T) {
			t.Run("successfuly creates", func(t *testing.T) {
				testfileRunner(t, "testdata/tasks.yml", func(t *testing.T, template *Template) {
					orgID := platform.ID(9000)

					fakeTaskSVC := mock.NewTaskService()
					fakeTaskSVC.CreateTaskFn = func(ctx context.Context, tc taskmodel.TaskCreate) (*taskmodel.Task, error) {
						reg := regexp.MustCompile(`name: "(.+)",`)
						names := reg.FindStringSubmatch(tc.Flux)
						if len(names) < 2 {
							return nil, errors.New("bad flux query provided: " + tc.Flux)
						}
						return &taskmodel.Task{
							ID:             platform.ID(fakeTaskSVC.CreateTaskCalls.Count() + 1),
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

					impact, err := svc.Apply(context.TODO(), orgID, 0, ApplyWithTemplate(template))
					require.NoError(t, err)

					sum := impact.Summary
					require.Len(t, sum.Tasks, 2)
					assert.NotZero(t, sum.Tasks[0].ID)
					assert.Equal(t, "task-1", sum.Tasks[0].MetaName)
					assert.Equal(t, "task-1", sum.Tasks[0].Name)
					assert.Equal(t, "desc_1", sum.Tasks[0].Description)

					assert.NotZero(t, sum.Tasks[1].ID)
					assert.Equal(t, "task-uuid", sum.Tasks[1].MetaName)
					assert.Equal(t, "task-0", sum.Tasks[1].Name)
					assert.Equal(t, "desc_0", sum.Tasks[1].Description)
				})
			})

			t.Run("rolls back all created tasks on an error", func(t *testing.T) {
				testfileRunner(t, "testdata/tasks.yml", func(t *testing.T, template *Template) {
					fakeTaskSVC := mock.NewTaskService()
					fakeTaskSVC.CreateTaskFn = func(ctx context.Context, tc taskmodel.TaskCreate) (*taskmodel.Task, error) {
						if fakeTaskSVC.CreateTaskCalls.Count() == 1 {
							return nil, errors.New("expected error")
						}
						return &taskmodel.Task{
							ID: platform.ID(fakeTaskSVC.CreateTaskCalls.Count() + 1),
						}, nil
					}

					svc := newTestService(WithTaskSVC(fakeTaskSVC))

					orgID := platform.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, 0, ApplyWithTemplate(template))
					require.Error(t, err)

					assert.Equal(t, 1, fakeTaskSVC.DeleteTaskCalls.Count())
				})
			})
		})

		t.Run("telegrafs", func(t *testing.T) {
			t.Run("successfuly creates", func(t *testing.T) {
				testfileRunner(t, "testdata/telegraf.yml", func(t *testing.T, template *Template) {
					orgID := platform.ID(9000)

					fakeTeleSVC := mock.NewTelegrafConfigStore()
					fakeTeleSVC.CreateTelegrafConfigF = func(_ context.Context, tc *influxdb.TelegrafConfig, userID platform.ID) error {
						tc.ID = 1
						return nil
					}

					svc := newTestService(WithTelegrafSVC(fakeTeleSVC))

					impact, err := svc.Apply(context.TODO(), orgID, 0, ApplyWithTemplate(template))
					require.NoError(t, err)

					sum := impact.Summary
					require.Len(t, sum.TelegrafConfigs, 2)
					assert.Equal(t, "display name", sum.TelegrafConfigs[0].TelegrafConfig.Name)
					assert.Equal(t, "desc", sum.TelegrafConfigs[0].TelegrafConfig.Description)
					assert.Equal(t, "tele-2", sum.TelegrafConfigs[1].TelegrafConfig.Name)
				})
			})

			t.Run("rolls back all created telegrafs on an error", func(t *testing.T) {
				testfileRunner(t, "testdata/telegraf.yml", func(t *testing.T, template *Template) {
					fakeTeleSVC := mock.NewTelegrafConfigStore()
					fakeTeleSVC.CreateTelegrafConfigF = func(_ context.Context, tc *influxdb.TelegrafConfig, userID platform.ID) error {
						t.Log("called")
						if fakeTeleSVC.CreateTelegrafConfigCalls.Count() == 1 {
							return errors.New("limit hit")
						}
						tc.ID = platform.ID(1)
						return nil
					}
					fakeTeleSVC.DeleteTelegrafConfigF = func(_ context.Context, id platform.ID) error {
						if id != 1 {
							return errors.New("wrong id here")
						}
						return nil
					}

					svc := newTestService(WithTelegrafSVC(fakeTeleSVC))

					orgID := platform.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, 0, ApplyWithTemplate(template))
					require.Error(t, err)

					assert.Equal(t, 1, fakeTeleSVC.DeleteTelegrafConfigCalls.Count())
				})
			})
		})

		t.Run("variables", func(t *testing.T) {
			t.Run("successfully creates template of variables", func(t *testing.T) {
				testfileRunner(t, "testdata/variables.yml", func(t *testing.T, template *Template) {
					fakeVarSVC := mock.NewVariableService()
					fakeVarSVC.CreateVariableF = func(_ context.Context, v *influxdb.Variable) error {
						v.ID = platform.ID(fakeVarSVC.CreateVariableCalls.Count() + 1)
						return nil
					}

					svc := newTestService(WithVariableSVC(fakeVarSVC))

					orgID := platform.ID(9000)

					impact, err := svc.Apply(context.TODO(), orgID, 0, ApplyWithTemplate(template))
					require.NoError(t, err)

					sum := impact.Summary
					require.Len(t, sum.Variables, 4)

					actual := sum.Variables[0]
					assert.True(t, actual.ID > 0 && actual.ID < 5)
					assert.Equal(t, SafeID(orgID), actual.OrgID)
					assert.Equal(t, "var-const-3", actual.Name)
					assert.Equal(t, "var-const-3 desc", actual.Description)
					require.NotNil(t, actual.Arguments)
					assert.Equal(t, influxdb.VariableConstantValues{"first val"}, actual.Arguments.Values)

					actual = sum.Variables[2]
					assert.Equal(t, []string{"rucket"}, actual.Selected)

					for _, actual := range sum.Variables {
						assert.Containsf(t, []SafeID{1, 2, 3, 4}, actual.ID, "actual var: %+v", actual)
					}
				})
			})

			t.Run("rolls back all created variables on an error", func(t *testing.T) {
				testfileRunner(t, "testdata/variables.yml", func(t *testing.T, template *Template) {
					fakeVarSVC := mock.NewVariableService()
					fakeVarSVC.CreateVariableF = func(_ context.Context, l *influxdb.Variable) error {
						// 4th variable will return the error here, and 3 before should be rolled back
						if fakeVarSVC.CreateVariableCalls.Count() == 2 {
							return errors.New("blowed up ")
						}
						return nil
					}

					svc := newTestService(WithVariableSVC(fakeVarSVC))

					orgID := platform.ID(9000)

					_, err := svc.Apply(context.TODO(), orgID, 0, ApplyWithTemplate(template))
					require.Error(t, err)

					assert.GreaterOrEqual(t, fakeVarSVC.DeleteVariableCalls.Count(), 1)
				})
			})

			t.Run("will not apply variable if no changes to be applied", func(t *testing.T) {
				testfileRunner(t, "testdata/variables.yml", func(t *testing.T, template *Template) {
					orgID := platform.ID(9000)

					fakeVarSVC := mock.NewVariableService()
					fakeVarSVC.FindVariablesF = func(ctx context.Context, f influxdb.VariableFilter, _ ...influxdb.FindOptions) ([]*influxdb.Variable, error) {
						return []*influxdb.Variable{
							{
								// makes all template changes same as they are on the existing
								ID:             platform.ID(1),
								OrganizationID: orgID,
								Name:           template.mVariables["var-const-3"].Name(),
								Arguments: &influxdb.VariableArguments{
									Type:   "constant",
									Values: influxdb.VariableConstantValues{"first val"},
								},
							},
						}, nil
					}
					fakeVarSVC.CreateVariableF = func(_ context.Context, l *influxdb.Variable) error {
						if l.Name == "var_const" {
							return errors.New("shouldn't get here")
						}
						return nil
					}
					fakeVarSVC.UpdateVariableF = func(_ context.Context, id platform.ID, v *influxdb.VariableUpdate) (*influxdb.Variable, error) {
						if id > platform.ID(1) {
							return nil, errors.New("this id should not be updated")
						}
						return &influxdb.Variable{ID: id}, nil
					}

					svc := newTestService(WithVariableSVC(fakeVarSVC))

					impact, err := svc.Apply(context.TODO(), orgID, 0, ApplyWithTemplate(template))
					require.NoError(t, err)

					sum := impact.Summary
					require.Len(t, sum.Variables, 4)
					expected := sum.Variables[0]
					assert.Equal(t, SafeID(1), expected.ID)
					assert.Equal(t, "var-const-3", expected.Name)

					assert.Equal(t, 3, fakeVarSVC.CreateVariableCalls.Count()) // only called for last 3 labels
				})
			})
		})
	})

	t.Run("Export", func(t *testing.T) {
		newThresholdBase := func(i int) icheck.Base {
			return icheck.Base{
				ID:          platform.ID(i),
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

		sortLabelsByName := func(labels []SummaryLabel) {
			sort.Slice(labels, func(i, j int) bool {
				return labels[i].Name < labels[j].Name
			})
		}

		t.Run("with existing resources", func(t *testing.T) {
			encodeAndDecode := func(t *testing.T, template *Template) *Template {
				t.Helper()

				b, err := template.Encode(EncodingJSON)
				require.NoError(t, err)

				newTemplate, err := Parse(EncodingJSON, FromReader(bytes.NewReader(b)))
				require.NoError(t, err)

				return newTemplate
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
						bktSVC.FindBucketByIDFn = func(_ context.Context, id platform.ID) (*influxdb.Bucket, error) {
							if id != expected.ID {
								return nil, errors.New("uh ohhh, wrong id here: " + id.String())
							}
							return expected, nil
						}
						bktSVC.FindBucketsFn = func(_ context.Context, filter influxdb.BucketFilter, _ ...influxdb.FindOptions) ([]*influxdb.Bucket, int, error) {
							if filter.ID != nil {
								if *filter.ID != expected.ID {
									return nil, 0, errors.New("uh ohhh, wrong id here: " + filter.ID.String())
								}
							} else if filter.Name != nil && *filter.Name != expected.Name {
								return nil, 0, errors.New("uh ohhh, wrong name here: " + *filter.Name)
							}
							return []*influxdb.Bucket{expected}, 1, nil
						}

						svc := newTestService(WithBucketSVC(bktSVC), WithLabelSVC(mock.NewLabelService()))

						resToClone := ResourceToClone{
							Kind: KindBucket,
							ID:   expected.ID,
							Name: tt.newName,
						}
						template, err := svc.Export(context.TODO(), ExportWithExistingResources(resToClone))
						require.NoError(t, err)

						newTemplate := encodeAndDecode(t, template)

						bkts := newTemplate.Summary().Buckets
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

			// todo: bucket names are unique.
			t.Run("bucket by name", func(t *testing.T) {
				knownBuckets := []*influxdb.Bucket{
					{
						ID:              platform.ID(1),
						Name:            "bucket",
						Description:     "desc",
						RetentionPeriod: time.Hour,
					},
					{
						ID:              platform.ID(2),
						Name:            "bucketCopy",
						Description:     "desc",
						RetentionPeriod: time.Hour,
					},
					{
						ID:              platform.ID(3),
						Name:            "bucket3",
						Description:     "desc",
						RetentionPeriod: time.Hour,
					},
				}

				tests := []struct {
					name     string
					findName string
					findID   platform.ID
					expected []*influxdb.Bucket
				}{
					{
						name:     "find bucket with unique name",
						findName: "bucket",
						expected: []*influxdb.Bucket{knownBuckets[0]},
					},
					{
						name:     "find no buckets",
						findName: "fakeBucket",
						expected: nil,
					},
					{
						name:     "find bucket by id",
						findID:   platform.ID(2),
						expected: []*influxdb.Bucket{knownBuckets[1]},
					},
					{
						// todo: verify this is intended behavior (it is in swagger)
						name:     "find by id, set new name",
						findID:   platform.ID(2),
						findName: "renamedBucket",
						expected: []*influxdb.Bucket{knownBuckets[1]},
					},
				}

				for _, tt := range tests {
					fn := func(t *testing.T) {
						bktSVC := mock.NewBucketService()
						bktSVC.FindBucketsFn = func(_ context.Context, filter influxdb.BucketFilter, _ ...influxdb.FindOptions) ([]*influxdb.Bucket, int, error) {
							if filter.ID != nil {
								for i := range knownBuckets {
									if knownBuckets[i].ID == *filter.ID {
										return []*influxdb.Bucket{knownBuckets[i]}, 1, nil
									}
								}
								return nil, 0, errors.New("uh ohhh, wrong id here: " + filter.ID.String())
							} else if filter.Name != nil {
								bkts := []*influxdb.Bucket{}

								for i := range knownBuckets {
									if knownBuckets[i].Name == *filter.Name {
										bkts = append(bkts, knownBuckets[i])
									}
								}

								if lBkts := len(bkts); lBkts > 0 {
									return bkts, lBkts, nil
								}
								return nil, 0, errors.New("uh ohhh, wrong name here: " + *filter.Name)
							}

							return knownBuckets, len(knownBuckets), nil
						}

						resToClone := ResourceToClone{
							Kind: KindBucket,
						}
						if tt.findName != "" {
							resToClone.Name = tt.findName
						}
						if tt.findID != platform.ID(0) {
							resToClone.ID = tt.findID
						}

						svc := newTestService(
							WithBucketSVC(bktSVC),
							WithLabelSVC(mock.NewLabelService()),
						)
						template, err := svc.Export(context.TODO(), ExportWithExistingResources(resToClone))
						if tt.expected == nil {
							require.Error(t, err)
						} else {
							require.NoError(t, err)

							if tt.findName != "" && tt.findID != platform.ID(0) {
								tt.expected[0].Name = tt.findName
							}

							actual := template.Summary().Buckets
							require.Len(t, actual, len(tt.expected))

							for i := range actual {
								// can't verify id's match due to the use of SafeID's
								assert.Equal(t, tt.expected[i].Name, actual[i].Name)
								assert.Equal(t, tt.expected[i].Description, actual[i].Description)
								assert.Equal(t, tt.expected[i].RetentionPeriod, actual[i].RetentionPeriod)
							}

							assert.True(t, encodeAndDecode(t, template) != nil)
						}
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
						id := platform.ID(1)
						tt.expected.SetID(id)

						checkSVC := mock.NewCheckService()
						checkSVC.FindCheckByIDFn = func(ctx context.Context, id platform.ID) (influxdb.Check, error) {
							if id != tt.expected.GetID() {
								return nil, errors.New("uh ohhh, wrong id here: " + id.String())
							}
							return tt.expected, nil
						}
						checkSVC.FindChecksFn = func(_ context.Context, filter influxdb.CheckFilter, _ ...influxdb.FindOptions) ([]influxdb.Check, int, error) {
							if filter.ID != nil {
								if *filter.ID != tt.expected.GetID() {
									return nil, 0, errors.New("uh ohhh, wrong id here: " + filter.ID.String())
								}
							} else if filter.Name != nil && *filter.Name != tt.expected.GetName() {
								return nil, 0, errors.New("uh ohhh, wrong name here: " + *filter.Name)
							}

							return []influxdb.Check{tt.expected}, 1, nil
						}

						svc := newTestService(WithCheckSVC(checkSVC))

						resToClone := ResourceToClone{
							Kind: KindCheck,
							ID:   tt.expected.GetID(),
							Name: tt.newName,
						}
						template, err := svc.Export(context.TODO(), ExportWithExistingResources(resToClone))
						require.NoError(t, err)

						newTemplate := encodeAndDecode(t, template)

						checks := newTemplate.Summary().Checks
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

			t.Run("checks by name", func(t *testing.T) {
				knownChecks := []influxdb.Check{
					&icheck.Threshold{
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
					&icheck.Deadman{
						Base:       newThresholdBase(1),
						TimeSince:  mustDuration(t, time.Hour),
						StaleTime:  mustDuration(t, 5*time.Hour),
						ReportZero: true,
						Level:      notification.Critical,
					},
					&icheck.Deadman{
						Base: icheck.Base{
							ID:          platform.ID(2),
							TaskID:      300,
							Name:        "check_1",
							Description: "desc_2",
							Every:       mustDuration(t, 2*time.Minute),
							Offset:      mustDuration(t, 30*time.Second),
							Query: influxdb.DashboardQuery{
								Text: `from(bucket: "telegraf") |> range(start: -1m) |> filter(fn: (r) => r._field == "usage_user")`,
							},
							StatusMessageTemplate: "Check: ${ r._check_name } is: ${ r._level }",
							Tags: []influxdb.Tag{
								{Key: "key_1", Value: "val_1"},
								{Key: "key_2", Value: "val_2"},
							},
						},
						TimeSince:  mustDuration(t, time.Hour),
						StaleTime:  mustDuration(t, 5*time.Hour),
						ReportZero: true,
						Level:      notification.Critical,
					},
				}

				tests := []struct {
					name     string
					findName string
					findID   platform.ID
					expected []influxdb.Check
				}{
					{
						name:     "find check with unique name",
						findName: "check_0",
						expected: []influxdb.Check{knownChecks[0]},
					},
					{
						name:     "find multiple checks with same name",
						findName: "check_1",
						expected: []influxdb.Check{knownChecks[1], knownChecks[2]},
					},
					{
						name:     "find no checks",
						findName: "fakeCheck",
						expected: nil,
					},
					{
						name:     "find check by id",
						findID:   platform.ID(1),
						expected: []influxdb.Check{knownChecks[1]},
					},
					{
						name:     "find check by id, set new name",
						findID:   platform.ID(1),
						findName: "chex original",
						expected: []influxdb.Check{knownChecks[1]},
					},
				}

				for _, tt := range tests {
					fn := func(t *testing.T) {
						checkSVC := mock.NewCheckService()
						checkSVC.FindChecksFn = func(_ context.Context, filter influxdb.CheckFilter, _ ...influxdb.FindOptions) ([]influxdb.Check, int, error) {
							if filter.ID != nil {
								for i := range knownChecks {
									if knownChecks[i].GetID() == *filter.ID {
										return []influxdb.Check{knownChecks[i]}, 1, nil
									}
								}
								return nil, 0, errors.New("uh ohhh, wrong id here: " + filter.ID.String())
							} else if filter.Name != nil {
								checks := []influxdb.Check{}

								for i := range knownChecks {
									if knownChecks[i].GetName() == *filter.Name {
										checks = append(checks, knownChecks[i])
									}
								}

								if lChecks := len(checks); lChecks > 0 {
									return checks, lChecks, nil
								}

								return nil, 0, errors.New("uh ohhh, wrong name here: " + *filter.Name)
							}

							return knownChecks, len(knownChecks), nil
						}

						resToClone := ResourceToClone{
							Kind: KindCheck,
						}
						if tt.findName != "" {
							resToClone.Name = tt.findName
						}
						if tt.findID != platform.ID(0) {
							resToClone.ID = tt.findID
						}

						svc := newTestService(WithCheckSVC(checkSVC))
						template, err := svc.Export(context.TODO(), ExportWithExistingResources(resToClone))
						if tt.expected == nil {
							require.Error(t, err)
						} else {
							require.NoError(t, err)

							if tt.findName != "" && tt.findID != platform.ID(0) {
								tt.expected[0].SetName(tt.findName)
							}

							actual := template.Summary().Checks
							require.Len(t, actual, len(tt.expected))
							sort.Slice(actual, func(i, j int) bool {
								return actual[i].Check.GetDescription() < actual[j].Check.GetDescription()
							})

							for i := range actual {
								assert.Equal(t, tt.expected[i].GetName(), actual[i].Check.GetName())
								assert.Equal(t, tt.expected[i].GetDescription(), actual[i].Check.GetDescription())
							}

							assert.True(t, encodeAndDecode(t, template) != nil)
						}
					}
					t.Run(tt.name, fn)
				}
			})

			newQuery := func() influxdb.DashboardQuery {
				return influxdb.DashboardQuery{
					Text:     "from(v.bucket) |> count()",
					EditMode: "advanced",
				}
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
							name:    "geo",
							newName: "new name",
							expectedView: influxdb.View{
								ViewContents: influxdb.ViewContents{
									Name: "view name",
								},
								Properties: influxdb.GeoViewProperties{
									Type:    influxdb.ViewPropertyTypeGeo,
									Queries: []influxdb.DashboardQuery{newQuery()},
									Center:  influxdb.Datum{Lat: 10, Lon: -4},
									Zoom:    4,
									GeoLayers: []influxdb.GeoLayer{{
										Type:           "circleMap",
										RadiusField:    "radius",
										IntensityField: "count",
										Radius:         5,
										Blur:           1,
										RadiusDimension: influxdb.Axis{
											Prefix: "$",
											Suffix: "%",
										},
										InterpolateColors: false,
										TrackWidth:        4,
										Speed:             1,
										RandomColors:      false,
										IsClustered:       false,
										ViewColors:        newColors("min", "max"),
									}},
									Note:              "a note",
									ShowNoteWhenEmpty: true,
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
									Type:                       influxdb.ViewPropertyTypeHeatMap,
									Note:                       "a note",
									Queries:                    []influxdb.DashboardQuery{newQuery()},
									ShowNoteWhenEmpty:          true,
									ViewColors:                 []string{"#8F8AF4", "#8F8AF4", "#8F8AF4"},
									XColumn:                    "x",
									GenerateXAxisTicks:         []string{"xTotalTicks", "xTickStart", "xTickStep"},
									XTotalTicks:                15,
									XTickStart:                 0,
									XTickStep:                  1000,
									YColumn:                    "y",
									GenerateYAxisTicks:         []string{"yTotalTicks", "yTickStart", "yTickStep"},
									YTotalTicks:                10,
									YTickStart:                 0,
									YTickStep:                  100,
									XDomain:                    []float64{0, 10},
									YDomain:                    []float64{0, 100},
									XAxisLabel:                 "x_label",
									XPrefix:                    "x_prefix",
									XSuffix:                    "x_suffix",
									YAxisLabel:                 "y_label",
									YPrefix:                    "y_prefix",
									YSuffix:                    "y_suffix",
									BinSize:                    10,
									TimeFormat:                 "",
									LegendColorizeRows:         true,
									LegendHide:                 false,
									LegendOpacity:              1.0,
									LegendOrientationThreshold: 5,
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
									Type:                       influxdb.ViewPropertyTypeHistogram,
									Note:                       "a note",
									Queries:                    []influxdb.DashboardQuery{newQuery()},
									ShowNoteWhenEmpty:          true,
									ViewColors:                 []influxdb.ViewColor{{Type: "scale", Hex: "#8F8AF4", Value: 0}, {Type: "scale", Hex: "#8F8AF4", Value: 0}, {Type: "scale", Hex: "#8F8AF4", Value: 0}},
									FillColumns:                []string{"a", "b"},
									XColumn:                    "_value",
									XDomain:                    []float64{0, 10},
									XAxisLabel:                 "x_label",
									BinCount:                   30,
									Position:                   "stacked",
									LegendColorizeRows:         true,
									LegendHide:                 false,
									LegendOpacity:              1.0,
									LegendOrientationThreshold: 5,
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
									Type:                       influxdb.ViewPropertyTypeScatter,
									Note:                       "a note",
									Queries:                    []influxdb.DashboardQuery{newQuery()},
									ShowNoteWhenEmpty:          true,
									ViewColors:                 []string{"#8F8AF4", "#8F8AF4", "#8F8AF4"},
									XColumn:                    "x",
									GenerateXAxisTicks:         []string{"xTotalTicks", "xTickStart", "xTickStep"},
									XTotalTicks:                15,
									XTickStart:                 0,
									XTickStep:                  1000,
									YColumn:                    "y",
									GenerateYAxisTicks:         []string{"yTotalTicks", "yTickStart", "yTickStep"},
									YTotalTicks:                10,
									YTickStart:                 0,
									YTickStep:                  100,
									XDomain:                    []float64{0, 10},
									YDomain:                    []float64{0, 100},
									XAxisLabel:                 "x_label",
									XPrefix:                    "x_prefix",
									XSuffix:                    "x_suffix",
									YAxisLabel:                 "y_label",
									YPrefix:                    "y_prefix",
									YSuffix:                    "y_suffix",
									TimeFormat:                 "",
									LegendColorizeRows:         true,
									LegendHide:                 false,
									LegendOpacity:              1.0,
									LegendOrientationThreshold: 5,
								},
							},
						},
						{
							name: "mosaic",
							expectedView: influxdb.View{
								ViewContents: influxdb.ViewContents{
									Name: "view name",
								},
								Properties: influxdb.MosaicViewProperties{
									Type:                       influxdb.ViewPropertyTypeMosaic,
									Note:                       "a note",
									Queries:                    []influxdb.DashboardQuery{newQuery()},
									ShowNoteWhenEmpty:          true,
									ViewColors:                 []string{"#8F8AF4", "#8F8AF4", "#8F8AF4"},
									HoverDimension:             "y",
									XColumn:                    "x",
									GenerateXAxisTicks:         []string{"xTotalTicks", "xTickStart", "xTickStep"},
									XTotalTicks:                15,
									XTickStart:                 0,
									XTickStep:                  1000,
									YLabelColumnSeparator:      ",",
									YLabelColumns:              []string{"foo"},
									YSeriesColumns:             []string{"y"},
									XDomain:                    []float64{0, 10},
									YDomain:                    []float64{0, 100},
									XAxisLabel:                 "x_label",
									XPrefix:                    "x_prefix",
									XSuffix:                    "x_suffix",
									YAxisLabel:                 "y_label",
									YPrefix:                    "y_prefix",
									YSuffix:                    "y_suffix",
									LegendColorizeRows:         true,
									LegendHide:                 false,
									LegendOpacity:              1.0,
									LegendOrientationThreshold: 5,
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
									Type:                       influxdb.ViewPropertyTypeSingleStatPlusLine,
									Axes:                       newAxes(),
									DecimalPlaces:              influxdb.DecimalPlaces{IsEnforced: true, Digits: 1},
									StaticLegend:               influxdb.StaticLegend{ColorizeRows: true, HeightRatio: 0.2, Hide: false, Opacity: 1.0, OrientationThreshold: 5, ValueAxis: "y", WidthRatio: 1.0},
									Note:                       "a note",
									Prefix:                     "pre",
									Suffix:                     "suf",
									Queries:                    []influxdb.DashboardQuery{newQuery()},
									ShadeBelow:                 true,
									HoverDimension:             "y",
									ShowNoteWhenEmpty:          true,
									ViewColors:                 []influxdb.ViewColor{{Type: "text", Hex: "red"}},
									XColumn:                    "x",
									GenerateXAxisTicks:         []string{"xTotalTicks", "xTickStart", "xTickStep"},
									XTotalTicks:                15,
									XTickStart:                 0,
									XTickStep:                  1000,
									YColumn:                    "y",
									GenerateYAxisTicks:         []string{"yTotalTicks", "yTickStart", "yTickStep"},
									YTotalTicks:                10,
									YTickStart:                 0,
									YTickStep:                  100,
									Position:                   "stacked",
									LegendColorizeRows:         true,
									LegendHide:                 false,
									LegendOpacity:              1.0,
									LegendOrientationThreshold: 5,
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
									Type:                       influxdb.ViewPropertyTypeXY,
									Axes:                       newAxes(),
									Geom:                       "step",
									StaticLegend:               influxdb.StaticLegend{ColorizeRows: true, HeightRatio: 0.2, Hide: false, Opacity: 1.0, OrientationThreshold: 5, ValueAxis: "y", WidthRatio: 1.0},
									Note:                       "a note",
									Queries:                    []influxdb.DashboardQuery{newQuery()},
									ShadeBelow:                 true,
									HoverDimension:             "y",
									ShowNoteWhenEmpty:          true,
									ViewColors:                 []influxdb.ViewColor{{Type: "text", Hex: "red"}},
									XColumn:                    "x",
									GenerateXAxisTicks:         []string{"xTotalTicks", "xTickStart", "xTickStep"},
									XTotalTicks:                15,
									XTickStart:                 0,
									XTickStep:                  1000,
									YColumn:                    "y",
									GenerateYAxisTicks:         []string{"yTotalTicks", "yTickStart", "yTickStep"},
									YTotalTicks:                10,
									YTickStart:                 0,
									YTickStep:                  100,
									Position:                   "overlaid",
									TimeFormat:                 "",
									LegendColorizeRows:         true,
									LegendHide:                 false,
									LegendOpacity:              1.0,
									LegendOrientationThreshold: 5,
								},
							},
						},
						{
							name:    "band",
							newName: "new name",
							expectedView: influxdb.View{
								ViewContents: influxdb.ViewContents{
									Name: "view name",
								},
								Properties: influxdb.BandViewProperties{
									Type:                       influxdb.ViewPropertyTypeBand,
									Axes:                       newAxes(),
									Geom:                       "step",
									StaticLegend:               influxdb.StaticLegend{ColorizeRows: true, HeightRatio: 0.2, Hide: false, Opacity: 1.0, OrientationThreshold: 5, ValueAxis: "y", WidthRatio: 1.0},
									Note:                       "a note",
									Queries:                    []influxdb.DashboardQuery{newQuery()},
									HoverDimension:             "y",
									ShowNoteWhenEmpty:          true,
									ViewColors:                 []influxdb.ViewColor{{Type: "text", Hex: "red"}},
									XColumn:                    "x",
									GenerateXAxisTicks:         []string{"xTotalTicks", "xTickStart", "xTickStep"},
									XTotalTicks:                15,
									XTickStart:                 0,
									XTickStep:                  1000,
									YColumn:                    "y",
									GenerateYAxisTicks:         []string{"yTotalTicks", "yTickStart", "yTickStep"},
									YTotalTicks:                10,
									YTickStart:                 0,
									YTickStep:                  100,
									UpperColumn:                "upper",
									MainColumn:                 "main",
									LowerColumn:                "lower",
									TimeFormat:                 "",
									LegendColorizeRows:         true,
									LegendHide:                 false,
									LegendOpacity:              1.0,
									LegendOrientationThreshold: 5,
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
						{
							// validate implementation resolves: https://github.com/influxdata/influxdb/issues/17708
							name:    "table converts table options correctly",
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
										Wrapping: "truncate",
									},
									FieldOptions: []influxdb.RenamableField{
										{
											InternalName: "_time",
											DisplayName:  "time (ms)",
											Visible:      true,
										},
										{
											InternalName: "_value",
											DisplayName:  "bytes",
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
							dashSVC.FindDashboardByIDF = func(_ context.Context, id platform.ID) (*influxdb.Dashboard, error) {
								if id != expected.ID {
									return nil, errors.New("uh ohhh, wrong id here: " + id.String())
								}
								return expected, nil
							}
							dashSVC.FindDashboardsF = func(_ context.Context, filter influxdb.DashboardFilter, _ influxdb.FindOptions) ([]*influxdb.Dashboard, int, error) {
								if len(filter.IDs) < 1 {
									return nil, 0, errors.New("uh ohhh, no id here")
								}
								if filter.IDs[0] != nil && *filter.IDs[0] != expected.ID {
									return nil, 0, errors.New("uh ohhh, wrong id here: " + filter.IDs[0].String())
								}
								return []*influxdb.Dashboard{expected}, 1, nil
							}
							dashSVC.GetDashboardCellViewF = func(_ context.Context, id platform.ID, cID platform.ID) (*influxdb.View, error) {
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
							template, err := svc.Export(context.TODO(), ExportWithExistingResources(resToClone))
							require.NoError(t, err)

							newTemplate := encodeAndDecode(t, template)

							dashs := newTemplate.Summary().Dashboards
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
					dashSVC.FindDashboardByIDF = func(_ context.Context, id platform.ID) (*influxdb.Dashboard, error) {
						return &influxdb.Dashboard{
							ID:          id,
							Name:        "dash name",
							Description: "desc",
						}, nil
					}
					dashSVC.FindDashboardsF = func(_ context.Context, filter influxdb.DashboardFilter, _ influxdb.FindOptions) ([]*influxdb.Dashboard, int, error) {
						if len(filter.IDs) < 1 {
							return nil, 0, errors.New("uh ohhh, no id here")
						}
						return []*influxdb.Dashboard{{
							ID:          *filter.IDs[0],
							Name:        "dash name",
							Description: "desc",
						}}, 1, nil
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
					template, err := svc.Export(context.TODO(), ExportWithExistingResources(resourcesToClone...))
					require.NoError(t, err)

					newTemplate := encodeAndDecode(t, template)

					dashs := newTemplate.Summary().Dashboards
					require.Len(t, dashs, len(resourcesToClone))

					for i := range resourcesToClone {
						actual := dashs[i]
						assert.Equal(t, "dash name", actual.Name)
						assert.Equal(t, "desc", actual.Description)
					}
				})
			})

			t.Run("dashboard by name", func(t *testing.T) {
				id := 0
				newDash := func(name string, view influxdb.View) *influxdb.Dashboard {
					id++
					return &influxdb.Dashboard{
						ID:          platform.ID(id),
						Name:        name,
						Description: fmt.Sprintf("desc_%d", id),
						Cells: []*influxdb.Cell{
							{
								ID:           0,
								CellProperty: influxdb.CellProperty{X: 1, Y: 2, W: 3, H: 4},
								View:         &view,
							},
						},
					}
				}
				knownDashboards := []*influxdb.Dashboard{
					newDash("dasher", influxdb.View{
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
					}),
					newDash("prancer", influxdb.View{
						ViewContents: influxdb.ViewContents{
							Name: "view name",
						},
						Properties: influxdb.HeatmapViewProperties{
							Type:               influxdb.ViewPropertyTypeHeatMap,
							Note:               "a note",
							Queries:            []influxdb.DashboardQuery{newQuery()},
							ShowNoteWhenEmpty:  true,
							ViewColors:         []string{"#8F8AF4", "#8F8AF4", "#8F8AF4"},
							XColumn:            "x",
							GenerateXAxisTicks: []string{"xTotalTicks", "xTickStart", "xTickStep"},
							XTotalTicks:        15,
							XTickStart:         0,
							XTickStep:          1000,
							YColumn:            "y",
							GenerateYAxisTicks: []string{"yTotalTicks", "yTickStart", "yTickStep"},
							YTotalTicks:        10,
							YTickStart:         0,
							YTickStep:          100,
							XDomain:            []float64{0, 10},
							YDomain:            []float64{0, 100},
							XAxisLabel:         "x_label",
							XPrefix:            "x_prefix",
							XSuffix:            "x_suffix",
							YAxisLabel:         "y_label",
							YPrefix:            "y_prefix",
							YSuffix:            "y_suffix",
							BinSize:            10,
							TimeFormat:         "",
						},
					}),
					newDash("prancer", influxdb.View{
						ViewContents: influxdb.ViewContents{
							Name: "view name",
						},
						Properties: influxdb.HistogramViewProperties{
							Type:                       influxdb.ViewPropertyTypeHistogram,
							Note:                       "a note",
							Queries:                    []influxdb.DashboardQuery{newQuery()},
							ShowNoteWhenEmpty:          true,
							ViewColors:                 []influxdb.ViewColor{{Type: "scale", Hex: "#8F8AF4", Value: 0}, {Type: "scale", Hex: "#8F8AF4", Value: 0}, {Type: "scale", Hex: "#8F8AF4", Value: 0}},
							FillColumns:                []string{"a", "b"},
							XColumn:                    "_value",
							XDomain:                    []float64{0, 10},
							XAxisLabel:                 "x_label",
							BinCount:                   30,
							Position:                   "stacked",
							LegendColorizeRows:         true,
							LegendHide:                 false,
							LegendOpacity:              1.0,
							LegendOrientationThreshold: 5,
						},
					}),
				}

				tests := []struct {
					name     string
					findName string
					findID   platform.ID
					expected []*influxdb.Dashboard
				}{
					{
						name:     "find dash with unique name",
						findName: "dasher",
						expected: []*influxdb.Dashboard{knownDashboards[0]},
					},
					{
						name:     "find multiple dash with shared name",
						findName: "prancer",
						expected: []*influxdb.Dashboard{knownDashboards[1], knownDashboards[2]},
					},
					{
						name:     "find no dash",
						findName: "fakeDash",
						expected: nil,
					},
					{
						name:     "find dash by id",
						findID:   1,
						expected: []*influxdb.Dashboard{knownDashboards[0]},
					},
					{
						name:     "find dash by id, set new name",
						findID:   1,
						findName: "dancer",
						expected: []*influxdb.Dashboard{knownDashboards[0]},
					},
				}

				for _, tt := range tests {
					fn := func(t *testing.T) {
						dashSVC := mock.NewDashboardService()
						dashSVC.FindDashboardsF = func(_ context.Context, filter influxdb.DashboardFilter, _ influxdb.FindOptions) ([]*influxdb.Dashboard, int, error) {
							if filter.IDs != nil && filter.IDs[0] != nil {
								for i := range knownDashboards {
									if knownDashboards[i].ID == *filter.IDs[0] {
										return []*influxdb.Dashboard{knownDashboards[i]}, 1, nil
									}
								}

								return nil, 0, errors.New("uh ohhh, wrong id here: " + filter.IDs[0].String())
							}

							return knownDashboards, len(knownDashboards), nil
						}

						dashSVC.GetDashboardCellViewF = func(_ context.Context, id platform.ID, cID platform.ID) (*influxdb.View, error) {
							for i := range knownDashboards {
								if knownDashboards[i].ID == id {
									return knownDashboards[i].Cells[0].View, nil
								}
							}

							return nil, errors.New("wrongo ids")
						}

						resToClone := ResourceToClone{
							Kind: KindDashboard,
						}
						if tt.findName != "" {
							resToClone.Name = tt.findName
						}
						if tt.findID != platform.ID(0) {
							resToClone.ID = tt.findID
						}

						svc := newTestService(
							WithDashboardSVC(dashSVC),
							WithLabelSVC(mock.NewLabelService()),
						)
						template, err := svc.Export(context.TODO(), ExportWithExistingResources(resToClone))
						if tt.expected == nil {
							require.Error(t, err)
						} else {
							require.NoError(t, err)

							if tt.findName != "" && tt.findID != platform.ID(0) {
								tt.expected[0].Name = tt.findName
							}

							actual := template.Summary().Dashboards
							require.Len(t, actual, len(tt.expected))
							sort.Slice(actual, func(i, j int) bool {
								return actual[i].Description < actual[j].Description
							})

							for i := range actual {
								assert.Equal(t, tt.expected[i].Name, actual[i].Name)
								assert.Equal(t, tt.expected[i].Description, actual[i].Description)

								require.Len(t, actual[i].Charts, 1)
								ch := actual[i].Charts[0]
								assert.Equal(t, int(tt.expected[i].Cells[0].CellProperty.X), ch.XPosition)
								assert.Equal(t, int(tt.expected[i].Cells[0].CellProperty.Y), ch.YPosition)
								assert.Equal(t, int(tt.expected[i].Cells[0].CellProperty.H), ch.Height)
								assert.Equal(t, int(tt.expected[i].Cells[0].CellProperty.W), ch.Width)
								assert.Equal(t, tt.expected[i].Cells[0].View.Properties, ch.Properties)
							}

							assert.True(t, encodeAndDecode(t, template) != nil)
						}
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
						labelSVC.FindLabelByIDFn = func(_ context.Context, id platform.ID) (*influxdb.Label, error) {
							if id != expectedLabel.ID {
								return nil, errors.New("uh ohhh, wrong id here: " + id.String())
							}
							return expectedLabel, nil
						}
						labelSVC.FindLabelsFn = func(_ context.Context, filter influxdb.LabelFilter) ([]*influxdb.Label, error) {
							if filter.Name != expectedLabel.Name {
								return nil, errors.New("uh ohhh, wrong name here: " + filter.Name)
							}
							return []*influxdb.Label{expectedLabel}, nil
						}

						svc := newTestService(WithLabelSVC(labelSVC))

						resToClone := ResourceToClone{
							Kind: KindLabel,
							ID:   expectedLabel.ID,
							Name: tt.newName,
						}
						template, err := svc.Export(context.TODO(), ExportWithExistingResources(resToClone))
						require.NoError(t, err)

						newTemplate := encodeAndDecode(t, template)

						newLabels := newTemplate.Summary().Labels
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

			t.Run("label by name", func(t *testing.T) {
				knownLabels := []*influxdb.Label{
					{
						ID:   1,
						Name: "label one",
						Properties: map[string]string{
							"description": "desc",
							"color":       "red",
						},
					},
					{
						ID:   2,
						Name: "label two",
						Properties: map[string]string{
							"description": "desc2",
							"color":       "green",
						},
					},
				}
				tests := []struct {
					name     string
					findName string
					findID   platform.ID
					expected []*influxdb.Label
				}{
					{
						name:     "find label by name",
						findName: "label one",
						expected: []*influxdb.Label{knownLabels[0]},
					},
					{
						name:     "find no label",
						findName: "label none",
						expected: nil,
					},
					{
						name:     "find label by id",
						findID:   platform.ID(2),
						expected: []*influxdb.Label{knownLabels[1]},
					},
					{
						name:     "find label by id, set new name",
						findName: "label three",
						findID:   platform.ID(2),
						expected: []*influxdb.Label{knownLabels[1]},
					},
				}

				for _, tt := range tests {
					fn := func(t *testing.T) {
						labelSVC := mock.NewLabelService()
						labelSVC.FindLabelByIDFn = func(_ context.Context, id platform.ID) (*influxdb.Label, error) {
							for i := range knownLabels {
								if knownLabels[i].ID == id {
									return knownLabels[i], nil
								}
							}

							return nil, errors.New("uh ohhh, wrong id here: " + id.String())
						}
						labelSVC.FindLabelsFn = func(_ context.Context, filter influxdb.LabelFilter) ([]*influxdb.Label, error) {
							if filter.Name != "" {
								for i := range knownLabels {
									if knownLabels[i].Name == filter.Name {
										return []*influxdb.Label{knownLabels[i]}, nil
									}
								}

								return nil, errors.New("uh ohhh, wrong name here: " + filter.Name)
							}

							return knownLabels, nil
						}

						resToClone := ResourceToClone{
							Kind: KindLabel,
						}
						if tt.findName != "" {
							resToClone.Name = tt.findName
						}
						if tt.findID != platform.ID(0) {
							resToClone.ID = tt.findID
						}

						svc := newTestService(WithLabelSVC(labelSVC))
						template, err := svc.Export(context.TODO(), ExportWithExistingResources(resToClone))
						if tt.expected == nil {
							require.Error(t, err)
						} else {
							require.NoError(t, err)

							if tt.findName != "" && tt.findID != platform.ID(0) {
								tt.expected[0].Name = tt.findName
							}

							actual := template.Summary().Labels
							require.Len(t, actual, len(tt.expected))

							for i := range actual {
								assert.Equal(t, tt.expected[i].Name, actual[i].Name)
								assert.Equal(t, tt.expected[i].Properties["color"], actual[i].Properties.Color)
								assert.Equal(t, tt.expected[i].Properties["description"], actual[i].Properties.Description)
							}

							assert.True(t, encodeAndDecode(t, template) != nil)
						}
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
								Status:      taskmodel.TaskStatusActive,
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
								Status:      taskmodel.TaskStatusActive,
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
								Status:      taskmodel.TaskStatusInactive,
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
								Status:      taskmodel.TaskStatusInactive,
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
								Status:      taskmodel.TaskStatusInactive,
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
								Status:      taskmodel.TaskStatusInactive,
							},
							AuthMethod: "none",
							Method:     "GET",
							URL:        "http://example.com",
						},
					},
				}

				for _, tt := range tests {
					fn := func(t *testing.T) {
						id := platform.ID(1)
						tt.expected.SetID(id)

						endpointSVC := mock.NewNotificationEndpointService()
						endpointSVC.FindNotificationEndpointByIDF = func(ctx context.Context, id platform.ID) (influxdb.NotificationEndpoint, error) {
							if id != tt.expected.GetID() {
								return nil, errors.New("uh ohhh, wrong id here: " + id.String())
							}
							return tt.expected, nil
						}
						endpointSVC.FindNotificationEndpointsF = func(ctx context.Context, filter influxdb.NotificationEndpointFilter, _ ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, int, error) {
							if filter.ID != nil && *filter.ID != tt.expected.GetID() {
								return nil, 0, errors.New("uh ohhh, wrong id here: " + filter.ID.String())
							}
							return []influxdb.NotificationEndpoint{tt.expected}, 1, nil
						}

						svc := newTestService(WithNotificationEndpointSVC(endpointSVC))

						resToClone := ResourceToClone{
							Kind: KindNotificationEndpoint,
							ID:   tt.expected.GetID(),
							Name: tt.newName,
						}
						template, err := svc.Export(context.TODO(), ExportWithExistingResources(resToClone))
						require.NoError(t, err)

						newTemplate := encodeAndDecode(t, template)

						endpoints := newTemplate.Summary().NotificationEndpoints
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

			knownEndpoints := []influxdb.NotificationEndpoint{
				&endpoint.PagerDuty{
					Base: endpoint.Base{
						ID:          newTestIDPtr(1),
						Name:        "pd endpoint",
						Description: "desc",
						Status:      taskmodel.TaskStatusActive,
					},
					ClientURL:  "http://example.com",
					RoutingKey: influxdb.SecretField{Key: "-routing-key"},
				},
				&endpoint.PagerDuty{
					Base: endpoint.Base{
						ID:          newTestIDPtr(2),
						Name:        "pd-endpoint",
						Description: "desc pd",
						Status:      taskmodel.TaskStatusActive,
					},
					ClientURL:  "http://example.com",
					RoutingKey: influxdb.SecretField{Key: "-routing-key"},
				},
				&endpoint.Slack{
					Base: endpoint.Base{
						ID:          newTestIDPtr(3),
						Name:        "slack endpoint",
						Description: "desc slack",
						Status:      taskmodel.TaskStatusInactive,
					},
					URL:   "http://example.com",
					Token: influxdb.SecretField{Key: "tokne"},
				},
			}

			t.Run("notification endpoints by name", func(t *testing.T) {
				tests := []struct {
					name     string
					findName string
					findID   platform.ID
					expected []influxdb.NotificationEndpoint
				}{
					{
						name:     "find notification endpoint with unique name",
						findName: "pd endpoint",
						expected: []influxdb.NotificationEndpoint{knownEndpoints[0]},
					},
					{
						name:     "find no notification endpoints",
						findName: "fakeEndpoint",
						expected: nil,
					},
					{
						name:     "find notification endpoint by id",
						findID:   platform.ID(2),
						expected: []influxdb.NotificationEndpoint{knownEndpoints[1]},
					},
					{
						name:     "find by id, set new name",
						findID:   platform.ID(3),
						findName: "slack-endpoint",
						expected: []influxdb.NotificationEndpoint{knownEndpoints[2]},
					},
				}

				for _, tt := range tests {
					fn := func(t *testing.T) {
						endpointSVC := mock.NewNotificationEndpointService()
						endpointSVC.FindNotificationEndpointByIDF = func(ctx context.Context, id platform.ID) (influxdb.NotificationEndpoint, error) {
							for i := range knownEndpoints {
								if knownEndpoints[i].GetID() == id {
									return knownEndpoints[i], nil
								}
							}

							return nil, errors.New("uh ohhh, wrong endpoint id here: " + id.String())
						}
						endpointSVC.FindNotificationEndpointsF = func(ctx context.Context, filter influxdb.NotificationEndpointFilter, _ ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, int, error) {
							if filter.ID != nil {
								for i := range knownEndpoints {
									if knownEndpoints[i].GetID() == *filter.ID {
										return []influxdb.NotificationEndpoint{knownEndpoints[i]}, 1, nil
									}
								}

								return nil, 0, errors.New("uh ohhh, wrong id here: " + filter.ID.String())
							}

							return knownEndpoints, len(knownEndpoints), nil
						}

						resToClone := ResourceToClone{
							Kind: KindNotificationEndpoint,
						}
						if tt.findName != "" {
							resToClone.Name = tt.findName
						}
						if tt.findID != platform.ID(0) {
							resToClone.ID = tt.findID
						}

						svc := newTestService(WithNotificationEndpointSVC(endpointSVC))
						template, err := svc.Export(context.TODO(), ExportWithExistingResources(resToClone))
						if tt.expected == nil {
							require.Error(t, err)
						} else {
							require.NoError(t, err)

							if tt.findName != "" && tt.findID != platform.ID(0) {
								tt.expected[0].SetName(tt.findName)
							}

							actual := template.Summary().NotificationEndpoints
							require.Len(t, actual, len(tt.expected))

							for i := range actual {
								assert.Equal(t, tt.expected[i].GetName(), actual[i].NotificationEndpoint.GetName())
								assert.Equal(t, tt.expected[i].GetDescription(), actual[i].NotificationEndpoint.GetDescription())
								assert.Equal(t, tt.expected[i].GetStatus(), actual[i].NotificationEndpoint.GetStatus())
								assert.Equal(t, tt.expected[i].SecretFields(), actual[i].NotificationEndpoint.SecretFields())
							}

							assert.True(t, encodeAndDecode(t, template) != nil)
						}
					}
					t.Run(tt.name, fn)
				}
			})

			t.Run("notification rules", func(t *testing.T) {
				newRuleBase := func(id int) rule.Base {
					return rule.Base{
						ID:          platform.ID(id),
						Name:        "old_name",
						Description: "desc",
						EndpointID:  platform.ID(id),
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
									Status:      taskmodel.TaskStatusActive,
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
									Status:      taskmodel.TaskStatusInactive,
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
									Status:      taskmodel.TaskStatusInactive,
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
							endpointSVC.FindNotificationEndpointByIDF = func(ctx context.Context, id platform.ID) (influxdb.NotificationEndpoint, error) {
								if id != tt.endpoint.GetID() {
									return nil, errors.New("uh ohhh, wrong id here: " + id.String())
								}
								return tt.endpoint, nil
							}
							ruleSVC := mock.NewNotificationRuleStore()
							ruleSVC.FindNotificationRuleByIDF = func(ctx context.Context, id platform.ID) (influxdb.NotificationRule, error) {
								return tt.rule, nil
							}
							ruleSVC.FindNotificationRulesF = func(ctx context.Context, _ influxdb.NotificationRuleFilter, _ ...influxdb.FindOptions) ([]influxdb.NotificationRule, int, error) {
								return []influxdb.NotificationRule{tt.rule}, 1, nil
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
							template, err := svc.Export(context.TODO(), ExportWithExistingResources(resToClone))
							require.NoError(t, err)

							newTemplate := encodeAndDecode(t, template)

							sum := newTemplate.Summary()
							require.Len(t, sum.NotificationRules, 1)

							actualRule := sum.NotificationRules[0]
							assert.Zero(t, actualRule.ID)
							assert.Zero(t, actualRule.EndpointID)
							assert.NotEmpty(t, actualRule.EndpointType)
							assert.NotEmpty(t, actualRule.EndpointMetaName)

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

							require.Len(t, template.Summary().NotificationEndpoints, 1)

							actualEndpoint := template.Summary().NotificationEndpoints[0].NotificationEndpoint
							assert.Equal(t, tt.endpoint.GetName(), actualEndpoint.GetName())
							assert.Equal(t, tt.endpoint.GetDescription(), actualEndpoint.GetDescription())
							assert.Equal(t, tt.endpoint.GetStatus(), actualEndpoint.GetStatus())
						}
						t.Run(tt.name, fn)
					}
				})

				t.Run("handles rules duplicate names", func(t *testing.T) {
					endpointSVC := mock.NewNotificationEndpointService()
					endpointSVC.FindNotificationEndpointByIDF = func(ctx context.Context, id platform.ID) (influxdb.NotificationEndpoint, error) {
						return &endpoint.HTTP{
							Base: endpoint.Base{
								ID:          &id,
								Name:        "endpoint_0",
								Description: "desc",
								Status:      taskmodel.TaskStatusInactive,
							},
							AuthMethod: "none",
							Method:     "GET",
							URL:        "http://example.com",
						}, nil
					}
					ruleSVC := mock.NewNotificationRuleStore()
					ruleSVC.FindNotificationRuleByIDF = func(ctx context.Context, id platform.ID) (influxdb.NotificationRule, error) {
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
					template, err := svc.Export(context.TODO(), ExportWithExistingResources(resourcesToClone...))
					require.NoError(t, err)

					newTemplate := encodeAndDecode(t, template)

					sum := newTemplate.Summary()
					require.Len(t, sum.NotificationRules, len(resourcesToClone))

					expectedSameEndpointName := sum.NotificationRules[0].EndpointMetaName
					assert.NotZero(t, expectedSameEndpointName)
					assert.NotEqual(t, "endpoint_0", expectedSameEndpointName)

					for i := range resourcesToClone {
						actual := sum.NotificationRules[i]
						assert.Equal(t, "old_name", actual.Name)
						assert.Equal(t, "desc", actual.Description)
						assert.Equal(t, expectedSameEndpointName, actual.EndpointMetaName)
					}

					require.Len(t, sum.NotificationEndpoints, 1)
					assert.Equal(t, "endpoint_0", sum.NotificationEndpoints[0].NotificationEndpoint.GetName())
				})
			})

			t.Run("notification rules by name", func(t *testing.T) {
				newRuleBase := func(id int, name string) rule.Base {
					return rule.Base{
						ID:          platform.ID(id),
						Name:        name,
						Description: fmt.Sprintf("desc %d", id),
						EndpointID:  platform.ID(1), // todo: setting to id as well likely doesn't work due to safeID
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

				knownRules := []influxdb.NotificationRule{
					&rule.PagerDuty{
						Base:            newRuleBase(1, "pd notify"),
						MessageTemplate: "Template",
					},
					&rule.PagerDuty{
						Base:            newRuleBase(2, "pd-notify"),
						MessageTemplate: "Template2 ",
					},
					&rule.Slack{
						Base:            newRuleBase(3, "pd-notify"),
						Channel:         "abc",
						MessageTemplate: "SLACK TEMPlate",
					},
				}

				tests := []struct {
					name     string
					findName string
					findID   platform.ID
					expected []influxdb.NotificationRule
				}{
					{
						name:     "find rule with unique name",
						findName: "pd notify",
						expected: []influxdb.NotificationRule{knownRules[0]},
					},
					{
						name:     "find multiple rules with shared name",
						findName: "pd-notify",
						expected: []influxdb.NotificationRule{knownRules[1], knownRules[2]},
					},
					{
						name:     "find no rules",
						findName: "fakeRule",
						expected: nil,
					},
					{
						name:     "find rule by id",
						findID:   platform.ID(2),
						expected: []influxdb.NotificationRule{knownRules[1]},
					},
					{
						name:     "find by id, set new name",
						findID:   platform.ID(3),
						findName: "slack-notify",
						expected: []influxdb.NotificationRule{knownRules[2]},
					},
				}

				for _, tt := range tests {
					fn := func(t *testing.T) {
						endpointSVC := mock.NewNotificationEndpointService()
						endpointSVC.FindNotificationEndpointByIDF = func(ctx context.Context, id platform.ID) (influxdb.NotificationEndpoint, error) {
							for i := range knownEndpoints {
								if knownEndpoints[i].GetID() == id {
									return knownEndpoints[i], nil
								}
							}

							return nil, errors.New("uh ohhh, wrong endpoint id here: " + id.String())
						}

						ruleSVC := mock.NewNotificationRuleStore()
						ruleSVC.FindNotificationRuleByIDF = func(ctx context.Context, id platform.ID) (influxdb.NotificationRule, error) {
							for i := range knownRules {
								if knownRules[i].GetID() == id {
									return knownRules[i], nil
								}
							}

							return nil, errors.New("uh ohhh, wrong rule id here: " + id.String())
						}
						ruleSVC.FindNotificationRulesF = func(ctx context.Context, _ influxdb.NotificationRuleFilter, _ ...influxdb.FindOptions) ([]influxdb.NotificationRule, int, error) {
							return knownRules, len(knownRules), nil
						}

						resToClone := ResourceToClone{
							Kind: KindNotificationRule,
						}
						if tt.findName != "" {
							resToClone.Name = tt.findName
						}
						if tt.findID != platform.ID(0) {
							resToClone.ID = tt.findID
						}

						svc := newTestService(
							WithNotificationEndpointSVC(endpointSVC),
							WithNotificationRuleSVC(ruleSVC),
						)
						template, err := svc.Export(context.TODO(), ExportWithExistingResources(resToClone))
						if tt.expected == nil {
							require.Error(t, err)
						} else {
							require.NoError(t, err)

							actual := template.Summary()
							require.Len(t, actual.NotificationRules, len(tt.expected))
							require.Len(t, actual.NotificationEndpoints, 1)
							sort.Slice(actual.NotificationRules, func(i, j int) bool {
								return actual.NotificationRules[i].Description < actual.NotificationRules[j].Description
							})
							sort.Slice(actual.NotificationEndpoints, func(i, j int) bool {
								return actual.NotificationEndpoints[i].NotificationEndpoint.GetDescription() < actual.NotificationEndpoints[j].NotificationEndpoint.GetDescription()
							})

							if tt.findName != "" && tt.findID != platform.ID(0) {
								tt.expected[0].SetName(tt.findName)
							}

							for i := range actual.NotificationRules {
								assert.Zero(t, actual.NotificationRules[i].ID)
								assert.Zero(t, actual.NotificationRules[i].EndpointID)
								assert.NotEmpty(t, actual.NotificationRules[i].EndpointType)
								assert.NotEmpty(t, actual.NotificationRules[i].EndpointMetaName)

								baseEqual := func(t *testing.T, base rule.Base) {
									assert.Equal(t, base.Name, actual.NotificationRules[i].Name)
									assert.Equal(t, base.Description, actual.NotificationRules[i].Description)
									assert.Equal(t, base.Every.TimeDuration().String(), actual.NotificationRules[i].Every)
									assert.Equal(t, base.Offset.TimeDuration().String(), actual.NotificationRules[i].Offset)

									for _, sRule := range base.StatusRules {
										expected := SummaryStatusRule{CurrentLevel: sRule.CurrentLevel.String()}
										if sRule.PreviousLevel != nil {
											expected.PreviousLevel = sRule.PreviousLevel.String()
										}
										assert.Contains(t, actual.NotificationRules[i].StatusRules, expected)
									}
									for _, tRule := range base.TagRules {
										expected := SummaryTagRule{
											Key:      tRule.Key,
											Value:    tRule.Value,
											Operator: tRule.Operator.String(),
										}
										assert.Contains(t, actual.NotificationRules[i].TagRules, expected)
									}
								}

								switch p := tt.expected[i].(type) {
								case *rule.HTTP:
									baseEqual(t, p.Base)
								case *rule.PagerDuty:
									baseEqual(t, p.Base)
									assert.Equal(t, p.MessageTemplate, actual.NotificationRules[i].MessageTemplate)
								case *rule.Slack:
									baseEqual(t, p.Base)
									assert.Equal(t, p.MessageTemplate, actual.NotificationRules[i].MessageTemplate)
								}

								for j := range actual.NotificationEndpoints {
									endpoint, err := endpointSVC.FindNotificationEndpointByIDF(context.Background(), tt.expected[i].GetEndpointID())
									require.NoError(t, err)

									assert.Equal(t, endpoint.GetName(), actual.NotificationEndpoints[j].NotificationEndpoint.GetName())
									assert.Equal(t, endpoint.GetDescription(), actual.NotificationEndpoints[j].NotificationEndpoint.GetDescription())
									assert.Equal(t, endpoint.GetStatus(), actual.NotificationEndpoints[j].NotificationEndpoint.GetStatus())
								}
							}

							assert.True(t, encodeAndDecode(t, template) != nil)
						}
					}
					t.Run(tt.name, fn)
				}
			})

			t.Run("tasks", func(t *testing.T) {
				t.Run("single task exports", func(t *testing.T) {
					tests := []struct {
						name    string
						newName string
						task    taskmodel.Task
					}{
						{
							name:    "every offset is set",
							newName: "new name",
							task: taskmodel.Task{
								ID:     1,
								Name:   "name_9000",
								Every:  time.Minute.String(),
								Offset: 10 * time.Second,
								Type:   taskmodel.TaskSystemType,
								Flux:   `option task = { name: "larry" } from(bucket: "rucket") |> yield()`,
							},
						},
						{
							name: "cron is set",
							task: taskmodel.Task{
								ID:   1,
								Name: "name_0",
								Cron: "2 * * * *",
								Type: taskmodel.TaskSystemType,
								Flux: `option task = { name: "larry" } from(bucket: "rucket") |> yield()`,
							},
						},
					}

					for _, tt := range tests {
						fn := func(t *testing.T) {
							taskSVC := mock.NewTaskService()
							taskSVC.FindTaskByIDFn = func(ctx context.Context, id platform.ID) (*taskmodel.Task, error) {
								if id != tt.task.ID {
									return nil, errors.New("wrong id provided: " + id.String())
								}
								return &tt.task, nil
							}
							taskSVC.FindTasksFn = func(ctx context.Context, filter taskmodel.TaskFilter) ([]*taskmodel.Task, int, error) {
								return []*taskmodel.Task{&tt.task}, 1, nil
							}

							svc := newTestService(WithTaskSVC(taskSVC))

							resToClone := ResourceToClone{
								Kind: KindTask,
								ID:   tt.task.ID,
								Name: tt.newName,
							}
							template, err := svc.Export(context.TODO(), ExportWithExistingResources(resToClone))
							require.NoError(t, err)

							newTemplate := encodeAndDecode(t, template)

							sum := newTemplate.Summary()

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
					taskSVC.FindTaskByIDFn = func(ctx context.Context, id platform.ID) (*taskmodel.Task, error) {
						return &taskmodel.Task{
							ID:          id,
							Type:        taskmodel.TaskSystemType,
							Name:        "same name",
							Description: "desc",
							Status:      taskmodel.TaskStatusActive,
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
					template, err := svc.Export(context.TODO(), ExportWithExistingResources(resourcesToClone...))
					require.NoError(t, err)

					newTemplate := encodeAndDecode(t, template)

					sum := newTemplate.Summary()

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

			t.Run("tasks by name", func(t *testing.T) {
				knownTasks := []*taskmodel.Task{
					{
						ID:          1,
						Name:        "task",
						Description: "task 1",
						Every:       time.Minute.String(),
						Offset:      10 * time.Second,
						Type:        taskmodel.TaskSystemType,
						Flux:        `option task = { name: "larry" } from(bucket: "rucket") |> yield()`,
					},
					{
						ID:          2,
						Name:        "taskCopy",
						Description: "task 2",
						Cron:        "2 * * * *",
						Type:        taskmodel.TaskSystemType,
						Flux:        `option task = { name: "curly" } from(bucket: "rucket") |> yield()`,
					},
					{
						ID:          3,
						Name:        "taskCopy",
						Description: "task 3",
						Cron:        "2 3 4 5 *",
						Type:        taskmodel.TaskSystemType,
						Flux:        `option task = { name: "moe" } from(bucket: "rucket") |> yield()`,
					},
				}

				tests := []struct {
					name     string
					findName string
					findID   platform.ID
					expected []*taskmodel.Task
				}{
					{
						name:     "find task with unique name",
						findName: "task",
						expected: []*taskmodel.Task{knownTasks[0]},
					},
					{
						name:     "find multiple tasks with shared name",
						findName: "taskCopy",
						expected: []*taskmodel.Task{knownTasks[1], knownTasks[2]},
					},
					{
						name:     "find no tasks",
						findName: "faketask",
						expected: nil,
					},
					{
						name:     "find task by id",
						findID:   platform.ID(2),
						expected: []*taskmodel.Task{knownTasks[1]},
					},
					{
						name:     "find by id, set new name",
						findID:   platform.ID(2),
						findName: "renamedTask",
						expected: []*taskmodel.Task{knownTasks[1]},
					},
				}

				for _, tt := range tests {
					fn := func(t *testing.T) {
						taskSVC := mock.NewTaskService()
						taskSVC.FindTaskByIDFn = func(ctx context.Context, id platform.ID) (*taskmodel.Task, error) {
							for i := range knownTasks {
								if knownTasks[i].ID == id {
									return knownTasks[i], nil
								}
							}

							return nil, errors.New("wrong id provided: " + id.String())
						}
						taskSVC.FindTasksFn = func(ctx context.Context, filter taskmodel.TaskFilter) ([]*taskmodel.Task, int, error) {
							tasks := []*taskmodel.Task{}
							for i := range knownTasks {
								if knownTasks[i].Name == *filter.Name {
									tasks = append(tasks, knownTasks[i])
								}
							}
							return tasks, len(tasks), nil
						}

						resToClone := ResourceToClone{
							Kind: KindTask,
						}
						if tt.findName != "" {
							resToClone.Name = tt.findName
						}
						if tt.findID != platform.ID(0) {
							resToClone.ID = tt.findID
						}

						svc := newTestService(WithTaskSVC(taskSVC))
						template, err := svc.Export(context.TODO(), ExportWithExistingResources(resToClone))
						if tt.expected == nil {
							require.Error(t, err)
						} else {
							require.NoError(t, err)

							if tt.findName != "" && tt.findID != platform.ID(0) {
								tt.expected[0].Name = tt.findName
							}

							actual := template.Summary().Tasks
							require.Len(t, actual, len(tt.expected))
							sort.Slice(actual, func(i, j int) bool {
								return actual[i].Description < actual[j].Description
							})

							for i := range actual {
								assert.Equal(t, tt.expected[i].Name, actual[i].Name)
								assert.Equal(t, tt.expected[i].Cron, actual[i].Cron)
								assert.Equal(t, tt.expected[i].Description, actual[i].Description)
								assert.Equal(t, tt.expected[i].Every, actual[i].Every)
								assert.Equal(t, durToStr(tt.expected[i].Offset), actual[i].Offset)

								assert.Equal(t, `from(bucket: "rucket") |> yield()`, actual[i].Query)
							}
							assert.True(t, encodeAndDecode(t, template) != nil)
						}
					}
					t.Run(tt.name, fn)
				}
			})

			t.Run("telegraf configs", func(t *testing.T) {
				t.Run("allows for duplicate telegraf names to be exported", func(t *testing.T) {
					tConfig := &influxdb.TelegrafConfig{
						OrgID:       9000,
						Name:        "same name",
						Description: "desc",
						Config:      "some config string",
					}
					teleStore := mock.NewTelegrafConfigStore()
					teleStore.FindTelegrafConfigByIDF = func(ctx context.Context, id platform.ID) (*influxdb.TelegrafConfig, error) {
						tConfig.ID = id
						return tConfig, nil
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

					teleStore.FindTelegrafConfigsF = func(ctx context.Context, filter influxdb.TelegrafConfigFilter, _ ...influxdb.FindOptions) ([]*influxdb.TelegrafConfig, int, error) {
						tgrafs := []*influxdb.TelegrafConfig{}
						for _, r := range resourcesToClone {
							t := tConfig
							t.ID = r.ID
							tgrafs = append(tgrafs, t)
						}
						return tgrafs, len(tgrafs), nil
					}

					template, err := svc.Export(context.TODO(), ExportWithExistingResources(resourcesToClone...))
					require.NoError(t, err)

					newTemplate := encodeAndDecode(t, template)

					sum := newTemplate.Summary()

					teles := sum.TelegrafConfigs
					sort.Slice(teles, func(i, j int) bool {
						return teles[i].TelegrafConfig.Name < teles[j].TelegrafConfig.Name
					})
					require.Len(t, teles, len(resourcesToClone))

					for i := range resourcesToClone {
						actual := teles[i]
						assert.Equal(t, "same name", actual.TelegrafConfig.Name)
						assert.Equal(t, "desc", actual.TelegrafConfig.Description)
						assert.Equal(t, "some config string", actual.TelegrafConfig.Config)
					}
				})
			})

			t.Run("telegraf configs by name", func(t *testing.T) {
				knownConfigs := []*influxdb.TelegrafConfig{
					{
						ID:          1,
						OrgID:       9000,
						Name:        "my config",
						Description: "desc1",
						Config:      "a config string",
					},
					{
						ID:          2,
						OrgID:       9000,
						Name:        "telConfig",
						Description: "desc2",
						Config:      "some config string",
					},
					{
						ID:          3,
						OrgID:       9000,
						Name:        "telConfig",
						Description: "desc3",
						Config:      "some other config string",
					},
				}

				tests := []struct {
					name     string
					findName string
					findID   platform.ID
					expected []*influxdb.TelegrafConfig
				}{
					{
						name:     "find telegraf with unique name",
						findName: "my config",
						expected: []*influxdb.TelegrafConfig{knownConfigs[0]},
					},
					{
						name:     "find multiple telegrafs with shared name",
						findName: "telConfig",
						expected: []*influxdb.TelegrafConfig{knownConfigs[1], knownConfigs[2]},
					},
					{
						name:     "find no telegrafs",
						findName: "fakeConfig",
						expected: nil,
					},
					{
						name:     "find telegraf by id",
						findID:   platform.ID(2),
						expected: []*influxdb.TelegrafConfig{knownConfigs[1]},
					},
					{
						name:     "find by id, set new name",
						findID:   platform.ID(2),
						findName: "newConfig",
						expected: []*influxdb.TelegrafConfig{knownConfigs[1]},
					},
				}

				for _, tt := range tests {
					fn := func(t *testing.T) {
						teleStore := mock.NewTelegrafConfigStore()
						teleStore.FindTelegrafConfigByIDF = func(_ context.Context, id platform.ID) (*influxdb.TelegrafConfig, error) {
							for i := range knownConfigs {
								if knownConfigs[i].ID == id {
									return knownConfigs[i], nil
								}
							}
							return nil, errors.New("uh ohhh, wrong id here: " + id.String())
						}
						teleStore.FindTelegrafConfigsF = func(_ context.Context, filter influxdb.TelegrafConfigFilter, _ ...influxdb.FindOptions) ([]*influxdb.TelegrafConfig, int, error) {
							return knownConfigs, len(knownConfigs), nil
						}

						resToClone := ResourceToClone{
							Kind: KindTelegraf,
						}
						if tt.findName != "" {
							resToClone.Name = tt.findName
						}
						if tt.findID != platform.ID(0) {
							resToClone.ID = tt.findID
						}

						svc := newTestService(WithTelegrafSVC(teleStore))
						template, err := svc.Export(context.TODO(), ExportWithExistingResources(resToClone))
						if tt.expected == nil {
							require.Error(t, err)
						} else {
							require.NoError(t, err)

							if tt.findName != "" && tt.findID != platform.ID(0) {
								tt.expected[0].Name = tt.findName
							}

							actual := template.Summary().TelegrafConfigs
							require.Len(t, actual, len(tt.expected))
							sort.Slice(actual, func(i, j int) bool {
								return actual[i].TelegrafConfig.Description < actual[j].TelegrafConfig.Description
							})

							for i := range actual {
								assert.Equal(t, tt.expected[i].Name, actual[i].TelegrafConfig.Name)
								assert.Equal(t, tt.expected[i].Description, actual[i].TelegrafConfig.Description)
								assert.Equal(t, tt.expected[i].Config, actual[i].TelegrafConfig.Config)
							}

							assert.True(t, encodeAndDecode(t, template) != nil)
						}
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
							Selected:    []string{"val"},
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
							ID:       1,
							Name:     "old name",
							Selected: []string{"val"},
							Arguments: &influxdb.VariableArguments{
								Type:   "constant",
								Values: influxdb.VariableConstantValues{"val"},
							},
						},
					},
					{
						name: "with map arg",
						expectedVar: influxdb.Variable{
							ID:       1,
							Name:     "old name",
							Selected: []string{"v"},
							Arguments: &influxdb.VariableArguments{
								Type:   "map",
								Values: influxdb.VariableMapValues{"k": "v"},
							},
						},
					},
					{
						name: "with query arg",
						expectedVar: influxdb.Variable{
							ID:       1,
							Name:     "old name",
							Selected: []string{"bucket-foo"},
							Arguments: &influxdb.VariableArguments{
								Type: "query",
								Values: influxdb.VariableQueryValues{
									Query:    "buckets()",
									Language: "flux",
								},
							},
						},
					},
				}

				for _, tt := range tests {
					fn := func(t *testing.T) {
						varSVC := mock.NewVariableService()
						varSVC.FindVariableByIDF = func(_ context.Context, id platform.ID) (*influxdb.Variable, error) {
							if id != tt.expectedVar.ID {
								return nil, errors.New("uh ohhh, wrong id here: " + id.String())
							}
							return &tt.expectedVar, nil
						}
						varSVC.FindVariablesF = func(_ context.Context, filter influxdb.VariableFilter, _ ...influxdb.FindOptions) ([]*influxdb.Variable, error) {
							if filter.ID != nil && *filter.ID != tt.expectedVar.ID {
								return nil, errors.New("uh ohhh, wrong id here: " + fmt.Sprint(*filter.ID))
							}
							return []*influxdb.Variable{&tt.expectedVar}, nil
						}

						svc := newTestService(WithVariableSVC(varSVC), WithLabelSVC(mock.NewLabelService()))

						resToClone := ResourceToClone{
							Kind: KindVariable,
							ID:   tt.expectedVar.ID,
							Name: tt.newName,
						}
						template, err := svc.Export(context.TODO(), ExportWithExistingResources(resToClone))
						require.NoError(t, err)

						newTemplate := encodeAndDecode(t, template)

						newVars := newTemplate.Summary().Variables
						require.Len(t, newVars, 1)

						actual := newVars[0]
						expectedName := tt.expectedVar.Name
						if tt.newName != "" {
							expectedName = tt.newName
						}
						assert.Equal(t, expectedName, actual.Name)
						assert.Equal(t, tt.expectedVar.Description, actual.Description)
						assert.Equal(t, tt.expectedVar.Selected, actual.Selected)
						assert.Equal(t, tt.expectedVar.Arguments, actual.Arguments)
					}
					t.Run(tt.name, fn)
				}
			})

			t.Run("variable by name", func(t *testing.T) {
				knownVariables := []*influxdb.Variable{
					{
						ID:          1,
						Name:        "variable",
						Description: "desc",
						Selected:    []string{"val1"},
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{"val"},
						},
					},
					{
						ID:       2,
						Name:     "var 2",
						Selected: []string{"val2"},
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{"val"},
						},
					},
					{
						ID:       3,
						Name:     "var 3",
						Selected: []string{"v"},
						Arguments: &influxdb.VariableArguments{
							Type:   "map",
							Values: influxdb.VariableMapValues{"k": "v"},
						},
					},
				}

				tests := []struct {
					name     string
					findName string
					findID   platform.ID
					expected []*influxdb.Variable
				}{
					{
						name:     "find variable with unique name",
						findName: "variable",
						expected: []*influxdb.Variable{knownVariables[0]},
					},
					{
						name:     "find no variables",
						findName: "fakeVariable",
						expected: nil,
					},
					{
						name:     "find variable by id",
						findID:   platform.ID(2),
						expected: []*influxdb.Variable{knownVariables[1]},
					},
					{
						name:     "find by id, set new name",
						findID:   platform.ID(2),
						findName: "useful var",
						expected: []*influxdb.Variable{knownVariables[1]},
					},
				}

				for _, tt := range tests {
					fn := func(t *testing.T) {
						varSVC := mock.NewVariableService()
						varSVC.FindVariableByIDF = func(_ context.Context, id platform.ID) (*influxdb.Variable, error) {
							for i := range knownVariables {
								if knownVariables[i].ID == id {
									return knownVariables[i], nil
								}
							}

							return nil, errors.New("uh ohhh, wrong id here: " + id.String())
						}
						varSVC.FindVariablesF = func(_ context.Context, filter influxdb.VariableFilter, _ ...influxdb.FindOptions) ([]*influxdb.Variable, error) {
							return knownVariables, nil
						}

						resToClone := ResourceToClone{
							Kind: KindVariable,
						}
						if tt.findName != "" {
							resToClone.Name = tt.findName
						}
						if tt.findID != platform.ID(0) {
							resToClone.ID = tt.findID
						}

						svc := newTestService(WithVariableSVC(varSVC), WithLabelSVC(mock.NewLabelService()))
						template, err := svc.Export(context.TODO(), ExportWithExistingResources(resToClone))
						if tt.expected == nil {
							require.Error(t, err)
						} else {
							require.NoError(t, err)

							if tt.findName != "" && tt.findID != platform.ID(0) {
								tt.expected[0].Name = tt.findName
							}

							actual := template.Summary().Variables
							require.Len(t, actual, len(tt.expected))

							for i := range actual {
								assert.Equal(t, tt.expected[i].Name, actual[i].Name)
								assert.Equal(t, tt.expected[i].Description, actual[i].Description)
								assert.Equal(t, tt.expected[i].Arguments, actual[i].Arguments)
							}

							assert.True(t, encodeAndDecode(t, template) != nil)
						}
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
					bktSVC.FindBucketByIDFn = func(_ context.Context, id platform.ID) (*influxdb.Bucket, error) {
						if id != expected.ID {
							return nil, errors.New("uh ohhh, wrong id here: " + id.String())
						}
						return expected, nil
					}
					bktSVC.FindBucketsFn = func(_ context.Context, f influxdb.BucketFilter, opts ...influxdb.FindOptions) ([]*influxdb.Bucket, int, error) {
						if f.ID != nil && *f.ID != expected.ID {
							return nil, 0, errors.New("not suppose to get here")
						}
						return []*influxdb.Bucket{expected}, 1, nil
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
					template, err := svc.Export(context.TODO(), ExportWithExistingResources(resToClone))
					require.NoError(t, err)

					newTemplate := encodeAndDecode(t, template)
					sum := newTemplate.Summary()

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
					bktSVC.FindBucketByIDFn = func(_ context.Context, id platform.ID) (*influxdb.Bucket, error) {
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

					bktSVC.FindBucketsFn = func(_ context.Context, f influxdb.BucketFilter, opts ...influxdb.FindOptions) ([]*influxdb.Bucket, int, error) {
						bkts := []*influxdb.Bucket{}
						for _, r := range resourcesToClone {
							bkts = append(bkts, &influxdb.Bucket{ID: r.ID, Name: strconv.Itoa(int(r.ID)), Type: influxdb.BucketTypeUser})
						}
						return bkts, len(bkts), nil
					}

					template, err := svc.Export(context.TODO(), ExportWithExistingResources(resourcesToClone...))
					require.NoError(t, err)

					newTemplate := encodeAndDecode(t, template)
					sum := newTemplate.Summary()

					bkts := sum.Buckets
					sort.Slice(bkts, func(i, j int) bool {
						return bkts[i].Name < bkts[j].Name
					})
					require.Len(t, bkts, 2)

					for i, actual := range bkts {
						sortLabelsByName(actual.LabelAssociations)
						assert.Equal(t, strconv.Itoa((i+1)*10), actual.Name)
						require.Len(t, actual.LabelAssociations, 2)
						assert.Equal(t, "label_1", actual.LabelAssociations[0].Name)
						assert.Equal(t, "label_2", actual.LabelAssociations[1].Name)
					}

					labels := sum.Labels
					sortLabelsByName(labels)
					require.Len(t, labels, 2)
					assert.Equal(t, "label_1", labels[0].Name)
					assert.Equal(t, "label_2", labels[1].Name)
				})

				t.Run("labels do not fetch associations", func(t *testing.T) {
					labelSVC := mock.NewLabelService()
					labelSVC.FindLabelByIDFn = func(_ context.Context, id platform.ID) (*influxdb.Label, error) {
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
					template, err := svc.Export(context.TODO(), ExportWithExistingResources(resToClone))
					require.NoError(t, err)

					newTemplate := encodeAndDecode(t, template)

					labels := newTemplate.Summary().Labels
					require.Len(t, labels, 1)
					assert.Equal(t, "label_1", labels[0].Name)
				})
			})
		})

		t.Run("with org id", func(t *testing.T) {
			orgID := platform.ID(9000)
			bkt := &influxdb.Bucket{ID: 1, Name: "bucket"}

			bktSVC := mock.NewBucketService()
			bktSVC.FindBucketsFn = func(_ context.Context, f influxdb.BucketFilter, opts ...influxdb.FindOptions) ([]*influxdb.Bucket, int, error) {
				if (f.ID != nil && *f.ID != bkt.ID) && (f.OrganizationID == nil || *f.OrganizationID != orgID) {
					return nil, 0, errors.New("not suppose to get here")
				}
				return []*influxdb.Bucket{bkt}, 1, nil
			}
			bktSVC.FindBucketByIDFn = func(_ context.Context, id platform.ID) (*influxdb.Bucket, error) {
				if id != 1 {
					return nil, errors.New("wrong id")
				}
				return bkt, nil
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
				if (f.ID != nil && *f.ID != expectedCheck.GetID()) && (f.OrgID == nil || *f.OrgID != orgID) {
					return nil, 0, errors.New("not suppose to get here")
				}
				return []influxdb.Check{expectedCheck}, 1, nil
			}
			checkSVC.FindCheckByIDFn = func(ctx context.Context, id platform.ID) (influxdb.Check, error) {
				return expectedCheck, nil
			}

			dash := &influxdb.Dashboard{
				ID:    2,
				Name:  "dashboard",
				Cells: []*influxdb.Cell{},
			}
			dashSVC := mock.NewDashboardService()
			dashSVC.FindDashboardsF = func(_ context.Context, f influxdb.DashboardFilter, _ influxdb.FindOptions) ([]*influxdb.Dashboard, int, error) {
				if (f.IDs != nil && len(f.IDs) > 0 &&
					f.IDs[0] != nil && *f.IDs[0] != dash.ID) &&
					(f.OrganizationID == nil || *f.OrganizationID != orgID) {
					return nil, 0, errors.New("not suppose to get here")
				}
				return []*influxdb.Dashboard{dash}, 1, nil
			}
			dashSVC.FindDashboardByIDF = func(_ context.Context, id platform.ID) (*influxdb.Dashboard, error) {
				if id != 2 {
					return nil, errors.New("wrong id")
				}
				return dash, nil
			}

			notificationEndpoint := &endpoint.HTTP{
				Base: endpoint.Base{
					ID:   newTestIDPtr(2),
					Name: "http",
				},
				URL:        "http://example.com/id",
				Username:   influxdb.SecretField{Key: "2-username"},
				Password:   influxdb.SecretField{Key: "2-password"},
				AuthMethod: "basic",
				Method:     "POST",
			}
			endpointSVC := mock.NewNotificationEndpointService()
			endpointSVC.FindNotificationEndpointsF = func(ctx context.Context, f influxdb.NotificationEndpointFilter, _ ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, int, error) {
				return []influxdb.NotificationEndpoint{notificationEndpoint}, 1, nil
			}
			endpointSVC.FindNotificationEndpointByIDF = func(ctx context.Context, id platform.ID) (influxdb.NotificationEndpoint, error) {
				return notificationEndpoint, nil
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
			ruleSVC.FindNotificationRuleByIDF = func(ctx context.Context, id platform.ID) (influxdb.NotificationRule, error) {
				return expectedRule, nil
			}

			labelSVC := mock.NewLabelService()
			labelSVC.FindLabelsFn = func(_ context.Context, f influxdb.LabelFilter) ([]*influxdb.Label, error) {
				if f.OrgID == nil || *f.OrgID != orgID {
					return nil, errors.New("not suppose to get here")
				}
				return []*influxdb.Label{{ID: 3, Name: "label"}}, nil
			}
			labelSVC.FindLabelByIDFn = func(_ context.Context, id platform.ID) (*influxdb.Label, error) {
				if id != 3 {
					return nil, errors.New("wrong id")
				}
				return &influxdb.Label{ID: 3, Name: "label"}, nil
			}

			taskSVC := mock.NewTaskService()
			taskSVC.FindTasksFn = func(ctx context.Context, f taskmodel.TaskFilter) ([]*taskmodel.Task, int, error) {
				if f.After != nil {
					return nil, 0, nil
				}
				return []*taskmodel.Task{
					{ID: 31, Type: taskmodel.TaskSystemType},
					{ID: expectedCheck.TaskID, Type: taskmodel.TaskSystemType}, // this one should be ignored in the return
					{ID: expectedRule.TaskID, Type: taskmodel.TaskSystemType},  // this one should be ignored in the return as well
					{ID: 99}, // this one should be skipped since it is not a system task
				}, 3, nil
			}
			taskSVC.FindTaskByIDFn = func(ctx context.Context, id platform.ID) (*taskmodel.Task, error) {
				if id != 31 {
					return nil, errors.New("wrong id: " + id.String())
				}
				return &taskmodel.Task{
					ID:     id,
					Name:   "task_0",
					Every:  time.Minute.String(),
					Offset: 10 * time.Second,
					Type:   taskmodel.TaskSystemType,
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
			varSVC.FindVariableByIDF = func(_ context.Context, id platform.ID) (*influxdb.Variable, error) {
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

			template, err := svc.Export(
				context.TODO(),
				ExportWithAllOrgResources(ExportByOrgIDOpt{
					OrgID: orgID,
				}),
			)
			require.NoError(t, err)

			summary := template.Summary()
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
			assert.NotEmpty(t, rules[0].EndpointMetaName)

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

			stack, err := svc.InitStack(context.Background(), 9000, StackCreate{OrgID: 3333})
			require.NoError(t, err)

			assert.Equal(t, platform.ID(3), stack.ID)
			assert.Equal(t, now, stack.CreatedAt)
			assert.Equal(t, now, stack.LatestEvent().UpdatedAt)
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
					expectedErrCode: errors2.EInternal,
					store: func() *fakeStore {
						return newFakeStore(func(ctx context.Context, stack Stack) error {
							return errors.New("unexpected error")
						})
					},
				},
				{
					name:            "unexpected conflict store err",
					expectedErrCode: errors2.EInternal,
					store: func() *fakeStore {
						return newFakeStore(func(ctx context.Context, stack Stack) error {
							return &errors2.Error{Code: errors2.EConflict}
						})
					},
				},
				{
					name:            "org does not exist produces conflict error",
					expectedErrCode: errors2.EConflict,
					store: func() *fakeStore {
						return newFakeStore(safeCreateFn)
					},
					orgSVC: func() influxdb.OrganizationService {
						orgSVC := mock.NewOrganizationService()
						orgSVC.FindOrganizationByIDF = func(ctx context.Context, id platform.ID) (*influxdb.Organization, error) {
							return nil, &errors2.Error{Code: errors2.ENotFound}
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

					_, err := svc.InitStack(context.Background(), 9000, StackCreate{OrgID: 3333})
					require.Error(t, err)
					assert.Equal(t, tt.expectedErrCode, errors2.ErrorCode(err))
				}
				t.Run(tt.name, fn)
			}
		})
	})

	t.Run("UpdateStack", func(t *testing.T) {
		now := time.Time{}.Add(10 * 24 * time.Hour)

		t.Run("when updating valid stack", func(t *testing.T) {
			tests := []struct {
				name     string
				input    StackUpdate
				expected StackEvent
			}{
				{
					name:  "update nothing",
					input: StackUpdate{},
					expected: StackEvent{
						EventType: StackEventUpdate,
						UpdatedAt: now,
					},
				},
				{
					name: "update name",
					input: StackUpdate{
						Name: strPtr("name"),
					},
					expected: StackEvent{
						EventType: StackEventUpdate,
						Name:      "name",
						UpdatedAt: now,
					},
				},
				{
					name: "update desc",
					input: StackUpdate{
						Description: strPtr("desc"),
					},
					expected: StackEvent{
						EventType:   StackEventUpdate,
						Description: "desc",
						UpdatedAt:   now,
					},
				},
				{
					name: "update URLs",
					input: StackUpdate{
						TemplateURLs: []string{"http://example.com"},
					},
					expected: StackEvent{
						EventType:    StackEventUpdate,
						TemplateURLs: []string{"http://example.com"},
						UpdatedAt:    now,
					},
				},
				{
					name: "update first 3",
					input: StackUpdate{
						Name:         strPtr("name"),
						Description:  strPtr("desc"),
						TemplateURLs: []string{"http://example.com"},
					},
					expected: StackEvent{
						EventType:    StackEventUpdate,
						Name:         "name",
						Description:  "desc",
						TemplateURLs: []string{"http://example.com"},
						UpdatedAt:    now,
					},
				},
				{
					name: "update with metaname collisions",
					input: StackUpdate{
						Name:         strPtr("name"),
						Description:  strPtr("desc"),
						TemplateURLs: []string{"http://example.com"},
						AdditionalResources: []StackAdditionalResource{
							{
								APIVersion: APIVersion,
								ID:         1,
								Kind:       KindLabel,
								MetaName:   "meta-label",
							},
							{
								APIVersion: APIVersion,
								ID:         2,
								Kind:       KindLabel,
								MetaName:   "meta-label",
							},
						},
					},
					expected: StackEvent{
						EventType:    StackEventUpdate,
						Name:         "name",
						Description:  "desc",
						TemplateURLs: []string{"http://example.com"},
						Resources: []StackResource{
							{
								APIVersion: APIVersion,
								ID:         2,
								Kind:       KindLabel,
								MetaName:   "collision-1-" + platform.ID(333).String()[10:],
							},
							{
								APIVersion: APIVersion,
								ID:         1,
								Kind:       KindLabel,
								MetaName:   "meta-label",
							},
						},
						UpdatedAt: now,
					},
				},
				{
					name: "update all",
					input: StackUpdate{
						Name:         strPtr("name"),
						Description:  strPtr("desc"),
						TemplateURLs: []string{"http://example.com"},
						AdditionalResources: []StackAdditionalResource{
							{
								APIVersion: APIVersion,
								ID:         1,
								Kind:       KindLabel,
								MetaName:   "meta-label",
							},
						},
					},
					expected: StackEvent{
						EventType:    StackEventUpdate,
						Name:         "name",
						Description:  "desc",
						TemplateURLs: []string{"http://example.com"},
						Resources: []StackResource{
							{
								APIVersion: APIVersion,
								ID:         1,
								Kind:       KindLabel,
								MetaName:   "meta-label",
							},
						},
						UpdatedAt: now,
					},
				},
			}

			for _, tt := range tests {
				fn := func(t *testing.T) {
					var collisions int
					nameGenFn := func() string {
						collisions++
						return "collision-" + strconv.Itoa(collisions)
					}

					svc := newTestService(
						WithIDGenerator(mock.IDGenerator{
							IDFn: func() platform.ID {
								return 333
							},
						}),
						withNameGen(nameGenFn),
						WithTimeGenerator(newTimeGen(now)),
						WithStore(&fakeStore{
							readFn: func(ctx context.Context, id platform.ID) (Stack, error) {
								if id != 33 {
									return Stack{}, errors.New("wrong id: " + id.String())
								}
								return Stack{ID: id, OrgID: 3}, nil
							},
							updateFn: func(ctx context.Context, stack Stack) error {
								return nil
							},
						}),
					)

					tt.input.ID = 33
					stack, err := svc.UpdateStack(context.Background(), tt.input)
					require.NoError(t, err)

					assert.Equal(t, platform.ID(33), stack.ID)
					assert.Equal(t, platform.ID(3), stack.OrgID)
					assert.Zero(t, stack.CreatedAt) // should always zero value in these tests
					assert.Equal(t, tt.expected, stack.LatestEvent())
				}

				t.Run(tt.name, fn)
			}
		})
	})
}

func Test_normalizeRemoteSources(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []url.URL
	}{
		{
			name:     "no urls provided",
			input:    []string{"byte stream", "string", ""},
			expected: nil,
		},
		{
			name:     "skips valid file url",
			input:    []string{"file:///example.com"},
			expected: nil,
		},
		{
			name:     "valid http url provided",
			input:    []string{"http://example.com"},
			expected: []url.URL{parseURL(t, "http://example.com")},
		},
		{
			name:     "valid https url provided",
			input:    []string{"https://example.com"},
			expected: []url.URL{parseURL(t, "https://example.com")},
		},
		{
			name:  "converts raw github user url to base github",
			input: []string{"https://raw.githubusercontent.com/influxdata/community-templates/master/github/github.yml"},
			expected: []url.URL{
				parseURL(t, "https://github.com/influxdata/community-templates/blob/master/github/github.yml"),
			},
		},
		{
			name:  "passes base github link unchanged",
			input: []string{"https://github.com/influxdata/community-templates/blob/master/github/github.yml"},
			expected: []url.URL{
				parseURL(t, "https://github.com/influxdata/community-templates/blob/master/github/github.yml"),
			},
		},
	}

	for _, tt := range tests {
		fn := func(t *testing.T) {
			actual := normalizeRemoteSources(tt.input)
			require.Len(t, actual, len(tt.expected))
			for i, expected := range tt.expected {
				assert.Equal(t, expected.String(), actual[i].String())
			}
		}
		t.Run(tt.name, fn)
	}
}

func newTestIDPtr(i int) *platform.ID {
	id := platform.ID(i)
	return &id
}

func levelPtr(l notification.CheckLevel) *notification.CheckLevel {
	return &l
}

type fakeStore struct {
	createFn func(ctx context.Context, stack Stack) error
	deleteFn func(ctx context.Context, id platform.ID) error
	readFn   func(ctx context.Context, id platform.ID) (Stack, error)
	updateFn func(ctx context.Context, stack Stack) error
}

var _ Store = (*fakeStore)(nil)

func (s *fakeStore) CreateStack(ctx context.Context, stack Stack) error {
	if s.createFn != nil {
		return s.createFn(ctx, stack)
	}
	panic("not implemented")
}

func (s *fakeStore) ListStacks(ctx context.Context, orgID platform.ID, f ListFilter) ([]Stack, error) {
	panic("not implemented")
}

func (s *fakeStore) ReadStackByID(ctx context.Context, id platform.ID) (Stack, error) {
	if s.readFn != nil {
		return s.readFn(ctx, id)
	}
	panic("not implemented")
}

func (s *fakeStore) UpdateStack(ctx context.Context, stack Stack) error {
	if s.updateFn != nil {
		return s.updateFn(ctx, stack)
	}
	panic("not implemented")
}

func (s *fakeStore) DeleteStack(ctx context.Context, id platform.ID) error {
	if s.deleteFn != nil {
		return s.deleteFn(ctx, id)
	}
	panic("not implemented")
}

type fakeIDGen func() platform.ID

func newFakeIDGen(id platform.ID) fakeIDGen {
	return func() platform.ID {
		return id
	}
}

func (f fakeIDGen) ID() platform.ID {
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

func parseURL(t *testing.T, rawAddr string) url.URL {
	t.Helper()
	u, err := url.Parse(rawAddr)
	require.NoError(t, err)
	return *u
}
