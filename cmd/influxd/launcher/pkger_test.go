package launcher

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	nethttp "net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/notification"
	"github.com/influxdata/influxdb/v2/notification/check"
	"github.com/influxdata/influxdb/v2/notification/endpoint"
	"github.com/influxdata/influxdb/v2/notification/rule"
	"github.com/influxdata/influxdb/v2/pkger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var ctx = context.Background()

func TestLauncher_Pkger(t *testing.T) {
	l := RunTestLauncherOrFail(t, ctx, "--log-level", "error")
	l.SetupOrFail(t)
	defer l.ShutdownOrFail(t, ctx)
	require.NoError(t, l.BucketService(t).DeleteBucket(ctx, l.Bucket.ID))

	svc := l.PkgerService(t)

	resourceCheck := newResourceChecker(l)

	newStackFn := func(t *testing.T, stack pkger.Stack) (pkger.Stack, func()) {
		t.Helper()

		if stack.OrgID == 0 {
			stack.OrgID = l.Org.ID
		}

		newStack, err := svc.InitStack(ctx, l.User.ID, stack)
		require.NoError(t, err)

		assert.NotZero(t, newStack.OrgID)
		assert.Equal(t, stack.Name, newStack.Name)
		assert.Equal(t, stack.Description, newStack.Description)
		assert.Equal(t, stack.URLs, newStack.URLs)
		assert.NotNil(t, newStack.Resources)
		assert.NotZero(t, newStack.CRUDLog)

		return newStack, func() {
			// deletes are idempotent, so any error encountered here is not a not found error
			// but rather an error to concern ourselves with.
			err := svc.DeleteStack(ctx, struct{ OrgID, UserID, StackID influxdb.ID }{
				OrgID:   l.Org.ID,
				UserID:  l.User.ID,
				StackID: newStack.ID,
			})
			require.NoError(t, err, "failed to delete stack and its associated resources")
		}
	}

	t.Run("managing pkg state with stacks", func(t *testing.T) {
		newPkg := func(objects ...pkger.Object) *pkger.Pkg {
			return &pkger.Pkg{Objects: objects}
		}

		newBucketObject := func(pkgName, name, desc string) pkger.Object {
			obj := pkger.BucketToObject("", influxdb.Bucket{
				Name:        name,
				Description: desc,
			})
			obj.SetMetadataName(pkgName)
			return obj
		}

		newCheckDeadmanObject := func(t *testing.T, pkgName, name string, every time.Duration) pkger.Object {
			t.Helper()

			d, err := notification.FromTimeDuration(every)
			require.NoError(t, err)

			obj := pkger.CheckToObject("", &check.Deadman{
				Base: check.Base{
					Name:  name,
					Every: &d,
					Query: influxdb.DashboardQuery{
						Text: `from(bucket: "rucket_1") |> range(start: -1d)`,
					},
					StatusMessageTemplate: "Check: ${ r._check_name } is: ${ r._level }",
				},
				Level: notification.Critical,
			})
			obj.SetMetadataName(pkgName)
			return obj
		}

		newDashObject := func(pkgName, name, desc string) pkger.Object {
			obj := pkger.DashboardToObject("", influxdb.Dashboard{
				Name:        name,
				Description: desc,
			})
			obj.SetMetadataName(pkgName)
			return obj
		}

		newEndpointHTTP := func(pkgName, name, description string) pkger.Object {
			obj := pkger.NotificationEndpointToObject("", &endpoint.HTTP{
				Base: endpoint.Base{
					Name:        name,
					Description: description,
					Status:      influxdb.Inactive,
				},
				AuthMethod: "none",
				URL:        "http://example.com",
				Method:     "GET",
			})
			obj.SetMetadataName(pkgName)
			return obj
		}

		newLabelObject := func(pkgName, name, desc, color string) pkger.Object {
			obj := pkger.LabelToObject("", influxdb.Label{
				Name: name,
				Properties: map[string]string{
					"color":       color,
					"description": desc,
				},
			})
			obj.SetMetadataName(pkgName)
			return obj
		}

		newRuleObject := func(t *testing.T, pkgName, name, endpointPkgName, desc string) pkger.Object {
			t.Helper()

			every, err := notification.FromTimeDuration(time.Hour)
			require.NoError(t, err)

			obj := pkger.NotificationRuleToObject("", endpointPkgName, &rule.HTTP{
				Base: rule.Base{
					Name:        name,
					Description: desc,
					Every:       &every,
					StatusRules: []notification.StatusRule{{CurrentLevel: notification.Critical}},
				},
			})
			obj.SetMetadataName(pkgName)
			return obj
		}

		newTaskObject := func(pkgName, name, description string) pkger.Object {
			obj := pkger.TaskToObject("", influxdb.Task{
				Name:        name,
				Description: description,
				Flux:        "buckets()",
				Every:       "1h",
			})
			obj.SetMetadataName(pkgName)
			return obj
		}

		newTelegrafObject := func(pkgName, name, description string) pkger.Object {
			obj := pkger.TelegrafToObject("", influxdb.TelegrafConfig{
				Name:        name,
				Description: description,
				Config:      telegrafCfg,
			})
			obj.SetMetadataName(pkgName)
			return obj
		}

		newVariableObject := func(pkgName, name, description string) pkger.Object {
			obj := pkger.VariableToObject("", influxdb.Variable{
				Name:        name,
				Description: description,
				Arguments: &influxdb.VariableArguments{
					Type:   "constant",
					Values: influxdb.VariableConstantValues{"a", "b"},
				},
			})
			obj.SetMetadataName(pkgName)
			return obj
		}

		t.Run("creating a stack", func(t *testing.T) {
			_, cleanup := newStackFn(t, pkger.Stack{
				OrgID:       l.Org.ID,
				Name:        "first stack",
				Description: "desc",
				URLs:        []string{"http://example.com"},
			})
			cleanup()
		})

		t.Run("list stacks", func(t *testing.T) {
			newStack1, cleanup1 := newStackFn(t, pkger.Stack{
				Name: "first stack",
			})
			defer cleanup1()

			newStack2, cleanup2 := newStackFn(t, pkger.Stack{
				Name: "second stack",
			})
			defer cleanup2()

			containsStack := func(t *testing.T, haystack []pkger.Stack, needle pkger.Stack) {
				t.Helper()
				for _, hay := range haystack {
					if hay.ID == needle.ID {
						return
					}
				}
				require.FailNowf(t, "did not find expected stack", "got: %+v", needle)
			}

			t.Run("returns all stacks when no filter args provided", func(t *testing.T) {
				stacks, err := svc.ListStacks(ctx, l.Org.ID, pkger.ListFilter{})
				require.NoError(t, err)

				containsStack(t, stacks, newStack1)
				containsStack(t, stacks, newStack2)
			})

			t.Run("filters stacks by ID filter", func(t *testing.T) {
				stacks, err := svc.ListStacks(ctx, l.Org.ID, pkger.ListFilter{
					StackIDs: []influxdb.ID{newStack1.ID},
				})
				require.NoError(t, err)
				require.Len(t, stacks, 1)
				containsStack(t, stacks, newStack1)
			})

			t.Run("filter stacks by names", func(t *testing.T) {
				stacks, err := svc.ListStacks(ctx, l.Org.ID, pkger.ListFilter{
					Names: []string{newStack2.Name},
				})
				require.NoError(t, err)
				require.Len(t, stacks, 1)
				containsStack(t, stacks, newStack2)
			})
		})

		t.Run("delete a stack", func(t *testing.T) {
			t.Run("should delete the stack and all resources associated with it", func(t *testing.T) {
				newStack, cleanup := newStackFn(t, pkger.Stack{})
				defer cleanup()

				newEndpointPkgName := "non-existent-endpoint"
				allResourcesPkg := newPkg(
					newBucketObject("non-existent-bucket", "", ""),
					newCheckDeadmanObject(t, "non-existent-check", "", time.Minute),
					newDashObject("non-existent-dash", "", ""),
					newEndpointHTTP(newEndpointPkgName, "", ""),
					newLabelObject("non-existent-label", "", "", ""),
					newRuleObject(t, "non-existent-rule", "", newEndpointPkgName, ""),
					newTaskObject("non-existent-task", "", ""),
					newTelegrafObject("non-existent-tele", "", ""),
					newVariableObject("non-existent-var", "", ""),
				)

				sum, _, err := svc.Apply(ctx, l.Org.ID, l.User.ID, allResourcesPkg, pkger.ApplyWithStackID(newStack.ID))
				require.NoError(t, err)

				require.Len(t, sum.Buckets, 1)
				assert.NotZero(t, sum.Buckets[0].ID)
				require.Len(t, sum.Checks, 1)
				assert.NotZero(t, sum.Checks[0].Check.GetID())
				require.Len(t, sum.Dashboards, 1)
				assert.NotZero(t, sum.Dashboards[0].ID)
				require.Len(t, sum.Labels, 1)
				assert.NotZero(t, sum.Labels[0].ID)
				require.Len(t, sum.NotificationEndpoints, 1)
				assert.NotZero(t, sum.NotificationEndpoints[0].NotificationEndpoint.GetID())
				require.Len(t, sum.NotificationRules, 1)
				assert.NotZero(t, sum.NotificationRules[0].ID)
				require.Len(t, sum.Tasks, 1)
				assert.NotZero(t, sum.Tasks[0].ID)
				require.Len(t, sum.TelegrafConfigs, 1)
				assert.NotZero(t, sum.TelegrafConfigs[0].TelegrafConfig.ID)
				require.Len(t, sum.Variables, 1)
				assert.NotZero(t, sum.Variables[0].ID)

				err = svc.DeleteStack(ctx, struct{ OrgID, UserID, StackID influxdb.ID }{
					OrgID:   l.Org.ID,
					UserID:  l.User.ID,
					StackID: newStack.ID,
				})
				require.NoError(t, err)

				matchingStacks, err := svc.ListStacks(ctx, l.Org.ID, pkger.ListFilter{
					StackIDs: []influxdb.ID{newStack.ID},
				})
				require.NoError(t, err)
				require.Empty(t, matchingStacks)

				_, err = resourceCheck.getBucket(t, byID(influxdb.ID(sum.Buckets[0].ID)))
				assert.Error(t, err)

				_, err = resourceCheck.getCheck(t, byID(sum.Checks[0].Check.GetID()))
				assert.Error(t, err)

				_, err = resourceCheck.getDashboard(t, byID(influxdb.ID(sum.Dashboards[0].ID)))
				assert.Error(t, err)

				_, err = resourceCheck.getLabel(t, byID(influxdb.ID(sum.Labels[0].ID)))
				assert.Error(t, err)

				_, err = resourceCheck.getEndpoint(t, byID(sum.NotificationEndpoints[0].NotificationEndpoint.GetID()))
				assert.Error(t, err)

				_, err = resourceCheck.getRule(t, byID(influxdb.ID(sum.NotificationRules[0].ID)))
				assert.Error(t, err)

				_, err = resourceCheck.getTask(t, byID(influxdb.ID(sum.Tasks[0].ID)))
				assert.Error(t, err)

				_, err = resourceCheck.getTelegrafConfig(t, byID(sum.TelegrafConfigs[0].TelegrafConfig.ID))
				assert.Error(t, err)

				_, err = resourceCheck.getVariable(t, byID(influxdb.ID(sum.Variables[0].ID)))
				assert.Error(t, err)
			})

			t.Run("that has already been deleted should be successful", func(t *testing.T) {
				newStack, _ := newStackFn(t, pkger.Stack{})

				err := svc.DeleteStack(ctx, struct{ OrgID, UserID, StackID influxdb.ID }{
					OrgID:   l.Org.ID,
					UserID:  l.User.ID,
					StackID: newStack.ID,
				})
				require.NoError(t, err)

				// delete same stack
				err = svc.DeleteStack(ctx, struct{ OrgID, UserID, StackID influxdb.ID }{
					OrgID:   l.Org.ID,
					UserID:  l.User.ID,
					StackID: newStack.ID,
				})
				require.NoError(t, err)
			})

			t.Run("that doesn't exist should be successful", func(t *testing.T) {
				// delete stack that doesn't exist
				err := svc.DeleteStack(ctx, struct{ OrgID, UserID, StackID influxdb.ID }{
					OrgID:   l.Org.ID,
					UserID:  l.User.ID,
					StackID: 9000,
				})
				require.NoError(t, err)
			})
		})

		t.Run("apply with only a stackID succeeds when stack has URLs", func(t *testing.T) {
			svr := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
				pkg := newPkg(newBucketObject("bucket-0", "", ""))
				b, err := pkg.Encode(pkger.EncodingJSON)
				if err != nil {
					w.WriteHeader(nethttp.StatusInternalServerError)
					return
				}
				w.Write(b)
			}))
			defer svr.Close()

			f, err := ioutil.TempFile("", "pkg.yml")
			require.NoError(t, err)
			defer f.Close()

			pkg := newPkg(newBucketObject("bucket-1", "", ""))
			b, err := pkg.Encode(pkger.EncodingYAML)
			require.NoError(t, err)
			f.Write(b)
			require.NoError(t, f.Close())

			expectedURLs := []string{
				// URL for http call
				svr.URL + "/pkg.json",
				// URL for file
				"file://" + f.Name(),
			}

			newStack, cleanup := newStackFn(t, pkger.Stack{
				URLs: expectedURLs,
			})
			defer cleanup()

			sumEquals := func(t *testing.T, sum pkger.Summary) {
				t.Helper()
				require.Len(t, sum.Buckets, 2)
				assert.Equal(t, "bucket-0", sum.Buckets[0].PkgName)
				assert.Equal(t, "bucket-0", sum.Buckets[0].Name)
				assert.Equal(t, "bucket-1", sum.Buckets[1].PkgName)
				assert.Equal(t, "bucket-1", sum.Buckets[1].Name)
			}

			sum, _, err := svc.DryRun(ctx, l.Org.ID, l.User.ID, nil, pkger.ApplyWithStackID(newStack.ID))
			require.NoError(t, err)
			sumEquals(t, sum)

			sum, _, err = svc.Apply(ctx, l.Org.ID, l.User.ID, nil, pkger.ApplyWithStackID(newStack.ID))
			require.NoError(t, err)
			sumEquals(t, sum)
		})

		t.Run("apply a pkg with a stack and associations", func(t *testing.T) {
			testLabelMappingFn := func(t *testing.T, stackID influxdb.ID, pkg *pkger.Pkg, assertAssociatedLabelsFn func(pkger.Summary, []*influxdb.Label, influxdb.ResourceType)) pkger.Summary {
				t.Helper()

				sum, _, err := svc.Apply(ctx, l.Org.ID, l.User.ID, pkg, pkger.ApplyWithStackID(stackID))
				require.NoError(t, err)

				require.Len(t, sum.Buckets, 1)
				assert.NotZero(t, sum.Buckets[0].ID)
				assert.Equal(t, "bucket", sum.Buckets[0].Name)

				require.Len(t, sum.Checks, 1)
				assert.NotZero(t, sum.Checks[0].Check.GetID())
				assert.Equal(t, "check-0", sum.Checks[0].Check.GetName())

				require.Len(t, sum.Dashboards, 1)
				assert.NotZero(t, sum.Dashboards[0].ID)
				assert.Equal(t, "dash-0", sum.Dashboards[0].Name)

				require.Len(t, sum.NotificationEndpoints, 1)
				assert.NotZero(t, sum.NotificationEndpoints[0].NotificationEndpoint.GetID())
				assert.Equal(t, "endpoint-0", sum.NotificationEndpoints[0].NotificationEndpoint.GetName())

				require.Len(t, sum.NotificationRules, 1)
				assert.NotZero(t, sum.NotificationRules[0].ID)
				assert.Equal(t, "rule-0", sum.NotificationRules[0].Name)

				require.Len(t, sum.Labels, 1)
				assert.NotZero(t, sum.Labels[0].ID)
				assert.Equal(t, "label 1", sum.Labels[0].Name)

				require.Len(t, sum.Tasks, 1)
				assert.NotZero(t, sum.Tasks[0].ID)
				assert.Equal(t, "task-0", sum.Tasks[0].Name)

				require.Len(t, sum.TelegrafConfigs, 1)
				assert.NotZero(t, sum.TelegrafConfigs[0].TelegrafConfig.ID)
				assert.Equal(t, "tele-0", sum.TelegrafConfigs[0].TelegrafConfig.Name)

				resources := []struct {
					resID        influxdb.ID
					resourceType influxdb.ResourceType
				}{
					{resID: influxdb.ID(sum.Buckets[0].ID), resourceType: influxdb.BucketsResourceType},
					{resID: sum.Checks[0].Check.GetID(), resourceType: influxdb.ChecksResourceType},
					{resID: influxdb.ID(sum.Dashboards[0].ID), resourceType: influxdb.DashboardsResourceType},
					{resID: sum.NotificationEndpoints[0].NotificationEndpoint.GetID(), resourceType: influxdb.NotificationEndpointResourceType},
					{resID: influxdb.ID(sum.NotificationRules[0].ID), resourceType: influxdb.NotificationRuleResourceType},
					{resID: influxdb.ID(sum.Tasks[0].ID), resourceType: influxdb.TasksResourceType},
					{resID: sum.TelegrafConfigs[0].TelegrafConfig.ID, resourceType: influxdb.TelegrafsResourceType},
					{resID: influxdb.ID(sum.Variables[0].ID), resourceType: influxdb.VariablesResourceType},
				}
				for _, res := range resources {
					mappedLabels, err := l.LabelService(t).FindResourceLabels(ctx, influxdb.LabelMappingFilter{
						ResourceID:   res.resID,
						ResourceType: res.resourceType,
					})
					require.NoError(t, err, "resource_type="+res.resourceType)
					assertAssociatedLabelsFn(sum, mappedLabels, res.resourceType)
				}

				return sum
			}

			newObjectsFn := func() []pkger.Object {
				return []pkger.Object{
					newBucketObject("bucket", "", ""),
					newCheckDeadmanObject(t, "check-0", "", time.Hour),
					newDashObject("dash-0", "", ""),
					newEndpointHTTP("endpoint-0", "", ""),
					newRuleObject(t, "rule-0", "", "endpoint-0", ""),
					newTaskObject("task-0", "", ""),
					newTelegrafObject("tele-0", "", ""),
					newVariableObject("var-0", "", ""),
				}
			}
			labelObj := newLabelObject("label-1", "label 1", "", "")

			testAssociationFn := func(t *testing.T) (pkger.Summary, pkger.Stack, func()) {
				t.Helper()

				stack, cleanup := newStackFn(t, pkger.Stack{})
				defer func() {
					if t.Failed() {
						// if test fails in setup, then we attempt to clean it up
						// so it doesn't pollute other tests
						cleanup()
					}
				}()

				pkgObjects := newObjectsFn()
				for _, obj := range pkgObjects {
					obj.AddAssociations(pkger.ObjectAssociation{
						Kind:    pkger.KindLabel,
						PkgName: labelObj.Name(),
					})
				}
				pkgObjects = append(pkgObjects, labelObj)

				pkg := newPkg(pkgObjects...)

				sum := testLabelMappingFn(t, stack.ID, pkg, func(sum pkger.Summary, mappedLabels []*influxdb.Label, resType influxdb.ResourceType) {
					require.Len(t, mappedLabels, 1, "resource_type="+resType)
					assert.Equal(t, sum.Labels[0].ID, pkger.SafeID(mappedLabels[0].ID))
				})

				return sum, stack, cleanup
			}

			t.Run("should associate resources with labels", func(t *testing.T) {
				_, _, cleanup := testAssociationFn(t)
				cleanup()
			})

			t.Run("should rollback to previous state when errors in creation", func(t *testing.T) {
				sum, stack, cleanup := testAssociationFn(t)
				defer cleanup()

				logger := l.log.With(zap.String("service", "pkger"))
				var svc pkger.SVC = pkger.NewService(
					pkger.WithLogger(logger),
					pkger.WithBucketSVC(l.BucketService(t)),
					pkger.WithDashboardSVC(l.DashboardService(t)),
					pkger.WithCheckSVC(l.CheckService()),
					pkger.WithLabelSVC(&fakeLabelSVC{
						// can't use the LabelService HTTP client b/c it doesn't cover the
						// all the resources pkger supports... :sadpanda:
						LabelService:    l.kvService,
						createKillCount: -1,
						deleteKillCount: 3,
					}),
					pkger.WithNotificationEndpointSVC(l.NotificationEndpointService(t)),
					pkger.WithNotificationRuleSVC(l.NotificationRuleService()),
					pkger.WithStore(pkger.NewStoreKV(l.Launcher.kvStore)),
					pkger.WithTaskSVC(l.TaskServiceKV()),
					pkger.WithTelegrafSVC(l.TelegrafService(t)),
					pkger.WithVariableSVC(l.VariableService(t)),
				)
				svc = pkger.MWLogging(logger)(svc)

				pkg := newPkg(append(newObjectsFn(), labelObj)...)
				_, _, err := svc.Apply(ctx, l.Org.ID, l.User.ID, pkg, pkger.ApplyWithStackID(stack.ID))
				require.Error(t, err)

				resources := []struct {
					resID        influxdb.ID
					resourceType influxdb.ResourceType
				}{
					{resID: influxdb.ID(sum.Buckets[0].ID), resourceType: influxdb.BucketsResourceType},
					{resID: sum.Checks[0].Check.GetID(), resourceType: influxdb.ChecksResourceType},
					{resID: influxdb.ID(sum.Dashboards[0].ID), resourceType: influxdb.DashboardsResourceType},
					{resID: sum.NotificationEndpoints[0].NotificationEndpoint.GetID(), resourceType: influxdb.NotificationEndpointResourceType},
					{resID: influxdb.ID(sum.NotificationRules[0].ID), resourceType: influxdb.NotificationRuleResourceType},
					{resID: influxdb.ID(sum.Tasks[0].ID), resourceType: influxdb.TasksResourceType},
					{resID: sum.TelegrafConfigs[0].TelegrafConfig.ID, resourceType: influxdb.TelegrafsResourceType},
					{resID: influxdb.ID(sum.Variables[0].ID), resourceType: influxdb.VariablesResourceType},
				}
				for _, res := range resources {
					mappedLabels, err := l.LabelService(t).FindResourceLabels(ctx, influxdb.LabelMappingFilter{
						ResourceID:   res.resID,
						ResourceType: res.resourceType,
					})
					require.NoError(t, err)
					assert.Len(t, mappedLabels, 1, res.resourceType)
					if len(mappedLabels) == 1 {
						assert.Equal(t, sum.Labels[0].ID, pkger.SafeID(mappedLabels[0].ID))
					}
				}
			})

			t.Run("should unassociate resources from label when removing association in pkg", func(t *testing.T) {
				_, stack, cleanup := testAssociationFn(t)
				defer cleanup()

				objects := newObjectsFn()
				pkg := newPkg(append(objects, labelObj)...)

				testLabelMappingFn(t, stack.ID, pkg, func(sum pkger.Summary, mappedLabels []*influxdb.Label, resType influxdb.ResourceType) {
					assert.Empty(t, mappedLabels, "res_type="+resType)
				})
			})
		})

		t.Run("apply a pkg with a stack", func(t *testing.T) {
			testStackApplyFn := func(t *testing.T) (pkger.Summary, pkger.Stack, func()) {
				t.Helper()

				stack, cleanup := newStackFn(t, pkger.Stack{})
				defer func() {
					if t.Failed() {
						cleanup()
					}
				}()

				var (
					initialBucketPkgName   = "rucketeer-1"
					initialCheckPkgName    = "checkers"
					initialDashPkgName     = "dash-of-salt"
					initialEndpointPkgName = "endzo"
					initialLabelPkgName    = "labelino"
					initialRulePkgName     = "oh-doyle-rules"
					initialTaskPkgName     = "tap"
					initialTelegrafPkgName = "teletype"
					initialVariablePkgName = "laces-out-dan"
				)
				initialPkg := newPkg(
					newBucketObject(initialBucketPkgName, "display name", "init desc"),
					newCheckDeadmanObject(t, initialCheckPkgName, "check_0", time.Minute),
					newDashObject(initialDashPkgName, "dash_0", "init desc"),
					newEndpointHTTP(initialEndpointPkgName, "endpoint_0", "init desc"),
					newLabelObject(initialLabelPkgName, "label 1", "init desc", "#222eee"),
					newRuleObject(t, initialRulePkgName, "rule_0", initialEndpointPkgName, "init desc"),
					newTaskObject(initialTaskPkgName, "task_0", "init desc"),
					newTelegrafObject(initialTelegrafPkgName, "tele_0", "init desc"),
					newVariableObject(initialVariablePkgName, "var char", "init desc"),
				)

				summary, _, err := svc.Apply(ctx, l.Org.ID, l.User.ID, initialPkg, pkger.ApplyWithStackID(stack.ID))
				require.NoError(t, err)

				require.Len(t, summary.Buckets, 1)
				assert.NotZero(t, summary.Buckets[0].ID)
				assert.Equal(t, "display name", summary.Buckets[0].Name)
				assert.Equal(t, "init desc", summary.Buckets[0].Description)

				require.Len(t, summary.Checks, 1)
				assert.NotZero(t, summary.Checks[0].Check.GetID())
				assert.Equal(t, "check_0", summary.Checks[0].Check.GetName())

				require.Len(t, summary.Dashboards, 1)
				assert.NotZero(t, summary.Dashboards[0].ID)
				assert.Equal(t, "dash_0", summary.Dashboards[0].Name)

				require.Len(t, summary.NotificationEndpoints, 1)
				assert.NotZero(t, summary.NotificationEndpoints[0].NotificationEndpoint.GetID())
				assert.Equal(t, "endpoint_0", summary.NotificationEndpoints[0].NotificationEndpoint.GetName())

				require.Len(t, summary.Labels, 1)
				assert.NotZero(t, summary.Labels[0].ID)
				assert.Equal(t, "label 1", summary.Labels[0].Name)
				assert.Equal(t, "init desc", summary.Labels[0].Properties.Description)
				assert.Equal(t, "#222eee", summary.Labels[0].Properties.Color)

				require.Len(t, summary.NotificationRules, 1)
				assert.NotZero(t, summary.NotificationRules[0].ID)
				assert.Equal(t, "rule_0", summary.NotificationRules[0].Name)
				assert.Equal(t, initialEndpointPkgName, summary.NotificationRules[0].EndpointPkgName)
				assert.Equal(t, "init desc", summary.NotificationRules[0].Description)

				require.Len(t, summary.Tasks, 1)
				assert.NotZero(t, summary.Tasks[0].ID)
				assert.Equal(t, "task_0", summary.Tasks[0].Name)
				assert.Equal(t, "init desc", summary.Tasks[0].Description)

				require.Len(t, summary.TelegrafConfigs, 1)
				assert.NotZero(t, summary.TelegrafConfigs[0].TelegrafConfig.ID)
				assert.Equal(t, "tele_0", summary.TelegrafConfigs[0].TelegrafConfig.Name)
				assert.Equal(t, "init desc", summary.TelegrafConfigs[0].TelegrafConfig.Description)

				require.Len(t, summary.Variables, 1)
				assert.NotZero(t, summary.Variables[0].ID)
				assert.Equal(t, "var char", summary.Variables[0].Name)
				assert.Equal(t, "init desc", summary.Variables[0].Description)

				t.Log("\tverify changes reflected in platform")
				{
					actualBkt := resourceCheck.mustGetBucket(t, byName("display name"))
					assert.Equal(t, summary.Buckets[0].ID, pkger.SafeID(actualBkt.ID))

					actualCheck := resourceCheck.mustGetCheck(t, byName("check_0"))
					assert.Equal(t, summary.Checks[0].Check.GetID(), actualCheck.GetID())

					actualDash := resourceCheck.mustGetDashboard(t, byName("dash_0"))
					assert.Equal(t, summary.Dashboards[0].ID, pkger.SafeID(actualDash.ID))

					actualEndpint := resourceCheck.mustGetEndpoint(t, byName("endpoint_0"))
					assert.Equal(t, summary.NotificationEndpoints[0].NotificationEndpoint.GetID(), actualEndpint.GetID())

					actualLabel := resourceCheck.mustGetLabel(t, byName("label 1"))
					assert.Equal(t, summary.Labels[0].ID, pkger.SafeID(actualLabel.ID))

					actualRule := resourceCheck.mustGetRule(t, byName("rule_0"))
					assert.Equal(t, summary.NotificationRules[0].ID, pkger.SafeID(actualRule.GetID()))

					actualTask := resourceCheck.mustGetTask(t, byName("task_0"))
					assert.Equal(t, summary.Tasks[0].ID, pkger.SafeID(actualTask.ID))

					actualTele := resourceCheck.mustGetTelegrafConfig(t, byName("tele_0"))
					assert.Equal(t, summary.TelegrafConfigs[0].TelegrafConfig.ID, actualTele.ID)

					actualVar := resourceCheck.mustGetVariable(t, byName("var char"))
					assert.Equal(t, summary.Variables[0].ID, pkger.SafeID(actualVar.ID))
				}

				return summary, stack, cleanup
			}

			t.Run("apply pkg with stack id", func(t *testing.T) {
				_, _, cleanup := testStackApplyFn(t)
				cleanup()
			})

			t.Run("apply pkg with stack id where resources change", func(t *testing.T) {
				initialSum, stack, cleanup := testStackApplyFn(t)
				defer cleanup()

				var (
					updateBucketName   = "new bucket"
					updateCheckName    = "new check"
					updateDashName     = "new dash"
					updateEndpointName = "new endpoint"
					updateLabelName    = "new label"
					updateRuleName     = "new rule"
					updateTaskName     = "new task"
					updateTelegrafName = "new telegraf"
					updateVariableName = "new variable"
				)
				updatedPkg := newPkg(
					newBucketObject(initialSum.Buckets[0].PkgName, updateBucketName, ""),
					newCheckDeadmanObject(t, initialSum.Checks[0].PkgName, updateCheckName, time.Hour),
					newDashObject(initialSum.Dashboards[0].PkgName, updateDashName, ""),
					newEndpointHTTP(initialSum.NotificationEndpoints[0].PkgName, updateEndpointName, ""),
					newLabelObject(initialSum.Labels[0].PkgName, updateLabelName, "", ""),
					newRuleObject(
						t,
						initialSum.NotificationRules[0].PkgName,
						updateRuleName,
						initialSum.NotificationEndpoints[0].PkgName,
						"",
					),
					newTaskObject(initialSum.Tasks[0].PkgName, updateTaskName, ""),
					newTelegrafObject(initialSum.TelegrafConfigs[0].PkgName, updateTelegrafName, ""),
					newVariableObject(initialSum.Variables[0].PkgName, updateVariableName, ""),
				)
				sum, _, err := svc.Apply(ctx, l.Org.ID, l.User.ID, updatedPkg, pkger.ApplyWithStackID(stack.ID))
				require.NoError(t, err)

				require.Len(t, sum.Buckets, 1)
				assert.Equal(t, initialSum.Buckets[0].ID, sum.Buckets[0].ID)
				assert.Equal(t, updateBucketName, sum.Buckets[0].Name)

				require.Len(t, sum.Checks, 1)
				assert.Equal(t, initialSum.Checks[0].Check.GetID(), sum.Checks[0].Check.GetID())
				assert.Equal(t, updateCheckName, sum.Checks[0].Check.GetName())

				require.Len(t, sum.Dashboards, 1)
				assert.Equal(t, initialSum.Dashboards[0].ID, sum.Dashboards[0].ID)
				assert.Equal(t, updateDashName, sum.Dashboards[0].Name)

				require.Len(t, sum.NotificationEndpoints, 1)
				sumEndpoint := sum.NotificationEndpoints[0].NotificationEndpoint
				assert.Equal(t, initialSum.NotificationEndpoints[0].NotificationEndpoint.GetID(), sumEndpoint.GetID())
				assert.Equal(t, updateEndpointName, sumEndpoint.GetName())

				require.Len(t, sum.NotificationRules, 1)
				sumRule := sum.NotificationRules[0]
				assert.Equal(t, initialSum.NotificationRules[0].ID, sumRule.ID)
				assert.Equal(t, updateRuleName, sumRule.Name)

				require.Len(t, sum.Labels, 1)
				assert.Equal(t, initialSum.Labels[0].ID, sum.Labels[0].ID)
				assert.Equal(t, updateLabelName, sum.Labels[0].Name)

				require.Len(t, sum.Tasks, 1)
				assert.Equal(t, initialSum.Tasks[0].ID, sum.Tasks[0].ID)
				assert.Equal(t, updateTaskName, sum.Tasks[0].Name)

				require.Len(t, sum.TelegrafConfigs, 1)
				updatedTele := sum.TelegrafConfigs[0].TelegrafConfig
				assert.Equal(t, initialSum.TelegrafConfigs[0].TelegrafConfig.ID, updatedTele.ID)
				assert.Equal(t, updateTelegrafName, updatedTele.Name)

				require.Len(t, sum.Variables, 1)
				assert.Equal(t, initialSum.Variables[0].ID, sum.Variables[0].ID)
				assert.Equal(t, updateVariableName, sum.Variables[0].Name)

				t.Log("\tverify changes reflected in platform")
				{
					actualBkt := resourceCheck.mustGetBucket(t, byName(updateBucketName))
					require.Equal(t, initialSum.Buckets[0].ID, pkger.SafeID(actualBkt.ID))

					actualCheck := resourceCheck.mustGetCheck(t, byName(updateCheckName))
					require.Equal(t, initialSum.Checks[0].Check.GetID(), actualCheck.GetID())

					actualDash := resourceCheck.mustGetDashboard(t, byName(updateDashName))
					require.Equal(t, initialSum.Dashboards[0].ID, pkger.SafeID(actualDash.ID))

					actualEndpoint := resourceCheck.mustGetEndpoint(t, byName(updateEndpointName))
					assert.Equal(t, sumEndpoint.GetID(), actualEndpoint.GetID())

					actualLabel := resourceCheck.mustGetLabel(t, byName(updateLabelName))
					require.Equal(t, initialSum.Labels[0].ID, pkger.SafeID(actualLabel.ID))

					actualRule := resourceCheck.mustGetRule(t, byName(updateRuleName))
					require.Equal(t, initialSum.NotificationRules[0].ID, pkger.SafeID(actualRule.GetID()))

					actualTask := resourceCheck.mustGetTask(t, byName(updateTaskName))
					require.Equal(t, initialSum.Tasks[0].ID, pkger.SafeID(actualTask.ID))

					actualTelegraf := resourceCheck.mustGetTelegrafConfig(t, byName(updateTelegrafName))
					require.Equal(t, initialSum.TelegrafConfigs[0].TelegrafConfig.ID, actualTelegraf.ID)

					actualVar := resourceCheck.mustGetVariable(t, byName(updateVariableName))
					assert.Equal(t, sum.Variables[0].ID, pkger.SafeID(actualVar.ID))
				}
			})

			t.Run("an error during application roles back resources to previous state", func(t *testing.T) {
				initialSum, stack, cleanup := testStackApplyFn(t)
				defer cleanup()

				logger := l.log.With(zap.String("service", "pkger"))
				var svc pkger.SVC = pkger.NewService(
					pkger.WithLogger(logger),
					pkger.WithBucketSVC(l.BucketService(t)),
					pkger.WithDashboardSVC(l.DashboardService(t)),
					pkger.WithCheckSVC(l.CheckService()),
					pkger.WithLabelSVC(l.LabelService(t)),
					pkger.WithNotificationEndpointSVC(l.NotificationEndpointService(t)),
					pkger.WithNotificationRuleSVC(&fakeRuleStore{
						NotificationRuleStore: l.NotificationRuleService(),
						createKillCount:       2,
					}),
					pkger.WithStore(pkger.NewStoreKV(l.Launcher.kvStore)),
					pkger.WithTaskSVC(l.TaskServiceKV()),
					pkger.WithTelegrafSVC(l.TelegrafService(t)),
					pkger.WithVariableSVC(l.VariableService(t)),
				)
				svc = pkger.MWLogging(logger)(svc)

				endpointPkgName := "z-endpoint-rolls-back"

				pkgWithDelete := newPkg(
					newBucketObject("z-roll-me-back", "", ""),
					newBucketObject("z-rolls-back-too", "", ""),
					newDashObject("z-rolls-dash", "", ""),
					newLabelObject("z-label-roller", "", "", ""),
					newCheckDeadmanObject(t, "z-check", "", time.Hour),
					newEndpointHTTP(endpointPkgName, "", ""),
					newRuleObject(t, "z-rules-back", "", endpointPkgName, ""),
					newRuleObject(t, "z-rules-back-2", "", endpointPkgName, ""),
					newRuleObject(t, "z-rules-back-3", "", endpointPkgName, ""),
					newTaskObject("z-task-rolls-back", "", ""),
					newTelegrafObject("z-telegraf-rolls-back", "", ""),
					newVariableObject("z-var-rolls-back", "", ""),
				)
				_, _, err := svc.Apply(ctx, l.Org.ID, l.User.ID, pkgWithDelete, pkger.ApplyWithStackID(stack.ID))
				require.Error(t, err)

				t.Log("validate all resources are rolled back")
				{
					actualBkt := resourceCheck.mustGetBucket(t, byName(initialSum.Buckets[0].Name))
					assert.NotEqual(t, initialSum.Buckets[0].ID, pkger.SafeID(actualBkt.ID))

					actualCheck := resourceCheck.mustGetCheck(t, byName(initialSum.Checks[0].Check.GetName()))
					assert.NotEqual(t, initialSum.Checks[0].Check.GetID(), actualCheck.GetID())

					actualDash := resourceCheck.mustGetDashboard(t, byName(initialSum.Dashboards[0].Name))
					assert.NotEqual(t, initialSum.Dashboards[0].ID, pkger.SafeID(actualDash.ID))

					actualEndpoint := resourceCheck.mustGetEndpoint(t, byName(initialSum.NotificationEndpoints[0].NotificationEndpoint.GetName()))
					assert.NotEqual(t, initialSum.NotificationEndpoints[0].NotificationEndpoint.GetID(), actualEndpoint.GetID())

					actualRule := resourceCheck.mustGetRule(t, byName(initialSum.NotificationRules[0].Name))
					assert.NotEqual(t, initialSum.NotificationRules[0].ID, pkger.SafeID(actualRule.GetID()))

					actualLabel := resourceCheck.mustGetLabel(t, byName(initialSum.Labels[0].Name))
					assert.NotEqual(t, initialSum.Labels[0].ID, pkger.SafeID(actualLabel.ID))

					actualTask := resourceCheck.mustGetTask(t, byName(initialSum.Tasks[0].Name))
					assert.NotEqual(t, initialSum.Tasks[0].ID, pkger.SafeID(actualTask.ID))

					actualTelegraf := resourceCheck.mustGetTelegrafConfig(t, byName(initialSum.TelegrafConfigs[0].TelegrafConfig.Name))
					assert.NotEqual(t, initialSum.TelegrafConfigs[0].TelegrafConfig.ID, actualTelegraf.ID)

					actualVariable := resourceCheck.mustGetVariable(t, byName(initialSum.Variables[0].Name))
					assert.NotEqual(t, initialSum.Variables[0].ID, pkger.SafeID(actualVariable.ID))
				}

				t.Log("validate all changes do not persist")
				{
					for _, name := range []string{"z-roll-me-back", "z-rolls-back-too"} {
						_, err := resourceCheck.getBucket(t, byName(name))
						assert.Error(t, err)
					}

					for _, name := range []string{"z-rules-back", "z-rules-back-2", "z-rules-back-3"} {
						_, err = resourceCheck.getRule(t, byName(name))
						assert.Error(t, err)
					}

					_, err := resourceCheck.getCheck(t, byName("z-check"))
					assert.Error(t, err)

					_, err = resourceCheck.getDashboard(t, byName("z-rolls_dash"))
					assert.Error(t, err)

					_, err = resourceCheck.getEndpoint(t, byName("z-endpoint-rolls-back"))
					assert.Error(t, err)

					_, err = resourceCheck.getLabel(t, byName("z-label-roller"))
					assert.Error(t, err)

					_, err = resourceCheck.getTelegrafConfig(t, byName("z-telegraf-rolls-back"))
					assert.Error(t, err)

					_, err = resourceCheck.getVariable(t, byName("z-var-rolls-back"))
					assert.Error(t, err)
				}
			})

			t.Run("apply pkg with stack id where resources have been removed since last run", func(t *testing.T) {
				initialSum, stack, cleanup := testStackApplyFn(t)
				defer cleanup()

				newEndpointPkgName := "non-existent-endpoint"
				allNewResourcesPkg := newPkg(
					newBucketObject("non-existent-bucket", "", ""),
					newCheckDeadmanObject(t, "non-existent-check", "", time.Minute),
					newDashObject("non-existent-dash", "", ""),
					newEndpointHTTP(newEndpointPkgName, "", ""),
					newLabelObject("non-existent-label", "", "", ""),
					newRuleObject(t, "non-existent-rule", "", newEndpointPkgName, ""),
					newTaskObject("non-existent-task", "", ""),
					newTelegrafObject("non-existent-tele", "", ""),
					newVariableObject("non-existent-var", "", ""),
				)
				sum, _, err := svc.Apply(ctx, l.Org.ID, l.User.ID, allNewResourcesPkg, pkger.ApplyWithStackID(stack.ID))
				require.NoError(t, err)

				require.Len(t, sum.Buckets, 1)
				assert.NotEqual(t, initialSum.Buckets[0].ID, sum.Buckets[0].ID)
				assert.NotZero(t, sum.Buckets[0].ID)
				assert.Equal(t, "non-existent-bucket", sum.Buckets[0].Name)

				require.Len(t, sum.Checks, 1)
				assert.NotEqual(t, initialSum.Checks[0].Check.GetID(), sum.Checks[0].Check.GetID())
				assert.NotZero(t, sum.Checks[0].Check.GetID())
				assert.Equal(t, "non-existent-check", sum.Checks[0].Check.GetName())

				require.Len(t, sum.Dashboards, 1)
				assert.NotEqual(t, initialSum.Dashboards[0].ID, sum.Dashboards[0].ID)
				assert.NotZero(t, sum.Dashboards[0].ID)
				assert.Equal(t, "non-existent-dash", sum.Dashboards[0].Name)

				require.Len(t, sum.NotificationEndpoints, 1)
				sumEndpoint := sum.NotificationEndpoints[0].NotificationEndpoint
				assert.NotEqual(t, initialSum.NotificationEndpoints[0].NotificationEndpoint.GetID(), sumEndpoint.GetID())
				assert.NotZero(t, sumEndpoint.GetID())
				assert.Equal(t, newEndpointPkgName, sumEndpoint.GetName())

				require.Len(t, sum.NotificationRules, 1)
				sumRule := sum.NotificationRules[0]
				assert.NotEqual(t, initialSum.NotificationRules[0].ID, sumRule.ID)
				assert.NotZero(t, sumRule.ID)
				assert.Equal(t, "non-existent-rule", sumRule.Name)

				require.Len(t, sum.Labels, 1)
				assert.NotEqual(t, initialSum.Labels[0].ID, sum.Labels[0].ID)
				assert.NotZero(t, sum.Labels[0].ID)
				assert.Equal(t, "non-existent-label", sum.Labels[0].Name)

				require.Len(t, sum.Tasks, 1)
				assert.NotEqual(t, initialSum.Tasks[0].ID, sum.Tasks[0].ID)
				assert.NotZero(t, sum.Tasks[0].ID)
				assert.Equal(t, "non-existent-task", sum.Tasks[0].Name)

				require.Len(t, sum.TelegrafConfigs, 1)
				newTele := sum.TelegrafConfigs[0].TelegrafConfig
				assert.NotEqual(t, initialSum.TelegrafConfigs[0].TelegrafConfig.ID, newTele.ID)
				assert.NotZero(t, newTele.ID)
				assert.Equal(t, "non-existent-tele", newTele.Name)

				require.Len(t, sum.Variables, 1)
				assert.NotEqual(t, initialSum.Variables[0].ID, sum.Variables[0].ID)
				assert.NotZero(t, sum.Variables[0].ID)
				assert.Equal(t, "non-existent-var", sum.Variables[0].Name)

				t.Log("\tvalidate all resources are created")
				{
					bkt := resourceCheck.mustGetBucket(t, byName("non-existent-bucket"))
					assert.Equal(t, pkger.SafeID(bkt.ID), sum.Buckets[0].ID)

					chk := resourceCheck.mustGetCheck(t, byName("non-existent-check"))
					assert.Equal(t, chk.GetID(), sum.Checks[0].Check.GetID())

					endpoint := resourceCheck.mustGetEndpoint(t, byName(newEndpointPkgName))
					assert.Equal(t, endpoint.GetID(), sum.NotificationEndpoints[0].NotificationEndpoint.GetID())

					label := resourceCheck.mustGetLabel(t, byName("non-existent-label"))
					assert.Equal(t, pkger.SafeID(label.ID), sum.Labels[0].ID)

					actualRule := resourceCheck.mustGetRule(t, byName("non-existent-rule"))
					assert.Equal(t, pkger.SafeID(actualRule.GetID()), sum.NotificationRules[0].ID)

					task := resourceCheck.mustGetTask(t, byName("non-existent-task"))
					assert.Equal(t, pkger.SafeID(task.ID), sum.Tasks[0].ID)

					tele := resourceCheck.mustGetTelegrafConfig(t, byName("non-existent-tele"))
					assert.Equal(t, tele.ID, sum.TelegrafConfigs[0].TelegrafConfig.ID)

					variable := resourceCheck.mustGetVariable(t, byName("non-existent-var"))
					assert.Equal(t, pkger.SafeID(variable.ID), sum.Variables[0].ID)
				}

				t.Log("\tvalidate all previous resources are removed")
				{
					_, err = resourceCheck.getBucket(t, byName(initialSum.Buckets[0].Name))
					require.Error(t, err)

					_, err = resourceCheck.getCheck(t, byName(initialSum.Checks[0].Check.GetName()))
					require.Error(t, err)

					_, err = resourceCheck.getEndpoint(t, byName(initialSum.NotificationEndpoints[0].NotificationEndpoint.GetName()))
					require.Error(t, err)

					_, err = resourceCheck.getLabel(t, byName(initialSum.Labels[0].Name))
					require.Error(t, err)

					_, err = resourceCheck.getTask(t, byName(initialSum.Tasks[0].Name))
					require.Error(t, err)

					_, err = resourceCheck.getTelegrafConfig(t, byName(initialSum.TelegrafConfigs[0].TelegrafConfig.Name))
					require.Error(t, err)

					_, err = resourceCheck.getVariable(t, byName(initialSum.Variables[0].Name))
					require.Error(t, err)
				}
			})
		})
	})

	t.Run("errors incurred during application of package rolls back to state before package", func(t *testing.T) {
		svc := pkger.NewService(
			pkger.WithBucketSVC(l.BucketService(t)),
			pkger.WithDashboardSVC(l.DashboardService(t)),
			pkger.WithCheckSVC(l.CheckService()),
			pkger.WithLabelSVC(&fakeLabelSVC{
				LabelService:    l.LabelService(t),
				createKillCount: 2, // hits error on 3rd attempt at creating a mapping
			}),
			pkger.WithNotificationEndpointSVC(l.NotificationEndpointService(t)),
			pkger.WithNotificationRuleSVC(l.NotificationRuleService()),
			pkger.WithTaskSVC(l.TaskServiceKV()),
			pkger.WithTelegrafSVC(l.TelegrafService(t)),
			pkger.WithVariableSVC(l.VariableService(t)),
		)

		_, _, err := svc.Apply(ctx, l.Org.ID, l.User.ID, newPkg(t))
		require.Error(t, err)

		bkts, _, err := l.BucketService(t).FindBuckets(ctx, influxdb.BucketFilter{OrganizationID: &l.Org.ID})
		require.NoError(t, err)
		for _, b := range bkts {
			if influxdb.BucketTypeSystem == b.Type {
				continue
			}
			// verify system buckets and org bucket are the buckets available
			assert.Equal(t, l.Bucket.Name, b.Name)
		}

		labels, err := l.LabelService(t).FindLabels(ctx, influxdb.LabelFilter{OrgID: &l.Org.ID})
		require.NoError(t, err)
		assert.Empty(t, labels)

		dashs, _, err := l.DashboardService(t).FindDashboards(ctx, influxdb.DashboardFilter{
			OrganizationID: &l.Org.ID,
		}, influxdb.DefaultDashboardFindOptions)
		require.NoError(t, err)
		assert.Empty(t, dashs)

		endpoints, _, err := l.NotificationEndpointService(t).FindNotificationEndpoints(ctx, influxdb.NotificationEndpointFilter{
			OrgID: &l.Org.ID,
		})
		require.NoError(t, err)
		assert.Empty(t, endpoints)

		rules, _, err := l.NotificationRuleService().FindNotificationRules(ctx, influxdb.NotificationRuleFilter{
			OrgID: &l.Org.ID,
		})
		require.NoError(t, err)
		assert.Empty(t, rules)

		tasks, _, err := l.TaskServiceKV().FindTasks(ctx, influxdb.TaskFilter{
			OrganizationID: &l.Org.ID,
		})
		require.NoError(t, err)
		assert.Empty(t, tasks)

		teles, _, err := l.TelegrafService(t).FindTelegrafConfigs(ctx, influxdb.TelegrafConfigFilter{
			OrgID: &l.Org.ID,
		})
		require.NoError(t, err)
		assert.Empty(t, teles)

		vars, err := l.VariableService(t).FindVariables(ctx, influxdb.VariableFilter{OrganizationID: &l.Org.ID})
		require.NoError(t, err)
		assert.Empty(t, vars)
	})

	hasLabelAssociations := func(t *testing.T, associations []pkger.SummaryLabel, numAss int, expectedNames ...string) {
		t.Helper()
		hasAss := func(t *testing.T, expected string) {
			t.Helper()
			for _, ass := range associations {
				if ass.Name == expected {
					return
				}
			}
			require.FailNow(t, "did not find expected association: "+expected)
		}

		require.Len(t, associations, numAss)
		for _, expected := range expectedNames {
			hasAss(t, expected)
		}
	}

	t.Run("dry run a package with no existing resources", func(t *testing.T) {
		sum, diff, err := svc.DryRun(ctx, l.Org.ID, l.User.ID, newPkg(t))
		require.NoError(t, err)

		require.Len(t, diff.Buckets, 1)
		assert.True(t, diff.Buckets[0].IsNew())

		require.Len(t, diff.Checks, 2)
		for _, ch := range diff.Checks {
			assert.True(t, ch.IsNew())
		}

		require.Len(t, diff.Labels, 2)
		assert.True(t, diff.Labels[0].IsNew())
		assert.True(t, diff.Labels[1].IsNew())

		require.Len(t, diff.Variables, 1)
		assert.True(t, diff.Variables[0].IsNew())

		require.Len(t, diff.NotificationRules, 1)
		// the pkg being run here has a relationship with the rule and the endpoint within the pkg.
		assert.Equal(t, "http", diff.NotificationRules[0].New.EndpointType)

		require.Len(t, diff.Dashboards, 1)
		require.Len(t, diff.NotificationEndpoints, 1)
		require.Len(t, diff.Tasks, 1)
		require.Len(t, diff.Telegrafs, 1)

		labels := sum.Labels
		require.Len(t, labels, 2)
		assert.Equal(t, "label-1", labels[0].Name)
		assert.Equal(t, "the 2nd label", labels[1].Name)

		bkts := sum.Buckets
		require.Len(t, bkts, 1)
		assert.Equal(t, "rucketeer", bkts[0].Name)
		hasLabelAssociations(t, bkts[0].LabelAssociations, 2, "label-1", "the 2nd label")

		checks := sum.Checks
		require.Len(t, checks, 2)
		assert.Equal(t, "check 0 name", checks[0].Check.GetName())
		hasLabelAssociations(t, checks[0].LabelAssociations, 1, "label-1")
		assert.Equal(t, "check-1", checks[1].Check.GetName())
		hasLabelAssociations(t, checks[1].LabelAssociations, 1, "label-1")

		dashs := sum.Dashboards
		require.Len(t, dashs, 1)
		assert.Equal(t, "dash_1", dashs[0].Name)
		assert.Equal(t, "desc1", dashs[0].Description)
		hasLabelAssociations(t, dashs[0].LabelAssociations, 2, "label-1", "the 2nd label")

		endpoints := sum.NotificationEndpoints
		require.Len(t, endpoints, 1)
		assert.Equal(t, "no auth endpoint", endpoints[0].NotificationEndpoint.GetName())
		assert.Equal(t, "http none auth desc", endpoints[0].NotificationEndpoint.GetDescription())
		hasLabelAssociations(t, endpoints[0].LabelAssociations, 1, "label-1")

		require.Len(t, sum.Tasks, 1)
		task := sum.Tasks[0]
		assert.Equal(t, "task_1", task.Name)
		assert.Equal(t, "desc_1", task.Description)
		assert.Equal(t, "15 * * * *", task.Cron)
		hasLabelAssociations(t, task.LabelAssociations, 1, "label-1")

		teles := sum.TelegrafConfigs
		require.Len(t, teles, 1)
		assert.Equal(t, "first tele config", teles[0].TelegrafConfig.Name)
		assert.Equal(t, "desc", teles[0].TelegrafConfig.Description)
		hasLabelAssociations(t, teles[0].LabelAssociations, 1, "label-1")

		vars := sum.Variables
		require.Len(t, vars, 1)
		assert.Equal(t, "query var", vars[0].Name)
		hasLabelAssociations(t, vars[0].LabelAssociations, 1, "label-1")
		varArgs := vars[0].Arguments
		require.NotNil(t, varArgs)
		assert.Equal(t, "query", varArgs.Type)
		assert.Equal(t, influxdb.VariableQueryValues{
			Query:    "buckets()  |> filter(fn: (r) => r.name !~ /^_/)  |> rename(columns: {name: \"_value\"})  |> keep(columns: [\"_value\"])",
			Language: "flux",
		}, varArgs.Values)
	})

	t.Run("dry run package with env ref", func(t *testing.T) {
		pkgStr := fmt.Sprintf(`
apiVersion: %[1]s
kind: Label
metadata:
  name:
    envRef:
      key: label-1-name-ref
spec:
---
apiVersion: %[1]s
kind: Bucket
metadata:
  name:
    envRef:
      key: bkt-1-name-ref
spec:
  associations:
    - kind: Label
      name:
        envRef:
          key: label-1-name-ref
`, pkger.APIVersion)

		pkg, err := pkger.Parse(pkger.EncodingYAML, pkger.FromString(pkgStr))
		require.NoError(t, err)

		sum, _, err := svc.DryRun(ctx, l.Org.ID, l.User.ID, pkg, pkger.ApplyWithEnvRefs(map[string]string{
			"bkt-1-name-ref":   "new-bkt-name",
			"label-1-name-ref": "new-label-name",
		}))
		require.NoError(t, err)

		require.Len(t, sum.Buckets, 1)
		assert.Equal(t, "new-bkt-name", sum.Buckets[0].Name)

		require.Len(t, sum.Labels, 1)
		assert.Equal(t, "new-label-name", sum.Labels[0].Name)
	})

	t.Run("apply a package of all new resources", func(t *testing.T) {
		// this initial test is also setup for the sub tests
		sum1, _, err := svc.Apply(ctx, l.Org.ID, l.User.ID, newPkg(t))
		require.NoError(t, err)

		labels := sum1.Labels
		require.Len(t, labels, 2)
		assert.NotZero(t, labels[0].ID)
		assert.Equal(t, "label-1", labels[0].Name)
		assert.Equal(t, "the 2nd label", labels[1].Name)

		bkts := sum1.Buckets
		require.Len(t, bkts, 1)
		assert.NotZero(t, bkts[0].ID)
		assert.NotEmpty(t, bkts[0].PkgName)
		assert.Equal(t, "rucketeer", bkts[0].Name)
		hasLabelAssociations(t, bkts[0].LabelAssociations, 2, "label-1", "the 2nd label")

		checks := sum1.Checks
		require.Len(t, checks, 2)
		assert.Equal(t, "check 0 name", checks[0].Check.GetName())
		hasLabelAssociations(t, checks[0].LabelAssociations, 1, "label-1")
		assert.Equal(t, "check-1", checks[1].Check.GetName())
		hasLabelAssociations(t, checks[1].LabelAssociations, 1, "label-1")
		for _, ch := range checks {
			assert.NotZero(t, ch.Check.GetID())
		}

		dashs := sum1.Dashboards
		require.Len(t, dashs, 1)
		assert.NotZero(t, dashs[0].ID)
		assert.NotEmpty(t, dashs[0].Name)
		assert.Equal(t, "dash_1", dashs[0].Name)
		assert.Equal(t, "desc1", dashs[0].Description)
		hasLabelAssociations(t, dashs[0].LabelAssociations, 2, "label-1", "the 2nd label")
		require.Len(t, dashs[0].Charts, 1)
		assert.Equal(t, influxdb.ViewPropertyTypeSingleStat, dashs[0].Charts[0].Properties.GetType())

		endpoints := sum1.NotificationEndpoints
		require.Len(t, endpoints, 1)
		assert.NotZero(t, endpoints[0].NotificationEndpoint.GetID())
		assert.Equal(t, "no auth endpoint", endpoints[0].NotificationEndpoint.GetName())
		assert.Equal(t, "http none auth desc", endpoints[0].NotificationEndpoint.GetDescription())
		assert.Equal(t, influxdb.TaskStatusInactive, string(endpoints[0].NotificationEndpoint.GetStatus()))
		hasLabelAssociations(t, endpoints[0].LabelAssociations, 1, "label-1")

		require.Len(t, sum1.NotificationRules, 1)
		rule := sum1.NotificationRules[0]
		assert.NotZero(t, rule.ID)
		assert.Equal(t, "rule_0", rule.Name)
		assert.Equal(t, pkger.SafeID(endpoints[0].NotificationEndpoint.GetID()), rule.EndpointID)
		assert.Equal(t, "http-none-auth-notification-endpoint", rule.EndpointPkgName)
		assert.Equalf(t, "http", rule.EndpointType, "rule: %+v", rule)

		require.Len(t, sum1.Tasks, 1)
		task := sum1.Tasks[0]
		assert.NotZero(t, task.ID)
		assert.Equal(t, "task_1", task.Name)
		assert.Equal(t, "desc_1", task.Description)

		teles := sum1.TelegrafConfigs
		require.Len(t, teles, 1)
		assert.NotZero(t, teles[0].TelegrafConfig.ID)
		assert.Equal(t, l.Org.ID, teles[0].TelegrafConfig.OrgID)
		assert.Equal(t, "first tele config", teles[0].TelegrafConfig.Name)
		assert.Equal(t, "desc", teles[0].TelegrafConfig.Description)
		assert.Equal(t, telegrafCfg, teles[0].TelegrafConfig.Config)

		vars := sum1.Variables
		require.Len(t, vars, 1)
		assert.NotZero(t, vars[0].ID)
		assert.Equal(t, "query var", vars[0].Name)
		hasLabelAssociations(t, vars[0].LabelAssociations, 1, "label-1")
		varArgs := vars[0].Arguments
		require.NotNil(t, varArgs)
		assert.Equal(t, "query", varArgs.Type)
		assert.Equal(t, influxdb.VariableQueryValues{
			Query:    "buckets()  |> filter(fn: (r) => r.name !~ /^_/)  |> rename(columns: {name: \"_value\"})  |> keep(columns: [\"_value\"])",
			Language: "flux",
		}, varArgs.Values)

		newSumMapping := func(id pkger.SafeID, pkgName, name string, rt influxdb.ResourceType) pkger.SummaryLabelMapping {
			return pkger.SummaryLabelMapping{
				Status:          pkger.StateStatusNew,
				ResourceID:      id,
				ResourceType:    rt,
				ResourcePkgName: pkgName,
				ResourceName:    name,
				LabelPkgName:    labels[0].PkgName,
				LabelName:       labels[0].Name,
				LabelID:         labels[0].ID,
			}
		}

		mappings := sum1.LabelMappings

		mappingsContain := func(t *testing.T, id pkger.SafeID, pkgName, name string, resourceType influxdb.ResourceType) {
			t.Helper()
			assert.Contains(t, mappings, newSumMapping(id, pkgName, name, resourceType))
		}

		require.Len(t, mappings, 11)
		mappingsContain(t, bkts[0].ID, bkts[0].PkgName, bkts[0].Name, influxdb.BucketsResourceType)
		mappingsContain(t, pkger.SafeID(checks[0].Check.GetID()), checks[0].PkgName, checks[0].Check.GetName(), influxdb.ChecksResourceType)
		mappingsContain(t, dashs[0].ID, dashs[0].PkgName, dashs[0].Name, influxdb.DashboardsResourceType)
		mappingsContain(t, pkger.SafeID(endpoints[0].NotificationEndpoint.GetID()), endpoints[0].PkgName, endpoints[0].NotificationEndpoint.GetName(), influxdb.NotificationEndpointResourceType)
		mappingsContain(t, rule.ID, rule.PkgName, rule.Name, influxdb.NotificationRuleResourceType)
		mappingsContain(t, task.ID, task.PkgName, task.Name, influxdb.TasksResourceType)
		mappingsContain(t, pkger.SafeID(teles[0].TelegrafConfig.ID), teles[0].PkgName, teles[0].TelegrafConfig.Name, influxdb.TelegrafsResourceType)
		mappingsContain(t, vars[0].ID, vars[0].PkgName, vars[0].Name, influxdb.VariablesResourceType)

		var (
			// used in dependent subtests
			sum1Bkts      = sum1.Buckets
			sum1Checks    = sum1.Checks
			sum1Dashs     = sum1.Dashboards
			sum1Endpoints = sum1.NotificationEndpoints
			sum1Labels    = sum1.Labels
			sum1Rules     = sum1.NotificationRules
			sum1Tasks     = sum1.Tasks
			sum1Teles     = sum1.TelegrafConfigs
			sum1Vars      = sum1.Variables
		)

		t.Run("exporting all resources for an org", func(t *testing.T) {
			t.Run("getting everything", func(t *testing.T) {
				newPkg, err := svc.CreatePkg(ctx, pkger.CreateWithAllOrgResources(
					pkger.CreateByOrgIDOpt{
						OrgID: l.Org.ID,
					},
				))
				require.NoError(t, err)

				sum := newPkg.Summary()

				labels := sum.Labels
				require.Len(t, labels, 2)
				assert.Equal(t, "label-1", labels[0].Name)
				assert.Equal(t, "the 2nd label", labels[1].Name)

				bkts := sum.Buckets
				require.Len(t, bkts, 1)
				assert.NotEmpty(t, bkts[0].PkgName)
				assert.Equal(t, "rucketeer", bkts[0].Name)
				hasLabelAssociations(t, bkts[0].LabelAssociations, 2, "label-1", "the 2nd label")

				checks := sum.Checks
				require.Len(t, checks, 2)
				assert.Equal(t, "check 0 name", checks[0].Check.GetName())
				hasLabelAssociations(t, checks[0].LabelAssociations, 1, "label-1")
				assert.Equal(t, "check-1", checks[1].Check.GetName())
				hasLabelAssociations(t, checks[1].LabelAssociations, 1, "label-1")

				dashs := sum.Dashboards
				require.Len(t, dashs, 1)
				assert.NotEmpty(t, dashs[0].Name)
				assert.Equal(t, "dash_1", dashs[0].Name)
				assert.Equal(t, "desc1", dashs[0].Description)
				hasLabelAssociations(t, dashs[0].LabelAssociations, 2, "label-1", "the 2nd label")
				require.Len(t, dashs[0].Charts, 1)
				assert.Equal(t, influxdb.ViewPropertyTypeSingleStat, dashs[0].Charts[0].Properties.GetType())

				endpoints := sum.NotificationEndpoints
				require.Len(t, endpoints, 1)
				assert.Equal(t, "no auth endpoint", endpoints[0].NotificationEndpoint.GetName())
				assert.Equal(t, "http none auth desc", endpoints[0].NotificationEndpoint.GetDescription())
				assert.Equal(t, influxdb.TaskStatusInactive, string(endpoints[0].NotificationEndpoint.GetStatus()))
				hasLabelAssociations(t, endpoints[0].LabelAssociations, 1, "label-1")

				require.Len(t, sum.NotificationRules, 1)
				rule := sum.NotificationRules[0]
				assert.Equal(t, "rule_0", rule.Name)
				assert.Equal(t, pkger.SafeID(endpoints[0].NotificationEndpoint.GetID()), rule.EndpointID)
				assert.NotEmpty(t, rule.EndpointPkgName)

				require.Len(t, sum.Tasks, 1)
				task := sum.Tasks[0]
				assert.Equal(t, "task_1", task.Name)
				assert.Equal(t, "desc_1", task.Description)

				teles := sum.TelegrafConfigs
				require.Len(t, teles, 1)
				assert.Equal(t, "first tele config", teles[0].TelegrafConfig.Name)
				assert.Equal(t, "desc", teles[0].TelegrafConfig.Description)
				assert.Equal(t, telegrafCfg, teles[0].TelegrafConfig.Config)

				vars := sum.Variables
				require.Len(t, vars, 1)
				assert.Equal(t, "query var", vars[0].Name)
				hasLabelAssociations(t, vars[0].LabelAssociations, 1, "label-1")
				varArgs := vars[0].Arguments
				require.NotNil(t, varArgs)
				assert.Equal(t, "query", varArgs.Type)
				assert.Equal(t, influxdb.VariableQueryValues{
					Query:    "buckets()  |> filter(fn: (r) => r.name !~ /^_/)  |> rename(columns: {name: \"_value\"})  |> keep(columns: [\"_value\"])",
					Language: "flux",
				}, varArgs.Values)

				newSumMapping := func(id pkger.SafeID, pkgName, name string, rt influxdb.ResourceType) pkger.SummaryLabelMapping {
					return pkger.SummaryLabelMapping{
						Status:          pkger.StateStatusNew,
						ResourceID:      id,
						ResourceType:    rt,
						ResourcePkgName: pkgName,
						ResourceName:    name,
						LabelPkgName:    labels[0].PkgName,
						LabelName:       labels[0].Name,
						LabelID:         labels[0].ID,
					}
				}

				mappings := sum.LabelMappings
				require.Len(t, mappings, 11)
				assert.Contains(t, mappings, newSumMapping(bkts[0].ID, bkts[0].PkgName, bkts[0].Name, influxdb.BucketsResourceType))

				ch0 := checks[0]
				assert.Contains(t, mappings, newSumMapping(pkger.SafeID(ch0.Check.GetID()), ch0.PkgName, ch0.Check.GetName(), influxdb.ChecksResourceType))

				ch1 := checks[0]
				assert.Contains(t, mappings, newSumMapping(pkger.SafeID(ch1.Check.GetID()), ch1.PkgName, ch1.Check.GetName(), influxdb.ChecksResourceType))

				ne := endpoints[0]
				assert.Contains(t, mappings, newSumMapping(pkger.SafeID(ne.NotificationEndpoint.GetID()), ne.PkgName, ne.NotificationEndpoint.GetName(), influxdb.NotificationEndpointResourceType))

				assert.Contains(t, mappings, newSumMapping(dashs[0].ID, dashs[0].PkgName, dashs[0].Name, influxdb.DashboardsResourceType))
				assert.Contains(t, mappings, newSumMapping(rule.ID, rule.PkgName, rule.Name, influxdb.NotificationRuleResourceType))
				assert.Contains(t, mappings, newSumMapping(task.ID, task.PkgName, task.Name, influxdb.TasksResourceType))
				assert.Contains(t, mappings, newSumMapping(pkger.SafeID(teles[0].TelegrafConfig.ID), teles[0].PkgName, teles[0].TelegrafConfig.Name, influxdb.TelegrafsResourceType))
				assert.Contains(t, mappings, newSumMapping(vars[0].ID, vars[0].PkgName, vars[0].Name, influxdb.VariablesResourceType))
			})

			t.Run("filtered by resource types", func(t *testing.T) {
				newPkg, err := svc.CreatePkg(ctx, pkger.CreateWithAllOrgResources(
					pkger.CreateByOrgIDOpt{
						OrgID:         l.Org.ID,
						ResourceKinds: []pkger.Kind{pkger.KindCheck, pkger.KindTask},
					},
				))
				require.NoError(t, err)

				newSum := newPkg.Summary()
				assert.NotEmpty(t, newSum.Checks)
				assert.NotEmpty(t, newSum.Labels)
				assert.NotEmpty(t, newSum.Tasks)
				assert.Empty(t, newSum.Buckets)
				assert.Empty(t, newSum.Dashboards)
				assert.Empty(t, newSum.NotificationEndpoints)
				assert.Empty(t, newSum.NotificationRules)
				assert.Empty(t, newSum.TelegrafConfigs)
				assert.Empty(t, newSum.Variables)
			})

			t.Run("filtered by label resource type", func(t *testing.T) {
				newPkg, err := svc.CreatePkg(ctx, pkger.CreateWithAllOrgResources(
					pkger.CreateByOrgIDOpt{
						OrgID:         l.Org.ID,
						ResourceKinds: []pkger.Kind{pkger.KindLabel},
					},
				))
				require.NoError(t, err)

				newSum := newPkg.Summary()
				assert.NotEmpty(t, newSum.Labels)
				assert.Empty(t, newSum.Buckets)
				assert.Empty(t, newSum.Checks)
				assert.Empty(t, newSum.Dashboards)
				assert.Empty(t, newSum.NotificationEndpoints)
				assert.Empty(t, newSum.NotificationRules)
				assert.Empty(t, newSum.Tasks)
				assert.Empty(t, newSum.TelegrafConfigs)
				assert.Empty(t, newSum.Variables)
			})

			t.Run("filtered by label name", func(t *testing.T) {
				newPkg, err := svc.CreatePkg(ctx, pkger.CreateWithAllOrgResources(
					pkger.CreateByOrgIDOpt{
						OrgID:      l.Org.ID,
						LabelNames: []string{"the 2nd label"},
					},
				))
				require.NoError(t, err)

				newSum := newPkg.Summary()
				assert.NotEmpty(t, newSum.Buckets)
				assert.NotEmpty(t, newSum.Dashboards)
				assert.NotEmpty(t, newSum.Labels)
				assert.Empty(t, newSum.Checks)
				assert.Empty(t, newSum.NotificationEndpoints)
				assert.Empty(t, newSum.NotificationRules)
				assert.Empty(t, newSum.Tasks)
				assert.Empty(t, newSum.Variables)
			})

			t.Run("filtered by label name and resource type", func(t *testing.T) {
				newPkg, err := svc.CreatePkg(ctx, pkger.CreateWithAllOrgResources(
					pkger.CreateByOrgIDOpt{
						OrgID:         l.Org.ID,
						LabelNames:    []string{"the 2nd label"},
						ResourceKinds: []pkger.Kind{pkger.KindDashboard},
					},
				))
				require.NoError(t, err)

				newSum := newPkg.Summary()
				assert.NotEmpty(t, newSum.Dashboards)
				assert.NotEmpty(t, newSum.Labels)
				assert.Empty(t, newSum.Buckets)
				assert.Empty(t, newSum.Checks)
				assert.Empty(t, newSum.NotificationEndpoints)
				assert.Empty(t, newSum.NotificationRules)
				assert.Empty(t, newSum.Tasks)
				assert.Empty(t, newSum.Variables)
			})
		})

		t.Run("pkg with same bkt-var-label does nto create new resources for them", func(t *testing.T) {
			// validate the new package doesn't create new resources for bkts/labels/vars
			// since names collide.
			sum2, _, err := svc.Apply(ctx, l.Org.ID, l.User.ID, newPkg(t))
			require.NoError(t, err)

			require.Equal(t, sum1.Buckets, sum2.Buckets)
			require.Equal(t, sum1.Labels, sum2.Labels)
			require.Equal(t, sum1.NotificationEndpoints, sum2.NotificationEndpoints)
			require.Equal(t, sum1.Variables, sum2.Variables)

			// dashboards should be new
			require.NotEqual(t, sum1.Dashboards, sum2.Dashboards)
		})

		t.Run("referenced secret values provided do not create new secrets", func(t *testing.T) {
			applyPkgStr := func(t *testing.T, pkgStr string) pkger.Summary {
				t.Helper()
				pkg, err := pkger.Parse(pkger.EncodingYAML, pkger.FromString(pkgStr))
				require.NoError(t, err)

				sum, _, err := svc.Apply(ctx, l.Org.ID, l.User.ID, pkg)
				require.NoError(t, err)
				return sum
			}

			pkgWithSecretRaw := fmt.Sprintf(`
apiVersion: %[1]s
kind: NotificationEndpointPagerDuty
metadata:
  name:      pager-duty-notification-endpoint
spec:
  url:  http://localhost:8080/orgs/7167eb6719fa34e5/alert-history
  routingKey: secret-sauce
`, pkger.APIVersion)

			secretSum := applyPkgStr(t, pkgWithSecretRaw)
			require.Len(t, secretSum.NotificationEndpoints, 1)

			id := secretSum.NotificationEndpoints[0].NotificationEndpoint.GetID()
			expected := influxdb.SecretField{
				Key: id.String() + "-routing-key",
			}
			secrets := secretSum.NotificationEndpoints[0].NotificationEndpoint.SecretFields()
			require.Len(t, secrets, 1)
			assert.Equal(t, expected, secrets[0])

			const pkgWithSecretRef = `
apiVersion: %[1]s
kind: NotificationEndpointPagerDuty
metadata:
  name:      pager-duty-notification-endpoint
spec:
  url:  http://localhost:8080/orgs/7167eb6719fa34e5/alert-history
  routingKey:
    secretRef:
      key: %s-routing-key
`
			secretSum = applyPkgStr(t, fmt.Sprintf(pkgWithSecretRef, pkger.APIVersion, id.String()))
			require.Len(t, secretSum.NotificationEndpoints, 1)

			expected = influxdb.SecretField{
				Key: id.String() + "-routing-key",
			}
			secrets = secretSum.NotificationEndpoints[0].NotificationEndpoint.SecretFields()
			require.Len(t, secrets, 1)
			assert.Equal(t, expected, secrets[0])
		})

		t.Run("exporting resources with existing ids should return a valid pkg", func(t *testing.T) {
			resToClone := []pkger.ResourceToClone{
				{
					Kind: pkger.KindBucket,
					ID:   influxdb.ID(sum1Bkts[0].ID),
				},
				{
					Kind: pkger.KindCheck,
					ID:   sum1Checks[0].Check.GetID(),
				},
				{
					Kind: pkger.KindCheck,
					ID:   sum1Checks[1].Check.GetID(),
				},
				{
					Kind: pkger.KindDashboard,
					ID:   influxdb.ID(sum1Dashs[0].ID),
				},
				{
					Kind: pkger.KindLabel,
					ID:   influxdb.ID(sum1Labels[0].ID),
				},
				{
					Kind: pkger.KindNotificationEndpoint,
					ID:   sum1Endpoints[0].NotificationEndpoint.GetID(),
				},
				{
					Kind: pkger.KindTask,
					ID:   influxdb.ID(sum1Tasks[0].ID),
				},
				{
					Kind: pkger.KindTelegraf,
					ID:   sum1Teles[0].TelegrafConfig.ID,
				},
			}

			resWithNewName := []pkger.ResourceToClone{
				{
					Kind: pkger.KindNotificationRule,
					Name: "new rule name",
					ID:   influxdb.ID(sum1Rules[0].ID),
				},
				{
					Kind: pkger.KindVariable,
					Name: "new name",
					ID:   influxdb.ID(sum1Vars[0].ID),
				},
			}

			newPkg, err := svc.CreatePkg(ctx,
				pkger.CreateWithExistingResources(append(resToClone, resWithNewName...)...),
			)
			require.NoError(t, err)

			newSum := newPkg.Summary()

			labels := newSum.Labels
			require.Len(t, labels, 2)
			assert.Zero(t, labels[0].ID)
			assert.Equal(t, "label-1", labels[0].Name)
			assert.Zero(t, labels[1].ID)
			assert.Equal(t, "the 2nd label", labels[1].Name)

			bkts := newSum.Buckets
			require.Len(t, bkts, 1)
			assert.Zero(t, bkts[0].ID)
			assert.Equal(t, "rucketeer", bkts[0].Name)
			hasLabelAssociations(t, bkts[0].LabelAssociations, 2, "label-1", "the 2nd label")

			checks := newSum.Checks
			require.Len(t, checks, 2)
			assert.Equal(t, "check 0 name", checks[0].Check.GetName())
			hasLabelAssociations(t, checks[0].LabelAssociations, 1, "label-1")
			assert.Equal(t, "check-1", checks[1].Check.GetName())
			hasLabelAssociations(t, checks[1].LabelAssociations, 1, "label-1")

			dashs := newSum.Dashboards
			require.Len(t, dashs, 1)
			assert.Zero(t, dashs[0].ID)
			assert.Equal(t, "dash_1", dashs[0].Name)
			assert.Equal(t, "desc1", dashs[0].Description)
			hasLabelAssociations(t, dashs[0].LabelAssociations, 2, "label-1", "the 2nd label")
			require.Len(t, dashs[0].Charts, 1)
			assert.Equal(t, influxdb.ViewPropertyTypeSingleStat, dashs[0].Charts[0].Properties.GetType())

			newEndpoints := newSum.NotificationEndpoints
			require.Len(t, newEndpoints, 1)
			assert.Equal(t, sum1Endpoints[0].NotificationEndpoint.GetName(), newEndpoints[0].NotificationEndpoint.GetName())
			assert.Equal(t, sum1Endpoints[0].NotificationEndpoint.GetDescription(), newEndpoints[0].NotificationEndpoint.GetDescription())
			hasLabelAssociations(t, newEndpoints[0].LabelAssociations, 1, "label-1")

			require.Len(t, newSum.NotificationRules, 1)
			newRule := newSum.NotificationRules[0]
			assert.Equal(t, "new rule name", newRule.Name)
			assert.Zero(t, newRule.EndpointID)
			assert.NotEmpty(t, newRule.EndpointPkgName)
			hasLabelAssociations(t, newRule.LabelAssociations, 1, "label-1")

			require.Len(t, newSum.Tasks, 1)
			newTask := newSum.Tasks[0]
			assert.Equal(t, sum1Tasks[0].Name, newTask.Name)
			assert.Equal(t, sum1Tasks[0].Description, newTask.Description)
			assert.Equal(t, sum1Tasks[0].Cron, newTask.Cron)
			assert.Equal(t, sum1Tasks[0].Every, newTask.Every)
			assert.Equal(t, sum1Tasks[0].Offset, newTask.Offset)
			assert.Equal(t, sum1Tasks[0].Query, newTask.Query)
			assert.Equal(t, sum1Tasks[0].Status, newTask.Status)

			require.Len(t, newSum.TelegrafConfigs, 1)
			assert.Equal(t, sum1Teles[0].TelegrafConfig.Name, newSum.TelegrafConfigs[0].TelegrafConfig.Name)
			assert.Equal(t, sum1Teles[0].TelegrafConfig.Description, newSum.TelegrafConfigs[0].TelegrafConfig.Description)
			hasLabelAssociations(t, newSum.TelegrafConfigs[0].LabelAssociations, 1, "label-1")

			vars := newSum.Variables
			require.Len(t, vars, 1)
			assert.Zero(t, vars[0].ID)
			assert.Equal(t, "new name", vars[0].Name) // new name
			hasLabelAssociations(t, vars[0].LabelAssociations, 1, "label-1")
			varArgs := vars[0].Arguments
			require.NotNil(t, varArgs)
			assert.Equal(t, "query", varArgs.Type)
			assert.Equal(t, influxdb.VariableQueryValues{
				Query:    "buckets()  |> filter(fn: (r) => r.name !~ /^_/)  |> rename(columns: {name: \"_value\"})  |> keep(columns: [\"_value\"])",
				Language: "flux",
			}, varArgs.Values)
		})

		t.Run("error incurs during package application when resources already exist rollsback to prev state", func(t *testing.T) {
			updatePkg, err := pkger.Parse(pkger.EncodingYAML, pkger.FromString(updatePkgYMLStr))
			require.NoError(t, err)

			svc := pkger.NewService(
				pkger.WithBucketSVC(&fakeBucketSVC{
					BucketService:   l.BucketService(t),
					updateKillCount: 0, // kill on first update for bucket
				}),
				pkger.WithCheckSVC(l.CheckService()),
				pkger.WithDashboardSVC(l.DashboardService(t)),
				pkger.WithLabelSVC(l.LabelService(t)),
				pkger.WithNotificationEndpointSVC(l.NotificationEndpointService(t)),
				pkger.WithNotificationRuleSVC(l.NotificationRuleService()),
				pkger.WithTaskSVC(l.TaskServiceKV()),
				pkger.WithTelegrafSVC(l.TelegrafService(t)),
				pkger.WithVariableSVC(l.VariableService(t)),
			)

			_, _, err = svc.Apply(ctx, l.Org.ID, 0, updatePkg)
			require.Error(t, err)

			bkt, err := l.BucketService(t).FindBucketByID(ctx, influxdb.ID(sum1Bkts[0].ID))
			require.NoError(t, err)
			// make sure the desc change is not applied and is rolled back to prev desc
			assert.Equal(t, sum1Bkts[0].Description, bkt.Description)

			ch, err := l.CheckService().FindCheckByID(ctx, sum1Checks[0].Check.GetID())
			require.NoError(t, err)
			ch.SetOwnerID(0)
			deadman, ok := ch.(*check.Threshold)
			require.True(t, ok)
			// validate the change to query is not persisting returned to previous state.
			// not checking entire bits, b/c we dont' save userID and so forth and makes a
			// direct comparison very annoying...
			assert.Equal(t, sum1Checks[0].Check.(*check.Threshold).Query.Text, deadman.Query.Text)

			label, err := l.LabelService(t).FindLabelByID(ctx, influxdb.ID(sum1Labels[0].ID))
			require.NoError(t, err)
			assert.Equal(t, sum1Labels[0].Properties.Description, label.Properties["description"])

			endpoint, err := l.NotificationEndpointService(t).FindNotificationEndpointByID(ctx, sum1Endpoints[0].NotificationEndpoint.GetID())
			require.NoError(t, err)
			assert.Equal(t, sum1Endpoints[0].NotificationEndpoint.GetDescription(), endpoint.GetDescription())

			v, err := l.VariableService(t).FindVariableByID(ctx, influxdb.ID(sum1Vars[0].ID))
			require.NoError(t, err)
			assert.Equal(t, sum1Vars[0].Description, v.Description)
		})
	})

	t.Run("apply a task pkg with a complex query", func(t *testing.T) {
		// validates bug: https://github.com/influxdata/influxdb/issues/17069

		pkgStr := fmt.Sprintf(`
apiVersion: %[1]s
kind: Task
metadata:
    name: http-post-synthetic
spec:
    name: Http.POST Synthetic (POST)
    every: 5m
    query: |-
        import "strings"
        import "csv"
        import "http"
        import "system"

        timeDiff = (t1, t2) => {
        	return duration(v: uint(v: t2) - uint(v: t1))
        }
        timeDiffNum = (t1, t2) => {
        	return uint(v: t2) - uint(v: t1)
        }
        urlToPost = "http://www.duckduckgo.com"
        timeBeforeCall = system.time()
        responseCode = http.post(url: urlToPost, data: bytes(v: "influxdata"))
        timeAfterCall = system.time()
        responseTime = timeDiff(t1: timeBeforeCall, t2: timeAfterCall)
        responseTimeNum = timeDiffNum(t1: timeBeforeCall, t2: timeAfterCall)
        data = "#group,false,false,true,true,true,true,true,true
        #datatype,string,long,string,string,string,string,string,string
        #default,mean,,,,,,,
        ,result,table,service,response_code,time_before,time_after,response_time_duration,response_time_ns
        ,,0,http_post_ping,${string(v: responseCode)},${string(v: timeBeforeCall)},${string(v: timeAfterCall)},${string(v: responseTime)},${string(v: responseTimeNum)}"
        theTable = csv.from(csv: data)

        theTable
        	|> map(fn: (r) =>
        		({r with _time: now()}))
        	|> map(fn: (r) =>
        		({r with _measurement: "PingService", url: urlToPost, method: "POST"}))
        	|> drop(columns: ["time_before", "time_after", "response_time_duration"])
        	|> to(bucket: "Pingpire", orgID: "039346c3777a1000", fieldFn: (r) =>
        		({"responseCode": r.response_code, "responseTime": int(v: r.response_time_ns)}))
`, pkger.APIVersion)

		pkg, err := pkger.Parse(pkger.EncodingYAML, pkger.FromString(pkgStr))
		require.NoError(t, err)

		sum, _, err := svc.Apply(ctx, l.Org.ID, l.User.ID, pkg)
		require.NoError(t, err)

		require.Len(t, sum.Tasks, 1)
	})

	t.Run("apply a package with env refs", func(t *testing.T) {
		pkgStr := fmt.Sprintf(`
apiVersion: %[1]s
kind: Bucket
metadata:
  name:
    envRef:
      key: "bkt-1-name-ref"
spec:
  associations:
    - kind: Label
      name:
        envRef:
          key: label-1-name-ref
---
apiVersion: %[1]s
kind: Label
metadata:
  name:
    envRef:
      key: "label-1-name-ref"
spec:
---
apiVersion: influxdata.com/v2alpha1
kind: CheckDeadman
metadata:
  name:
    envRef:
      key: check-1-name-ref
spec:
  every: 5m
  level: cRiT
  query:  >
    from(bucket: "rucket_1") |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  statusMessageTemplate: "Check: ${ r._check_name } is: ${ r._level }"
---
apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name:
    envRef:
      key: dash-1-name-ref
spec:
---
apiVersion: influxdata.com/v2alpha1
kind: NotificationEndpointSlack
metadata:
  name:
    envRef:
      key: endpoint-1-name-ref
spec:
  url: https://hooks.slack.com/services/bip/piddy/boppidy
---
apiVersion: influxdata.com/v2alpha1
kind: NotificationRule
metadata:
  name:
    envRef:
      key: rule-1-name-ref
spec:
  endpointName:
    envRef:
      key: endpoint-1-name-ref
  every: 10m
  messageTemplate: "Notification Rule: ${ r._notification_rule_name } triggered by check: ${ r._check_name }: ${ r._message }"
  statusRules:
    - currentLevel: WARN
---
apiVersion: influxdata.com/v2alpha1
kind: Telegraf
metadata:
  name:
    envRef:
      key: telegraf-1-name-ref
spec:
  config: |
    [agent]
      interval = "10s"
---
apiVersion: influxdata.com/v2alpha1
kind: Task
metadata:
  name:
    envRef:
      key: task-1-name-ref
spec:
  cron: 15 * * * *
  query:  >
    from(bucket: "rucket_1")
---
apiVersion: influxdata.com/v2alpha1
kind: Variable
metadata:
  name:
    envRef:
      key: var-1-name-ref
spec:
  type: constant
  values: [first val]
`, pkger.APIVersion)

		pkg, err := pkger.Parse(pkger.EncodingYAML, pkger.FromString(pkgStr))
		require.NoError(t, err)

		sum, _, err := svc.DryRun(ctx, l.Org.ID, l.User.ID, pkg)
		require.NoError(t, err)

		require.Len(t, sum.Buckets, 1)
		assert.Equal(t, "env-bkt-1-name-ref", sum.Buckets[0].Name)
		assert.Len(t, sum.Buckets[0].LabelAssociations, 1)
		require.Len(t, sum.Checks, 1)
		assert.Equal(t, "env-check-1-name-ref", sum.Checks[0].Check.GetName())
		require.Len(t, sum.Dashboards, 1)
		assert.Equal(t, "env-dash-1-name-ref", sum.Dashboards[0].Name)
		require.Len(t, sum.Labels, 1)
		assert.Equal(t, "env-label-1-name-ref", sum.Labels[0].Name)
		require.Len(t, sum.NotificationEndpoints, 1)
		assert.Equal(t, "env-endpoint-1-name-ref", sum.NotificationEndpoints[0].NotificationEndpoint.GetName())
		require.Len(t, sum.NotificationRules, 1)
		assert.Equal(t, "env-rule-1-name-ref", sum.NotificationRules[0].Name)
		require.Len(t, sum.TelegrafConfigs, 1)
		assert.Equal(t, "env-task-1-name-ref", sum.Tasks[0].Name)
		require.Len(t, sum.TelegrafConfigs, 1)
		assert.Equal(t, "env-telegraf-1-name-ref", sum.TelegrafConfigs[0].TelegrafConfig.Name)
		require.Len(t, sum.Variables, 1)
		assert.Equal(t, "env-var-1-name-ref", sum.Variables[0].Name)

		expectedMissingEnvs := []string{
			"bkt-1-name-ref",
			"check-1-name-ref",
			"dash-1-name-ref",
			"endpoint-1-name-ref",
			"label-1-name-ref",
			"rule-1-name-ref",
			"task-1-name-ref",
			"telegraf-1-name-ref",
			"var-1-name-ref",
		}
		assert.Equal(t, expectedMissingEnvs, sum.MissingEnvs)

		sum, _, err = svc.Apply(ctx, l.Org.ID, l.User.ID, pkg, pkger.ApplyWithEnvRefs(map[string]string{
			"bkt-1-name-ref":      "rucket_threeve",
			"check-1-name-ref":    "check_threeve",
			"dash-1-name-ref":     "dash_threeve",
			"endpoint-1-name-ref": "endpoint_threeve",
			"label-1-name-ref":    "label_threeve",
			"rule-1-name-ref":     "rule_threeve",
			"telegraf-1-name-ref": "telegraf_threeve",
			"task-1-name-ref":     "task_threeve",
			"var-1-name-ref":      "var_threeve",
		}))
		require.NoError(t, err)

		assert.Equal(t, "rucket_threeve", sum.Buckets[0].Name)
		assert.Equal(t, "check_threeve", sum.Checks[0].Check.GetName())
		assert.Equal(t, "dash_threeve", sum.Dashboards[0].Name)
		assert.Equal(t, "endpoint_threeve", sum.NotificationEndpoints[0].NotificationEndpoint.GetName())
		assert.Equal(t, "label_threeve", sum.Labels[0].Name)
		assert.Equal(t, "rule_threeve", sum.NotificationRules[0].Name)
		assert.Equal(t, "endpoint_threeve", sum.NotificationRules[0].EndpointPkgName)
		assert.Equal(t, "telegraf_threeve", sum.TelegrafConfigs[0].TelegrafConfig.Name)
		assert.Equal(t, "task_threeve", sum.Tasks[0].Name)
		assert.Equal(t, "var_threeve", sum.Variables[0].Name)
		assert.Empty(t, sum.MissingEnvs)
	})
}

func newPkg(t *testing.T) *pkger.Pkg {
	t.Helper()

	pkg, err := pkger.Parse(pkger.EncodingYAML, pkger.FromString(pkgYMLStr))
	require.NoError(t, err)
	return pkg
}

const telegrafCfg = `[agent]
  interval = "10s"
  metric_batch_size = 1000
  metric_buffer_limit = 10000
  collection_jitter = "0s"
  flush_interval = "10s"
[[outputs.influxdb_v2]]
  urls = ["http://localhost:9999"]
  token = "$INFLUX_TOKEN"
  organization = "rg"
  bucket = "rucket_3"
[[inputs.cpu]]
  percpu = true
`

var pkgYMLStr = fmt.Sprintf(`
apiVersion: %[1]s
kind: Label
metadata:
  name: label-1
---
apiVersion: %[1]s
kind: Label
metadata:
  name: the-2nd-label
spec:
  name: the 2nd label
---
apiVersion: %[1]s
kind: Bucket
metadata:
  name: rucket-1
spec:
  name: rucketeer
  associations:
    - kind: Label
      name: label-1
    - kind: Label
      name: the-2nd-label
---
apiVersion: %[1]s
kind: Dashboard
metadata:
  name: dash-uuid
spec:
  name: dash_1
  description: desc1
  charts:
    - kind:   Single_Stat
      name:   single stat
      suffix: days
      width:  6
      height: 3
      shade: true
      queries:
        - query: >
            from(bucket: v.bucket) |> range(start: v.timeRangeStart) |> filter(fn: (r) => r._measurement == "system") |> filter(fn: (r) => r._field == "uptime") |> last() |> map(fn: (r) => ({r with _value: r._value / 86400})) |> yield(name: "last")
      colors:
        - name: laser
          type: text
          hex: "#8F8AF4"
  associations:
    - kind: Label
      name: label-1
    - kind: Label
      name: the-2nd-label
---
apiVersion: %[1]s
kind: Variable
metadata:
  name:  var-query-1
spec:
  name: query var
  description: var_query_1 desc
  type: query
  language: flux
  query: |
    buckets()  |> filter(fn: (r) => r.name !~ /^_/)  |> rename(columns: {name: "_value"})  |> keep(columns: ["_value"])
  associations:
    - kind: Label
      name: label-1
---
apiVersion: %[1]s
kind: Telegraf
metadata:
  name:  first-tele-config
spec:
  name: first tele config
  description: desc
  associations:
    - kind: Label
      name: label-1
  config: %+q
---
apiVersion: %[1]s
kind: NotificationEndpointHTTP
metadata:
  name:  http-none-auth-notification-endpoint # on export of resource created from this, will not be same name as this
spec:
  name: no auth endpoint
  type: none
  description: http none auth desc
  method: GET
  url:  https://www.example.com/endpoint/noneauth
  status: inactive
  associations:
    - kind: Label
      name: label-1
---
apiVersion: %[1]s
kind: CheckThreshold
metadata:
  name:  check-0
spec:
  name: check 0 name
  every: 1m
  query:  >
    from(bucket: "rucket_1")
      |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
      |> filter(fn: (r) => r._measurement == "cpu")
      |> filter(fn: (r) => r._field == "usage_idle")
      |> aggregateWindow(every: 1m, fn: mean)
      |> yield(name: "mean")
  statusMessageTemplate: "Check: ${ r._check_name } is: ${ r._level }"
  tags:
    - key: tag_1
      value: val_1
  thresholds:
    - type: inside_range
      level: INfO
      min: 30.0
      max: 45.0
    - type: outside_range
      level: WARN
      min: 60.0
      max: 70.0
    - type: greater
      level: CRIT
      val: 80
    - type: lesser
      level: OK
      val: 30
  associations:
    - kind: Label
      name: label-1
---
apiVersion: %[1]s
kind: CheckDeadman
metadata:
  name:  check-1
spec:
  description: desc_1
  every: 5m
  level: cRiT
  offset: 10s
  query:  >
    from(bucket: "rucket_1")
      |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
      |> filter(fn: (r) => r._measurement == "cpu")
      |> filter(fn: (r) => r._field == "usage_idle")
      |> aggregateWindow(every: 1m, fn: mean)
      |> yield(name: "mean")
  reportZero: true
  staleTime: 10m
  statusMessageTemplate: "Check: ${ r._check_name } is: ${ r._level }"
  timeSince: 90s
  associations:
    - kind: Label
      name: label-1
---
apiVersion: %[1]s
kind: NotificationRule
metadata:
  name:  rule-uuid
spec:
  name:  rule_0
  description: desc_0
  endpointName: http-none-auth-notification-endpoint
  every: 10m
  offset: 30s
  messageTemplate: "Notification Rule: ${ r._notification_rule_name } triggered by check: ${ r._check_name }: ${ r._message }"
  status: active
  statusRules:
    - currentLevel: WARN
    - currentLevel: CRIT
      previousLevel: OK
  tagRules:
    - key: k1
      value: v2
      operator: eQuAl
    - key: k1
      value: v1
      operator: eQuAl
  associations:
    - kind: Label
      name: label-1
---
apiVersion: %[1]s
kind: Task
metadata:
  name:  task-uuid
spec:
  name:  task_1
  description: desc_1
  cron: 15 * * * *
  query:  >
    from(bucket: "rucket_1")
      |> yield()
  associations:
    - kind: Label
      name: label-1
`, pkger.APIVersion, telegrafCfg)

var updatePkgYMLStr = fmt.Sprintf(`
apiVersion: %[1]s
kind: Label
metadata:
  name:  label-1
spec:
  descriptin: new desc
---
apiVersion: %[1]s
kind: Bucket
metadata:
  name:  rucket-1
spec:
  descriptin: new desc
  associations:
    - kind: Label
      name: label-1
---
apiVersion: %[1]s
kind: Variable
metadata:
  name:  var-query-1
spec:
  description: new desc
  type: query
  language: flux
  query: |
    buckets()  |> filter(fn: (r) => r.name !~ /^_/)  |> rename(columns: {name: "_value"})  |> keep(columns: ["_value"])
  associations:
    - kind: Label
      name: label-1
---
apiVersion: %[1]s
kind: NotificationEndpointHTTP
metadata:
  name:  http-none-auth-notification-endpoint
spec:
  name: no auth endpoint
  type: none
  description: new desc
  method: GET
  url:  https://www.example.com/endpoint/noneauth
  status: active
---
apiVersion: %[1]s
kind: CheckThreshold
metadata:
  name:  check-0
spec:
  every: 1m
  query:  >
    from("rucket1") |> yield()
  statusMessageTemplate: "Check: ${ r._check_name } is: ${ r._level }"
  thresholds:
    - type: inside_range
      level: INfO
      min: 30.0
      max: 45.0
`, pkger.APIVersion)

type fakeBucketSVC struct {
	influxdb.BucketService
	createCallCount mock.SafeCount
	createKillCount int
	updateCallCount mock.SafeCount
	updateKillCount int
}

func (f *fakeBucketSVC) CreateBucket(ctx context.Context, b *influxdb.Bucket) error {
	defer f.createCallCount.IncrFn()()
	if f.createCallCount.Count() == f.createKillCount {
		return errors.New("reached kill count")
	}
	return f.BucketService.CreateBucket(ctx, b)
}

func (f *fakeBucketSVC) UpdateBucket(ctx context.Context, id influxdb.ID, upd influxdb.BucketUpdate) (*influxdb.Bucket, error) {
	defer f.updateCallCount.IncrFn()()
	if f.updateCallCount.Count() == f.updateKillCount {
		return nil, errors.New("reached kill count")
	}
	return f.BucketService.UpdateBucket(ctx, id, upd)
}

type fakeLabelSVC struct {
	influxdb.LabelService
	createCallCount mock.SafeCount
	createKillCount int

	deleteCallCount mock.SafeCount
	deleteKillCount int
}

func (f *fakeLabelSVC) CreateLabelMapping(ctx context.Context, m *influxdb.LabelMapping) error {
	defer f.createCallCount.IncrFn()()
	if f.createCallCount.Count() == f.createKillCount {
		return errors.New("reached kill count")
	}
	return f.LabelService.CreateLabelMapping(ctx, m)
}

func (f *fakeLabelSVC) DeleteLabelMapping(ctx context.Context, m *influxdb.LabelMapping) error {
	defer f.deleteCallCount.IncrFn()()
	if f.deleteCallCount.Count() == f.deleteKillCount {
		return errors.New("reached kill count")
	}
	return f.LabelService.DeleteLabelMapping(ctx, m)
}

type fakeRuleStore struct {
	influxdb.NotificationRuleStore
	createCallCount mock.SafeCount
	createKillCount int
}

func (f *fakeRuleStore) CreateNotificationRule(ctx context.Context, nr influxdb.NotificationRuleCreate, userID influxdb.ID) error {
	defer f.createCallCount.IncrFn()()
	if f.createCallCount.Count() == f.createKillCount {
		return errors.New("reached kill count")
	}
	return f.NotificationRuleStore.CreateNotificationRule(ctx, nr, userID)
}

type resourceChecker struct {
	tl *TestLauncher
}

func newResourceChecker(tl *TestLauncher) resourceChecker {
	return resourceChecker{tl: tl}
}

type (
	getResourceOpt struct {
		id   influxdb.ID
		name string
	}

	getResourceOptFn func() getResourceOpt
)

func byID(id influxdb.ID) getResourceOptFn {
	return func() getResourceOpt {
		return getResourceOpt{id: id}
	}
}

func byName(name string) getResourceOptFn {
	return func() getResourceOpt {
		return getResourceOpt{name: name}
	}
}

func (r resourceChecker) getBucket(t *testing.T, getOpt getResourceOptFn) (influxdb.Bucket, error) {
	t.Helper()

	bktSVC := r.tl.BucketService(t)

	var (
		bkt *influxdb.Bucket
		err error
	)
	switch opt := getOpt(); {
	case opt.name != "":
		bkt, err = bktSVC.FindBucketByName(ctx, r.tl.Org.ID, opt.name)
	case opt.id != 0:
		bkt, err = bktSVC.FindBucketByID(ctx, opt.id)
	default:
		require.Fail(t, "did not provide any get option")
	}
	if err != nil {
		return influxdb.Bucket{}, err
	}

	return *bkt, nil
}

func (r resourceChecker) mustGetBucket(t *testing.T, getOpt getResourceOptFn) influxdb.Bucket {
	t.Helper()

	bkt, err := r.getBucket(t, getOpt)
	require.NoError(t, err)
	return bkt
}

func (r resourceChecker) getCheck(t *testing.T, getOpt getResourceOptFn) (influxdb.Check, error) {
	t.Helper()

	checkSVC := r.tl.CheckService()

	var (
		ch  influxdb.Check
		err error
	)
	switch opt := getOpt(); {
	case opt.name != "":
		ch, err = checkSVC.FindCheck(ctx, influxdb.CheckFilter{
			Name:  &opt.name,
			OrgID: &r.tl.Org.ID,
		})
	case opt.id != 0:
		ch, err = checkSVC.FindCheckByID(ctx, opt.id)
	default:
		require.Fail(t, "did not provide any get option")
	}

	return ch, err
}

func (r resourceChecker) mustGetCheck(t *testing.T, getOpt getResourceOptFn) influxdb.Check {
	t.Helper()

	c, err := r.getCheck(t, getOpt)
	require.NoError(t, err)
	return c
}

func (r resourceChecker) getDashboard(t *testing.T, getOpt getResourceOptFn) (influxdb.Dashboard, error) {
	t.Helper()

	dashSVC := r.tl.DashboardService(t)

	var (
		dashboard *influxdb.Dashboard
		err       error
	)
	opt := getOpt()
	switch {
	case opt.name != "":
		dashs, _, err := dashSVC.FindDashboards(ctx, influxdb.DashboardFilter{}, influxdb.DefaultDashboardFindOptions)
		if err != nil {
			return influxdb.Dashboard{}, err
		}
		for _, d := range dashs {
			if d.Name == opt.name {
				dashboard = d
				break
			}
		}
	case opt.id != 0:
		dashboard, err = dashSVC.FindDashboardByID(ctx, opt.id)
	default:
		require.Fail(t, "did not provide any get option")
	}
	if err != nil {
		return influxdb.Dashboard{}, err
	}
	if dashboard == nil {
		return influxdb.Dashboard{}, fmt.Errorf("failed to find desired dashboard with opts: %+v", opt)
	}

	return *dashboard, nil
}

func (r resourceChecker) mustGetDashboard(t *testing.T, getOpt getResourceOptFn) influxdb.Dashboard {
	t.Helper()

	dash, err := r.getDashboard(t, getOpt)
	require.NoError(t, err)
	return dash
}

func (r resourceChecker) getEndpoint(t *testing.T, getOpt getResourceOptFn) (influxdb.NotificationEndpoint, error) {
	t.Helper()

	endpointSVC := r.tl.NotificationEndpointService(t)

	var (
		e   influxdb.NotificationEndpoint
		err error
	)
	switch opt := getOpt(); {
	case opt.name != "":
		var endpoints []influxdb.NotificationEndpoint
		endpoints, _, err = endpointSVC.FindNotificationEndpoints(ctx, influxdb.NotificationEndpointFilter{
			OrgID: &r.tl.Org.ID,
		})
		for _, existing := range endpoints {
			if existing.GetName() == opt.name {
				e = existing
				break
			}
		}
	case opt.id != 0:
		e, err = endpointSVC.FindNotificationEndpointByID(ctx, opt.id)
	default:
		require.Fail(t, "did not provide any get option")
	}

	if e == nil {
		return nil, errors.New("did not find endpoint")
	}

	return e, err
}

func (r resourceChecker) mustGetEndpoint(t *testing.T, getOpt getResourceOptFn) influxdb.NotificationEndpoint {
	t.Helper()

	e, err := r.getEndpoint(t, getOpt)
	require.NoError(t, err)
	return e
}

func (r resourceChecker) getLabel(t *testing.T, getOpt getResourceOptFn) (influxdb.Label, error) {
	t.Helper()

	labelSVC := r.tl.LabelService(t)

	var (
		label *influxdb.Label
		err   error
	)
	switch opt := getOpt(); {
	case opt.name != "":
		labels, err := labelSVC.FindLabels(
			ctx,
			influxdb.LabelFilter{
				Name:  opt.name,
				OrgID: &r.tl.Org.ID,
			},
			influxdb.FindOptions{Limit: 1},
		)
		if err != nil {
			return influxdb.Label{}, err
		}
		if len(labels) == 0 {
			return influxdb.Label{}, errors.New("did not find label: " + opt.name)
		}
		label = labels[0]
	case opt.id != 0:
		label, err = labelSVC.FindLabelByID(ctx, opt.id)
	default:
		require.Fail(t, "did not provide any get option")
	}
	if err != nil {
		return influxdb.Label{}, err
	}

	return *label, nil
}

func (r resourceChecker) mustGetLabel(t *testing.T, getOpt getResourceOptFn) influxdb.Label {
	t.Helper()

	l, err := r.getLabel(t, getOpt)
	require.NoError(t, err)
	return l
}

func (r resourceChecker) getRule(t *testing.T, getOpt getResourceOptFn) (influxdb.NotificationRule, error) {
	t.Helper()

	ruleSVC := r.tl.NotificationRuleService()

	var (
		rule influxdb.NotificationRule
		err  error
	)
	switch opt := getOpt(); {
	case opt.name != "":
		var rules []influxdb.NotificationRule
		rules, _, err = ruleSVC.FindNotificationRules(ctx, influxdb.NotificationRuleFilter{
			OrgID: &r.tl.Org.ID,
		})
		for _, existing := range rules {
			if existing.GetName() == opt.name {
				rule = existing
				break
			}
		}
	case opt.id != 0:
		rule, err = ruleSVC.FindNotificationRuleByID(ctx, opt.id)
	default:
		require.Fail(t, "did not provide any get option")
	}

	if rule == nil {
		return nil, errors.New("did not find rule")
	}

	return rule, err
}

func (r resourceChecker) mustGetRule(t *testing.T, getOpt getResourceOptFn) influxdb.NotificationRule {
	t.Helper()

	rule, err := r.getRule(t, getOpt)
	require.NoError(t, err)
	return rule
}

func (r resourceChecker) getTask(t *testing.T, getOpt getResourceOptFn) (http.Task, error) {
	t.Helper()

	taskSVC := r.tl.TaskService(t)

	var (
		task *http.Task
		err  error
	)
	switch opt := getOpt(); {
	case opt.name != "":
		tasks, _, err := taskSVC.FindTasks(ctx, influxdb.TaskFilter{
			Name:           &opt.name,
			OrganizationID: &r.tl.Org.ID,
		})
		if err != nil {
			return http.Task{}, err
		}
		for _, tt := range tasks {
			if tt.Name == opt.name {
				task = &tasks[0]
				break
			}
		}
	case opt.id != 0:
		task, err = taskSVC.FindTaskByID(ctx, opt.id)
	default:
		require.Fail(t, "did not provide a valid get option")
	}
	if task == nil {
		return http.Task{}, errors.New("did not find expected task by name")
	}

	return *task, err
}

func (r resourceChecker) mustGetTask(t *testing.T, getOpt getResourceOptFn) http.Task {
	t.Helper()

	task, err := r.getTask(t, getOpt)
	require.NoError(t, err)
	return task
}

func (r resourceChecker) getTelegrafConfig(t *testing.T, getOpt getResourceOptFn) (influxdb.TelegrafConfig, error) {
	t.Helper()

	teleSVC := r.tl.TelegrafService(t)

	var (
		config *influxdb.TelegrafConfig
		err    error
	)
	switch opt := getOpt(); {
	case opt.name != "":
		teles, _, _ := teleSVC.FindTelegrafConfigs(ctx, influxdb.TelegrafConfigFilter{
			OrgID: &r.tl.Org.ID,
		})
		for _, tt := range teles {
			if opt.name != "" && tt.Name == opt.name {
				config = teles[0]
				break
			}
		}
	case opt.id != 0:
		config, err = teleSVC.FindTelegrafConfigByID(ctx, opt.id)
	default:
		require.Fail(t, "did not provide a valid get option")
	}
	if config == nil {
		return influxdb.TelegrafConfig{}, errors.New("did not find expected telegraf by name")
	}

	return *config, err
}

func (r resourceChecker) mustGetTelegrafConfig(t *testing.T, getOpt getResourceOptFn) influxdb.TelegrafConfig {
	t.Helper()

	tele, err := r.getTelegrafConfig(t, getOpt)
	require.NoError(t, err)
	return tele
}

func (r resourceChecker) getVariable(t *testing.T, getOpt getResourceOptFn) (influxdb.Variable, error) {
	t.Helper()

	varSVC := r.tl.VariableService(t)

	var (
		variable *influxdb.Variable
		err      error
	)
	switch opt := getOpt(); {
	case opt.name != "":
		vars, err := varSVC.FindVariables(ctx, influxdb.VariableFilter{
			OrganizationID: &r.tl.Org.ID,
		})
		if err != nil {
			return influxdb.Variable{}, err
		}
		for i := range vars {
			v := vars[i]
			if v.Name == opt.name {
				variable = v
				break
			}
		}
		if variable == nil {
			return influxdb.Variable{}, errors.New("did not find variable: " + opt.name)
		}
	case opt.id != 0:
		variable, err = varSVC.FindVariableByID(ctx, opt.id)
	default:
		require.Fail(t, "did not provide any get option")
	}
	if err != nil {
		return influxdb.Variable{}, err
	}
	return *variable, nil
}

func (r resourceChecker) mustGetVariable(t *testing.T, getOpt getResourceOptFn) influxdb.Variable {
	t.Helper()

	l, err := r.getVariable(t, getOpt)
	require.NoError(t, err)
	return l
}
