package launcher

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/notification/check"
	"github.com/influxdata/influxdb/pkger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var ctx = context.Background()

func TestLauncher_Pkger(t *testing.T) {
	l := RunTestLauncherOrFail(t, ctx)
	l.SetupOrFail(t)
	defer l.ShutdownOrFail(t, ctx)

	svc := l.PkgerService(t)

	deleteBucket := func(t *testing.T, id influxdb.ID) {
		t.Helper()

		require.NoError(t, l.BucketService(t).DeleteBucket(ctx, id))
	}

	t.Run("managing pkg state with stacks", func(t *testing.T) {
		type object struct {
			raw string
		}

		newBucketObject := func(pkgName, name, desc string) object {
			raw := fmt.Sprintf(`
apiVersion: %[1]s
kind: Bucket
metadata:
  name:  %s
spec:
  name: %s
  description: %s
`, pkger.APIVersion, pkgName, name, desc)

			return object{raw: raw}
		}

		newBucketPkgFn := func(t *testing.T, objects ...object) *pkger.Pkg {
			rawObjs := make([]string, 0, len(objects))
			for _, o := range objects {
				rawObjs = append(rawObjs, o.raw)
			}
			pkg, err := pkger.Parse(pkger.EncodingYAML, pkger.FromString(strings.Join(rawObjs, "\n---\n")))
			require.NoError(t, err)

			return pkg
		}

		t.Run("creating a stack", func(t *testing.T) {
			expectedURLs := []string{"http://example.com"}

			newStack, err := svc.InitStack(timedCtx(5*time.Second), l.User.ID, pkger.Stack{
				OrgID:       l.Org.ID,
				Name:        "first stack",
				Description: "desc",
				URLs:        expectedURLs,
			})
			require.NoError(t, err)

			assert.NotZero(t, newStack.ID)
			assert.Equal(t, l.Org.ID, newStack.OrgID)
			assert.Equal(t, "first stack", newStack.Name)
			assert.Equal(t, "desc", newStack.Description)
			assert.Equal(t, expectedURLs, newStack.URLs)
			assert.NotNil(t, newStack.Resources)
			assert.NotZero(t, newStack.CRUDLog)
		})

		t.Run("apply a pkg with a stack", func(t *testing.T) {
			// each test t.Log() represents a test case, but b/c we are dependent
			// on the test before it succeeding, we are using t.Log instead of t.Run
			// to run a sub test.

			getBucket := func(t *testing.T, name string) (influxdb.Bucket, error) {
				bkt, err := l.
					BucketService(t).
					FindBucketByName(timedCtx(time.Second), l.Org.ID, name)
				if err != nil {
					return influxdb.Bucket{}, err
				}
				return *bkt, nil
			}

			initBucketPkgName := "rucketeer_1"
			newPkg := newBucketPkgFn(t, newBucketObject(initBucketPkgName, "display name", "init desc"))

			stack, err := svc.InitStack(timedCtx(5*time.Second), l.User.ID, pkger.Stack{
				OrgID: l.Org.ID,
			})
			require.NoError(t, err)

			var initialSum pkger.Summary
			t.Log("apply pkg with stack id")
			{
				sum, err := svc.Apply(timedCtx(5*time.Second), l.Org.ID, l.User.ID, newPkg, pkger.ApplyWithStackID(stack.ID))
				require.NoError(t, err)
				initialSum = sum

				require.Len(t, sum.Buckets, 1)
				assert.NotZero(t, sum.Buckets[0].ID)
				assert.Equal(t, "display name", sum.Buckets[0].Name)
				assert.Equal(t, "init desc", sum.Buckets[0].Description)

				actualBkt, _ := getBucket(t, "display name")
				assert.Equal(t, sum.Buckets[0].ID, pkger.SafeID(actualBkt.ID))
			}

			updateName := "new name"
			t.Log("apply pkg with stack id where resources change")
			{
				updatedPkg := newBucketPkgFn(t, newBucketObject(initBucketPkgName, updateName, ""))
				sum, err := svc.Apply(timedCtx(5*time.Second), l.Org.ID, l.User.ID, updatedPkg, pkger.ApplyWithStackID(stack.ID))
				require.NoError(t, err)

				require.Len(t, sum.Buckets, 1)
				assert.Equal(t, initialSum.Buckets[0].ID, sum.Buckets[0].ID)
				assert.Equal(t, updateName, sum.Buckets[0].Name)
				assert.Empty(t, sum.Buckets[0].Description)

				actualBkt, err := getBucket(t, updateName)
				require.NoError(t, err)
				require.Equal(t, initialSum.Buckets[0].ID, pkger.SafeID(actualBkt.ID))
			}

			t.Log("an error during application roles back resources to previous state")
			{
				logger := l.log.With(zap.String("service", "pkger"))
				var svc pkger.SVC = pkger.NewService(
					pkger.WithLogger(logger),
					pkger.WithBucketSVC(&fakeBucketSVC{
						BucketService:   l.BucketService(t),
						createKillCount: 1, // kill it after first bucket is created
					}),
					pkger.WithDashboardSVC(l.DashboardService(t)),
					pkger.WithCheckSVC(l.CheckService()),
					pkger.WithLabelSVC(l.LabelService(t)),
					pkger.WithNotificationEndpointSVC(l.NotificationEndpointService(t)),
					pkger.WithNotificationRuleSVC(l.NotificationRuleService()),
					pkger.WithStore(pkger.NewStoreKV(l.Launcher.kvStore)),
					pkger.WithTaskSVC(l.TaskServiceKV()),
					pkger.WithTelegrafSVC(l.TelegrafService(t)),
					pkger.WithVariableSVC(l.VariableService(t)),
				)
				svc = pkger.MWLogging(logger)(svc)

				pkgWithDelete := newBucketPkgFn(t,
					newBucketObject("z_delete_rolls_back", "z_roll_me_back", ""),
					newBucketObject("z_also_rolls_back", "z_rolls_back_too", ""),
				)
				_, err := svc.Apply(timedCtx(5*time.Second), l.Org.ID, l.User.ID, pkgWithDelete, pkger.ApplyWithStackID(stack.ID))
				require.Error(t, err)

				for _, name := range []string{"z_roll_me_back", "z_rolls_back_too"} {
					_, err := getBucket(t, name)
					require.Error(t, err)
				}

				actualBkt, err := getBucket(t, updateName)
				require.NoError(t, err)
				assert.NotEmpty(t, actualBkt.ID)
				assert.NotEqual(t, initialSum.Buckets[0].ID, pkger.SafeID(actualBkt.ID))
			}

			t.Log("apply pkg with stack id where resources have been removed since last run")
			{
				updatedPkg := newBucketPkgFn(t, newBucketObject("non_existent", "non_existent_name", ""))
				sum, err := svc.Apply(timedCtx(5*time.Second), l.Org.ID, l.User.ID, updatedPkg, pkger.ApplyWithStackID(stack.ID))
				require.NoError(t, err)

				require.Len(t, sum.Buckets, 1)
				assert.NotEqual(t, initialSum.Buckets[0].ID, sum.Buckets[0].ID)
				assert.NotZero(t, sum.Buckets[0].ID)
				defer deleteBucket(t, influxdb.ID(sum.Buckets[0].ID))
				assert.Equal(t, "non_existent_name", sum.Buckets[0].Name)
				assert.Empty(t, sum.Buckets[0].Description)

				bkt, err := getBucket(t, "non_existent_name")
				require.NoError(t, err)
				assert.Equal(t, pkger.SafeID(bkt.ID), sum.Buckets[0].ID)

				_, err = getBucket(t, updateName)
				require.Error(t, err)
			}
		})
	})

	t.Run("errors incurred during application of package rolls back to state before package", func(t *testing.T) {
		svc := pkger.NewService(
			pkger.WithBucketSVC(l.BucketService(t)),
			pkger.WithDashboardSVC(l.DashboardService(t)),
			pkger.WithCheckSVC(l.CheckService()),
			pkger.WithLabelSVC(&fakeLabelSVC{
				LabelService: l.LabelService(t),
				killCount:    2, // hits error on 3rd attempt at creating a mapping
			}),
			pkger.WithNotificationEndpointSVC(l.NotificationEndpointService(t)),
			pkger.WithNotificationRuleSVC(l.NotificationRuleService()),
			pkger.WithTaskSVC(l.TaskServiceKV()),
			pkger.WithTelegrafSVC(l.TelegrafService(t)),
			pkger.WithVariableSVC(l.VariableService(t)),
		)

		_, err := svc.Apply(ctx, l.Org.ID, l.User.ID, newPkg(t))
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

	hasMapping := func(t *testing.T, actuals []pkger.SummaryLabelMapping, expected pkger.SummaryLabelMapping) {
		t.Helper()

		for _, actual := range actuals {
			if actual == expected {
				return
			}
		}
		require.FailNowf(t, "did not find expected mapping", "expected: %v", expected)
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
		assert.Equal(t, "http", diff.NotificationRules[0].EndpointType)

		require.Len(t, diff.Dashboards, 1)
		require.Len(t, diff.NotificationEndpoints, 1)
		require.Len(t, diff.Tasks, 1)
		require.Len(t, diff.Telegrafs, 1)

		labels := sum.Labels
		require.Len(t, labels, 2)
		assert.Equal(t, "label_1", labels[0].Name)
		assert.Equal(t, "the 2nd label", labels[1].Name)

		bkts := sum.Buckets
		require.Len(t, bkts, 1)
		assert.Equal(t, "rucketeer", bkts[0].Name)
		hasLabelAssociations(t, bkts[0].LabelAssociations, 2, "label_1", "the 2nd label")

		checks := sum.Checks
		require.Len(t, checks, 2)
		assert.Equal(t, "check 0 name", checks[0].Check.GetName())
		hasLabelAssociations(t, checks[0].LabelAssociations, 1, "label_1")
		assert.Equal(t, "check_1", checks[1].Check.GetName())
		hasLabelAssociations(t, checks[1].LabelAssociations, 1, "label_1")

		dashs := sum.Dashboards
		require.Len(t, dashs, 1)
		assert.Equal(t, "dash_1", dashs[0].Name)
		assert.Equal(t, "desc1", dashs[0].Description)
		hasLabelAssociations(t, dashs[0].LabelAssociations, 2, "label_1", "the 2nd label")

		endpoints := sum.NotificationEndpoints
		require.Len(t, endpoints, 1)
		assert.Equal(t, "no auth endpoint", endpoints[0].NotificationEndpoint.GetName())
		assert.Equal(t, "http none auth desc", endpoints[0].NotificationEndpoint.GetDescription())
		hasLabelAssociations(t, endpoints[0].LabelAssociations, 1, "label_1")

		require.Len(t, sum.Tasks, 1)
		task := sum.Tasks[0]
		assert.Equal(t, "task_1", task.Name)
		assert.Equal(t, "desc_1", task.Description)
		assert.Equal(t, "15 * * * *", task.Cron)
		hasLabelAssociations(t, task.LabelAssociations, 1, "label_1")

		teles := sum.TelegrafConfigs
		require.Len(t, teles, 1)
		assert.Equal(t, "first tele config", teles[0].TelegrafConfig.Name)
		assert.Equal(t, "desc", teles[0].TelegrafConfig.Description)
		hasLabelAssociations(t, teles[0].LabelAssociations, 1, "label_1")

		vars := sum.Variables
		require.Len(t, vars, 1)
		assert.Equal(t, "query var", vars[0].Name)
		hasLabelAssociations(t, vars[0].LabelAssociations, 1, "label_1")
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

		sum, _, err := svc.DryRun(timedCtx(2*time.Second), l.Org.ID, l.User.ID, pkg, pkger.ApplyWithEnvRefs(map[string]string{
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
		sum1, err := svc.Apply(timedCtx(5*time.Second), l.Org.ID, l.User.ID, newPkg(t))
		require.NoError(t, err)

		verifyCompleteSummary := func(t *testing.T, sum1 pkger.Summary, exportAllSum bool) {
			t.Helper()

			labels := sum1.Labels
			require.Len(t, labels, 2)
			if !exportAllSum {
				assert.NotZero(t, labels[0].ID)
			}
			assert.Equal(t, "label_1", labels[0].Name)
			assert.Equal(t, "the 2nd label", labels[1].Name)

			bkts := sum1.Buckets
			if exportAllSum {
				require.Len(t, bkts, 2)
				assert.Equal(t, l.Bucket.Name, bkts[0].Name)
				bkts = bkts[1:]
			}
			require.Len(t, bkts, 1)
			if !exportAllSum {
				assert.NotZero(t, bkts[0].ID)
			}
			assert.Equal(t, "rucketeer", bkts[0].Name)
			hasLabelAssociations(t, bkts[0].LabelAssociations, 2, "label_1", "the 2nd label")

			checks := sum1.Checks
			require.Len(t, checks, 2)
			assert.Equal(t, "check 0 name", checks[0].Check.GetName())
			hasLabelAssociations(t, checks[0].LabelAssociations, 1, "label_1")
			assert.Equal(t, "check_1", checks[1].Check.GetName())
			hasLabelAssociations(t, checks[1].LabelAssociations, 1, "label_1")
			for _, ch := range checks {
				if !exportAllSum {
					assert.NotZero(t, ch.Check.GetID())
				}
			}

			dashs := sum1.Dashboards
			require.Len(t, dashs, 1)
			if !exportAllSum {
				assert.NotZero(t, dashs[0].ID)
			}
			assert.Equal(t, "dash_1", dashs[0].Name)
			assert.Equal(t, "desc1", dashs[0].Description)
			hasLabelAssociations(t, dashs[0].LabelAssociations, 2, "label_1", "the 2nd label")
			require.Len(t, dashs[0].Charts, 1)
			assert.Equal(t, influxdb.ViewPropertyTypeSingleStat, dashs[0].Charts[0].Properties.GetType())

			endpoints := sum1.NotificationEndpoints
			require.Len(t, endpoints, 1)
			if !exportAllSum {
				assert.NotZero(t, endpoints[0].NotificationEndpoint.GetID())
			}
			assert.Equal(t, "no auth endpoint", endpoints[0].NotificationEndpoint.GetName())
			assert.Equal(t, "http none auth desc", endpoints[0].NotificationEndpoint.GetDescription())
			assert.Equal(t, influxdb.TaskStatusInactive, string(endpoints[0].NotificationEndpoint.GetStatus()))
			hasLabelAssociations(t, endpoints[0].LabelAssociations, 1, "label_1")

			require.Len(t, sum1.NotificationRules, 1)
			rule := sum1.NotificationRules[0]
			if !exportAllSum {
				assert.NotZero(t, rule.ID)
			}
			assert.Equal(t, "rule_0", rule.Name)
			assert.Equal(t, pkger.SafeID(endpoints[0].NotificationEndpoint.GetID()), rule.EndpointID)
			if !exportAllSum {
				assert.Equal(t, "http_none_auth_notification_endpoint", rule.EndpointName)
				assert.Equalf(t, "http", rule.EndpointType, "rule: %+v", rule)
			} else {
				assert.NotEmpty(t, rule.EndpointName)
			}

			require.Len(t, sum1.Tasks, 1)
			task := sum1.Tasks[0]
			if !exportAllSum {
				assert.NotZero(t, task.ID)
			}
			assert.Equal(t, "task_1", task.Name)
			assert.Equal(t, "desc_1", task.Description)

			teles := sum1.TelegrafConfigs
			require.Len(t, teles, 1)
			if !exportAllSum {
				assert.NotZero(t, teles[0].TelegrafConfig.ID)
				assert.Equal(t, l.Org.ID, teles[0].TelegrafConfig.OrgID)
			}
			assert.Equal(t, "first tele config", teles[0].TelegrafConfig.Name)
			assert.Equal(t, "desc", teles[0].TelegrafConfig.Description)
			assert.Equal(t, telConf, teles[0].TelegrafConfig.Config)

			vars := sum1.Variables
			require.Len(t, vars, 1)
			if !exportAllSum {
				assert.NotZero(t, vars[0].ID)
			}
			assert.Equal(t, "query var", vars[0].Name)
			hasLabelAssociations(t, vars[0].LabelAssociations, 1, "label_1")
			varArgs := vars[0].Arguments
			require.NotNil(t, varArgs)
			assert.Equal(t, "query", varArgs.Type)
			assert.Equal(t, influxdb.VariableQueryValues{
				Query:    "buckets()  |> filter(fn: (r) => r.name !~ /^_/)  |> rename(columns: {name: \"_value\"})  |> keep(columns: [\"_value\"])",
				Language: "flux",
			}, varArgs.Values)

			newSumMapping := func(id pkger.SafeID, name string, rt influxdb.ResourceType) pkger.SummaryLabelMapping {
				return pkger.SummaryLabelMapping{
					ResourceName: name,
					LabelName:    labels[0].Name,
					LabelID:      labels[0].ID,
					ResourceID:   id,
					ResourceType: rt,
				}
			}

			mappings := sum1.LabelMappings
			require.Len(t, mappings, 11)
			hasMapping(t, mappings, newSumMapping(bkts[0].ID, bkts[0].Name, influxdb.BucketsResourceType))
			hasMapping(t, mappings, newSumMapping(pkger.SafeID(checks[0].Check.GetID()), checks[0].Check.GetName(), influxdb.ChecksResourceType))
			hasMapping(t, mappings, newSumMapping(pkger.SafeID(checks[1].Check.GetID()), checks[1].Check.GetName(), influxdb.ChecksResourceType))
			hasMapping(t, mappings, newSumMapping(dashs[0].ID, dashs[0].Name, influxdb.DashboardsResourceType))
			hasMapping(t, mappings, newSumMapping(pkger.SafeID(endpoints[0].NotificationEndpoint.GetID()), endpoints[0].NotificationEndpoint.GetName(), influxdb.NotificationEndpointResourceType))
			hasMapping(t, mappings, newSumMapping(rule.ID, rule.Name, influxdb.NotificationRuleResourceType))
			hasMapping(t, mappings, newSumMapping(task.ID, task.Name, influxdb.TasksResourceType))
			hasMapping(t, mappings, newSumMapping(pkger.SafeID(teles[0].TelegrafConfig.ID), teles[0].TelegrafConfig.Name, influxdb.TelegrafsResourceType))
			hasMapping(t, mappings, newSumMapping(vars[0].ID, vars[0].Name, influxdb.VariablesResourceType))
		}

		verifyCompleteSummary(t, sum1, false)

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
				newPkg, err := svc.CreatePkg(timedCtx(2*time.Second), pkger.CreateWithAllOrgResources(
					pkger.CreateByOrgIDOpt{
						OrgID: l.Org.ID,
					},
				))
				require.NoError(t, err)

				verifyCompleteSummary(t, newPkg.Summary(), true)
			})

			t.Run("filtered by resource types", func(t *testing.T) {
				newPkg, err := svc.CreatePkg(timedCtx(2*time.Second), pkger.CreateWithAllOrgResources(
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
				newPkg, err := svc.CreatePkg(timedCtx(2*time.Second), pkger.CreateWithAllOrgResources(
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
				newPkg, err := svc.CreatePkg(timedCtx(2*time.Second), pkger.CreateWithAllOrgResources(
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
				newPkg, err := svc.CreatePkg(timedCtx(2*time.Second), pkger.CreateWithAllOrgResources(
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
			sum2, err := svc.Apply(timedCtx(5*time.Second), l.Org.ID, l.User.ID, newPkg(t))
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

				sum, err := svc.Apply(ctx, l.Org.ID, l.User.ID, pkg)
				require.NoError(t, err)
				return sum
			}

			pkgWithSecretRaw := fmt.Sprintf(`
apiVersion: %[1]s
kind: NotificationEndpointPagerDuty
metadata:
  name:      pager_duty_notification_endpoint
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
  name:      pager_duty_notification_endpoint
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

			newPkg, err := svc.CreatePkg(timedCtx(2*time.Second),
				pkger.CreateWithExistingResources(append(resToClone, resWithNewName...)...),
			)
			require.NoError(t, err)

			newSum := newPkg.Summary()

			labels := newSum.Labels
			require.Len(t, labels, 2)
			assert.Zero(t, labels[0].ID)
			assert.Equal(t, "label_1", labels[0].Name)
			assert.Zero(t, labels[1].ID)
			assert.Equal(t, "the 2nd label", labels[1].Name)

			bkts := newSum.Buckets
			require.Len(t, bkts, 1)
			assert.Zero(t, bkts[0].ID)
			assert.Equal(t, "rucketeer", bkts[0].Name)
			hasLabelAssociations(t, bkts[0].LabelAssociations, 2, "label_1", "the 2nd label")

			checks := newSum.Checks
			require.Len(t, checks, 2)
			assert.Equal(t, "check 0 name", checks[0].Check.GetName())
			hasLabelAssociations(t, checks[0].LabelAssociations, 1, "label_1")
			assert.Equal(t, "check_1", checks[1].Check.GetName())
			hasLabelAssociations(t, checks[1].LabelAssociations, 1, "label_1")

			dashs := newSum.Dashboards
			require.Len(t, dashs, 1)
			assert.Zero(t, dashs[0].ID)
			assert.Equal(t, "dash_1", dashs[0].Name)
			assert.Equal(t, "desc1", dashs[0].Description)
			hasLabelAssociations(t, dashs[0].LabelAssociations, 2, "label_1", "the 2nd label")
			require.Len(t, dashs[0].Charts, 1)
			assert.Equal(t, influxdb.ViewPropertyTypeSingleStat, dashs[0].Charts[0].Properties.GetType())

			newEndpoints := newSum.NotificationEndpoints
			require.Len(t, newEndpoints, 1)
			assert.Equal(t, sum1Endpoints[0].NotificationEndpoint.GetName(), newEndpoints[0].NotificationEndpoint.GetName())
			assert.Equal(t, sum1Endpoints[0].NotificationEndpoint.GetDescription(), newEndpoints[0].NotificationEndpoint.GetDescription())
			hasLabelAssociations(t, newEndpoints[0].LabelAssociations, 1, "label_1")

			require.Len(t, newSum.NotificationRules, 1)
			newRule := newSum.NotificationRules[0]
			assert.Equal(t, "new rule name", newRule.Name)
			assert.Zero(t, newRule.EndpointID)
			assert.NotEmpty(t, newRule.EndpointName)
			hasLabelAssociations(t, newRule.LabelAssociations, 1, "label_1")

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
			hasLabelAssociations(t, newSum.TelegrafConfigs[0].LabelAssociations, 1, "label_1")

			vars := newSum.Variables
			require.Len(t, vars, 1)
			assert.Zero(t, vars[0].ID)
			assert.Equal(t, "new name", vars[0].Name) // new name
			hasLabelAssociations(t, vars[0].LabelAssociations, 1, "label_1")
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
				pkger.WithTaskSVC(l.TaskServiceKV()),
				pkger.WithTelegrafSVC(l.TelegrafService(t)),
				pkger.WithVariableSVC(l.VariableService(t)),
			)

			_, err = svc.Apply(ctx, l.Org.ID, 0, updatePkg)
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
    name: Http.POST Synthetic (POST)
spec:
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

		sum, err := svc.Apply(timedCtx(time.Second), l.Org.ID, l.User.ID, pkg)
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

		sum, _, err := svc.DryRun(timedCtx(time.Second), l.Org.ID, l.User.ID, pkg)
		require.NoError(t, err)

		require.Len(t, sum.Buckets, 1)
		assert.Equal(t, "$bkt-1-name-ref", sum.Buckets[0].Name)
		assert.Len(t, sum.Buckets[0].LabelAssociations, 1)
		require.Len(t, sum.Checks, 1)
		assert.Equal(t, "$check-1-name-ref", sum.Checks[0].Check.GetName())
		require.Len(t, sum.Dashboards, 1)
		assert.Equal(t, "$dash-1-name-ref", sum.Dashboards[0].Name)
		require.Len(t, sum.Labels, 1)
		assert.Equal(t, "$label-1-name-ref", sum.Labels[0].Name)
		require.Len(t, sum.NotificationEndpoints, 1)
		assert.Equal(t, "$endpoint-1-name-ref", sum.NotificationEndpoints[0].NotificationEndpoint.GetName())
		require.Len(t, sum.NotificationRules, 1)
		assert.Equal(t, "$rule-1-name-ref", sum.NotificationRules[0].Name)
		require.Len(t, sum.TelegrafConfigs, 1)
		assert.Equal(t, "$task-1-name-ref", sum.Tasks[0].Name)
		require.Len(t, sum.TelegrafConfigs, 1)
		assert.Equal(t, "$telegraf-1-name-ref", sum.TelegrafConfigs[0].TelegrafConfig.Name)
		require.Len(t, sum.Variables, 1)
		assert.Equal(t, "$var-1-name-ref", sum.Variables[0].Name)

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

		sum, err = svc.Apply(timedCtx(5*time.Second), l.Org.ID, l.User.ID, pkg, pkger.ApplyWithEnvRefs(map[string]string{
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
		assert.Equal(t, "endpoint_threeve", sum.NotificationRules[0].EndpointName)
		assert.Equal(t, "telegraf_threeve", sum.TelegrafConfigs[0].TelegrafConfig.Name)
		assert.Equal(t, "task_threeve", sum.Tasks[0].Name)
		assert.Equal(t, "var_threeve", sum.Variables[0].Name)
		assert.Empty(t, sum.MissingEnvs)
	})
}

func timedCtx(d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(ctx, d)
	var _ = cancel
	return ctx
}

func newPkg(t *testing.T) *pkger.Pkg {
	t.Helper()

	pkg, err := pkger.Parse(pkger.EncodingYAML, pkger.FromString(pkgYMLStr))
	require.NoError(t, err)
	return pkg
}

const telConf = `[agent]
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
  name: label_1
---
apiVersion: %[1]s
kind: Label
metadata:
  name: the 2nd label
spec:
  name: the 2nd label
---
apiVersion: %[1]s
kind: Bucket
metadata:
  name: rucket_1
spec:
  name: rucketeer
  associations:
    - kind: Label
      name: label_1
    - kind: Label
      name: the 2nd label
---
apiVersion: %[1]s
kind: Dashboard
metadata:
  name: dash_UUID
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
      name: label_1
    - kind: Label
      name: the 2nd label
---
apiVersion: %[1]s
kind: Variable
metadata:
  name:  var_query_1
spec:
  name: query var
  description: var_query_1 desc
  type: query
  language: flux
  query: |
    buckets()  |> filter(fn: (r) => r.name !~ /^_/)  |> rename(columns: {name: "_value"})  |> keep(columns: ["_value"])
  associations:
    - kind: Label
      name: label_1
---
apiVersion: %[1]s
kind: Telegraf
metadata:
  name:  first_tele_config
spec:
  name: first tele config
  description: desc
  associations:
    - kind: Label
      name: label_1
  config: %+q
---
apiVersion: %[1]s
kind: NotificationEndpointHTTP
metadata:
  name:  http_none_auth_notification_endpoint # on export of resource created from this, will not be same name as this
spec:
  name: no auth endpoint
  type: none
  description: http none auth desc
  method: GET
  url:  https://www.example.com/endpoint/noneauth
  status: inactive
  associations:
    - kind: Label
      name: label_1
---
apiVersion: %[1]s
kind: CheckThreshold
metadata:
  name:  check_0
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
      name: label_1
---
apiVersion: %[1]s
kind: CheckDeadman
metadata:
  name:  check_1
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
      name: label_1
---
apiVersion: %[1]s
kind: NotificationRule
metadata:
  name:  rule_UUID
spec:
  name:  rule_0
  description: desc_0
  endpointName: http_none_auth_notification_endpoint
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
      name: label_1
---
apiVersion: %[1]s
kind: Task
metadata:
  name:  task_UUID
spec:
  name:  task_1
  description: desc_1
  cron: 15 * * * *
  query:  >
    from(bucket: "rucket_1")
      |> yield()
  associations:
    - kind: Label
      name: label_1
`, pkger.APIVersion, telConf)

var updatePkgYMLStr = fmt.Sprintf(`
apiVersion: %[1]s
kind: Label
metadata:
  name:  label_1
spec:
  descriptin: new desc
---
apiVersion: %[1]s
kind: Bucket
metadata:
  name:  rucket_1
spec:
  descriptin: new desc
  associations:
    - kind: Label
      name: label_1
---
apiVersion: %[1]s
kind: Variable
metadata:
  name:  var_query_1
spec:
  description: new desc
  type: query
  language: flux
  query: |
    buckets()  |> filter(fn: (r) => r.name !~ /^_/)  |> rename(columns: {name: "_value"})  |> keep(columns: ["_value"])
  associations:
    - kind: Label
      name: label_1
---
apiVersion: %[1]s
kind: NotificationEndpointHTTP
metadata:
  name:  http_none_auth_notification_endpoint
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
  name:  check_0
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
	callCount mock.SafeCount
	killCount int
}

func (f *fakeLabelSVC) CreateLabelMapping(ctx context.Context, m *influxdb.LabelMapping) error {
	defer f.callCount.IncrFn()()
	if f.callCount.Count() == f.killCount {
		return errors.New("reached kill count")
	}
	return f.LabelService.CreateLabelMapping(ctx, m)
}
