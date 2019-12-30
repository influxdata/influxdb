package launcher_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/cmd/influxd/launcher"
	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/notification/check"
	"github.com/influxdata/influxdb/pkger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLauncher_Pkger(t *testing.T) {
	l := launcher.RunTestLauncherOrFail(t, ctx)
	l.SetupOrFail(t)
	defer l.ShutdownOrFail(t, ctx)

	svc := l.PkgerService(t)

	t.Run("create a new package", func(t *testing.T) {
		newPkg, err := svc.CreatePkg(timedCtx(time.Second),
			pkger.CreateWithMetadata(pkger.Metadata{
				Description: "new desc",
				Name:        "new name",
				Version:     "v1.0.0",
			}),
		)
		require.NoError(t, err)

		assert.Equal(t, "new name", newPkg.Metadata.Name)
		assert.Equal(t, "new desc", newPkg.Metadata.Description)
		assert.Equal(t, "v1.0.0", newPkg.Metadata.Version)
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

		require.Len(t, diff.Labels, 1)
		assert.True(t, diff.Labels[0].IsNew())

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
		require.Len(t, labels, 1)
		assert.Equal(t, "label_1", labels[0].Name)

		bkts := sum.Buckets
		require.Len(t, bkts, 1)
		assert.Equal(t, "rucket_1", bkts[0].Name)
		hasLabelAssociations(t, bkts[0].LabelAssociations, 1, "label_1")

		checks := sum.Checks
		require.Len(t, checks, 2)
		for i, ch := range checks {
			assert.Equal(t, fmt.Sprintf("check_%d", i), ch.Check.GetName())
			hasLabelAssociations(t, ch.LabelAssociations, 1, "label_1")
		}

		dashs := sum.Dashboards
		require.Len(t, dashs, 1)
		assert.Equal(t, "dash_1", dashs[0].Name)
		assert.Equal(t, "desc1", dashs[0].Description)
		hasLabelAssociations(t, dashs[0].LabelAssociations, 1, "label_1")

		endpoints := sum.NotificationEndpoints
		require.Len(t, endpoints, 1)
		assert.Equal(t, "http_none_auth_notification_endpoint", endpoints[0].NotificationEndpoint.GetName())
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
		assert.Equal(t, "first_tele_config", teles[0].TelegrafConfig.Name)
		assert.Equal(t, "desc", teles[0].TelegrafConfig.Description)
		hasLabelAssociations(t, teles[0].LabelAssociations, 1, "label_1")

		vars := sum.Variables
		require.Len(t, vars, 1)
		assert.Equal(t, "var_query_1", vars[0].Name)
		hasLabelAssociations(t, vars[0].LabelAssociations, 1, "label_1")
		varArgs := vars[0].Arguments
		require.NotNil(t, varArgs)
		assert.Equal(t, "query", varArgs.Type)
		assert.Equal(t, influxdb.VariableQueryValues{
			Query:    "buckets()  |> filter(fn: (r) => r.name !~ /^_/)  |> rename(columns: {name: \"_value\"})  |> keep(columns: [\"_value\"])",
			Language: "flux",
		}, varArgs.Values)
	})

	t.Run("apply a package of all new resources", func(t *testing.T) {
		// this initial test is also setup for the sub tests
		sum1, err := svc.Apply(timedCtx(5*time.Second), l.Org.ID, l.User.ID, newPkg(t))
		require.NoError(t, err)

		labels := sum1.Labels
		require.Len(t, labels, 1)
		assert.NotZero(t, labels[0].ID)
		assert.Equal(t, "label_1", labels[0].Name)

		bkts := sum1.Buckets
		require.Len(t, bkts, 1)
		assert.NotZero(t, bkts[0].ID)
		assert.Equal(t, "rucket_1", bkts[0].Name)
		hasLabelAssociations(t, bkts[0].LabelAssociations, 1, "label_1")

		checks := sum1.Checks
		require.Len(t, checks, 2)
		for i, ch := range checks {
			assert.NotZero(t, ch.Check.GetID())
			assert.Equal(t, fmt.Sprintf("check_%d", i), ch.Check.GetName())
			hasLabelAssociations(t, ch.LabelAssociations, 1, "label_1")
		}

		dashs := sum1.Dashboards
		require.Len(t, dashs, 1)
		assert.NotZero(t, dashs[0].ID)
		assert.Equal(t, "dash_1", dashs[0].Name)
		assert.Equal(t, "desc1", dashs[0].Description)
		hasLabelAssociations(t, dashs[0].LabelAssociations, 1, "label_1")
		require.Len(t, dashs[0].Charts, 1)
		assert.Equal(t, influxdb.ViewPropertyTypeSingleStat, dashs[0].Charts[0].Properties.GetType())

		endpoints := sum1.NotificationEndpoints
		require.Len(t, endpoints, 1)
		assert.NotZero(t, endpoints[0].NotificationEndpoint.GetID())
		assert.Equal(t, "http_none_auth_notification_endpoint", endpoints[0].NotificationEndpoint.GetName())
		assert.Equal(t, "http none auth desc", endpoints[0].NotificationEndpoint.GetDescription())
		assert.Equal(t, influxdb.TaskStatusInactive, string(endpoints[0].NotificationEndpoint.GetStatus()))
		hasLabelAssociations(t, endpoints[0].LabelAssociations, 1, "label_1")

		require.Len(t, sum1.NotificationRules, 1)
		rule := sum1.NotificationRules[0]
		assert.NotZero(t, rule.ID)
		assert.Equal(t, "rule_0", rule.Name)
		assert.Equal(t, pkger.SafeID(endpoints[0].NotificationEndpoint.GetID()), rule.EndpointID)
		assert.Equal(t, "http_none_auth_notification_endpoint", rule.EndpointName)
		assert.Equal(t, "http", rule.EndpointType)

		require.Len(t, sum1.Tasks, 1)
		task := sum1.Tasks[0]
		assert.NotZero(t, task.ID)
		assert.Equal(t, "task_1", task.Name)
		assert.Equal(t, "desc_1", task.Description)

		teles := sum1.TelegrafConfigs
		require.Len(t, teles, 1)
		assert.NotZero(t, teles[0].TelegrafConfig.ID)
		assert.Equal(t, l.Org.ID, teles[0].TelegrafConfig.OrgID)
		assert.Equal(t, "first_tele_config", teles[0].TelegrafConfig.Name)
		assert.Equal(t, "desc", teles[0].TelegrafConfig.Description)
		assert.Equal(t, telConf, teles[0].TelegrafConfig.Config)

		vars := sum1.Variables
		require.Len(t, vars, 1)
		assert.NotZero(t, vars[0].ID)
		assert.Equal(t, "var_query_1", vars[0].Name)
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
		require.Len(t, mappings, 9)
		hasMapping(t, mappings, newSumMapping(bkts[0].ID, bkts[0].Name, influxdb.BucketsResourceType))
		hasMapping(t, mappings, newSumMapping(pkger.SafeID(checks[0].Check.GetID()), checks[0].Check.GetName(), influxdb.ChecksResourceType))
		hasMapping(t, mappings, newSumMapping(pkger.SafeID(checks[1].Check.GetID()), checks[1].Check.GetName(), influxdb.ChecksResourceType))
		hasMapping(t, mappings, newSumMapping(dashs[0].ID, dashs[0].Name, influxdb.DashboardsResourceType))
		hasMapping(t, mappings, newSumMapping(pkger.SafeID(endpoints[0].NotificationEndpoint.GetID()), endpoints[0].NotificationEndpoint.GetName(), influxdb.NotificationEndpointResourceType))
		hasMapping(t, mappings, newSumMapping(rule.ID, rule.Name, influxdb.NotificationRuleResourceType))
		hasMapping(t, mappings, newSumMapping(task.ID, task.Name, influxdb.TasksResourceType))
		hasMapping(t, mappings, newSumMapping(pkger.SafeID(teles[0].TelegrafConfig.ID), teles[0].TelegrafConfig.Name, influxdb.TelegrafsResourceType))
		hasMapping(t, mappings, newSumMapping(vars[0].ID, vars[0].Name, influxdb.VariablesResourceType))

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

			const pkgWithSecretRaw = `apiVersion: 0.1.0
kind: Package
meta:
  pkgName:      pkg_name
  pkgVersion:   1
  description:  pack description
spec:
  resources:
    - kind: Notification_Endpoint_Pager_Duty
      name: pager_duty_notification_endpoint
      url:  http://localhost:8080/orgs/7167eb6719fa34e5/alert-history
      routingKey: secret-sauce
`
			secretSum := applyPkgStr(t, pkgWithSecretRaw)
			require.Len(t, secretSum.NotificationEndpoints, 1)

			id := secretSum.NotificationEndpoints[0].NotificationEndpoint.GetID()
			expected := influxdb.SecretField{
				Key: id.String() + "-routing-key",
			}
			secrets := secretSum.NotificationEndpoints[0].NotificationEndpoint.SecretFields()
			require.Len(t, secrets, 1)
			assert.Equal(t, expected, secrets[0])

			const pkgWithSecretRef = `apiVersion: 0.1.0
kind: Package
meta:
  pkgName:      pkg_name
  pkgVersion:   1
  description:  pack description
spec:
  resources:
    - kind: Notification_Endpoint_Pager_Duty
      name: pager_duty_notification_endpoint
      url:  http://localhost:8080/orgs/7167eb6719fa34e5/alert-history
      routingKey:
        secretRef:
          key: %s-routing-key
`
			secretSum = applyPkgStr(t, fmt.Sprintf(pkgWithSecretRef, id.String()))
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
					ID:   influxdb.ID(bkts[0].ID),
				},
				{
					Kind: pkger.KindCheck,
					ID:   checks[0].Check.GetID(),
				},
				{
					Kind: pkger.KindCheck,
					ID:   checks[1].Check.GetID(),
				},
				{
					Kind: pkger.KindDashboard,
					ID:   influxdb.ID(dashs[0].ID),
				},
				{
					Kind: pkger.KindLabel,
					ID:   influxdb.ID(labels[0].ID),
				},
				{
					Kind: pkger.KindNotificationEndpoint,
					ID:   endpoints[0].NotificationEndpoint.GetID(),
				},
				{
					Kind: pkger.KindTask,
					ID:   influxdb.ID(task.ID),
				},
				{
					Kind: pkger.KindTelegraf,
					ID:   teles[0].TelegrafConfig.ID,
				},
			}

			resWithNewName := []pkger.ResourceToClone{
				{
					Kind: pkger.KindNotificationRule,
					Name: "new rule name",
					ID:   influxdb.ID(rule.ID),
				},
				{
					Kind: pkger.KindVariable,
					Name: "new name",
					ID:   influxdb.ID(vars[0].ID),
				},
			}

			newPkg, err := svc.CreatePkg(timedCtx(2*time.Second),
				pkger.CreateWithMetadata(pkger.Metadata{
					Description: "newest desc",
					Name:        "newest name",
					Version:     "v1.0.1",
				}),
				pkger.CreateWithExistingResources(append(resToClone, resWithNewName...)...),
			)
			require.NoError(t, err)

			assert.Equal(t, "newest desc", newPkg.Metadata.Description)
			assert.Equal(t, "newest name", newPkg.Metadata.Name)
			assert.Equal(t, "v1.0.1", newPkg.Metadata.Version)

			newSum := newPkg.Summary()

			labels := newSum.Labels
			require.Len(t, labels, 1)
			assert.Zero(t, labels[0].ID)
			assert.Equal(t, "label_1", labels[0].Name)

			bkts := newSum.Buckets
			require.Len(t, bkts, 1)
			assert.Zero(t, bkts[0].ID)
			assert.Equal(t, "rucket_1", bkts[0].Name)
			hasLabelAssociations(t, bkts[0].LabelAssociations, 1, "label_1")

			checks := newSum.Checks
			require.Len(t, checks, 2)
			for i := range make([]struct{}, 2) {
				assert.Zero(t, checks[0].Check.GetID())
				assert.Equal(t, fmt.Sprintf("check_%d", i), checks[i].Check.GetName())
				hasLabelAssociations(t, checks[i].LabelAssociations, 1, "label_1")
			}

			dashs := newSum.Dashboards
			require.Len(t, dashs, 1)
			assert.Zero(t, dashs[0].ID)
			assert.Equal(t, "dash_1", dashs[0].Name)
			assert.Equal(t, "desc1", dashs[0].Description)
			hasLabelAssociations(t, dashs[0].LabelAssociations, 1, "label_1")
			require.Len(t, dashs[0].Charts, 1)
			assert.Equal(t, influxdb.ViewPropertyTypeSingleStat, dashs[0].Charts[0].Properties.GetType())

			newEndpoints := newSum.NotificationEndpoints
			require.Len(t, newEndpoints, 1)
			assert.Equal(t, endpoints[0].NotificationEndpoint.GetName(), newEndpoints[0].NotificationEndpoint.GetName())
			assert.Equal(t, endpoints[0].NotificationEndpoint.GetDescription(), newEndpoints[0].NotificationEndpoint.GetDescription())
			hasLabelAssociations(t, newEndpoints[0].LabelAssociations, 1, "label_1")

			require.Len(t, newSum.NotificationRules, 1)
			newRule := newSum.NotificationRules[0]
			assert.Equal(t, "new rule name", newRule.Name)
			assert.Zero(t, newRule.EndpointID)
			assert.Equal(t, rule.EndpointName, newRule.EndpointName)
			hasLabelAssociations(t, newRule.LabelAssociations, 1, "label_1")

			require.Len(t, newSum.Tasks, 1)
			newTask := newSum.Tasks[0]
			assert.Equal(t, task.Name, newTask.Name)
			assert.Equal(t, task.Description, newTask.Description)
			assert.Equal(t, task.Cron, newTask.Cron)
			assert.Equal(t, task.Every, newTask.Every)
			assert.Equal(t, task.Offset, newTask.Offset)
			assert.Equal(t, task.Query, newTask.Query)
			assert.Equal(t, task.Status, newTask.Status)

			require.Len(t, newSum.TelegrafConfigs, 1)
			assert.Equal(t, teles[0].TelegrafConfig.Name, newSum.TelegrafConfigs[0].TelegrafConfig.Name)
			assert.Equal(t, teles[0].TelegrafConfig.Description, newSum.TelegrafConfigs[0].TelegrafConfig.Description)
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
					BucketService: l.BucketService(t),
					killCount:     0, // kill on first update for bucket
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

			bkt, err := l.BucketService(t).FindBucketByID(ctx, influxdb.ID(bkts[0].ID))
			require.NoError(t, err)
			// make sure the desc change is not applied and is rolled back to prev desc
			assert.Equal(t, bkts[0].Description, bkt.Description)

			ch, err := l.CheckService().FindCheckByID(ctx, checks[0].Check.GetID())
			require.NoError(t, err)
			ch.SetOwnerID(0)
			deadman, ok := ch.(*check.Threshold)
			require.True(t, ok)
			// validate the change to query is not persisting returned to previous state.
			// not checking entire bits, b/c we dont' save userID and so forth and makes a
			// direct comparison very annoying...
			assert.Equal(t, checks[0].Check.(*check.Threshold).Query.Text, deadman.Query.Text)

			label, err := l.LabelService(t).FindLabelByID(ctx, influxdb.ID(labels[0].ID))
			require.NoError(t, err)
			assert.Equal(t, labels[0].Properties.Description, label.Properties["description"])

			endpoint, err := l.NotificationEndpointService(t).FindNotificationEndpointByID(ctx, endpoints[0].NotificationEndpoint.GetID())
			require.NoError(t, err)
			assert.Equal(t, endpoints[0].NotificationEndpoint.GetDescription(), endpoint.GetDescription())

			v, err := l.VariableService(t).FindVariableByID(ctx, influxdb.ID(vars[0].ID))
			require.NoError(t, err)
			assert.Equal(t, vars[0].Description, v.Description)
		})
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

var pkgYMLStr = fmt.Sprintf(`apiVersion: 0.1.0
kind: Package
meta:
  pkgName:      pkg_name
  pkgVersion:   1
  description:  pack description
spec:
  resources:
    - kind: Label
      name: label_1
    - kind: Bucket
      name: rucket_1
      associations:
        - kind: Label
          name: label_1
    - kind: Dashboard
      name: dash_1
      description: desc1
      associations:
        - kind: Label
          name: label_1
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
    - kind: Variable
      name: var_query_1
      description: var_query_1 desc
      type: query
      language: flux
      query: |
        buckets()  |> filter(fn: (r) => r.name !~ /^_/)  |> rename(columns: {name: "_value"})  |> keep(columns: ["_value"])
      associations:
        - kind: Label
          name: label_1
    - kind: Telegraf
      name: first_tele_config
      description: desc
      associations:
        - kind: Label
          name: label_1
      config: %+q
    - kind: Notification_Endpoint_HTTP
      name: http_none_auth_notification_endpoint
      type: none
      description: http none auth desc
      method: GET
      url:  https://www.example.com/endpoint/noneauth
      status: inactive
      associations:
      - kind: Label
        name: label_1
    - kind: Check_Threshold
      name: check_0
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
    - kind: Check_Deadman
      name: check_1
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
    - kind: Notification_Rule
      name: rule_0
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
    - kind: Task
      name: task_1
      description: desc_1
      cron: 15 * * * *
      query:  >
        from(bucket: "rucket_1")
          |> yield()
      associations:
        - kind: Label
          name: label_1
`, telConf)

const updatePkgYMLStr = `apiVersion: 0.1.0
kind: Package
meta:
  pkgName:      pkg_name
  pkgVersion:   1
  description:  pack description
spec:
  resources:
    - kind: Label
      name: label_1
      description: new desc
    - kind: Bucket
      name: rucket_1
      description: new desc
      associations:
        - kind: Label
          name: label_1
    - kind: Variable
      name: var_query_1
      description: new desc
      type: query
      language: flux
      query: |
        buckets()  |> filter(fn: (r) => r.name !~ /^_/)  |> rename(columns: {name: "_value"})  |> keep(columns: ["_value"])
      associations:
        - kind: Label
          name: label_1
    - kind: Notification_Endpoint_HTTP
      name: http_none_auth_notification_endpoint
      type: none
      description: new desc
      method: GET
      url:  https://www.example.com/endpoint/noneauth
      status: active
    - kind: Check_Threshold
      name: check_0
      every: 1m
      query:  from("rucket1") |> yield()
      statusMessageTemplate: "Check: ${ r._check_name } is: ${ r._level }"
      thresholds:
        - type: inside_range
          level: INfO
          min: 30.0
          max: 45.0
`

type fakeBucketSVC struct {
	influxdb.BucketService
	updateCallCount mock.SafeCount
	killCount       int
}

func (f *fakeBucketSVC) UpdateBucket(ctx context.Context, id influxdb.ID, upd influxdb.BucketUpdate) (*influxdb.Bucket, error) {
	if f.updateCallCount.Count() == f.killCount {
		return nil, errors.New("reached kill count")
	}
	defer f.updateCallCount.IncrFn()()
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
