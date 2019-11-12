package launcher_test

import (
	"context"
	"testing"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/cmd/influxd/launcher"
	"github.com/influxdata/influxdb/pkger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLauncher_Pkger(t *testing.T) {
	l := launcher.RunTestLauncherOrFail(t, ctx)
	l.SetupOrFail(t)
	defer l.ShutdownOrFail(t, ctx)

	svc := pkger.NewService(
		pkger.WithBucketSVC(l.BucketService()),
		pkger.WithDashboardSVC(l.DashboardService()),
		pkger.WithLabelSVC(l.LabelService()),
		pkger.WithVariableSVC(l.VariableService()),
	)

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

	hasLabelAssociations := func(t *testing.T, associations []influxdb.Label, numAss int, expectedNames ...string) {
		t.Helper()

		require.Len(t, associations, numAss)

		hasAss := func(t *testing.T, expected string) {
			t.Helper()
			for _, ass := range associations {
				if ass.Name == expected {
					return
				}
			}
			require.FailNow(t, "did not find expected association: "+expected)
		}

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
		pkg, err := pkger.Parse(pkger.EncodingYAML, pkger.FromString(pkgYMLStr))
		require.NoError(t, err)

		sum, diff, err := svc.DryRun(ctx, l.Org.ID, pkg)
		require.NoError(t, err)

		diffBkts := diff.Buckets
		require.Len(t, diffBkts, 1)
		assert.True(t, diffBkts[0].IsNew())

		diffLabels := diff.Labels
		require.Len(t, diffLabels, 1)
		assert.True(t, diffLabels[0].IsNew())

		diffVars := diff.Variables
		require.Len(t, diffVars, 1)
		assert.True(t, diffVars[0].IsNew())

		require.Len(t, diff.Dashboards, 1)

		labels := sum.Labels
		require.Len(t, labels, 1)
		assert.Equal(t, "label_1", labels[0].Name)

		bkts := sum.Buckets
		require.Len(t, bkts, 1)
		assert.Equal(t, "rucket_1", bkts[0].Name)
		hasLabelAssociations(t, bkts[0].LabelAssociations, 1, "label_1")

		dashs := sum.Dashboards
		require.Len(t, dashs, 1)
		assert.Equal(t, "dash_1", dashs[0].Name)
		assert.Equal(t, "desc1", dashs[0].Description)
		hasLabelAssociations(t, dashs[0].LabelAssociations, 1, "label_1")

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

		pkg, err := pkger.Parse(pkger.EncodingYAML, pkger.FromString(pkgYMLStr))
		require.NoError(t, err)

		sum1, err := svc.Apply(timedCtx(5*time.Second), l.Org.ID, pkg)
		require.NoError(t, err)

		labels := sum1.Labels
		require.Len(t, labels, 1)
		assert.NotEqual(t, influxdb.ID(0), labels[0].ID)
		assert.Equal(t, "label_1", labels[0].Name)

		bkts := sum1.Buckets
		require.Len(t, bkts, 1)
		assert.NotEqual(t, influxdb.ID(0), bkts[0].ID)
		assert.Equal(t, "rucket_1", bkts[0].Name)
		hasLabelAssociations(t, bkts[0].LabelAssociations, 1, "label_1")

		dashs := sum1.Dashboards
		require.Len(t, dashs, 1)
		assert.NotEqual(t, influxdb.ID(0), dashs[0].ID)
		assert.Equal(t, "dash_1", dashs[0].Name)
		assert.Equal(t, "desc1", dashs[0].Description)
		hasLabelAssociations(t, dashs[0].LabelAssociations, 1, "label_1")

		vars := sum1.Variables
		require.Len(t, vars, 1)
		assert.NotEqual(t, influxdb.ID(0), vars[0].ID)
		assert.Equal(t, "var_query_1", vars[0].Name)
		hasLabelAssociations(t, vars[0].LabelAssociations, 1, "label_1")
		varArgs := vars[0].Arguments
		require.NotNil(t, varArgs)
		assert.Equal(t, "query", varArgs.Type)
		assert.Equal(t, influxdb.VariableQueryValues{
			Query:    "buckets()  |> filter(fn: (r) => r.name !~ /^_/)  |> rename(columns: {name: \"_value\"})  |> keep(columns: [\"_value\"])",
			Language: "flux",
		}, varArgs.Values)

		newSumMapping := func(id influxdb.ID, name string, rt influxdb.ResourceType) pkger.SummaryLabelMapping {
			return pkger.SummaryLabelMapping{
				ResourceName: name,
				LabelName:    labels[0].Name,
				LabelMapping: influxdb.LabelMapping{
					LabelID:      labels[0].ID,
					ResourceID:   id,
					ResourceType: rt,
				},
			}
		}

		mappings := sum1.LabelMappings
		require.Len(t, mappings, 3)
		hasMapping(t, mappings, newSumMapping(bkts[0].ID, bkts[0].Name, influxdb.BucketsResourceType))
		hasMapping(t, mappings, newSumMapping(influxdb.ID(dashs[0].ID), dashs[0].Name, influxdb.DashboardsResourceType))
		hasMapping(t, mappings, newSumMapping(influxdb.ID(vars[0].ID), vars[0].Name, influxdb.VariablesResourceType))

		pkg, err = pkger.Parse(pkger.EncodingYAML, pkger.FromString(pkgYMLStr))
		require.NoError(t, err)

		t.Run("pkg with same bkt-var-label does nto create new resources for them", func(t *testing.T) {
			// validate the new package doesn't create new resources for bkts/labels/vars
			// since names collide.
			sum2, err := svc.Apply(timedCtx(5*time.Second), l.Org.ID, pkg)
			require.NoError(t, err)

			require.Equal(t, sum1.Buckets, sum2.Buckets)
			require.Equal(t, sum1.Labels, sum2.Labels)
			require.Equal(t, sum1.Variables, sum2.Variables)

			// dashboards should be new
			require.NotEqual(t, sum1.Dashboards, sum2.Dashboards)
		})

		t.Run("exporting resources with existing ids should return a valid pkg", func(t *testing.T) {
			resToClone := []pkger.ResourceToClone{
				{
					Kind: pkger.KindBucket,
					ID:   bkts[0].ID,
				},
				{
					Kind: pkger.KindDashboard,
					ID:   influxdb.ID(dashs[0].ID),
				},
				{
					Kind: pkger.KindLabel,
					ID:   labels[0].ID,
				},
			}

			resWithNewName := []pkger.ResourceToClone{
				{
					Kind: pkger.KindVariable,
					Name: "new name",
					ID:   vars[0].ID,
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

			dashs := newSum.Dashboards
			require.Len(t, dashs, 1)
			assert.Zero(t, dashs[0].ID)
			assert.Equal(t, "dash_1", dashs[0].Name)
			assert.Equal(t, "desc1", dashs[0].Description)
			hasLabelAssociations(t, dashs[0].LabelAssociations, 1, "label_1")

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
	})
}

func timedCtx(d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(ctx, d)
	var _ = cancel
	return ctx
}

const pkgYMLStr = `apiVersion: 0.1.0
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
          name: label_1`
