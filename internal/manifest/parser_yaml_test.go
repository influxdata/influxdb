package manifest_test

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/internal/manifest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParser_YAML(t *testing.T) {
	type baseAsserts struct {
		version     string
		kind        string
		metaName    string
		metaVersion string
	}

	validParsedManifest := func(t *testing.T, yamlStr string, expected baseAsserts) *manifest.Manifest {
		t.Helper()

		fest, err := manifest.ParseYAML(strings.NewReader(yamlStr))
		require.NoError(t, err)

		assert.Equal(t, expected.version, fest.Version)
		assert.Equal(t, expected.kind, fest.Kind)
		assert.Equal(t, expected.metaName, fest.Metadata.Name)
		assert.Equal(t, expected.metaVersion, fest.Metadata.Version)

		return fest
	}

	containsLabel := func(t *testing.T, name string, labels []*manifest.Label) {
		t.Helper()

		for _, l := range labels {
			if l.Name == name {
				assert.NotNil(t, l.Properties)
				return
			}
		}

		t.Error("did not find label: " + name)
	}

	t.Run("file with just a bucket", func(t *testing.T) {
		yamlFile := `apiVersion: 1
kind: Package
metadata:
  name:      first_bucket_package
  version:   1
spec:
  resources:
    - kind: Bucket
      name: buck_1
      retention_period: 1h
`
		fest := validParsedManifest(t, yamlFile, baseAsserts{
			version:     "1",
			kind:        "Package",
			metaName:    "first_bucket_package",
			metaVersion: "1",
		})

		buckets := fest.Buckets()
		require.Len(t, buckets, 1)

		bucket := buckets[0]
		expectedBucket := manifest.Bucket{
			Name:            "buck_1",
			RetentionPeriod: time.Hour,
		}
		assert.Equal(t, expectedBucket, *bucket)
	})

	t.Run("file with just a label", func(t *testing.T) {
		yamlFile := `apiVersion: 1
kind: Package
metadata:
  name:      foo_package
  version:   1
spec:
  resources:
    - kind: Label
      name: label_1
      properties:
        key1: v1
        key2: v2
        color: "#aaa111"
        description: description
`
		fest := validParsedManifest(t, yamlFile, baseAsserts{
			version:     "1",
			kind:        "Package",
			metaName:    "foo_package",
			metaVersion: "1",
		})

		labels := fest.Labels()
		require.Len(t, labels, 1)

		label := labels[0]
		expectedLabel := manifest.Label{
			Name: "label_1",
			Properties: map[string]string{
				"color":       "#aaa111",
				"description": "description",
			},
		}
		assert.Equal(t, expectedLabel, *label)
	})

	t.Run("with bucket and nested label", func(t *testing.T) {
		yamlFile := `apiVersion: 1
kind: Package
metadata:
  name:      second_package
  version:   1
spec:
  resources:
    - kind: Bucket
      name: buck_1
      retention_period: 1h
      resources:
        - kind: Label
          name: label_1
          properties:
            key1: v1
            key1: v1
            color: "#aaa111"
            description: description
`
		fest := validParsedManifest(t, yamlFile, baseAsserts{
			version:     "1",
			kind:        "Package",
			metaName:    "second_package",
			metaVersion: "1",
		})

		buckets := fest.Buckets()
		require.Len(t, buckets, 1)

		bucket := buckets[0]
		assert.Equal(t, "buck_1", bucket.Name)
		assert.Equal(t, time.Hour, bucket.RetentionPeriod)

		labels := fest.Labels()
		require.Len(t, labels, 1)

		label := labels[0]
		expectedLabel := manifest.Label{
			Name: "label_1",
			Properties: map[string]string{
				"color":       "#aaa111",
				"description": "description",
			},
		}
		assert.Equal(t, expectedLabel, *label)
	})

	t.Run("with bucket and inherits a label", func(t *testing.T) {
		yamlFile := `apiVersion: 1
kind: Package
metadata:
  name:      second_package
  version:   1
spec:
  resources:
    - kind: Bucket
      name: buck_1
      retention_period: 1h
      resources:
        - kind: Label
          name: label_1
          inherit: true
        - kind: Label
          name: label_2
    - kind: Label
      name: label_1
`
		fest := validParsedManifest(t, yamlFile, baseAsserts{
			version:     "1",
			kind:        "Package",
			metaName:    "second_package",
			metaVersion: "1",
		})

		buckets := fest.Buckets()
		require.Len(t, buckets, 1)

		bucket := buckets[0]
		assert.Equal(t, "buck_1", bucket.Name)
		assert.Equal(t, time.Hour, bucket.RetentionPeriod)

		labels := fest.Labels()
		require.Len(t, labels, 2)

		containsLabel(t, "label_1", fest.Labels())
		containsLabel(t, "label_2", fest.Labels())
	})

	t.Run("with many bucket and labels", func(t *testing.T) {
		yamlFile := `apiVersion: 1
kind: Package
metadata:
  name:      second_package
  version:   1
spec:
  resources:
    - kind: Bucket
      name: buck_1
      retention_period: 1h
      description: bucket 1 description
      resources:
        - kind: Label
          name: label_1
          inherit: true
        - kind: Label
          name: label_2
    - kind: Bucket
      name: buck_2
      retention_period: 10h
      description: bucket 2 description
      resources:
        - kind: Label
          name: label_1
          inherit: true
        - kind: Label
          name: label_3
          inherit: true
        - kind: Label
          name: label_4
    - kind: Bucket
      name: empty_bucket
    - kind: Label
      name: label_1
    - kind: label
      name: label_3
    - kind: label
      name: label_5
`
		fest := validParsedManifest(t, yamlFile, baseAsserts{
			version:     "1",
			kind:        "Package",
			metaName:    "second_package",
			metaVersion: "1",
		})

		expectedBuckets := []manifest.Bucket{
			{
				Name:            "buck_1",
				RetentionPeriod: time.Hour,
				Description:     "bucket 1 description",
				Labels:          []*manifest.Label{nil, nil},
			},
			{
				Name:            "buck_2",
				RetentionPeriod: 10 * time.Hour,
				Description:     "bucket 2 description",
				Labels:          []*manifest.Label{nil, nil, nil},
			},
			{Name: "empty_bucket"},
		}

		buckets := fest.Buckets()
		require.Len(t, buckets, len(expectedBuckets))

		for i, expectedBucket := range expectedBuckets {
			bucket := buckets[i]
			assert.Equal(t, expectedBucket.Description, bucket.Description)
			assert.Equal(t, expectedBucket.Name, bucket.Name)
			assert.Equal(t, expectedBucket.RetentionPeriod, bucket.RetentionPeriod)
			assert.Len(t, bucket.Labels, len(expectedBucket.Labels))
		}

		labels := fest.Labels()
		require.Len(t, labels, 5)

		for i := 1; i < 6; i++ {
			containsLabel(t, "label_"+strconv.Itoa(i), fest.Labels())
		}
	})

	t.Run("with dashboard", func(t *testing.T) {
		cellsMatch := func(t *testing.T, expected, actual manifest.Cell) {
			t.Helper()

			assert.Equal(t, expected.X, actual.X)
			assert.Equal(t, expected.Y, actual.Y)
			assert.Equal(t, expected.Height, actual.Height)
			assert.Equal(t, expected.Width, actual.Width)
		}

		t.Run("alone", func(t *testing.T) {
			yamlFile := `apiVersion: 1
kind: Package
metadata:
  name:      baz_package
  version:   1.0.1


axisDefault: &axisDefault
  bounds:
    -
    -
  label:
  prefix:
  suffix:
  base: 10
  scale: linear

colorLaser: &colorLaser
  id:    base
  type:	 text
  hex:   "#00C9FF"
  name:	 laser
  value: 0

spec:
  resources:
    - kind:         Dashboard
      name:         dash_3
      description: dash description
      resources:
        - kind: Label
          name: label_1
          properties:
            color: "#FFB94A"
            description: descriptionness
      cells:
        - positions:
            x_position: 0
            y_position: 0
            height: 2
            width:  3
          view:
            name: view_1
            properties:
              type:        Line_With_Single_Stat
              name:        line_graph_1
              note:        a note about view_1
              noteOnEmpty: true
              prefix:      fooprefix
              suffix:      foosuffix
              xColumn:     "_time"
              yColumn:     "_value"
              shade:       true
              axes:
                x:
                  <<: *axisDefault
                # weird bug in parser, will not find y as a key for some reason
                y1:
                  <<: *axisDefault
                  suffix: percent
                y2:
                  <<: *axisDefault
              legend:
                type: foo_type
                orientation: horizontal
              decimalPlaces:
                enforced: true
                digits: 1
              queries:
                - name: query_1
                  editMode: advanced
                  text: >
                    from(bucket: v.bucket)
                    |> range(start: v.timeRangeStart)
                    |> filter(fn: (r) => r._measurement == "mem")
                    |> filter(fn: (r) => r._field == "used_percent")
                    |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
                    |> yield(name: "mean")
                  config:
                    tags:
                      - key: "_measurement"
              colors:
                - <<: *colorLaser      
`

			fest := validParsedManifest(t, yamlFile, baseAsserts{
				version:     "1",
				kind:        "Package",
				metaName:    "baz_package",
				metaVersion: "1.0.1",
			})

			labels := fest.Labels()
			require.Len(t, labels, 1)
			containsLabel(t, "label_1", labels)

			dashboards := fest.Dashboards()
			require.Len(t, dashboards, 1)

			dash := dashboards[0]
			assert.Equal(t, "dash_3", dash.Name)
			assert.Equal(t, "dash description", dash.Description)

			require.Len(t, dash.Labels, 1)
			containsLabel(t, "label_1", dash.Labels)

			require.Len(t, dash.Cells, 1)

			cell := *dash.Cells[0]
			expectedCell1 := manifest.Cell{
				Height: 2,
				Width:  3,
			}
			cellsMatch(t, expectedCell1, cell)

			view := cell.View
			require.NotNil(t, view)

			assert.Equal(t, "view_1", view.Name)

			propface := view.Properties()
			require.Equal(t, "line-plus-single-stat", propface.GetType())

			properties, ok := propface.(influxdb.LinePlusSingleStatProperties)
			require.True(t, ok)
			assert.Equal(t, "line-plus-single-stat", properties.Type)
			assert.Equal(t, "fooprefix", properties.Prefix)
			assert.Equal(t, "foosuffix", properties.Suffix)
			assert.Equal(t, "a note about view_1", properties.Note)
			assert.True(t, properties.ShowNoteWhenEmpty)
			assert.True(t, properties.ShadeBelow)
			assert.Equal(t, "_time", properties.XColumn)
			assert.Equal(t, "_value", properties.YColumn)

			axes := properties.Axes

			isBaseAxis := func(t *testing.T, axis influxdb.Axis) {
				require.NotNil(t, axis)
				assert.Equal(t, "linear", axis.Scale)
				assert.Equal(t, "10", axis.Base)
				assert.Len(t, axis.Base, 2)
			}

			for _, key := range []string{"x", "y1", "y2"} {
				axis, ok := axes[key]
				require.True(t, ok, key)
				isBaseAxis(t, axis)
			}

			legend := properties.Legend
			assert.Equal(t, "foo_type", legend.Type)
			assert.Equal(t, "horizontal", legend.Orientation)

			decimalPlaces := properties.DecimalPlaces
			assert.True(t, decimalPlaces.IsEnforced)
			assert.Equal(t, int32(1.0), decimalPlaces.Digits)

			colors := properties.ViewColors
			require.Len(t, colors, 1)
			assert.Equal(t, "base", colors[0].ID)
			assert.Equal(t, "laser", colors[0].Name)
			assert.Equal(t, "#00C9FF", colors[0].Hex)
			assert.Zero(t, colors[0].Value)

			queries := properties.Queries
			require.Len(t, queries, 1)
			assert.Equal(t, "query_1", queries[0].Name)
			assert.Equal(t, "advanced", queries[0].EditMode)

			queryStr := "from(bucket: v.bucket) |> range(start: v.timeRangeStart) |> filter(fn: (r) => r._measurement == \"mem\") |> filter(fn: (r) => r._field == \"used_percent\") |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false) |> yield(name: \"mean\")\n"
			assert.Equal(t, queryStr, queries[0].Text)

			queryCFG := queries[0].BuilderConfig

			cfgTags := queryCFG.Tags
			require.Len(t, cfgTags, 1)
			assert.Equal(t, "_measurement", cfgTags[0].Key)
			assert.Nil(t, cfgTags[0].Values)
		})

		t.Run("and bucket with cells and no views", func(t *testing.T) {
			t.Skip("TODO: update this, going to fail b/c of missing views")

			yamlFile := `apiVersion: 1
kind: Package
metadata:
  name:      foo_package
  version:   1.0.1
spec:
  resources:
    - kind: Bucket
      name: buck_1
      retention_period: 1h
      description: bucket 1 description
    - kind:         Dashboard
      name:         dash_1
      description: dash description
      resources:
        - kind: Label
          name: label_3
          inherit: true
        - kind: Label
          name: label_2
      cells:
        - positions:
            x_position: 0
            y_position: 30
            height: 150
            width:  250
        - positions:
            x_position: 100
            y_position: 200
            height: 150
            width:  250
    - kind: Label
      name: label_1
    - kind: label
      name: label_3
`
			fest := validParsedManifest(t, yamlFile, baseAsserts{
				version:     "1",
				kind:        "Package",
				metaName:    "foo_package",
				metaVersion: "1.0.1",
			})

			buckets := fest.Buckets()
			require.Len(t, buckets, 1)

			expectedBucket := manifest.Bucket{
				Name:            "buck_1",
				RetentionPeriod: time.Hour,
			}

			bucket := buckets[0]
			assert.Equal(t, expectedBucket.Name, bucket.Name)
			assert.Equal(t, expectedBucket.RetentionPeriod, bucket.RetentionPeriod)
			assert.Zero(t, bucket.Labels)

			labels := fest.Labels()
			require.Len(t, labels, 3)

			containsLabel(t, "label_1", fest.Labels())
			containsLabel(t, "label_2", fest.Labels())
			containsLabel(t, "label_3", fest.Labels())

			dashboards := fest.Dashboards()
			require.Len(t, dashboards, 1)

			dash := dashboards[0]
			assert.Equal(t, "dash_1", dash.Name)
			assert.Equal(t, "dash description", dash.Description)

			require.Len(t, dash.Labels, 2)
			containsLabel(t, "label_2", dash.Labels)
			containsLabel(t, "label_3", dash.Labels)

			cellsMatch := func(t *testing.T, expected, actual manifest.Cell) {
				t.Helper()

				assert.Equal(t, expected.X, actual.X)
				assert.Equal(t, expected.Y, actual.Y)
				assert.Equal(t, expected.Height, actual.Height)
				assert.Equal(t, expected.Width, actual.Width)
			}

			require.Len(t, dash.Cells, 2)

			expectedCell1 := manifest.Cell{
				Y:      30,
				Height: 150,
				Width:  250,
			}
			cellsMatch(t, expectedCell1, *dash.Cells[0])

			expectedCell2 := manifest.Cell{
				X:      100,
				Y:      200,
				Height: 150,
				Width:  250,
			}
			cellsMatch(t, expectedCell2, *dash.Cells[1])
		})

		t.Run("and bucket cell and view", func(t *testing.T) {
			yamlFile := `apiVersion: 1
kind: Package
metadata:
  name:      baz_package
  version:   1.0.1
axisDefault: &axisDefault
  bounds:
    -
    -
  label:
  prefix:
  suffix:
  base: 10
  scale: linear
spec:
  resources:
    - kind: Bucket
      name: buck_1
    - kind:         Dashboard
      name:         dash_1
      description: dash description
      resources:
        - kind: Label
          name: label_1
      cells:
        - positions:
            x_position: 0
            y_position: 0
            height: 150
            width:  250
          view:
            name: view_1
            properties:
              type:        Line_With_Single_Stat
              name:        line_graph_1
              prefix:      fooprefix
              suffix:      foosuffix
              note:        a note about view_1
              noteOnEmpty: true
              xColumn:     x_column
              yColumn:     y_column
              shade:       true
              axes:
                x:
                  <<: *axisDefault
                # weird bug in parser, will not find y as a key for some reason
                y1:
                  <<: *axisDefault
                  suffix: percent
                y2:
                  <<: *axisDefault
              legend:
                type: foo_type
                orientation: horizontal
              decimalPlaces:
                enforced: true
                digits: 10
              colors:
                - label_type: Min
                  id: color_id
                  name: blurple
                  hex: aaaaaa
                  value: 0.3
              queries:
                - name: query_1
                  editMode: advanced
                  text: >
                    from(bucket: v.bucket)
                     |> range(start: v.timeRangeStart)
                     |> filter(fn: (r) => r._measurement == "diskio")
                     |> filter(fn: (r) => r._field == "read_bytes" or r._field == "write_bytes")
                     |> aggregateWindow(every: v.windowPeriod, fn: last, createEmpty: false)
                     |> derivative(unit: 1s, nonNegative: false)
                     |> yield(name: "derivative")
                  config:
                    buckets:
                      - buck1
                      - buck2
                    tags:
                      - key: _measurement
                        values:
                          - val_1
                    functions:
                      - name: func_1
                    aggregateWindow:
                      period: 1s
`
			fest := validParsedManifest(t, yamlFile, baseAsserts{
				version:     "1",
				kind:        "Package",
				metaName:    "baz_package",
				metaVersion: "1.0.1",
			})

			buckets := fest.Buckets()
			require.Len(t, buckets, 1)
			assert.Equal(t, "buck_1", buckets[0].Name)

			labels := fest.Labels()
			require.Len(t, labels, 1)
			containsLabel(t, "label_1", labels)

			dashboards := fest.Dashboards()
			require.Len(t, dashboards, 1)

			dash := dashboards[0]
			assert.Equal(t, "dash_1", dash.Name)
			assert.Equal(t, "dash description", dash.Description)

			require.Len(t, dash.Labels, 1)
			containsLabel(t, "label_1", dash.Labels)

			require.Len(t, dash.Cells, 1)

			cell := *dash.Cells[0]
			expectedCell1 := manifest.Cell{
				Height: 150,
				Width:  250,
			}
			cellsMatch(t, expectedCell1, cell)

			view := cell.View
			require.NotNil(t, view)

			assert.Equal(t, "view_1", view.Name)

			propface := view.Properties()
			require.Equal(t, "line-plus-single-stat", propface.GetType())

			properties, ok := propface.(influxdb.LinePlusSingleStatProperties)
			require.True(t, ok)
			assert.Equal(t, "line-plus-single-stat", properties.Type)
			assert.Equal(t, "fooprefix", properties.Prefix)
			assert.Equal(t, "foosuffix", properties.Suffix)
			assert.Equal(t, "a note about view_1", properties.Note)
			assert.True(t, properties.ShowNoteWhenEmpty)
			assert.True(t, properties.ShadeBelow)
			assert.Equal(t, "x_column", properties.XColumn)
			assert.Equal(t, "y_column", properties.YColumn)

			axes := properties.Axes

			isBaseAxis := func(t *testing.T, axis influxdb.Axis) {
				require.NotNil(t, axis)
				assert.Equal(t, "linear", axis.Scale)
				assert.Equal(t, "10", axis.Base)
				assert.Len(t, axis.Base, 2)
			}

			for _, key := range []string{"x", "y1", "y2"} {
				axis, ok := axes[key]
				require.True(t, ok, key)
				isBaseAxis(t, axis)
			}

			legend := properties.Legend
			assert.Equal(t, "foo_type", legend.Type)
			assert.Equal(t, "horizontal", legend.Orientation)

			decimalPlaces := properties.DecimalPlaces
			assert.True(t, decimalPlaces.IsEnforced)
			assert.Equal(t, int32(10.0), decimalPlaces.Digits)

			colors := properties.ViewColors
			require.Len(t, colors, 1)
			assert.Equal(t, "Min", colors[0].Type)
			assert.Equal(t, "color_id", colors[0].ID)
			assert.Equal(t, "blurple", colors[0].Name)
			assert.Equal(t, "aaaaaa", colors[0].Hex)
			assert.Equal(t, 0.3, colors[0].Value)

			queries := properties.Queries
			require.Len(t, queries, 1)
			assert.Equal(t, "query_1", queries[0].Name)
			assert.Equal(t, "advanced", queries[0].EditMode)

			queryStr := "from(bucket: v.bucket)\n |> range(start: v.timeRangeStart)\n |> filter(fn: (r) => r._measurement == \"diskio\")\n |> filter(fn: (r) => r._field == \"read_bytes\" or r._field == \"write_bytes\")\n |> aggregateWindow(every: v.windowPeriod, fn: last, createEmpty: false)\n |> derivative(unit: 1s, nonNegative: false)\n |> yield(name: \"derivative\")\n"
			assert.Equal(t, queryStr, queries[0].Text)

			queryCFG := queries[0].BuilderConfig
			assert.Equal(t, "1s", queryCFG.AggregateWindow.Period)
			assert.Equal(t, []string{"buck1", "buck2"}, queryCFG.Buckets)

			cfgTags := queryCFG.Tags
			require.Len(t, cfgTags, 1)
			assert.Equal(t, "_measurement", cfgTags[0].Key)
			assert.Equal(t, []string{"val_1"}, cfgTags[0].Values)

			cfgFuncs := queryCFG.Functions
			require.Len(t, cfgFuncs, 1)
			assert.Equal(t, "func_1", cfgFuncs[0].Name)
		})
	})
}
