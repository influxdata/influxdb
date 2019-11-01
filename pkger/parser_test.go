package pkger

import (
	"testing"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	t.Run("pkg has all necessary metadata", func(t *testing.T) {
		t.Run("has valid metadata and at least 1 resource", func(t *testing.T) {
			testfileRunner(t, "testdata/bucket", nil)
		})

		t.Run("malformed required metadata", func(t *testing.T) {
			containsField := func(t *testing.T, expected []string, actual string) {
				t.Helper()

				for _, e := range expected {
					if e == actual {
						return
					}
				}
				assert.Fail(t, "did not find field: "+actual)
			}

			tests := []struct {
				name           string
				in             string
				expectedFields []string
			}{
				{
					name: "missing apiVersion",
					in: `kind: Package
meta:
  pkgName:      first_bucket_package
  pkgVersion:   1
spec:
  resources:
    - kind: Bucket
      name: buck_1
      retention_period: 1h
`,
					expectedFields: []string{"apiVersion"},
				},
				{
					name: "apiVersion is invalid version",
					in: `apiVersion: 222.2 #invalid apiVersion
kind: Package
meta:
  pkgName:      first_bucket_package
  pkgVersion:   1
spec:
  resources:
    - kind: Bucket
      name: buck_1
      retention_period: 1h
`,
					expectedFields: []string{"apiVersion"},
				},
				{
					name: "missing kind",
					in: `apiVersion: 0.1.0
meta:
  pkgName:   first_bucket_package
  pkgVersion:   1
spec:
  resources:
    - kind: Bucket
      name: buck_1
      retention_period: 1h
`,
					expectedFields: []string{"kind"},
				},
				{
					name: "missing pkgName",
					in: `apiVersion: 0.1.0
kind: Package
meta:
  pkgVersion:   1
spec:
  resources:
    - kind: Bucket
      name: buck_1
      retention_period: 1h
`,
					expectedFields: []string{"pkgName"},
				},
				{
					name: "missing pkgVersion",
					in: `apiVersion: 0.1.0
kind: Package
meta:
  pkgName:   foo_name
spec:
  resources:
    - kind: Bucket
      name: buck_1
      retention_period: 1h
`,
					expectedFields: []string{"pkgVersion"},
				},
				{
					name: "missing multiple",
					in: `spec:
  resources:
    - kind: Bucket
      name: buck_1
      retention_period: 1h
`,
					expectedFields: []string{"pkgVersion", "pkgName", "kind", "apiVersion"},
				},
			}

			for _, tt := range tests {
				fn := func(t *testing.T) {
					_, err := Parse(EncodingYAML, FromString(tt.in))
					require.Error(t, err)

					pErr, ok := IsParseErr(err)
					require.True(t, ok)

					require.Len(t, pErr.Resources, 1)

					failedResource := pErr.Resources[0]
					assert.Equal(t, "Package", failedResource.Kind)

					require.Len(t, failedResource.ValidationFails, len(tt.expectedFields))

					for _, f := range failedResource.ValidationFails {
						containsField(t, tt.expectedFields, f.Field)
					}
				}

				t.Run(tt.name, fn)
			}
		})
	})

	t.Run("pkg with just a bucket", func(t *testing.T) {
		t.Run("with valid bucket pkg should be valid", func(t *testing.T) {
			testfileRunner(t, "testdata/bucket", func(t *testing.T, pkg *Pkg) {
				buckets := pkg.buckets()
				require.Len(t, buckets, 1)

				actual := buckets[0]
				expectedBucket := bucket{
					Name:            "rucket_11",
					Description:     "bucket 1 description",
					RetentionPeriod: time.Hour,
				}
				assert.Equal(t, expectedBucket, *actual)
			})
		})

		t.Run("with missing bucket name should error", func(t *testing.T) {
			tests := []struct {
				name    string
				numErrs int
				in      string
			}{
				{
					name:    "missing name",
					numErrs: 1,
					in: `apiVersion: 1
kind: Package
meta:
  pkgName:      first_bucket_package
  pkgVersion:   1
spec:
  resources:
    - kind: Bucket
      retention_period: 1h
`,
				}, {
					name:    "mixed valid and missing name",
					numErrs: 1,
					in: `apiVersion: 1
kind: Package
meta:
  pkgName:      first_bucket_package
  pkgVersion:   1
spec:
  resources:
    - kind: Bucket
      retention_period: 1h
      name: valid name
    - kind: Bucket
      retention_period: 1h
`,
				}, {
					name:    "mixed valid and multiple bad names",
					numErrs: 2,
					in: `apiVersion: 0.1.0
kind: Package
meta:
  pkgName:      first_bucket_package
  pkgVersion:   1
spec:
  resources:
    - kind: Bucket
      retention_period: 1h
      name: valid name
    - kind: Bucket
      retention_period: 1h
    - kind: Bucket
      retention_period: 1h
`,
				},
			}

			for _, tt := range tests {
				fn := func(t *testing.T) {
					_, err := Parse(EncodingYAML, FromString(tt.in))
					pErr, ok := IsParseErr(err)
					require.True(t, ok)
					assert.Len(t, pErr.Resources, tt.numErrs)
				}

				t.Run(tt.name, fn)
			}
		})

		t.Run("with duplicate buckets should error", func(t *testing.T) {
			yamlFile := `apiVersion: 0.1.0
kind: Package
meta:
  pkgName:      first_bucket_package
  pkgVersion:   1
spec:
  resources:
    - kind: Bucket
      retention_period: 1h
      name: valid name
    - kind: Bucket
      retention_period: 1h
      name: valid name
`
			_, err := Parse(EncodingYAML, FromString(yamlFile))
			require.Error(t, err)

			pErr, ok := IsParseErr(err)
			require.True(t, ok)
			assert.Len(t, pErr.Resources, 1)

			fields := pErr.Resources[0].ValidationFails
			require.Len(t, fields, 1)
			assert.Equal(t, "name", fields[0].Field)
		})
	})

	t.Run("pkg with just a label", func(t *testing.T) {
		t.Run("with valid label pkg should be valid", func(t *testing.T) {
			testfileRunner(t, "testdata/label", func(t *testing.T, pkg *Pkg) {
				labels := pkg.labels()
				require.Len(t, labels, 2)

				expectedLabel1 := label{
					Name:        "label_1",
					Description: "label 1 description",
					Color:       "#FFFFFF",
				}
				assert.Equal(t, expectedLabel1, *labels[0])

				expectedLabel2 := label{
					Name:        "label_2",
					Description: "label 2 description",
					Color:       "#000000",
				}
				assert.Equal(t, expectedLabel2, *labels[1])
			})
		})

		t.Run("with missing label name should error", func(t *testing.T) {
			tests := []struct {
				name    string
				numErrs int
				in      string
			}{
				{
					name:    "missing name",
					numErrs: 1,
					in: `apiVersion: 0.1.0
kind: Package
meta:
  pkgName: first_label_pkg 
  pkgVersion:   1
spec:
  resources:
    - kind: Label 
`,
				},
				{
					name:    "mixed valid and missing name",
					numErrs: 1,
					in: `apiVersion: 0.1.0
kind: Package
meta:
  pkgName: label_pkg
  pkgVersion: 1
spec:
  resources:
    - kind: Label
      name: valid name
    - kind: Label
`,
				},
				{
					name:    "multiple labels with missing name",
					numErrs: 2,
					in: `apiVersion: 0.1.0
kind: Package
meta:
  pkgName: label_pkg
  pkgVersion: 1
spec:
  resources:
    - kind: Label
    - kind: Label
`,
				},
			}

			for _, tt := range tests {
				fn := func(t *testing.T) {
					_, err := Parse(EncodingYAML, FromString(tt.in))
					pErr, ok := IsParseErr(err)
					require.True(t, ok)
					assert.Len(t, pErr.Resources, tt.numErrs)
				}

				t.Run(tt.name, fn)
			}
		})
	})

	t.Run("pkg with buckets and labels associated", func(t *testing.T) {
		testfileRunner(t, "testdata/bucket_associates_label", func(t *testing.T, pkg *Pkg) {
			sum := pkg.Summary()
			require.Len(t, sum.Labels, 2)

			bkts := sum.Buckets
			require.Len(t, bkts, 3)

			expectedLabels := []struct {
				bktName string
				labels  []string
			}{
				{
					bktName: "rucket_1",
					labels:  []string{"label_1"},
				},
				{
					bktName: "rucket_2",
					labels:  []string{"label_2"},
				},
				{
					bktName: "rucket_3",
					labels:  []string{"label_1", "label_2"},
				},
			}
			for i, expected := range expectedLabels {
				bkt := bkts[i]
				require.Len(t, bkt.LabelAssociations, len(expected.labels))

				for j, label := range expected.labels {
					assert.Equal(t, label, bkt.LabelAssociations[j].Name)
				}
			}

			expectedMappings := []SummaryLabelMapping{
				{
					ResourceName: "rucket_1",
					LabelName:    "label_1",
				},
				{
					ResourceName: "rucket_2",
					LabelName:    "label_2",
				},
				{
					ResourceName: "rucket_3",
					LabelName:    "label_1",
				},
				{
					ResourceName: "rucket_3",
					LabelName:    "label_2",
				},
			}

			require.Len(t, sum.LabelMappings, len(expectedMappings))
			for i, expected := range expectedMappings {
				expected.LabelMapping.ResourceType = influxdb.BucketsResourceType
				assert.Equal(t, expected, sum.LabelMappings[i])
			}
		})

		t.Run("association doesn't exist then provides an error", func(t *testing.T) {
			tests := []struct {
				name    string
				numErrs int
				in      string
				errIdxs []int
			}{
				{
					name:    "no labels provided",
					numErrs: 1,
					errIdxs: []int{0},
					in: `apiVersion: 0.1.0
kind: Package
meta:
  pkgName: label_pkg
  pkgVersion: 1
spec:
  resources:
    - kind: Bucket
      name: buck_1
      associations:
        - kind: Label
          name: label_1
`,
				},
				{
					name:    "mixed found and not found",
					numErrs: 1,
					errIdxs: []int{1},
					in: `apiVersion: 0.1.0
kind: Package
meta:
  pkgName: label_pkg
  pkgVersion: 1
spec:
  resources:
    - kind: Label
      name: label_1
    - kind: Bucket
      name: buck_1
      associations:
        - kind: Label
          name: label_1
        - kind: Label
          name: unfound label
`,
				},
				{
					name:    "multiple not found",
					numErrs: 1,
					errIdxs: []int{0, 1},
					in: `apiVersion: 0.1.0
kind: Package
meta:
  pkgName: label_pkg
  pkgVersion: 1
spec:
  resources:
    - kind: Label
      name: label_1
    - kind: Bucket
      name: buck_1
      associations:
        - kind: Label
          name: not found 1
        - kind: Label
          name: unfound label
`,
				},
				{
					name:    "duplicate valid nested labels",
					numErrs: 1,
					errIdxs: []int{1},
					in: `apiVersion: 0.1.0
kind: Package
meta:
  pkgName: label_pkg
  pkgVersion: 1
spec:
  resources:
    - kind: Label
      name: label_1
    - kind: Bucket
      name: buck_1
      associations:
        - kind: Label
          name: label_1
        - kind: Label
          name: label_1
`,
				},
			}

			for _, tt := range tests {
				fn := func(t *testing.T) {
					_, err := Parse(EncodingYAML, FromString(tt.in))

					pErr, ok := IsParseErr(err)
					require.True(t, ok)
					require.Len(t, pErr.Resources, tt.numErrs)

					assFails := pErr.Resources[0].AssociationFails
					require.Len(t, assFails, len(tt.errIdxs))
					assert.Equal(t, "associations", assFails[0].Field)

					for i, f := range assFails {
						assert.Equal(t, tt.errIdxs[i], f.Index)
					}
				}

				t.Run(tt.name, fn)
			}
		})
	})

	t.Run("pkg with single dashboard and single chart", func(t *testing.T) {
		t.Run("single stat chart", func(t *testing.T) {
			testfileRunner(t, "testdata/dashboard", func(t *testing.T, pkg *Pkg) {
				sum := pkg.Summary()
				require.Len(t, sum.Dashboards, 1)

				actual := sum.Dashboards[0]
				assert.Equal(t, "dash_1", actual.Name)
				assert.Equal(t, "desc1", actual.Description)

				require.Len(t, actual.Charts, 1)
				actualChart := actual.Charts[0]
				assert.Equal(t, ChartKindSingleStat, actualChart.Kind)
				assert.Equal(t, 3, actualChart.Height)
				assert.Equal(t, 6, actualChart.Width)
				assert.Equal(t, 1, actualChart.XPosition)
				assert.Equal(t, 2, actualChart.YPosition)

				props, ok := actualChart.Properties.(influxdb.SingleStatViewProperties)
				require.True(t, ok)
				assert.Equal(t, "single stat note", props.Note)
				assert.True(t, props.ShowNoteWhenEmpty)
				assert.True(t, props.DecimalPlaces.IsEnforced)
				assert.Equal(t, int32(1), props.DecimalPlaces.Digits)
				assert.Equal(t, "days", props.Suffix)
				assert.Equal(t, "sumtin", props.Prefix)

				require.Len(t, props.Queries, 1)
				q := props.Queries[0]
				queryText := `from(bucket: v.bucket) |> range(start: v.timeRangeStart) |> filter(fn: (r) => r._measurement == "processes") |> filter(fn: (r) => r._field == "running" or r._field == "blocked") |> aggregateWindow(every: v.windowPeriod, fn: max) |> yield(name: "max")`
				assert.Equal(t, queryText, q.Text)
				assert.Equal(t, "advanced", q.EditMode)

				require.Len(t, props.ViewColors, 1)
				c := props.ViewColors[0]
				assert.NotZero(t, c.ID)
				assert.Equal(t, "laser", c.Name)
				assert.Equal(t, "text", c.Type)
				assert.Equal(t, "#8F8AF4", c.Hex)
				assert.Equal(t, 3.0, c.Value)
			})

			t.Run("handles invalid config", func(t *testing.T) {
				tests := []struct {
					name      string
					ymlStr    string
					numErrs   int
					errFields []string
				}{
					{
						name:      "color missing hex value",
						numErrs:   1,
						errFields: []string{"charts[0].colors[0].hex"},
						ymlStr: `apiVersion: 0.1.0
kind: Package
meta:
  pkgName:      pkg_name
  pkgVersion:   1
  description:  pack description
spec:
  resources:
    - kind: Dashboard
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
`,
					},
					{
						name:      "no colors provided",
						numErrs:   1,
						errFields: []string{"charts[0].colors"},
						ymlStr: `apiVersion: 0.1.0
kind: Package
meta:
  pkgName:      pkg_name
  pkgVersion:   1
  description:  pack description
spec:
  resources:
    - kind: Dashboard
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
`,
					},
					{
						name:      "query missing text value",
						numErrs:   1,
						errFields: []string{"charts[0].queries[0].query"},
						ymlStr: `apiVersion: 0.1.0
kind: Package
meta:
  pkgName:      pkg_name
  pkgVersion:   1
  description:  pack description
spec:
  resources:
    - kind: Dashboard
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
            - query: 
          colors:
            - name: laser
              type: text
              hex: "#aaa222"
`,
					},
					{
						name:      "no queries provided",
						numErrs:   1,
						errFields: []string{"charts[0].queries"},
						ymlStr: `apiVersion: 0.1.0
kind: Package
meta:
  pkgName:      pkg_name
  pkgVersion:   1
  description:  pack description
spec:
  resources:
    - kind: Dashboard
      name: dash_1
      description: desc1
      charts:
        - kind:   Single_Stat
          name:   single stat
          suffix: days
          width:  6
          height: 3
          shade: true
          colors:
            - name: laser
              type: text
              hex: "#aaa222"
`,
					},
					{
						name:      "no width provided",
						numErrs:   1,
						errFields: []string{"charts[0].width"},
						ymlStr: `apiVersion: 0.1.0
kind: Package
meta:
  pkgName:      pkg_name
  pkgVersion:   1
  description:  pack description
spec:
  resources:
    - kind: Dashboard
      name: dash_1
      description: desc1
      charts:
        - kind:   Single_Stat
          name:   single stat
          suffix: days
          height: 3
          shade: true
          queries:
            - query: >
                from(bucket: v.bucket) |> range(start: v.timeRangeStart) |> filter(fn: (r) => r._measurement == "system") |> filter(fn: (r) => r._field == "uptime") |> last() |> map(fn: (r) => ({r with _value: r._value / 86400})) |> yield(name: "last")
          colors:
            - name: laser
              type: text
              hex: "#aaa333"
`,
					},
					{
						name:      "no height provided",
						numErrs:   1,
						errFields: []string{"charts[0].height"},
						ymlStr: `apiVersion: 0.1.0
kind: Package
meta:
  pkgName:      pkg_name
  pkgVersion:   1
  description:  pack description
spec:
  resources:
    - kind: Dashboard
      name: dash_1
      description: desc1
      charts:
        - kind:   Single_Stat
          name:   single stat
          suffix: days
          width: 3
          shade: true
          queries:
            - query: >
                from(bucket: v.bucket) |> range(start: v.timeRangeStart) |> filter(fn: (r) => r._measurement == "system") |> filter(fn: (r) => r._field == "uptime") |> last() |> map(fn: (r) => ({r with _value: r._value / 86400})) |> yield(name: "last")
          colors:
            - name: laser
              type: text
              hex: "#aaa333"
`,
					},
				}

				for _, tt := range tests {
					fn := func(t *testing.T) {
						_, err := Parse(EncodingYAML, FromString(tt.ymlStr))
						require.Error(t, err)

						pErr, ok := IsParseErr(err)
						require.True(t, ok, err)

						require.Len(t, pErr.Resources, 1)

						resErr := pErr.Resources[0]
						assert.Equal(t, "dashboard", resErr.Kind)

						require.Len(t, resErr.ValidationFails, 1)
						for i, vFail := range resErr.ValidationFails {
							assert.Equal(t, tt.errFields[i], vFail.Field)
						}
					}
					t.Run(tt.name, fn)
				}
			})
		})
	})

	t.Run("pkg with dashboard and labels associated", func(t *testing.T) {
		testfileRunner(t, "testdata/dashboard_associates_label", func(t *testing.T, pkg *Pkg) {
			sum := pkg.Summary()
			require.Len(t, sum.Dashboards, 1)

			actual := sum.Dashboards[0]
			assert.Equal(t, "dash_1", actual.Name)
			assert.Equal(t, "desc1", actual.Description)

			require.Len(t, actual.LabelAssociations, 1)
			actualLabel := actual.LabelAssociations[0]
			assert.Equal(t, "label_1", actualLabel.Name)

			expectedMappings := []SummaryLabelMapping{
				{
					ResourceName: "dash_1",
					LabelName:    "label_1",
				},
			}
			require.Len(t, sum.LabelMappings, len(expectedMappings))

			for i, expected := range expectedMappings {
				expected.LabelMapping.ResourceType = influxdb.DashboardsResourceType
				assert.Equal(t, expected, sum.LabelMappings[i])
			}
		})

		t.Run("association doesn't exist then provides an error", func(t *testing.T) {
			tests := []struct {
				name    string
				numErrs int
				in      string
				errIdxs []int
			}{
				{
					name:    "no labels provided",
					numErrs: 1,
					errIdxs: []int{0},
					in: `apiVersion: 0.1.0
kind: Package
meta:
  pkgName: label_pkg
  pkgVersion: 1
spec:
  resources:
    - kind: Dashboard
      name: dash_1
      associations:
        - kind: Label
          name: label_1
`,
				},
				{
					name:    "mixed found and not found",
					numErrs: 1,
					errIdxs: []int{1},
					in: `apiVersion: 0.1.0
kind: Package
meta:
  pkgName: label_pkg
  pkgVersion: 1
spec:
  resources:
    - kind: Label
      name: label_1
    - kind: Dashboard
      name: dash_1
      associations:
        - kind: Label
          name: label_1
        - kind: Label
          name: unfound label
`,
				},
				{
					name:    "multiple not found",
					numErrs: 1,
					errIdxs: []int{0, 1},
					in: `apiVersion: 0.1.0
kind: Package
meta:
  pkgName: label_pkg
  pkgVersion: 1
spec:
  resources:
    - kind: Label
      name: label_1
    - kind: Dashboard
      name: dash_1
      associations:
        - kind: Label
          name: not found 1
        - kind: Label
          name: unfound label
`,
				},
				{
					name:    "duplicate valid nested labels",
					numErrs: 1,
					errIdxs: []int{1},
					in: `apiVersion: 0.1.0
kind: Package
meta:
  pkgName: label_pkg
  pkgVersion: 1
spec:
  resources:
    - kind: Label
      name: label_1
    - kind: Dashboard
      name: dash_1
      associations:
        - kind: Label
          name: label_1
        - kind: Label
          name: label_1
`,
				},
			}

			for _, tt := range tests {
				fn := func(t *testing.T) {
					_, err := Parse(EncodingYAML, FromString(tt.in))

					pErr, ok := IsParseErr(err)
					require.True(t, ok)
					require.Len(t, pErr.Resources, tt.numErrs)

					assFails := pErr.Resources[0].AssociationFails
					require.Len(t, assFails, len(tt.errIdxs))
					assert.Equal(t, "associations", assFails[0].Field)

					for i, f := range assFails {
						assert.Equal(t, tt.errIdxs[i], f.Index)
					}
				}

				t.Run(tt.name, fn)
			}
		})
	})
}

type baseAsserts struct {
	version     string
	kind        string
	description string
	metaName    string
	metaVersion string
}

func validParsedPkg(t *testing.T, path string, encoding Encoding, expected baseAsserts) *Pkg {
	t.Helper()

	pkg, err := Parse(encoding, FromFile(path))
	require.NoError(t, err)

	require.Equal(t, expected.version, pkg.APIVersion)
	require.Equal(t, expected.kind, pkg.Kind)
	require.Equal(t, expected.description, pkg.Metadata.Description)
	require.Equal(t, expected.metaName, pkg.Metadata.Name)
	require.Equal(t, expected.metaVersion, pkg.Metadata.Version)

	return pkg
}

func testfileRunner(t *testing.T, path string, testFn func(t *testing.T, pkg *Pkg)) {
	t.Helper()

	tests := []struct {
		name      string
		extension string
		encoding  Encoding
	}{
		{
			name:      "yaml",
			extension: "yml",
			encoding:  EncodingYAML,
		},
		{
			name:      "json",
			extension: "json",
			encoding:  EncodingJSON,
		},
	}

	for _, tt := range tests {
		fn := func(t *testing.T) {
			t.Helper()

			pkg := validParsedPkg(t, path+"."+tt.extension, tt.encoding, baseAsserts{
				version:     "0.1.0",
				kind:        "Package",
				description: "pack description",
				metaName:    "pkg_name",
				metaVersion: "1",
			})
			if testFn != nil {
				testFn(t, pkg)
			}
		}
		t.Run(tt.name, fn)
	}

}
