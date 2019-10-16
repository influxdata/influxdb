package manifest_test

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/influxdata/influxdb/internal/manifest"
	"github.com/stretchr/testify/require"
)

func TestParser_YAML(t *testing.T) {
	t.Run("parses yaml", func(t *testing.T) {
		type baseAsserts struct {
			version     string
			kind        string
			metaName    string
			metaVersion string
		}

		validParsedManifest := func(t *testing.T, yamlStr string, expected baseAsserts) manifest.Manifest {
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
			assert.Equal(t, expectedBucket, bucket)
		})

		t.Run("with bucket and nested label", func(t *testing.T) {
			yamlFile := `apiVersion: 1
kind: Package
name: second_package
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
			}
			assert.Equal(t, expectedLabel, label)
		})

		t.Run("with bucket and inherits a label", func(t *testing.T) {
			yamlFile := `apiVersion: 1
kind: Package
name: second_package
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
name: second_package
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

		t.Run("with bucket and dashboard without cells", func(t *testing.T) {
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
    - kind: Dashboard
      name: dash_1
      resources:
        - kind: Label
          name: label_3
          inherit: true
        - kind: Label
          name: label_2
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
			require.Len(t, dash.Labels, 2)

			containsLabel(t, "label_2", dash.Labels)
			containsLabel(t, "label_3", dash.Labels)
		})

		t.Run("with bucket and dashboard with cells", func(t *testing.T) {
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
	})
}
