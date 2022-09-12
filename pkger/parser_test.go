package pkger

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
	errors2 "github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/notification"
	icheck "github.com/influxdata/influxdb/v2/notification/check"
	"github.com/influxdata/influxdb/v2/notification/endpoint"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	t.Run("template with a bucket", func(t *testing.T) {
		t.Run("with valid bucket template should be valid", func(t *testing.T) {
			testfileRunner(t, "testdata/bucket", func(t *testing.T, template *Template) {
				buckets := template.Summary().Buckets
				require.Len(t, buckets, 2)

				actual := buckets[0]
				expectedBucket := SummaryBucket{
					SummaryIdentifier: SummaryIdentifier{
						Kind:          KindBucket,
						MetaName:      "rucket-11",
						EnvReferences: []SummaryReference{},
					},
					Name:              "rucket-11",
					Description:       "bucket 1 description",
					RetentionPeriod:   time.Hour,
					LabelAssociations: []SummaryLabel{},
				}
				assert.Equal(t, expectedBucket, actual)

				actual = buckets[1]
				expectedBucket = SummaryBucket{
					SummaryIdentifier: SummaryIdentifier{
						Kind:          KindBucket,
						MetaName:      "rucket-22",
						EnvReferences: []SummaryReference{},
					},
					Name:              "display name",
					Description:       "bucket 2 description",
					LabelAssociations: []SummaryLabel{},
				}
				assert.Equal(t, expectedBucket, actual)
			})
		})

		t.Run("with valid bucket and schema should be valid", func(t *testing.T) {
			template := validParsedTemplateFromFile(t, "testdata/bucket_schema.yml", EncodingYAML)
			buckets := template.Summary().Buckets
			require.Len(t, buckets, 1)

			exp := SummaryBucket{
				SummaryIdentifier: SummaryIdentifier{
					Kind:          KindBucket,
					MetaName:      "explicit-11",
					EnvReferences: []SummaryReference{},
				},
				Name:              "my_explicit",
				SchemaType:        "explicit",
				LabelAssociations: []SummaryLabel{},
				MeasurementSchemas: []SummaryMeasurementSchema{
					{
						Name: "cpu",
						Columns: []SummaryMeasurementSchemaColumn{
							{Name: "host", Type: "tag"},
							{Name: "time", Type: "timestamp"},
							{Name: "usage_user", Type: "field", DataType: "float"},
						},
					},
				},
			}

			assert.Equal(t, exp, buckets[0])
		})

		t.Run("with env refs should be valid", func(t *testing.T) {
			testfileRunner(t, "testdata/bucket_ref.yml", func(t *testing.T, template *Template) {
				actual := template.Summary().Buckets
				require.Len(t, actual, 1)

				expectedEnvRefs := []SummaryReference{
					{
						Field:        "metadata.name",
						EnvRefKey:    "meta-name",
						DefaultValue: "meta",
					},
					{
						Field:        "spec.name",
						EnvRefKey:    "spec-name",
						DefaultValue: "spectacles",
					},
					{
						Field:     "spec.associations[0].name",
						EnvRefKey: "label-meta-name",
					},
				}
				assert.Equal(t, expectedEnvRefs, actual[0].EnvReferences)
			})
		})

		t.Run("should handle bad config", func(t *testing.T) {
			tests := []testTemplateResourceError{
				{
					name:           "missing name",
					validationErrs: 1,
					valFields:      []string{fieldMetadata, fieldName},
					templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Bucket
metadata:
spec:
`,
				},
				{
					name:           "mixed valid and missing name",
					validationErrs: 1,
					valFields:      []string{fieldMetadata, fieldName},
					templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Bucket
metadata:
  name:  rucket-11
---
apiVersion: influxdata.com/v2alpha1
kind: Bucket
metadata:
spec:
`,
				},
				{
					name:           "mixed valid and multiple bad names",
					resourceErrs:   2,
					validationErrs: 1,
					valFields:      []string{fieldMetadata, fieldName},
					templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Bucket
metadata:
  name:  rucket-11
---
apiVersion: influxdata.com/v2alpha1
kind: Bucket
metadata:
spec:
---
apiVersion: influxdata.com/v2alpha1
kind: Bucket
metadata:
spec:
`,
				},
				{
					name:           "duplicate bucket names",
					resourceErrs:   1,
					validationErrs: 1,
					valFields:      []string{fieldMetadata, fieldName},
					templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Bucket
metadata:
  name:  valid-name
---
apiVersion: influxdata.com/v2alpha1
kind: Bucket
metadata:
  name:  valid-name
`,
				},
				{
					name:           "duplicate meta name and spec name",
					resourceErrs:   1,
					validationErrs: 1,
					valFields:      []string{fieldSpec, fieldName},
					templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Bucket
metadata:
  name:  rucket-1
---
apiVersion: influxdata.com/v2alpha1
kind: Bucket
metadata:
  name:  valid-name
spec:
  name:  rucket-1
`,
				},
				{
					name:           "spec name too short",
					resourceErrs:   1,
					validationErrs: 1,
					valFields:      []string{fieldSpec, fieldName},
					templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Bucket
metadata:
  name:  rucket-1
---
apiVersion: influxdata.com/v2alpha1
kind: Bucket
metadata:
  name:  invalid-name
spec:
  name:  f
`,
				},
				{
					name:           "invalid measurement name",
					resourceErrs:   1,
					validationErrs: 1,
					valFields:      []string{strings.Join([]string{fieldSpec, fieldMeasurementSchemas}, ".")},
					templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Bucket
metadata:
  name: foo-1
spec:
  name: foo
  schemaType: explicit
  measurementSchemas:
    - name: _cpu
      columns: 
        - name: time
          type: timestamp
        - name: usage_user
          type: field
          dataType: float
`,
				},
				{
					name:           "invalid semantic type",
					resourceErrs:   1,
					validationErrs: 1,
					valFields:      []string{strings.Join([]string{fieldSpec, fieldMeasurementSchemas, fieldMeasurementSchemaColumns}, ".")},
					templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Bucket
metadata:
  name: foo-1
spec:
  name: foo
  schemaType: explicit
  measurementSchemas:
    - name: _cpu
      columns: 
        - name: time
          type: field
        - name: usage_user
          type: field
          dataType: float
`,
				},
				{
					name:           "missing time column",
					resourceErrs:   1,
					validationErrs: 1,
					valFields:      []string{strings.Join([]string{fieldSpec, fieldMeasurementSchemas}, ".")},
					templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Bucket
metadata:
  name: foo-1
spec:
  name: foo
  schemaType: explicit
  measurementSchemas:
    - name: cpu
      columns: 
        - name: usage_user
          type: field
          dataType: float
`,
				},
			}

			for _, tt := range tests {
				testTemplateErrors(t, KindBucket, tt)
			}
		})
	})

	t.Run("template with a label", func(t *testing.T) {
		t.Run("with valid label template should be valid", func(t *testing.T) {
			testfileRunner(t, "testdata/label", func(t *testing.T, template *Template) {
				labels := template.Summary().Labels
				require.Len(t, labels, 3)

				expectedLabel := sumLabelGen("label-1", "label-1", "#FFFFFF", "label 1 description")
				assert.Equal(t, expectedLabel, labels[0])

				expectedLabel = sumLabelGen("label-2", "label-2", "#000000", "label 2 description")
				assert.Equal(t, expectedLabel, labels[1])

				expectedLabel = sumLabelGen("label-3", "display name", "", "label 3 description")
				assert.Equal(t, expectedLabel, labels[2])
			})
		})

		t.Run("with env refs should be valid", func(t *testing.T) {
			testfileRunner(t, "testdata/label_ref.yml", func(t *testing.T, template *Template) {
				actual := template.Summary().Labels
				require.Len(t, actual, 1)

				expected := sumLabelGen("env-meta-name", "env-spec-name", "", "",
					SummaryReference{
						Field:     "metadata.name",
						EnvRefKey: "meta-name",
					},
					SummaryReference{
						Field:     "spec.name",
						EnvRefKey: "spec-name",
					},
				)
				assert.Contains(t, actual, expected)
			})
		})

		t.Run("with missing label name should error", func(t *testing.T) {
			tests := []testTemplateResourceError{
				{
					name:           "missing name",
					validationErrs: 1,
					valFields:      []string{fieldMetadata, fieldName},
					templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Label
metadata:
spec:
`,
				},
				{
					name:           "mixed valid and missing name",
					validationErrs: 1,
					valFields:      []string{fieldMetadata, fieldName},
					templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Label
metadata:
  name: valid-name
spec:
---
apiVersion: influxdata.com/v2alpha1
kind: Label
metadata:
spec:
`,
				},
				{
					name:           "duplicate names",
					validationErrs: 1,
					valFields:      []string{fieldMetadata, fieldName},
					templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Label
metadata:
  name: valid-name
spec:
---
apiVersion: influxdata.com/v2alpha1
kind: Label
metadata:
  name: valid-name
spec:
`,
				},
				{
					name:           "multiple labels with missing name",
					resourceErrs:   2,
					validationErrs: 1,
					valFields:      []string{fieldMetadata, fieldName},
					templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Label
---
apiVersion: influxdata.com/v2alpha1
kind: Label
`,
				},
				{
					name:           "duplicate meta name and spec name",
					validationErrs: 1,
					valFields:      []string{fieldSpec, fieldName},
					templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Label
metadata:
  name: valid-name
spec:
---
apiVersion: influxdata.com/v2alpha1
kind: Label
metadata:
  name: label-1
spec:
  name: valid-name
`,
				},
				{
					name:           "spec name to short",
					validationErrs: 1,
					valFields:      []string{fieldSpec, fieldName},
					templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Label
metadata:
  name: valid-name
spec:
---
apiVersion: influxdata.com/v2alpha1
kind: Label
metadata:
  name: label-1
spec:
  name: a
`,
				},
			}

			for _, tt := range tests {
				testTemplateErrors(t, KindLabel, tt)
			}
		})
	})

	t.Run("template with buckets and labels associated", func(t *testing.T) {
		t.Run("happy path", func(t *testing.T) {
			testfileRunner(t, "testdata/bucket_associates_label", func(t *testing.T, template *Template) {
				sum := template.Summary()
				require.Len(t, sum.Labels, 2)

				bkts := sum.Buckets
				require.Len(t, bkts, 3)

				expectedLabels := []struct {
					bktName string
					labels  []string
				}{
					{
						bktName: "rucket-1",
						labels:  []string{"label-1"},
					},
					{
						bktName: "rucket-2",
						labels:  []string{"label-2"},
					},
					{
						bktName: "rucket-3",
						labels:  []string{"label-1", "label-2"},
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
						ResourceMetaName: "rucket-1",
						ResourceName:     "rucket-1",
						LabelMetaName:    "label-1",
						LabelName:        "label-1",
					},
					{
						ResourceMetaName: "rucket-2",
						ResourceName:     "rucket-2",
						LabelMetaName:    "label-2",
						LabelName:        "label-2",
					},
					{
						ResourceMetaName: "rucket-3",
						ResourceName:     "rucket-3",
						LabelMetaName:    "label-1",
						LabelName:        "label-1",
					},
					{
						ResourceMetaName: "rucket-3",
						ResourceName:     "rucket-3",
						LabelMetaName:    "label-2",
						LabelName:        "label-2",
					},
				}

				for _, expectedMapping := range expectedMappings {
					expectedMapping.Status = StateStatusNew
					expectedMapping.ResourceType = influxdb.BucketsResourceType
					assert.Contains(t, sum.LabelMappings, expectedMapping)
				}
			})
		})

		t.Run("association doesn't exist then provides an error", func(t *testing.T) {
			tests := []testTemplateResourceError{
				{
					name:    "no labels provided",
					assErrs: 1,
					assIdxs: []int{0},
					templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Bucket
metadata:
  name: rucket-1
spec:
  associations:
    - kind: Label
      name: label-1
`,
				},
				{
					name:    "mixed found and not found",
					assErrs: 1,
					assIdxs: []int{1},
					templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Label
metadata:
  name: label-1
---
apiVersion: influxdata.com/v2alpha1
kind: Bucket
metadata:
  name: rucket-3
spec:
  associations:
    - kind: Label
      name: label-1
    - kind: Label
      name: NOT TO BE FOUND
`,
				},
				{
					name:    "multiple not found",
					assErrs: 1,
					assIdxs: []int{0, 1},
					templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Bucket
metadata:
  name: rucket-3
spec:
  associations:
    - kind: Label
      name: label-1
    - kind: Label
      name: label-2
`,
				},
				{
					name:    "duplicate valid nested labels",
					assErrs: 1,
					assIdxs: []int{1},
					templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Label
metadata:
  name: label-1
---
apiVersion: influxdata.com/v2alpha1
kind: Bucket
metadata:
  name: rucket-3
spec:
  associations:
    - kind: Label
      name: label-1
    - kind: Label
      name: label-1
`,
				},
			}

			for _, tt := range tests {
				testTemplateErrors(t, KindBucket, tt)
			}
		})
	})

	t.Run("template with checks", func(t *testing.T) {
		t.Run("happy path", func(t *testing.T) {
			testfileRunner(t, "testdata/checks", func(t *testing.T, template *Template) {
				sum := template.Summary()
				require.Len(t, sum.Checks, 2)

				check1 := sum.Checks[0]
				assert.Equal(t, KindCheckThreshold, check1.Kind)
				thresholdCheck, ok := check1.Check.(*icheck.Threshold)
				require.Truef(t, ok, "got: %#v", check1)

				expectedBase := icheck.Base{
					Name:                  "check-0",
					Description:           "desc_0",
					Every:                 mustDuration(t, time.Minute),
					Offset:                mustDuration(t, 15*time.Second),
					StatusMessageTemplate: "Check: ${ r._check_name } is: ${ r._level }",
					Tags: []influxdb.Tag{
						{Key: "tag_1", Value: "val_1"},
						{Key: "tag_2", Value: "val_2"},
					},
				}
				expectedBase.Query.Text = "from(bucket: \"rucket_1\")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == \"cpu\")\n  |> filter(fn: (r) => r._field == \"usage_idle\")\n  |> aggregateWindow(every: 1m, fn: mean)\n  |> yield(name: \"mean\")"
				assert.Equal(t, expectedBase, thresholdCheck.Base)

				expectedThresholds := []icheck.ThresholdConfig{
					icheck.Greater{
						ThresholdConfigBase: icheck.ThresholdConfigBase{
							AllValues: true,
							Level:     notification.Critical,
						},
						Value: 50.0,
					},
					icheck.Lesser{
						ThresholdConfigBase: icheck.ThresholdConfigBase{Level: notification.Warn},
						Value:               49.9,
					},
					icheck.Range{
						ThresholdConfigBase: icheck.ThresholdConfigBase{Level: notification.Info},
						Within:              true,
						Min:                 30.0,
						Max:                 45.0,
					},
					icheck.Range{
						ThresholdConfigBase: icheck.ThresholdConfigBase{Level: notification.Ok},
						Min:                 30.0,
						Max:                 35.0,
					},
				}
				assert.Equal(t, expectedThresholds, thresholdCheck.Thresholds)
				assert.Equal(t, influxdb.Inactive, check1.Status)
				assert.Len(t, check1.LabelAssociations, 1)

				check2 := sum.Checks[1]
				assert.Equal(t, KindCheckDeadman, check2.Kind)
				deadmanCheck, ok := check2.Check.(*icheck.Deadman)
				require.Truef(t, ok, "got: %#v", check2)

				expectedBase = icheck.Base{
					Name:                  "display name",
					Description:           "desc_1",
					Every:                 mustDuration(t, 5*time.Minute),
					Offset:                mustDuration(t, 10*time.Second),
					StatusMessageTemplate: "Check: ${ r._check_name } is: ${ r._level }",
					Tags: []influxdb.Tag{
						{Key: "tag_1", Value: "val_1"},
						{Key: "tag_2", Value: "val_2"},
					},
				}
				expectedBase.Query.Text = "from(bucket: \"rucket_1\")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == \"cpu\")\n  |> filter(fn: (r) => r._field == \"usage_idle\")\n  |> aggregateWindow(every: 1m, fn: mean)\n  |> yield(name: \"mean\")"
				assert.Equal(t, expectedBase, deadmanCheck.Base)
				assert.Equal(t, influxdb.Active, check2.Status)
				assert.Equal(t, mustDuration(t, 10*time.Minute), deadmanCheck.StaleTime)
				assert.Equal(t, mustDuration(t, 90*time.Second), deadmanCheck.TimeSince)
				assert.True(t, deadmanCheck.ReportZero)
				assert.Len(t, check2.LabelAssociations, 1)

				expectedMappings := []SummaryLabelMapping{
					{
						LabelMetaName:    "label-1",
						LabelName:        "label-1",
						ResourceMetaName: "check-0",
						ResourceName:     "check-0",
					},
					{
						LabelMetaName:    "label-1",
						LabelName:        "label-1",
						ResourceMetaName: "check-1",
						ResourceName:     "display name",
					},
				}
				for _, expected := range expectedMappings {
					expected.Status = StateStatusNew
					expected.ResourceType = influxdb.ChecksResourceType
					assert.Contains(t, sum.LabelMappings, expected)
				}
			})
		})

		t.Run("with env refs should be successful", func(t *testing.T) {
			testfileRunner(t, "testdata/checks_ref.yml", func(t *testing.T, template *Template) {
				actual := template.Summary().Checks
				require.Len(t, actual, 1)

				expectedEnvRefs := []SummaryReference{
					{
						Field:        "metadata.name",
						EnvRefKey:    "meta-name",
						DefaultValue: "meta",
					},
					{
						Field:        "spec.name",
						EnvRefKey:    "spec-name",
						DefaultValue: "spectacles",
					},
					{
						Field:     "spec.associations[0].name",
						EnvRefKey: "label-meta-name",
					},
				}
				assert.Equal(t, expectedEnvRefs, actual[0].EnvReferences)
			})
		})

		t.Run("handles bad config", func(t *testing.T) {
			tests := []struct {
				kind   Kind
				resErr testTemplateResourceError
			}{
				{
					kind: KindCheckDeadman,
					resErr: testTemplateResourceError{
						name:           "duplicate name",
						validationErrs: 1,
						valFields:      []string{fieldMetadata, fieldName},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: CheckDeadman
metadata:
  name: check-1
spec:
  every: 5m
  level: cRiT
  query:  >
    from(bucket: "rucket_1") |> yield(name: "mean")
  statusMessageTemplate: "Check: ${ r._check_name } is: ${ r._level }"
---
apiVersion: influxdata.com/v2alpha1
kind: CheckDeadman
metadata:
  name: check-1
spec:
  every: 5m
  level: cRiT
  query:  >
    from(bucket: "rucket_1") |> yield(name: "mean")
  statusMessageTemplate: "Check: ${ r._check_name } is: ${ r._level }"
`,
					},
				},
				{
					kind: KindCheckThreshold,
					resErr: testTemplateResourceError{
						name:           "missing every duration",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldEvery},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: CheckThreshold
metadata:
  name: check-0
spec:
  query:  >
    from(bucket: "rucket_1") |> yield(name: "mean")
  statusMessageTemplate: "Check: ${ r._check_name } is: ${ r._level }"
  thresholds:
    - type: outside_range
      level: ok
      min: 30.0
      max: 35.0

`,
					},
				},
				{
					kind: KindCheckThreshold,
					resErr: testTemplateResourceError{
						name:           "invalid threshold value provided",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldLevel},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: CheckThreshold
metadata:
  name: check-0
spec:
  every: 1m
  query:  >
    from(bucket: "rucket_1") |> yield(name: "mean")
  statusMessageTemplate: "Check: ${ r._check_name } is: ${ r._level }"
  thresholds:
    - type: greater
      level: RANDO
      value: 50.0
`,
					},
				},
				{
					kind: KindCheckThreshold,
					resErr: testTemplateResourceError{
						name:           "invalid threshold type provided",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldType},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: CheckThreshold
metadata:
  name: check-0
spec:
  every: 1m
  query:  >
    from(bucket: "rucket_1") |> yield(name: "mean")
  statusMessageTemplate: "Check: ${ r._check_name } is: ${ r._level }"
  thresholds:
    - type: RANDO_TYPE
      level: CRIT
      value: 50.0
`,
					},
				},
				{
					kind: KindCheckThreshold,
					resErr: testTemplateResourceError{
						name:           "invalid min for inside range",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldMin},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: CheckThreshold
metadata:
  name: check-0
spec:
  every: 1m
  query:  >
    from(bucket: "rucket_1") |> yield(name: "mean")
  statusMessageTemplate: "Check: ${ r._check_name } is: ${ r._level }"
  thresholds:
    - type: inside_range
      level: INfO
      min: 45.0
      max: 30.0
`,
					},
				},
				{
					kind: KindCheckThreshold,
					resErr: testTemplateResourceError{
						name:           "no threshold values provided",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldCheckThresholds},
						templateStr: `---
apiVersion: influxdata.com/v2alpha1
kind: CheckThreshold
metadata:
  name: check-0
spec:
  every: 1m
  query:  >
    from(bucket: "rucket_1") |> yield(name: "mean")
  statusMessageTemplate: "Check: ${ r._check_name } is: ${ r._level }"
  thresholds:
`,
					},
				},
				{
					kind: KindCheckThreshold,
					resErr: testTemplateResourceError{
						name:           "threshold missing query",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldQuery},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: CheckThreshold
metadata:
  name: check-0
spec:
  every: 1m
  statusMessageTemplate: "Check: ${ r._check_name } is: ${ r._level }"
  thresholds:
    - type: greater
      level: CRIT
      value: 50.0
`,
					},
				},
				{
					kind: KindCheckThreshold,
					resErr: testTemplateResourceError{
						name:           "invalid status provided",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldStatus},
						templateStr: `---
apiVersion: influxdata.com/v2alpha1
kind: CheckThreshold
metadata:
  name: check-0
spec:
  every: 1m
  query:  >
    from(bucket: "rucket_1") |> yield(name: "mean")
  status: RANDO STATUS
  statusMessageTemplate: "Check: ${ r._check_name } is: ${ r._level }"
  thresholds:
    - type: greater
      level: CRIT
      value: 50.0
      allValues: true
`,
					},
				},
				{
					kind: KindCheckThreshold,
					resErr: testTemplateResourceError{
						name:           "missing status message template",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldCheckStatusMessageTemplate},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: CheckThreshold
metadata:
  name: check-0
spec:
  every: 1m
  query:  >
    from(bucket: "rucket_1")
  thresholds:
    - type: greater
      level: CRIT
      value: 50.0
`,
					},
				},
				{
					kind: KindCheckDeadman,
					resErr: testTemplateResourceError{
						name:           "missing every",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldEvery},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: CheckDeadman
metadata:
  name: check-1
spec:
  level: cRiT
  query:  >
    from(bucket: "rucket_1") |> yield(name: "mean")
  statusMessageTemplate: "Check: ${ r._check_name } is: ${ r._level }"
  timeSince: 90s
`,
					},
				},
				{
					kind: KindCheckDeadman,
					resErr: testTemplateResourceError{
						name:           "deadman missing every",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldQuery},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: CheckDeadman
metadata:
  name: check-1
spec:
  every: 5m
  level: cRiT
  statusMessageTemplate: "Check: ${ r._check_name } is: ${ r._level }"
  timeSince: 90s
`,
					},
				},
				{
					kind: KindCheckDeadman,
					resErr: testTemplateResourceError{
						name:           "missing association label",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldAssociations},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: CheckDeadman
metadata:
  name: check-1
spec:
  every: 5m
  level: cRiT
  query:  >
    from(bucket: "rucket_1") |> yield(name: "mean")
  statusMessageTemplate: "Check: ${ r._check_name } is: ${ r._level }"
  timeSince: 90s
  associations:
    - kind: Label
      name: label-1
`,
					},
				},
				{
					kind: KindCheckDeadman,
					resErr: testTemplateResourceError{
						name:           "duplicate association labels",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldAssociations},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Label
metadata:
  name: label-1
---
apiVersion: influxdata.com/v2alpha1
kind: CheckDeadman
metadata:
  name: check-1
spec:
  every: 5m
  level: cRiT
  query:  >
    from(bucket: "rucket_1") |> yield(name: "mean")
  statusMessageTemplate: "Check: ${ r._check_name } is: ${ r._level }"
  timeSince: 90s
  associations:
    - kind: Label
      name: label-1
    - kind: Label
      name: label-1
`,
					},
				},
				/* checks are not name unique
							{
								kind: KindCheckDeadman,
								resErr: testTemplateResourceError{
									name:           "duplicate meta name and spec name",
									validationErrs: 1,
									valFields:      []string{fieldSpec, fieldAssociations},
									templateStr: `
				apiVersion: influxdata.com/v2alpha1
				kind: CheckDeadman
				metadata:
					name: check-1
				spec:
					every: 5m
					level: cRiT
					query:  >
						from(bucket: "rucket_1") |> yield(name: "mean")
					statusMessageTemplate: "Check: ${ r._check_name } is: ${ r._level }"
					timeSince: 90s
				---
				apiVersion: influxdata.com/v2alpha1
				kind: CheckDeadman
				metadata:
					name: valid-name
				spec:
					name: check-1
					every: 5m
					level: cRiT
					query:  >
						from(bucket: "rucket_1") |> yield(name: "mean")
					statusMessageTemplate: "Check: ${ r._check_name } is: ${ r._level }"
					timeSince: 90s
				`,
								},
							},
				*/
			}

			for _, tt := range tests {
				testTemplateErrors(t, tt.kind, tt.resErr)
			}
		})
	})

	t.Run("template with dashboard", func(t *testing.T) {
		t.Run("single chart should be successful", func(t *testing.T) {
			t.Run("gauge chart", func(t *testing.T) {
				t.Run("happy path", func(t *testing.T) {
					testfileRunner(t, "testdata/dashboard_gauge", func(t *testing.T, template *Template) {
						sum := template.Summary()
						require.Len(t, sum.Dashboards, 1)

						actual := sum.Dashboards[0]
						assert.Equal(t, KindDashboard, actual.Kind)
						assert.Equal(t, "dash-1", actual.Name)
						assert.Equal(t, "desc1", actual.Description)

						require.Len(t, actual.Charts, 1)
						actualChart := actual.Charts[0]
						assert.Equal(t, 3, actualChart.Height)
						assert.Equal(t, 6, actualChart.Width)
						assert.Equal(t, 1, actualChart.XPosition)
						assert.Equal(t, 2, actualChart.YPosition)

						props, ok := actualChart.Properties.(influxdb.GaugeViewProperties)
						require.True(t, ok)
						assert.Equal(t, "gauge", props.GetType())
						assert.Equal(t, "gauge note", props.Note)
						assert.True(t, props.ShowNoteWhenEmpty)

						require.Len(t, props.Queries, 1)
						q := props.Queries[0]
						queryText := `from(bucket: v.bucket)  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)  |> filter(fn: (r) => r._measurement == "boltdb_writes_total")  |> filter(fn: (r) => r._field == "counter")`
						assert.Equal(t, queryText, q.Text)
						assert.Equal(t, "advanced", q.EditMode)

						require.Len(t, props.ViewColors, 3)
						c := props.ViewColors[0]
						assert.Equal(t, "laser", c.Name)
						assert.Equal(t, "min", c.Type)
						assert.Equal(t, "#8F8AF4", c.Hex)
						assert.Equal(t, 0.0, c.Value)
					})
				})

				t.Run("handles invalid config", func(t *testing.T) {
					tests := []testTemplateResourceError{
						{
							name:           "color mixing a hex value",
							validationErrs: 1,
							valFields:      []string{fieldSpec, "charts[0].colors[0].hex"},
							templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name: dash-1
spec:
  description: desc1
  charts:
    - kind:   gauge
      name:   gauge
      note: gauge note
      noteOnEmpty: true
      xPos:  1
      yPos:  2
      width:  6
      height: 3
      queries:
        - query: >
            from(bucket: v.bucket)  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)  |> filter(fn: (r) => r._measurement == "boltdb_writes_total")  |> filter(fn: (r) => r._field == "counter")
      colors:
        - name: laser
          type: min
          value: 0
        - name: laser
          type: threshold
          hex: "#8F8AF4"
          value: 700
        - name: laser
          type: max
          hex: "#8F8AF4"
          value: 5000
`,
						},
					}

					for _, tt := range tests {
						testTemplateErrors(t, KindDashboard, tt)
					}
				})
			})

			t.Run("heatmap chart", func(t *testing.T) {
				t.Run("happy path", func(t *testing.T) {
					testfileRunner(t, "testdata/dashboard_heatmap", func(t *testing.T, template *Template) {
						sum := template.Summary()
						require.Len(t, sum.Dashboards, 1)

						actual := sum.Dashboards[0]
						assert.Equal(t, KindDashboard, actual.Kind)
						assert.Equal(t, "dash-0", actual.Name)
						assert.Equal(t, "a dashboard w/ heatmap chart", actual.Description)

						require.Len(t, actual.Charts, 1)
						actualChart := actual.Charts[0]
						assert.Equal(t, 3, actualChart.Height)
						assert.Equal(t, 6, actualChart.Width)
						assert.Equal(t, 1, actualChart.XPosition)
						assert.Equal(t, 2, actualChart.YPosition)

						props, ok := actualChart.Properties.(influxdb.HeatmapViewProperties)
						require.True(t, ok)
						assert.Equal(t, "heatmap", props.GetType())
						assert.Equal(t, "heatmap note", props.Note)
						assert.Equal(t, int32(10), props.BinSize)
						assert.Equal(t, []string{"xTotalTicks", "xTickStart", "xTickStep"}, props.GenerateXAxisTicks)
						assert.Equal(t, 15, props.XTotalTicks)
						assert.Equal(t, 0.0, props.XTickStart)
						assert.Equal(t, 1000.0, props.XTickStep)
						assert.Equal(t, []string{"yTotalTicks", "yTickStart", "yTickStep"}, props.GenerateYAxisTicks)
						assert.Equal(t, 10, props.YTotalTicks)
						assert.Equal(t, 0.0, props.YTickStart)
						assert.Equal(t, 100.0, props.YTickStep)
						assert.Equal(t, true, props.LegendColorizeRows)
						assert.Equal(t, false, props.LegendHide)
						assert.Equal(t, 1.0, props.LegendOpacity)
						assert.Equal(t, 5, props.LegendOrientationThreshold)
						assert.True(t, props.ShowNoteWhenEmpty)

						assert.Equal(t, []float64{0, 10}, props.XDomain)
						assert.Equal(t, []float64{0, 100}, props.YDomain)

						require.Len(t, props.Queries, 1)
						q := props.Queries[0]
						queryText := `from(bucket: v.bucket)  |> range(start: v.timeRangeStart)  |> filter(fn: (r) => r._measurement == "mem")  |> filter(fn: (r) => r._field == "used_percent")  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)  |> yield(name: "mean")`
						assert.Equal(t, queryText, q.Text)
						assert.Equal(t, "advanced", q.EditMode)

						require.Len(t, props.ViewColors, 12)
						c := props.ViewColors[0]
						assert.Equal(t, "#000004", c)
					})
				})

				t.Run("handles invalid config", func(t *testing.T) {
					tests := []testTemplateResourceError{
						{
							name:           "a color is missing a hex value",
							validationErrs: 1,
							valFields:      []string{fieldSpec, "charts[0].colors[2].hex"},
							templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name: dash-0
spec:
  charts:
    - kind:   heatmap
      name:   heatmap
      xPos:  1
      yPos:  2
      width:  6
      height: 3
      binSize: 10
      xCol: _time
      yCol: _value
      queries:
        - query: >
            from(bucket: v.bucket)  |> range(start: v.timeRangeStart)  |> filter(fn: (r) => r._measurement == "mem")  |> filter(fn: (r) => r._field == "used_percent")  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)  |> yield(name: "mean")
      colors:
        - hex: "#fbb61a"
        - hex: "#f4df53"
        - hex: ""
      axes:
        - name: "x"
          label: "x_label"
          prefix: "x_prefix"
          suffix: "x_suffix"
          domain:
            - 0
            - 10
        - name: "y"
          label: "y_label"
          prefix: "y_prefix"
          suffix: "y_suffix"
          domain:
            - 0
            - 100
`,
						},
						{
							name:           "missing axes",
							validationErrs: 1,
							valFields:      []string{fieldSpec, "charts[0].axes"},
							templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name: dash-0
spec:
  charts:
    - kind:   heatmap
      name:   heatmap
      xPos:  1
      yPos:  2
      width:  6
      height: 3
      binSize: 10
      xCol: _time
      yCol: _value
      queries:
        - query: >
            from(bucket: v.bucket)  |> range(start: v.timeRangeStart)  |> filter(fn: (r) => r._measurement == "mem")  |> filter(fn: (r) => r._field == "used_percent")  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)  |> yield(name: "mean")
      colors:
        - hex: "#000004"
`,
						},
					}

					for _, tt := range tests {
						testTemplateErrors(t, KindDashboard, tt)
					}
				})
			})

			t.Run("histogram chart", func(t *testing.T) {
				t.Run("happy path", func(t *testing.T) {
					testfileRunner(t, "testdata/dashboard_histogram", func(t *testing.T, template *Template) {
						sum := template.Summary()
						require.Len(t, sum.Dashboards, 1)

						actual := sum.Dashboards[0]
						assert.Equal(t, KindDashboard, actual.Kind)
						assert.Equal(t, "dash-0", actual.Name)
						assert.Equal(t, "a dashboard w/ single histogram chart", actual.Description)

						require.Len(t, actual.Charts, 1)
						actualChart := actual.Charts[0]
						assert.Equal(t, 3, actualChart.Height)
						assert.Equal(t, 6, actualChart.Width)

						props, ok := actualChart.Properties.(influxdb.HistogramViewProperties)
						require.True(t, ok)
						assert.Equal(t, "histogram", props.GetType())
						assert.Equal(t, "histogram note", props.Note)
						assert.Equal(t, 30, props.BinCount)
						assert.Equal(t, true, props.LegendColorizeRows)
						assert.Equal(t, false, props.LegendHide)
						assert.Equal(t, 1.0, props.LegendOpacity)
						assert.Equal(t, 5, props.LegendOrientationThreshold)
						assert.True(t, props.ShowNoteWhenEmpty)
						assert.Equal(t, []float64{0, 10}, props.XDomain)
						assert.Equal(t, []string{"a", "b"}, props.FillColumns)
						require.Len(t, props.Queries, 1)
						q := props.Queries[0]
						queryText := `from(bucket: v.bucket) |> range(start: v.timeRangeStart, stop: v.timeRangeStop) |> filter(fn: (r) => r._measurement == "boltdb_reads_total") |> filter(fn: (r) => r._field == "counter")`
						assert.Equal(t, queryText, q.Text)
						assert.Equal(t, "advanced", q.EditMode)

						require.Len(t, props.ViewColors, 3)
						assert.Equal(t, "#8F8AF4", props.ViewColors[0].Hex)
						assert.Equal(t, "#F4CF31", props.ViewColors[1].Hex)
						assert.Equal(t, "#FFFFFF", props.ViewColors[2].Hex)
					})
				})

				t.Run("handles invalid config", func(t *testing.T) {
					tests := []testTemplateResourceError{
						{
							name:           "missing x-axis",
							validationErrs: 1,
							valFields:      []string{fieldSpec, "charts[0].axes"},
							templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name: dash-0
spec:
  description: a dashboard w/ single histogram chart
  charts:
    - kind: Histogram
      name: histogram chart
      xCol: _value
      width:  6
      height: 3
      binCount: 30
      queries:
        - query: >
            from(bucket: v.bucket) |> range(start: v.timeRangeStart, stop: v.timeRangeStop) |> filter(fn: (r) => r._measurement == "boltdb_reads_total") |> filter(fn: (r) => r._field == "counter")
      colors:
        - hex: "#8F8AF4"
          type: scale
          value: 0
          name: mycolor
      axes:
`,
						},
					}

					for _, tt := range tests {
						testTemplateErrors(t, KindDashboard, tt)
					}
				})
			})

			t.Run("markdown chart", func(t *testing.T) {
				t.Run("happy path", func(t *testing.T) {
					testfileRunner(t, "testdata/dashboard_markdown", func(t *testing.T, template *Template) {
						sum := template.Summary()
						require.Len(t, sum.Dashboards, 1)

						actual := sum.Dashboards[0]
						assert.Equal(t, KindDashboard, actual.Kind)
						assert.Equal(t, "dash-0", actual.Name)
						assert.Equal(t, "a dashboard w/ single markdown chart", actual.Description)

						require.Len(t, actual.Charts, 1)
						actualChart := actual.Charts[0]

						props, ok := actualChart.Properties.(influxdb.MarkdownViewProperties)
						require.True(t, ok)
						assert.Equal(t, "markdown", props.GetType())
						assert.Equal(t, "## markdown note", props.Note)
					})
				})
			})

			t.Run("mosaic chart", func(t *testing.T) {
				t.Run("happy path", func(t *testing.T) {
					testfileRunner(t, "testdata/dashboard_mosaic.yml", func(t *testing.T, template *Template) {
						sum := template.Summary()
						require.Len(t, sum.Dashboards, 1)

						actual := sum.Dashboards[0]
						assert.Equal(t, KindDashboard, actual.Kind)
						assert.Equal(t, "dash-0", actual.Name)
						assert.Equal(t, "a dashboard w/ single mosaic chart", actual.Description)

						require.Len(t, actual.Charts, 1)
						actualChart := actual.Charts[0]
						assert.Equal(t, 3, actualChart.Height)
						assert.Equal(t, 6, actualChart.Width)
						assert.Equal(t, 1, actualChart.XPosition)
						assert.Equal(t, 2, actualChart.YPosition)

						props, ok := actualChart.Properties.(influxdb.MosaicViewProperties)
						require.True(t, ok)
						assert.Equal(t, "mosaic note", props.Note)
						assert.Equal(t, "y", props.HoverDimension)
						assert.True(t, props.ShowNoteWhenEmpty)

						require.Len(t, props.Queries, 1)
						q := props.Queries[0]
						expectedQuery := `from(bucket: v.bucket)  |> range(start: v.timeRangeStart)  |> filter(fn: (r) => r._measurement == "mem")  |> filter(fn: (r) => r._field == "used_percent")  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)  |> yield(name: "mean")`
						assert.Equal(t, expectedQuery, q.Text)
						assert.Equal(t, "advanced", q.EditMode)

						assert.Equal(t, ",", props.YLabelColumnSeparator)
						assert.Equal(t, []string{"foo"}, props.YLabelColumns)
						assert.Equal(t, []string{"_value", "foo"}, props.YSeriesColumns)
						assert.Equal(t, []float64{0, 10}, props.XDomain)
						assert.Equal(t, []float64{0, 100}, props.YDomain)
						assert.Equal(t, "x_label", props.XAxisLabel)
						assert.Equal(t, "y_label", props.YAxisLabel)
						assert.Equal(t, "x_prefix", props.XPrefix)
						assert.Equal(t, "y_prefix", props.YPrefix)
						assert.Equal(t, "x_suffix", props.XSuffix)
						assert.Equal(t, "y_suffix", props.YSuffix)
						assert.Equal(t, []string{"xTotalTicks", "xTickStart", "xTickStep"}, props.GenerateXAxisTicks)
						assert.Equal(t, 15, props.XTotalTicks)
						assert.Equal(t, 0.0, props.XTickStart)
						assert.Equal(t, 1000.0, props.XTickStep)
						assert.Equal(t, true, props.LegendColorizeRows)
						assert.Equal(t, false, props.LegendHide)
						assert.Equal(t, 1.0, props.LegendOpacity)
						assert.Equal(t, 5, props.LegendOrientationThreshold)
					})
				})
			})

			t.Run("band chart", func(t *testing.T) {
				t.Run("happy path", func(t *testing.T) {
					testfileRunner(t, "testdata/dashboard_band.yml", func(t *testing.T, template *Template) {
						sum := template.Summary()
						require.Len(t, sum.Dashboards, 1)

						actual := sum.Dashboards[0]
						assert.Equal(t, KindDashboard, actual.Kind)
						assert.Equal(t, "dash-1", actual.Name)
						assert.Equal(t, "a dashboard w/ single band chart", actual.Description)

						require.Len(t, actual.Charts, 1)
						actualChart := actual.Charts[0]
						assert.Equal(t, 3, actualChart.Height)
						assert.Equal(t, 6, actualChart.Width)
						assert.Equal(t, 1, actualChart.XPosition)
						assert.Equal(t, 2, actualChart.YPosition)

						props, ok := actualChart.Properties.(influxdb.BandViewProperties)
						require.True(t, ok)
						assert.Equal(t, "band note", props.Note)
						assert.True(t, props.ShowNoteWhenEmpty)
						assert.Equal(t, "y", props.HoverDimension)
						assert.Equal(t, "foo", props.UpperColumn)
						assert.Equal(t, "baz", props.MainColumn)
						assert.Equal(t, "bar", props.LowerColumn)
						assert.Equal(t, []string{"xTotalTicks", "xTickStart", "xTickStep"}, props.GenerateXAxisTicks)
						assert.Equal(t, 15, props.XTotalTicks)
						assert.Equal(t, 0.0, props.XTickStart)
						assert.Equal(t, 1000.0, props.XTickStep)
						assert.Equal(t, []string{"yTotalTicks", "yTickStart", "yTickStep"}, props.GenerateYAxisTicks)
						assert.Equal(t, 10, props.YTotalTicks)
						assert.Equal(t, 0.0, props.YTickStart)
						assert.Equal(t, 100.0, props.YTickStep)
						assert.Equal(t, true, props.LegendColorizeRows)
						assert.Equal(t, false, props.LegendHide)
						assert.Equal(t, 1.0, props.LegendOpacity)
						assert.Equal(t, 5, props.LegendOrientationThreshold)
						assert.Equal(t, true, props.StaticLegend.ColorizeRows)
						assert.Equal(t, 0.2, props.StaticLegend.HeightRatio)
						assert.Equal(t, true, props.StaticLegend.Show)
						assert.Equal(t, 1.0, props.StaticLegend.Opacity)
						assert.Equal(t, 5, props.StaticLegend.OrientationThreshold)
						assert.Equal(t, "y", props.StaticLegend.ValueAxis)
						assert.Equal(t, 1.0, props.StaticLegend.WidthRatio)

						require.Len(t, props.ViewColors, 1)
						c := props.ViewColors[0]
						assert.Equal(t, "laser", c.Name)
						assert.Equal(t, "scale", c.Type)
						assert.Equal(t, "#8F8AF4", c.Hex)
						assert.Equal(t, 3.0, c.Value)

						require.Len(t, props.Queries, 1)
						q := props.Queries[0]
						expectedQuery := `from(bucket: v.bucket)  |> range(start: v.timeRangeStart)  |> filter(fn: (r) => r._measurement == "mem")  |> filter(fn: (r) => r._field == "used_percent")  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)  |> yield(name: "mean")`
						assert.Equal(t, expectedQuery, q.Text)
						assert.Equal(t, "advanced", q.EditMode)

						for _, key := range []string{"x", "y"} {
							xAxis, ok := props.Axes[key]
							require.True(t, ok, "key="+key)
							assert.Equal(t, key+"_label", xAxis.Label, "key="+key)
							assert.Equal(t, key+"_prefix", xAxis.Prefix, "key="+key)
							assert.Equal(t, key+"_suffix", xAxis.Suffix, "key="+key)
						}

					})
				})
			})

			t.Run("scatter chart", func(t *testing.T) {
				t.Run("happy path", func(t *testing.T) {
					testfileRunner(t, "testdata/dashboard_scatter", func(t *testing.T, template *Template) {
						sum := template.Summary()
						require.Len(t, sum.Dashboards, 1)

						actual := sum.Dashboards[0]
						assert.Equal(t, KindDashboard, actual.Kind)
						assert.Equal(t, "dash-0", actual.Name)
						assert.Equal(t, "a dashboard w/ single scatter chart", actual.Description)

						require.Len(t, actual.Charts, 1)
						actualChart := actual.Charts[0]
						assert.Equal(t, 3, actualChart.Height)
						assert.Equal(t, 6, actualChart.Width)
						assert.Equal(t, 1, actualChart.XPosition)
						assert.Equal(t, 2, actualChart.YPosition)

						props, ok := actualChart.Properties.(influxdb.ScatterViewProperties)
						require.True(t, ok)
						assert.Equal(t, "scatter note", props.Note)
						assert.True(t, props.ShowNoteWhenEmpty)

						require.Len(t, props.Queries, 1)
						q := props.Queries[0]
						expectedQuery := `from(bucket: v.bucket)  |> range(start: v.timeRangeStart)  |> filter(fn: (r) => r._measurement == "mem")  |> filter(fn: (r) => r._field == "used_percent")  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)  |> yield(name: "mean")`
						assert.Equal(t, expectedQuery, q.Text)
						assert.Equal(t, "advanced", q.EditMode)

						assert.Equal(t, []float64{0, 10}, props.XDomain)
						assert.Equal(t, []float64{0, 100}, props.YDomain)
						assert.Equal(t, "x_label", props.XAxisLabel)
						assert.Equal(t, "y_label", props.YAxisLabel)
						assert.Equal(t, "x_prefix", props.XPrefix)
						assert.Equal(t, "y_prefix", props.YPrefix)
						assert.Equal(t, "x_suffix", props.XSuffix)
						assert.Equal(t, "y_suffix", props.YSuffix)
						assert.Equal(t, []string{"xTotalTicks", "xTickStart", "xTickStep"}, props.GenerateXAxisTicks)
						assert.Equal(t, 15, props.XTotalTicks)
						assert.Equal(t, 0.0, props.XTickStart)
						assert.Equal(t, 1000.0, props.XTickStep)
						assert.Equal(t, []string{"yTotalTicks", "yTickStart", "yTickStep"}, props.GenerateYAxisTicks)
						assert.Equal(t, 10, props.YTotalTicks)
						assert.Equal(t, 0.0, props.YTickStart)
						assert.Equal(t, 100.0, props.YTickStep)
						assert.Equal(t, true, props.LegendColorizeRows)
						assert.Equal(t, false, props.LegendHide)
						assert.Equal(t, 1.0, props.LegendOpacity)
						assert.Equal(t, 5, props.LegendOrientationThreshold)
					})
				})

				t.Run("handles invalid config", func(t *testing.T) {
					tests := []testTemplateResourceError{
						{
							name:           "missing axes",
							validationErrs: 1,
							valFields:      []string{fieldSpec, "charts[0].axes"},
							templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name:  dash-0
spec:
  description: a dashboard w/ single scatter chart
  charts:
    - kind:   Scatter
      name:   scatter chart
      xPos:  1
      yPos:  2
      xCol: _time
      yCol: _value
      width:  6
      height: 3
      queries:
        - query: >
            from(bucket: v.bucket)  |> range(start: v.timeRangeStart)  |> filter(fn: (r) => r._measurement == "mem")  |> filter(fn: (r) => r._field == "used_percent")  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)  |> yield(name: "mean")
      colors:
        - hex: "#8F8AF4"
        - hex: "#F4CF31"
`,
						},
						{
							name:           "no width provided",
							validationErrs: 1,
							valFields:      []string{fieldSpec, "charts[0].width"},
							templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name:  dash-0
spec:
  description: a dashboard w/ single scatter chart
  charts:
    - kind:   Scatter
      name:   scatter chart
      xPos:  1
      yPos:  2
      xCol: _time
      yCol: _value
      height: 3
      queries:
        - query: >
            from(bucket: v.bucket)  |> range(start: v.timeRangeStart)  |> filter(fn: (r) => r._measurement == "mem")  |> filter(fn: (r) => r._field == "used_percent")  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)  |> yield(name: "mean")
      colors:
        - hex: "#8F8AF4"
        - hex: "#F4CF31"
        - hex: "#FFFFFF"
      axes:
        - name : "x"
          label: x_label
          prefix: x_prefix
          suffix: x_suffix
          domain:
            - 0
            - 10
        - name: "y"
          label: y_label
          prefix: y_prefix
          suffix: y_suffix
          domain:
            - 0
            - 100
`,
						},
						{
							name:           "no height provided",
							validationErrs: 1,
							valFields:      []string{fieldSpec, "charts[0].height"},
							templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name:  dash-0
spec:
  description: a dashboard w/ single scatter chart
  charts:
    - kind:   Scatter
      name:   scatter chart
      xPos:  1
      yPos:  2
      xCol: _time
      yCol: _value
      width:  6
      queries:
        - query: >
            from(bucket: v.bucket)  |> range(start: v.timeRangeStart)  |> filter(fn: (r) => r._measurement == "mem")  |> filter(fn: (r) => r._field == "used_percent")  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)  |> yield(name: "mean")
      colors:
        - hex: "#8F8AF4"
        - hex: "#F4CF31"
        - hex: "#FFFFFF"
      axes:
        - name : "x"
          label: x_label
          prefix: x_prefix
          suffix: x_suffix
          domain:
            - 0
            - 10
        - name: "y"
          label: y_label
          prefix: y_prefix
          suffix: y_suffix
          domain:
            - 0
            - 100
`,
						},
						{
							name:           "missing hex color",
							validationErrs: 1,
							valFields:      []string{fieldSpec, "charts[0].colors[0].hex"},
							templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name:  dash-0
spec:
  description: a dashboard w/ single scatter chart
  charts:
    - kind:   Scatter
      name:   scatter chart
      note: scatter note
      noteOnEmpty: true
      prefix: sumtin
      suffix: days
      xPos:  1
      yPos:  2
      xCol: _time
      yCol: _value
      width:  6
      height: 3
      queries:
        - query: >
            from(bucket: v.bucket)  |> range(start: v.timeRangeStart)  |> filter(fn: (r) => r._measurement == "mem")  |> filter(fn: (r) => r._field == "used_percent")  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)  |> yield(name: "mean")
      colors:
        - hex: ""
      axes:
        - name : "x"
          label: x_label
          prefix: x_prefix
          suffix: x_suffix
          domain:
            - 0
            - 10
        - name: "y"
          label: y_label
          prefix: y_prefix
          suffix: y_suffix
          domain:
            - 0
            - 100
`,
						},
						{
							name:           "missing x axis",
							validationErrs: 1,
							valFields:      []string{fieldSpec, "charts[0].axes"},
							templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name:  dash-0
spec:
  description: a dashboard w/ single scatter chart
  charts:
    - kind:   Scatter
      name:   scatter chart
      xPos:  1
      yPos:  2
      xCol: _time
      yCol: _value
      width:  6
      height: 3
      queries:
        - query: >
            from(bucket: v.bucket)  |> range(start: v.timeRangeStart)  |> filter(fn: (r) => r._measurement == "mem")  |> filter(fn: (r) => r._field == "used_percent")  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)  |> yield(name: "mean")
      colors:
        - hex: "#8F8AF4"
        - hex: "#F4CF31"
        - hex: "#FFFFFF"
      axes:
        - name: "y"
          label: y_label
          prefix: y_prefix
          suffix: y_suffix
          domain:
            - 0
            - 100
`,
						},
						{
							name:           "missing y axis",
							validationErrs: 1,
							valFields:      []string{fieldSpec, "charts[0].axes"},
							templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name:  dash-0
spec:
  description: a dashboard w/ single scatter chart
  charts:
    - kind:   Scatter
      name:   scatter chart
      xPos:  1
      yPos:  2
      xCol: _time
      yCol: _value
      width:  6
      height: 3
      queries:
        - query: >
            from(bucket: v.bucket)  |> range(start: v.timeRangeStart)  |> filter(fn: (r) => r._measurement == "mem")  |> filter(fn: (r) => r._field == "used_percent")  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)  |> yield(name: "mean")
      colors:
        - hex: "#8F8AF4"
        - hex: "#F4CF31"
        - hex: "#FFFFFF"
      axes:
        - name : "x"
          label: x_label
          prefix: x_prefix
          suffix: x_suffix
          domain:
            - 0
            - 10
`,
						},
					}

					for _, tt := range tests {
						testTemplateErrors(t, KindDashboard, tt)
					}
				})
			})

			t.Run("single stat chart", func(t *testing.T) {
				t.Run("happy path", func(t *testing.T) {
					testfileRunner(t, "testdata/dashboard", func(t *testing.T, template *Template) {
						sum := template.Summary()
						require.Len(t, sum.Dashboards, 2)

						actual := sum.Dashboards[0]
						assert.Equal(t, KindDashboard, actual.Kind)
						assert.Equal(t, "dash-1", actual.MetaName)
						assert.Equal(t, "display name", actual.Name)
						assert.Equal(t, "desc1", actual.Description)

						require.Len(t, actual.Charts, 1)
						actualChart := actual.Charts[0]
						assert.Equal(t, 3, actualChart.Height)
						assert.Equal(t, 6, actualChart.Width)
						assert.Equal(t, 1, actualChart.XPosition)
						assert.Equal(t, 2, actualChart.YPosition)

						props, ok := actualChart.Properties.(influxdb.SingleStatViewProperties)
						require.True(t, ok)
						assert.Equal(t, "single-stat", props.GetType())
						assert.Equal(t, "single stat note", props.Note)
						assert.True(t, props.ShowNoteWhenEmpty)
						assert.True(t, props.DecimalPlaces.IsEnforced)
						assert.Equal(t, int32(1), props.DecimalPlaces.Digits)
						assert.Equal(t, "days", props.Suffix)
						assert.Equal(t, "true", props.TickSuffix)
						assert.Equal(t, "sumtin", props.Prefix)
						assert.Equal(t, "true", props.TickPrefix)

						require.Len(t, props.Queries, 1)
						q := props.Queries[0]
						queryText := `from(bucket: v.bucket) |> range(start: v.timeRangeStart) |> filter(fn: (r) => r._measurement == "processes") |> filter(fn: (r) => r._field == "running" or r._field == "blocked") |> aggregateWindow(every: v.windowPeriod, fn: max) |> yield(name: "max")`
						assert.Equal(t, queryText, q.Text)
						assert.Equal(t, "advanced", q.EditMode)

						require.Len(t, props.ViewColors, 1)
						c := props.ViewColors[0]
						assert.Equal(t, "laser", c.Name)
						assert.Equal(t, "text", c.Type)
						assert.Equal(t, "#8F8AF4", c.Hex)
						assert.Equal(t, 3.0, c.Value)

						actual2 := sum.Dashboards[1]
						assert.Equal(t, "dash-2", actual2.MetaName)
						assert.Equal(t, "dash-2", actual2.Name)
						assert.Equal(t, "desc", actual2.Description)
					})
				})

				t.Run("handles invalid config", func(t *testing.T) {
					tests := []testTemplateResourceError{
						{
							name:           "color missing hex value",
							validationErrs: 1,
							valFields:      []string{fieldSpec, "charts[0].colors[0].hex"},
							templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name: dash-1
spec:
  description: desc1
  charts:
    - kind:   Single_Stat
      name:   single stat
      xPos: 1
      yPos: 2
      width:  6
      height: 3
      decimalPlaces: 1
      shade: true
      hoverDimension: y
      queries:
        - query: "from(bucket: v.bucket) |> range(start: v.timeRangeStart) |> filter(fn: (r) => r._measurement == \"processes\") |> filter(fn: (r) => r._field == \"running\" or r._field == \"blocked\") |> aggregateWindow(every: v.windowPeriod, fn: max) |> yield(name: \"max\")"
      colors:
        - name: laser
          type: text
          value: 3
`,
						},
						{
							name:           "no width provided",
							validationErrs: 1,
							valFields:      []string{fieldSpec, "charts[0].width"},
							templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name: dash-1
spec:
  description: desc1
  charts:
    - kind:   Single_Stat
      name:   single stat
      xPos: 1
      yPos: 2
      height: 3
      queries:
        - query: "from(bucket: v.bucket) |> range(start: v.timeRangeStart) |> filter(fn: (r) => r._measurement == \"processes\") |> filter(fn: (r) => r._field == \"running\" or r._field == \"blocked\") |> aggregateWindow(every: v.windowPeriod, fn: max) |> yield(name: \"max\")"
      colors:
        - name: laser
          type: text
          hex: "#8F8AF4"
`,
						},
						{
							name:           "no height provided",
							validationErrs: 1,
							valFields:      []string{fieldSpec, "charts[0].height"},
							templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name: dash-1
spec:
  description: desc1
  charts:
    - kind:   Single_Stat
      name:   single stat
      xPos: 1
      yPos: 2
      width: 3
      queries:
        - query: "from(bucket: v.bucket) |> range(start: v.timeRangeStart) |> filter(fn: (r) => r._measurement == \"processes\") |> filter(fn: (r) => r._field == \"running\" or r._field == \"blocked\") |> aggregateWindow(every: v.windowPeriod, fn: max) |> yield(name: \"max\")"
      colors:
        - name: laser
          type: text
          hex: "#8F8AF4"
`,
						},
						{
							name:           "duplicate metadata names",
							validationErrs: 1,
							valFields:      []string{fieldMetadata, fieldName},
							templateStr: `
apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name: dash-1
spec:
---
apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name: dash-1
spec:
`,
						},
						{
							name:           "spec name too short",
							validationErrs: 1,
							valFields:      []string{fieldSpec, fieldName},
							templateStr: `
apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name: dash-1
spec:
  name: d
`,
						},
					}

					for _, tt := range tests {
						testTemplateErrors(t, KindDashboard, tt)
					}
				})
			})

			t.Run("single stat plus line chart", func(t *testing.T) {
				t.Run("happy path", func(t *testing.T) {
					testfileRunner(t, "testdata/dashboard_single_stat_plus_line", func(t *testing.T, template *Template) {
						sum := template.Summary()
						require.Len(t, sum.Dashboards, 1)

						actual := sum.Dashboards[0]
						assert.Equal(t, KindDashboard, actual.Kind)
						assert.Equal(t, "dash-1", actual.Name)
						assert.Equal(t, "desc1", actual.Description)

						require.Len(t, actual.Charts, 1)
						actualChart := actual.Charts[0]
						assert.Equal(t, 3, actualChart.Height)
						assert.Equal(t, 6, actualChart.Width)
						assert.Equal(t, 1, actualChart.XPosition)
						assert.Equal(t, 2, actualChart.YPosition)

						props, ok := actualChart.Properties.(influxdb.LinePlusSingleStatProperties)
						require.True(t, ok)
						assert.Equal(t, "single stat plus line note", props.Note)
						assert.True(t, props.ShowNoteWhenEmpty)
						assert.True(t, props.DecimalPlaces.IsEnforced)
						assert.Equal(t, int32(1), props.DecimalPlaces.Digits)
						assert.Equal(t, "days", props.Suffix)
						assert.Equal(t, "sumtin", props.Prefix)
						assert.Equal(t, "overlaid", props.Position)
						assert.Equal(t, []string{"xTotalTicks", "xTickStart", "xTickStep"}, props.GenerateXAxisTicks)
						assert.Equal(t, 15, props.XTotalTicks)
						assert.Equal(t, 0.0, props.XTickStart)
						assert.Equal(t, 1000.0, props.XTickStep)
						assert.Equal(t, []string{"yTotalTicks", "yTickStart", "yTickStep"}, props.GenerateYAxisTicks)
						assert.Equal(t, 10, props.YTotalTicks)
						assert.Equal(t, 0.0, props.YTickStart)
						assert.Equal(t, 100.0, props.YTickStep)
						assert.Equal(t, true, props.LegendColorizeRows)
						assert.Equal(t, false, props.LegendHide)
						assert.Equal(t, 1.0, props.LegendOpacity)
						assert.Equal(t, 5, props.LegendOrientationThreshold)
						assert.Equal(t, true, props.StaticLegend.ColorizeRows)
						assert.Equal(t, 0.2, props.StaticLegend.HeightRatio)
						assert.Equal(t, true, props.StaticLegend.Show)
						assert.Equal(t, 1.0, props.StaticLegend.Opacity)
						assert.Equal(t, 5, props.StaticLegend.OrientationThreshold)
						assert.Equal(t, "y", props.StaticLegend.ValueAxis)
						assert.Equal(t, 1.0, props.StaticLegend.WidthRatio)

						require.Len(t, props.Queries, 1)
						q := props.Queries[0]
						expectedQuery := `from(bucket: v.bucket)  |> range(start: v.timeRangeStart)  |> filter(fn: (r) => r._measurement == "mem")  |> filter(fn: (r) => r._field == "used_percent")  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)  |> yield(name: "mean")`
						assert.Equal(t, expectedQuery, q.Text)
						assert.Equal(t, "advanced", q.EditMode)

						for _, key := range []string{"x", "y"} {
							xAxis, ok := props.Axes[key]
							require.True(t, ok, "key="+key)
							assert.Equal(t, "10", xAxis.Base, "key="+key)
							assert.Equal(t, key+"_label", xAxis.Label, "key="+key)
							assert.Equal(t, key+"_prefix", xAxis.Prefix, "key="+key)
							assert.Equal(t, "linear", xAxis.Scale, "key="+key)
							assert.Equal(t, key+"_suffix", xAxis.Suffix, "key="+key)
						}

						require.Len(t, props.ViewColors, 2)
						c := props.ViewColors[0]
						assert.Equal(t, "base", c.ID)
						assert.Equal(t, "laser", c.Name)
						assert.Equal(t, "text", c.Type)
						assert.Equal(t, "#8F8AF4", c.Hex)
						assert.Equal(t, 3.0, c.Value)

						c = props.ViewColors[1]
						assert.Equal(t, "base", c.ID)
						assert.Equal(t, "android", c.Name)
						assert.Equal(t, "scale", c.Type)
						assert.Equal(t, "#F4CF31", c.Hex)
						assert.Equal(t, 1.0, c.Value)
					})
				})

				t.Run("handles invalid config", func(t *testing.T) {
					tests := []testTemplateResourceError{
						{
							name:           "color missing hex value",
							validationErrs: 1,
							valFields:      []string{fieldSpec, "charts[0].colors[0].hex"},
							templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name: dash-1
spec:
  description: desc1
  charts:
    - kind:   Single_Stat_Plus_Line
      name:   single stat plus line
      xPos:  1
      yPos:  2
      width:  6
      height: 3
      position: overlaid
      queries:
        - query: >
            from(bucket: v.bucket)  |> range(start: v.timeRangeStart)  |> filter(fn: (r) => r._measurement == "mem")  |> filter(fn: (r) => r._field == "used_percent")  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)  |> yield(name: "mean")
      colors:
        - name: laser
          type: text
          value: 3
      axes:
        - name : "x"
          label: x_label
          prefix: x_prefix
          suffix: x_suffix
          base: 10
          scale: linear
        - name: "y"
          label: y_label
          prefix: y_prefix
          suffix: y_suffix
          base: 10
          scale: linear
`,
						},
						{
							name:           "no width provided",
							validationErrs: 1,
							valFields:      []string{fieldSpec, "charts[0].width"},
							templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name: dash-1
spec:
  description: desc1
  charts:
    - kind:   Single_Stat_Plus_Line
      name:   single stat plus line
      xPos:  1
      yPos:  2
      height: 3
      shade: true
      hoverDimension: "y"
      position: overlaid
      queries:
        - query: >
            from(bucket: v.bucket)  |> range(start: v.timeRangeStart)  |> filter(fn: (r) => r._measurement == "mem")  |> filter(fn: (r) => r._field == "used_percent")  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)  |> yield(name: "mean")
      colors:
        - name: laser
          type: text
          hex: "#8F8AF4"
          value: 3
        - name: android
          type: scale
          hex: "#F4CF31"
          value: 1
      axes:
        - name : "x"
          label: x_label
          prefix: x_prefix
          suffix: x_suffix
          base: 10
          scale: linear
        - name: "y"
          label: y_label
          prefix: y_prefix
          suffix: y_suffix
          base: 10
          scale: linear
`,
						},
						{
							name:           "no height provided",
							validationErrs: 1,
							valFields:      []string{fieldSpec, "charts[0].height"},
							templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name: dash-1
spec:
  description: desc1
  charts:
    - kind:   Single_Stat_Plus_Line
      name:   single stat plus line
      xPos:  1
      yPos:  2
      width:  6
      position: overlaid
      queries:
        - query: >
            from(bucket: v.bucket)  |> range(start: v.timeRangeStart)  |> filter(fn: (r) => r._measurement == "mem")  |> filter(fn: (r) => r._field == "used_percent")  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)  |> yield(name: "mean")
      colors:
        - name: laser
          type: text
          hex: "#8F8AF4"
          value: 3
        - name: android
          type: scale
          hex: "#F4CF31"
          value: 1
      axes:
        - name : "x"
          label: x_label
          prefix: x_prefix
          suffix: x_suffix
          base: 10
          scale: linear
        - name: "y"
          label: y_label
          prefix: y_prefix
          suffix: y_suffix
          base: 10
          scale: linear
`,
						},
						{
							name:           "missing x axis",
							validationErrs: 1,
							valFields:      []string{fieldSpec, "charts[0].axes"},
							templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name: dash-1
spec:
  description: desc1
  charts:
    - kind:   Single_Stat_Plus_Line
      name:   single stat plus line
      xPos:  1
      yPos:  2
      width:  6
      height: 3
      position: overlaid
      queries:
        - query: >
            from(bucket: v.bucket)  |> range(start: v.timeRangeStart)  |> filter(fn: (r) => r._measurement == "mem")  |> filter(fn: (r) => r._field == "used_percent")  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)  |> yield(name: "mean")
      colors:
        - name: laser
          type: text
          hex: "#8F8AF4"
          value: 3
        - name: android
          type: scale
          hex: "#F4CF31"
          value: 1
      axes:
        - name: "y"
          label: y_label
          prefix: y_prefix
          suffix: y_suffix
          base: 10
          scale: linear
`,
						},
						{
							name:           "missing y axis",
							validationErrs: 1,
							valFields:      []string{fieldSpec, "charts[0].axes"},
							templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name: dash-1
spec:
  description: desc1
  charts:
    - kind:   Single_Stat_Plus_Line
      name:   single stat plus line
      xPos:  1
      yPos:  2
      width:  6
      height: 3
      position: overlaid
      queries:
        - query: >
            from(bucket: v.bucket)  |> range(start: v.timeRangeStart)  |> filter(fn: (r) => r._measurement == "mem")  |> filter(fn: (r) => r._field == "used_percent")  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)  |> yield(name: "mean")
      colors:
        - name: laser
          type: text
          hex: "#8F8AF4"
          value: 3
        - name: android
          type: scale
          hex: "#F4CF31"
          value: 1
      axes:
        - name : "x"
          label: x_label
          prefix: x_prefix
          suffix: x_suffix
          base: 10
          scale: linear
`,
						},
					}

					for _, tt := range tests {
						testTemplateErrors(t, KindDashboard, tt)
					}
				})
			})

			t.Run("table chart", func(t *testing.T) {
				t.Run("happy path", func(t *testing.T) {
					testfileRunner(t, "testdata/dashboard_table", func(t *testing.T, template *Template) {
						sum := template.Summary()
						require.Len(t, sum.Dashboards, 1)

						actual := sum.Dashboards[0]
						assert.Equal(t, KindDashboard, actual.Kind)
						assert.Equal(t, "dash-1", actual.Name)
						assert.Equal(t, "desc1", actual.Description)

						require.Len(t, actual.Charts, 1)
						actualChart := actual.Charts[0]
						assert.Equal(t, 3, actualChart.Height)
						assert.Equal(t, 6, actualChart.Width)
						assert.Equal(t, 1, actualChart.XPosition)
						assert.Equal(t, 2, actualChart.YPosition)

						props, ok := actualChart.Properties.(influxdb.TableViewProperties)
						require.True(t, ok)
						assert.Equal(t, "table note", props.Note)
						assert.True(t, props.ShowNoteWhenEmpty)
						assert.True(t, props.DecimalPlaces.IsEnforced)
						assert.Equal(t, int32(1), props.DecimalPlaces.Digits)
						assert.Equal(t, "YYYY:MMMM:DD", props.TimeFormat)

						require.Len(t, props.Queries, 1)
						q := props.Queries[0]
						expectedQuery := `from(bucket: v.bucket)  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)  |> filter(fn: (r) => r._measurement == "boltdb_writes_total")  |> filter(fn: (r) => r._field == "counter")`
						assert.Equal(t, expectedQuery, q.Text)
						assert.Equal(t, "advanced", q.EditMode)

						require.Len(t, props.ViewColors, 1)
						c := props.ViewColors[0]
						assert.Equal(t, "laser", c.Name)
						assert.Equal(t, "min", c.Type)
						assert.Equal(t, "#8F8AF4", c.Hex)
						assert.Equal(t, 3.0, c.Value)

						tableOpts := props.TableOptions
						assert.True(t, tableOpts.VerticalTimeAxis)
						assert.Equal(t, "_time", tableOpts.SortBy.InternalName)
						assert.Equal(t, "truncate", tableOpts.Wrapping)
						assert.True(t, tableOpts.FixFirstColumn)

						assert.Contains(t, props.FieldOptions, influxdb.RenamableField{
							InternalName: "_value",
							DisplayName:  "MB",
							Visible:      true,
						})
						assert.Contains(t, props.FieldOptions, influxdb.RenamableField{
							InternalName: "_time",
							DisplayName:  "time (ms)",
							Visible:      true,
						})
					})
				})

				t.Run("handles invalid config", func(t *testing.T) {
					tests := []testTemplateResourceError{
						{
							name:           "color missing hex value",
							validationErrs: 1,
							valFields:      []string{fieldSpec, "charts[0].colors[0].hex"},
							templateStr: `
apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name: dash-1
spec:
  description: desc1
  charts:
    - kind:   Table
      name:   table
      xPos:  1
      yPos:  2
      width:  6
      height: 3
      queries:
        - query: >
            from(bucket: v.bucket)  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)  |> filter(fn: (r) => r._measurement == "boltdb_writes_total")  |> filter(fn: (r) => r._field == "counter")
      colors:
        - name: laser
          type: min
          hex:
          value: 3.0`,
						},
						{
							name:           "no width provided",
							validationErrs: 1,
							valFields:      []string{fieldSpec, "charts[0].width"},
							templateStr: `
apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name: dash-1
spec:
  description: desc1
  charts:
    - kind:   Table
      name:   table
      xPos:  1
      yPos:  2
      height: 3
      queries:
        - query: >
            from(bucket: v.bucket)  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)  |> filter(fn: (r) => r._measurement == "boltdb_writes_total")  |> filter(fn: (r) => r._field == "counter")
      colors:
        - name: laser
          type: min
          hex: peru
          value: 3.0`,
						},
						{
							name:           "no height provided",
							validationErrs: 1,
							valFields:      []string{fieldSpec, "charts[0].height"},
							templateStr: `
apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name: dash-1
spec:
  description: desc1
  charts:
    - kind:   Table
      name:   table
      xPos:  1
      yPos:  2
      width:  6
      queries:
        - query: >
            from(bucket: v.bucket)  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)  |> filter(fn: (r) => r._measurement == "boltdb_writes_total")  |> filter(fn: (r) => r._field == "counter")
      colors:
        - name: laser
          type: min
          hex: peru
          value: 3.0`,
						},
						{
							name:           "invalid wrapping table option",
							validationErrs: 1,
							valFields:      []string{fieldSpec, "charts[0].tableOptions.wrapping"},
							templateStr: `
apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name: dash-1
spec:
  description: desc1
  charts:
    - kind:   Table
      name:   table
      xPos:  1
      yPos:  2
      width:  6
      height: 3
      tableOptions:
        sortBy: _time
        wrapping: WRONGO wrapping
      queries:
        - query: >
            from(bucket: v.bucket)  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)  |> filter(fn: (r) => r._measurement == "boltdb_writes_total")  |> filter(fn: (r) => r._field == "counter")
      colors:
        - name: laser
          type: min
          hex: "#8F8AF4"
          value: 3.0
`,
						},
					}

					for _, tt := range tests {
						testTemplateErrors(t, KindDashboard, tt)
					}
				})
			})

			t.Run("xy chart", func(t *testing.T) {
				t.Run("happy path", func(t *testing.T) {
					testfileRunner(t, "testdata/dashboard_xy", func(t *testing.T, template *Template) {
						sum := template.Summary()
						require.Len(t, sum.Dashboards, 1)

						actual := sum.Dashboards[0]
						assert.Equal(t, KindDashboard, actual.Kind)
						assert.Equal(t, "dash-1", actual.Name)
						assert.Equal(t, "desc1", actual.Description)

						require.Len(t, actual.Charts, 1)
						actualChart := actual.Charts[0]
						assert.Equal(t, 3, actualChart.Height)
						assert.Equal(t, 6, actualChart.Width)
						assert.Equal(t, 1, actualChart.XPosition)
						assert.Equal(t, 2, actualChart.YPosition)

						props, ok := actualChart.Properties.(influxdb.XYViewProperties)
						require.True(t, ok)
						assert.Equal(t, "xy", props.GetType())
						assert.Equal(t, true, props.ShadeBelow)
						assert.Equal(t, "y", props.HoverDimension)
						assert.Equal(t, "xy chart note", props.Note)
						assert.True(t, props.ShowNoteWhenEmpty)
						assert.Equal(t, "stacked", props.Position)
						assert.Equal(t, []string{"xTotalTicks", "xTickStart", "xTickStep"}, props.GenerateXAxisTicks)
						assert.Equal(t, 15, props.XTotalTicks)
						assert.Equal(t, 0.0, props.XTickStart)
						assert.Equal(t, 1000.0, props.XTickStep)
						assert.Equal(t, []string{"yTotalTicks", "yTickStart", "yTickStep"}, props.GenerateYAxisTicks)
						assert.Equal(t, 10, props.YTotalTicks)
						assert.Equal(t, 0.0, props.YTickStart)
						assert.Equal(t, 100.0, props.YTickStep)
						assert.Equal(t, true, props.LegendColorizeRows)
						assert.Equal(t, false, props.LegendHide)
						assert.Equal(t, 1.0, props.LegendOpacity)
						assert.Equal(t, 5, props.LegendOrientationThreshold)
						assert.Equal(t, true, props.StaticLegend.ColorizeRows)
						assert.Equal(t, 0.2, props.StaticLegend.HeightRatio)
						assert.Equal(t, true, props.StaticLegend.Show)
						assert.Equal(t, 1.0, props.StaticLegend.Opacity)
						assert.Equal(t, 5, props.StaticLegend.OrientationThreshold)
						assert.Equal(t, "y", props.StaticLegend.ValueAxis)
						assert.Equal(t, 1.0, props.StaticLegend.WidthRatio)

						require.Len(t, props.Queries, 1)
						q := props.Queries[0]
						queryText := `from(bucket: v.bucket)  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)  |> filter(fn: (r) => r._measurement == "boltdb_writes_total")  |> filter(fn: (r) => r._field == "counter")`
						assert.Equal(t, queryText, q.Text)
						assert.Equal(t, "advanced", q.EditMode)

						require.Len(t, props.ViewColors, 1)
						c := props.ViewColors[0]
						assert.Equal(t, "laser", c.Name)
						assert.Equal(t, "scale", c.Type)
						assert.Equal(t, "#8F8AF4", c.Hex)
						assert.Equal(t, 3.0, c.Value)
					})
				})

				t.Run("handles invalid config", func(t *testing.T) {
					tests := []testTemplateResourceError{
						{
							name:           "color missing hex value",
							validationErrs: 1,
							valFields:      []string{fieldSpec, "charts[0].colors[0].hex"},
							templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name: dash-1
spec:
  description: desc1
  charts:
    - kind:   XY
      name:   xy chart
      xPos:  1
      yPos:  2
      width:  6
      height: 3
      geom: line
      position: stacked
      queries:
        - query: >
            from(bucket: v.bucket)  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)  |> filter(fn: (r) => r._measurement == "boltdb_writes_total")  |> filter(fn: (r) => r._field == "counter")
      colors:
        - name: laser
          type: scale
          value: 3
      axes:
        - name : "x"
          label: x_label
          prefix: x_prefix
          suffix: x_suffix
          base: 10
          scale: linear
        - name: "y"
          label: y_label
          prefix: y_prefix
          suffix: y_suffix
          base: 10
          scale: linear
`,
						},
						{
							name:           "invalid geom flag",
							validationErrs: 1,
							valFields:      []string{fieldSpec, fieldDashCharts, fieldChartGeom},
							templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name: dash-1
spec:
  description: desc1
  charts:
    - kind:   XY
      name:   xy chart
      xPos:  1
      yPos:  2
      width:  6
      height: 3
      position: stacked
      staticLegend:
      queries:
        - query: >
            from(bucket: v.bucket)  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)  |> filter(fn: (r) => r._measurement == "boltdb_writes_total")  |> filter(fn: (r) => r._field == "counter")
      colors:
        - name: laser
          type: scale
          hex: "#8F8AF4"
          value: 3
      axes:
        - name : "x"
          label: x_label
          prefix: x_prefix
          suffix: x_suffix
          base: 10
          scale: linear
        - name: "y"
          label: y_label
          prefix: y_prefix
          suffix: y_suffix
          base: 10
          scale: linear
`,
						},
					}

					for _, tt := range tests {
						testTemplateErrors(t, KindDashboard, tt)
					}
				})
			})
		})

		t.Run("with params option should be parameterizable", func(t *testing.T) {
			testfileRunner(t, "testdata/dashboard_params.yml", func(t *testing.T, template *Template) {
				sum := template.Summary()
				require.Len(t, sum.Dashboards, 1)

				actual := sum.Dashboards[0]
				assert.Equal(t, KindDashboard, actual.Kind)
				assert.Equal(t, "dash-1", actual.MetaName)

				require.Len(t, actual.Charts, 1)
				actualChart := actual.Charts[0]
				assert.Equal(t, 3, actualChart.Height)
				assert.Equal(t, 6, actualChart.Width)
				assert.Equal(t, 1, actualChart.XPosition)
				assert.Equal(t, 2, actualChart.YPosition)

				props, ok := actualChart.Properties.(influxdb.SingleStatViewProperties)
				require.True(t, ok)
				assert.Equal(t, "single-stat", props.GetType())

				require.Len(t, props.Queries, 1)

				// parmas
				queryText := `option params = {
    bucket: "bar",
    start: -24h0m0s,
    stop: now(),
    name: "max",
    floatVal: 37.2,
    minVal: 10,
}

from(bucket: params.bucket)
    |> range(start: params.start, stop: params.stop)
    |> filter(fn: (r) => r._measurement == "processes")
    |> filter(fn: (r) => r.floater == params.floatVal)
    |> filter(fn: (r) => r._value > params.minVal)
    |> aggregateWindow(every: v.windowPeriod, fn: max)
    |> yield(name: params.name)
`

				q := props.Queries[0]
				assert.Equal(t, queryText, q.Text)
				assert.Equal(t, "advanced", q.EditMode)

				expectedRefs := []SummaryReference{
					{
						Field:        "spec.charts[0].queries[0].params.bucket",
						EnvRefKey:    `dashboards[dash-1].spec.charts[0].queries[0].params.bucket`,
						ValType:      "string",
						DefaultValue: "bar",
					},
					{
						Field:        "spec.charts[0].queries[0].params.floatVal",
						EnvRefKey:    `dashboards[dash-1].spec.charts[0].queries[0].params.floatVal`,
						ValType:      "float",
						DefaultValue: 37.2,
					},
					{
						Field:        "spec.charts[0].queries[0].params.minVal",
						EnvRefKey:    `dashboards[dash-1].spec.charts[0].queries[0].params.minVal`,
						ValType:      "integer",
						DefaultValue: int64(10),
					},
					{
						Field:        "spec.charts[0].queries[0].params.name",
						EnvRefKey:    `dashboards[dash-1].spec.charts[0].queries[0].params.name`,
						ValType:      "string",
						DefaultValue: "max",
					},
					{
						Field:        "spec.charts[0].queries[0].params.start",
						EnvRefKey:    `dashboards[dash-1].spec.charts[0].queries[0].params.start`,
						ValType:      "duration",
						DefaultValue: "-24h0m0s",
					},
					{
						Field:        "spec.charts[0].queries[0].params.stop",
						EnvRefKey:    `dashboards[dash-1].spec.charts[0].queries[0].params.stop`,
						ValType:      "time",
						DefaultValue: "now()",
					},
				}
				assert.Equal(t, expectedRefs, actual.EnvReferences)
			})
		})

		t.Run("with env refs should be valid", func(t *testing.T) {
			testfileRunner(t, "testdata/dashboard_ref.yml", func(t *testing.T, template *Template) {
				actual := template.Summary().Dashboards
				require.Len(t, actual, 1)

				expected := []SummaryReference{
					{
						Field:     "spec.associations[0].name",
						EnvRefKey: "label-meta-name",
					},
					{
						Field:        "metadata.name",
						EnvRefKey:    "meta-name",
						DefaultValue: "meta",
					},
					{
						Field:        "spec.name",
						EnvRefKey:    "spec-name",
						DefaultValue: "spectacles",
					},
				}
				assert.Equal(t, expected, actual[0].EnvReferences)
			})
		})

		t.Run("and labels associated should be successful", func(t *testing.T) {
			t.Run("happy path", func(t *testing.T) {
				testfileRunner(t, "testdata/dashboard_associates_label", func(t *testing.T, template *Template) {
					sum := template.Summary()
					require.Len(t, sum.Dashboards, 1)

					actual := sum.Dashboards[0]
					assert.Equal(t, "dash-1", actual.Name)

					require.Len(t, actual.LabelAssociations, 2)
					assert.Equal(t, "label-1", actual.LabelAssociations[0].Name)
					assert.Equal(t, "label-2", actual.LabelAssociations[1].Name)

					expectedMappings := []SummaryLabelMapping{
						{
							Status:           StateStatusNew,
							ResourceType:     influxdb.DashboardsResourceType,
							ResourceMetaName: "dash-1",
							ResourceName:     "dash-1",
							LabelMetaName:    "label-1",
							LabelName:        "label-1",
						},
						{
							Status:           StateStatusNew,
							ResourceType:     influxdb.DashboardsResourceType,
							ResourceMetaName: "dash-1",
							ResourceName:     "dash-1",
							LabelMetaName:    "label-2",
							LabelName:        "label-2",
						},
					}

					for _, expectedMapping := range expectedMappings {
						assert.Contains(t, sum.LabelMappings, expectedMapping)
					}
				})
			})

			t.Run("association doesn't exist then provides an error", func(t *testing.T) {
				tests := []testTemplateResourceError{
					{
						name:    "no labels provided",
						assErrs: 1,
						assIdxs: []int{0},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name: dash-1
spec:
  associations:
    - kind: Label
      name: label-1
`,
					},
					{
						name:    "mixed found and not found",
						assErrs: 1,
						assIdxs: []int{1},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Label
metadata:
  name: label-1
---
apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name: dash-1
spec:
  associations:
    - kind: Label
      name: label-1
    - kind: Label
      name: unfound label
`,
					},
					{
						name:    "multiple not found",
						assErrs: 1,
						assIdxs: []int{0, 1},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Label
metadata:
  name: label-1
---
apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name: dash-1
spec:
  associations:
    - kind: Label
      name: not found 1
    - kind: Label
      name: unfound label
`,
					},
					{
						name:    "duplicate valid nested labels",
						assErrs: 1,
						assIdxs: []int{1},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Label
metadata:
  name: label-1
---
apiVersion: influxdata.com/v2alpha1
kind: Dashboard
metadata:
  name: dash-1
spec:
  associations:
    - kind: Label
      name: label-1
    - kind: Label
      name: label-1
`,
					},
				}

				for _, tt := range tests {
					testTemplateErrors(t, KindDashboard, tt)
				}
			})
		})
	})

	t.Run("template with notification endpoints", func(t *testing.T) {
		t.Run("and labels associated should be successful", func(t *testing.T) {
			testfileRunner(t, "testdata/notification_endpoint", func(t *testing.T, template *Template) {
				expectedEndpoints := []SummaryNotificationEndpoint{
					{
						SummaryIdentifier: SummaryIdentifier{
							Kind:     KindNotificationEndpointHTTP,
							MetaName: "http-basic-auth-notification-endpoint",
						},
						NotificationEndpoint: &endpoint.HTTP{
							Base: endpoint.Base{
								Name:        "basic endpoint name",
								Description: "http basic auth desc",
								Status:      taskmodel.TaskStatusInactive,
							},
							URL:        "https://www.example.com/endpoint/basicauth",
							AuthMethod: "basic",
							Method:     "POST",
							Username:   influxdb.SecretField{Value: strPtr("secret username")},
							Password:   influxdb.SecretField{Value: strPtr("secret password")},
						},
					},
					{
						SummaryIdentifier: SummaryIdentifier{
							Kind:     KindNotificationEndpointHTTP,
							MetaName: "http-bearer-auth-notification-endpoint",
						},
						NotificationEndpoint: &endpoint.HTTP{
							Base: endpoint.Base{
								Name:        "http-bearer-auth-notification-endpoint",
								Description: "http bearer auth desc",
								Status:      taskmodel.TaskStatusActive,
							},
							URL:        "https://www.example.com/endpoint/bearerauth",
							AuthMethod: "bearer",
							Method:     "PUT",
							Token:      influxdb.SecretField{Value: strPtr("secret token")},
						},
					},
					{
						SummaryIdentifier: SummaryIdentifier{
							Kind:     KindNotificationEndpointHTTP,
							MetaName: "http-none-auth-notification-endpoint",
						},
						NotificationEndpoint: &endpoint.HTTP{
							Base: endpoint.Base{
								Name:        "http-none-auth-notification-endpoint",
								Description: "http none auth desc",
								Status:      taskmodel.TaskStatusActive,
							},
							URL:        "https://www.example.com/endpoint/noneauth",
							AuthMethod: "none",
							Method:     "GET",
						},
					},
					{
						SummaryIdentifier: SummaryIdentifier{
							Kind:     KindNotificationEndpointPagerDuty,
							MetaName: "pager-duty-notification-endpoint",
						},
						NotificationEndpoint: &endpoint.PagerDuty{
							Base: endpoint.Base{
								Name:        "pager duty name",
								Description: "pager duty desc",
								Status:      taskmodel.TaskStatusActive,
							},
							ClientURL:  "http://localhost:8080/orgs/7167eb6719fa34e5/alert-history",
							RoutingKey: influxdb.SecretField{Value: strPtr("secret routing-key")},
						},
					},
					{
						SummaryIdentifier: SummaryIdentifier{
							Kind:     KindNotificationEndpointHTTP,
							MetaName: "slack-notification-endpoint",
						},
						NotificationEndpoint: &endpoint.Slack{
							Base: endpoint.Base{
								Name:        "slack name",
								Description: "slack desc",
								Status:      taskmodel.TaskStatusActive,
							},
							URL:   "https://hooks.slack.com/services/bip/piddy/boppidy",
							Token: influxdb.SecretField{Value: strPtr("tokenval")},
						},
					},
				}

				sum := template.Summary()
				endpoints := sum.NotificationEndpoints
				require.Len(t, endpoints, len(expectedEndpoints))
				require.Len(t, sum.LabelMappings, len(expectedEndpoints))

				for i := range expectedEndpoints {
					expected, actual := expectedEndpoints[i], endpoints[i]
					assert.Equalf(t, expected.NotificationEndpoint, actual.NotificationEndpoint, "index=%d", i)
					require.Len(t, actual.LabelAssociations, 1)
					assert.Equal(t, "label-1", actual.LabelAssociations[0].Name)

					assert.Contains(t, sum.LabelMappings, SummaryLabelMapping{
						Status:           StateStatusNew,
						ResourceType:     influxdb.NotificationEndpointResourceType,
						ResourceMetaName: expected.MetaName,
						ResourceName:     expected.NotificationEndpoint.GetName(),
						LabelMetaName:    "label-1",
						LabelName:        "label-1",
					})
				}
			})
		})

		t.Run("with env refs should be valid", func(t *testing.T) {
			testfileRunner(t, "testdata/notification_endpoint_ref.yml", func(t *testing.T, template *Template) {
				actual := template.Summary().NotificationEndpoints
				require.Len(t, actual, 1)

				expectedEnvRefs := []SummaryReference{
					{
						Field:        "metadata.name",
						EnvRefKey:    "meta-name",
						DefaultValue: "meta",
					},
					{
						Field:        "spec.name",
						EnvRefKey:    "spec-name",
						DefaultValue: "spectacles",
					},
					{
						Field:     "spec.associations[0].name",
						EnvRefKey: "label-meta-name",
					},
				}
				assert.Equal(t, expectedEnvRefs, actual[0].EnvReferences)
			})
		})

		t.Run("handles bad config", func(t *testing.T) {
			tests := []struct {
				kind   Kind
				resErr testTemplateResourceError
			}{
				{
					kind: KindNotificationEndpointSlack,
					resErr: testTemplateResourceError{
						name:           "missing slack url",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldNotificationEndpointURL},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: NotificationEndpointSlack
metadata:
  name: slack-notification-endpoint
spec:
`,
					},
				},
				{
					kind: KindNotificationEndpointPagerDuty,
					resErr: testTemplateResourceError{
						name:           "missing pager duty url",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldNotificationEndpointURL},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: NotificationEndpointPagerDuty
metadata:
  name: pager-duty-notification-endpoint
spec:
`,
					},
				},
				{
					kind: KindNotificationEndpointHTTP,
					resErr: testTemplateResourceError{
						name:           "missing http url",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldNotificationEndpointURL},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: NotificationEndpointHTTP
metadata:
  name: http-none-auth-notification-endpoint
spec:
  type: none
  method: get
`,
					},
				},
				{
					kind: KindNotificationEndpointHTTP,
					resErr: testTemplateResourceError{
						name:           "bad url",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldNotificationEndpointURL},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: NotificationEndpointHTTP
metadata:
  name: http-none-auth-notification-endpoint
spec:
  type: none
  method: get
  url: d_____-_8**(*https://www.examples.coms
`,
					},
				},
				{
					kind: KindNotificationEndpointHTTP,
					resErr: testTemplateResourceError{
						name:           "missing http method",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldNotificationEndpointHTTPMethod},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: NotificationEndpointHTTP
metadata:
  name: http-none-auth-notification-endpoint
spec:
  type: none
  url:  https://www.example.com/endpoint/noneauth
`,
					},
				},
				{
					kind: KindNotificationEndpointHTTP,
					resErr: testTemplateResourceError{
						name:           "invalid http method",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldNotificationEndpointHTTPMethod},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: NotificationEndpointHTTP
metadata:
  name: http-basic-auth-notification-endpoint
spec:
  type: none
  description: http none auth desc
  method: GHOST
  url:  https://www.example.com/endpoint/noneauth
`,
					},
				},
				{
					kind: KindNotificationEndpointHTTP,
					resErr: testTemplateResourceError{
						name:           "missing basic username",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldNotificationEndpointUsername},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: NotificationEndpointHTTP
metadata:
  name: http-basic-auth-notification-endpoint
spec:
  type: basic
  method: POST
  url:  https://www.example.com/endpoint/basicauth
  password: "secret password"
`,
					},
				},
				{
					kind: KindNotificationEndpointHTTP,
					resErr: testTemplateResourceError{
						name:           "missing basic password",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldNotificationEndpointPassword},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: NotificationEndpointHTTP
metadata:
  name: http-basic-auth-notification-endpoint
spec:
  type: basic
  method: POST
  url:  https://www.example.com/endpoint/basicauth
  username: username
`,
					},
				},
				{
					kind: KindNotificationEndpointHTTP,
					resErr: testTemplateResourceError{
						name:           "missing basic password and username",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldNotificationEndpointPassword, fieldNotificationEndpointUsername},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: NotificationEndpointHTTP
metadata:
  name: http-basic-auth-notification-endpoint
spec:
  description: http basic auth desc
  type: basic
  method: pOsT
  url:  https://www.example.com/endpoint/basicauth
`,
					},
				},
				{
					kind: KindNotificationEndpointHTTP,
					resErr: testTemplateResourceError{
						name:           "missing bearer token",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldNotificationEndpointToken},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: NotificationEndpointHTTP
metadata:
  name: http-bearer-auth-notification-endpoint
spec:
  description: http bearer auth desc
  type: bearer
  method: puT
  url:  https://www.example.com/endpoint/bearerauth
`,
					},
				},
				{
					kind: KindNotificationEndpointHTTP,
					resErr: testTemplateResourceError{
						name:           "invalid http type",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldType},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: NotificationEndpointHTTP
metadata:
  name: http-basic-auth-notification-endpoint
spec:
  type: RANDOM WRONG TYPE
  description: http none auth desc
  method: get
  url:  https://www.example.com/endpoint/noneauth
`,
					},
				},
				{
					kind: KindNotificationEndpointSlack,
					resErr: testTemplateResourceError{
						name:           "duplicate endpoints",
						validationErrs: 1,
						valFields:      []string{fieldMetadata, fieldName},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: NotificationEndpointSlack
metadata:
  name: slack-notification-endpoint
spec:
  url: https://hooks.slack.com/services/bip/piddy/boppidy
---
apiVersion: influxdata.com/v2alpha1
kind: NotificationEndpointSlack
metadata:
  name: slack_notification_endpoint
spec:
  url: https://hooks.slack.com/services/bip/piddy/boppidy
`,
					},
				},
				{
					kind: KindNotificationEndpointSlack,
					resErr: testTemplateResourceError{
						name:           "invalid status",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldStatus},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: NotificationEndpointSlack
metadata:
  name: slack-notification-endpoint
spec:
  description: slack desc
  url: https://hooks.slack.com/services/bip/piddy/boppidy
  status: RANDO STATUS
`,
					},
				},
				{
					kind: KindNotificationEndpointSlack,
					resErr: testTemplateResourceError{
						name:           "duplicate meta name and spec name",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldName},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: NotificationEndpointSlack
metadata:
  name: slack
spec:
  description: slack desc
  url: https://hooks.slack.com/services/bip/piddy/boppidy
---
apiVersion: influxdata.com/v2alpha1
kind: NotificationEndpointSlack
metadata:
  name: slack-notification-endpoint
spec:
  name: slack
  description: slack desc
  url: https://hooks.slack.com/services/bip/piddy/boppidy
`,
					},
				},
			}

			for _, tt := range tests {
				testTemplateErrors(t, tt.kind, tt.resErr)
			}
		})
	})

	t.Run("template with notification rules", func(t *testing.T) {
		t.Run("happy path", func(t *testing.T) {
			testfileRunner(t, "testdata/notification_rule", func(t *testing.T, template *Template) {
				sum := template.Summary()
				rules := sum.NotificationRules
				require.Len(t, rules, 1)

				rule := rules[0]
				assert.Equal(t, KindNotificationRule, rule.Kind)
				assert.Equal(t, "rule_0", rule.Name)
				assert.Equal(t, "endpoint-0", rule.EndpointMetaName)
				assert.Equal(t, "desc_0", rule.Description)
				assert.Equal(t, (10 * time.Minute).String(), rule.Every)
				assert.Equal(t, (30 * time.Second).String(), rule.Offset)
				expectedMsgTempl := "Notification Rule: ${ r._notification_rule_name } triggered by check: ${ r._check_name }: ${ r._message }"
				assert.Equal(t, expectedMsgTempl, rule.MessageTemplate)
				assert.Equal(t, influxdb.Active, rule.Status)

				expectedStatusRules := []SummaryStatusRule{
					{CurrentLevel: "CRIT", PreviousLevel: "OK"},
					{CurrentLevel: "WARN"},
				}
				assert.Equal(t, expectedStatusRules, rule.StatusRules)

				expectedTagRules := []SummaryTagRule{
					{Key: "k1", Value: "v1", Operator: "equal"},
					{Key: "k1", Value: "v2", Operator: "equal"},
				}
				assert.Equal(t, expectedTagRules, rule.TagRules)

				require.Len(t, sum.Labels, 2)
				require.Len(t, rule.LabelAssociations, 2)
				assert.Equal(t, "label-1", rule.LabelAssociations[0].MetaName)
				assert.Equal(t, "label-2", rule.LabelAssociations[1].MetaName)
			})
		})

		t.Run("with env refs should be valid", func(t *testing.T) {
			testfileRunner(t, "testdata/notification_rule_ref.yml", func(t *testing.T, template *Template) {
				actual := template.Summary().NotificationRules
				require.Len(t, actual, 1)

				expectedEnvRefs := []SummaryReference{
					{
						Field:        "metadata.name",
						EnvRefKey:    "meta-name",
						DefaultValue: "meta",
					},
					{
						Field:        "spec.name",
						EnvRefKey:    "spec-name",
						DefaultValue: "spectacles",
					},
					{
						Field:     "spec.associations[0].name",
						EnvRefKey: "label-meta-name",
					},
					{
						Field:     "spec.endpointName",
						EnvRefKey: "endpoint-meta-name",
					},
				}
				assert.Equal(t, expectedEnvRefs, actual[0].EnvReferences)
			})
		})

		t.Run("handles bad config", func(t *testing.T) {
			templateWithValidEndpint := func(resource string) string {
				return fmt.Sprintf(`
apiVersion: influxdata.com/v2alpha1
kind: NotificationEndpointSlack
metadata:
  name: endpoint-0
spec:
  url: https://hooks.slack.com/services/bip/piddy/boppidy
---
%s
`, resource)
			}

			tests := []struct {
				kind   Kind
				resErr testTemplateResourceError
			}{
				{
					kind: KindNotificationRule,
					resErr: testTemplateResourceError{
						name:      "missing name",
						valFields: []string{fieldMetadata, fieldName},
						templateStr: templateWithValidEndpint(`apiVersion: influxdata.com/v2alpha1
kind: NotificationRule
metadata:
spec:
  endpointName: endpoint-0
  every: 10m
  messageTemplate: "Notification Rule: ${ r._notification_rule_name } triggered by check: ${ r._check_name }: ${ r._message }"
  statusRules:
    - currentLevel: WARN
`),
					},
				},
				{
					kind: KindNotificationRule,
					resErr: testTemplateResourceError{
						name:      "missing endpoint name",
						valFields: []string{fieldSpec, fieldNotificationRuleEndpointName},
						templateStr: templateWithValidEndpint(`apiVersion: influxdata.com/v2alpha1
kind: NotificationRule
metadata:
  name: rule-0
spec:
  every: 10m
  messageTemplate: "Notification Rule: ${ r._notification_rule_name } triggered by check: ${ r._check_name }: ${ r._message }"
  statusRules:
    - currentLevel: WARN
`),
					},
				},
				{
					kind: KindNotificationRule,
					resErr: testTemplateResourceError{
						name:      "missing every",
						valFields: []string{fieldSpec, fieldEvery},
						templateStr: templateWithValidEndpint(`apiVersion: influxdata.com/v2alpha1
kind: NotificationRule
metadata:
  name: rule-0
spec:
  endpointName: endpoint-0
  messageTemplate: "Notification Rule: ${ r._notification_rule_name } triggered by check: ${ r._check_name }: ${ r._message }"
  statusRules:
    - currentLevel: WARN
`),
					},
				},
				{
					kind: KindNotificationRule,
					resErr: testTemplateResourceError{
						name:      "missing status rules",
						valFields: []string{fieldSpec, fieldNotificationRuleStatusRules},
						templateStr: templateWithValidEndpint(`apiVersion: influxdata.com/v2alpha1
kind: NotificationRule
metadata:
  name: rule-0
spec:
  every: 10m
  endpointName: endpoint-0
  messageTemplate: "Notification Rule: ${ r._notification_rule_name } triggered by check: ${ r._check_name }: ${ r._message }"
`),
					},
				},
				{
					kind: KindNotificationRule,
					resErr: testTemplateResourceError{
						name:      "bad current status rule level",
						valFields: []string{fieldSpec, fieldNotificationRuleStatusRules},
						templateStr: templateWithValidEndpint(`apiVersion: influxdata.com/v2alpha1
kind: NotificationRule
metadata:
  name: rule-0
spec:
  every: 10m
  endpointName: endpoint-0
  messageTemplate: "Notification Rule: ${ r._notification_rule_name } triggered by check: ${ r._check_name }: ${ r._message }"
  statusRules:
    - currentLevel: WRONGO
`),
					},
				},
				{
					kind: KindNotificationRule,
					resErr: testTemplateResourceError{
						name:      "bad previous status rule level",
						valFields: []string{fieldSpec, fieldNotificationRuleStatusRules},
						templateStr: templateWithValidEndpint(`apiVersion: influxdata.com/v2alpha1
kind: NotificationRule
metadata:
  name: rule-0
spec:
  endpointName: endpoint-0
  every: 10m
  messageTemplate: "Notification Rule: ${ r._notification_rule_name } triggered by check: ${ r._check_name }: ${ r._message }"
  statusRules:
    - currentLevel: CRIT
      previousLevel: WRONG
`),
					},
				},
				{
					kind: KindNotificationRule,
					resErr: testTemplateResourceError{
						name:      "bad tag rule operator",
						valFields: []string{fieldSpec, fieldNotificationRuleTagRules},
						templateStr: templateWithValidEndpint(`apiVersion: influxdata.com/v2alpha1
kind: NotificationRule
metadata:
  name: rule-0
spec:
  endpointName: endpoint-0
  every: 10m
  messageTemplate: "Notification Rule: ${ r._notification_rule_name } triggered by check: ${ r._check_name }: ${ r._message }"
  statusRules:
    - currentLevel: WARN
  tagRules:
    - key: k1
      value: v2
      operator: WRONG
`),
					},
				},
				{
					kind: KindNotificationRule,
					resErr: testTemplateResourceError{
						name:      "bad status provided",
						valFields: []string{fieldSpec, fieldStatus},
						templateStr: templateWithValidEndpint(`apiVersion: influxdata.com/v2alpha1
kind: NotificationRule
metadata:
  name: rule-0
spec:
  endpointName: endpoint-0
  every: 10m
  messageTemplate: "Notification Rule: ${ r._notification_rule_name } triggered by check: ${ r._check_name }: ${ r._message }"
  status: RANDO STATUS
  statusRules:
    - currentLevel: WARN
`),
					},
				},
				{
					kind: KindNotificationRule,
					resErr: testTemplateResourceError{
						name:      "label association does not exist",
						valFields: []string{fieldSpec, fieldAssociations},
						templateStr: templateWithValidEndpint(`apiVersion: influxdata.com/v2alpha1
kind: NotificationRule
metadata:
  name: rule-0
spec:
  endpointName: endpoint-0
  every: 10m
  messageTemplate: "Notification Rule: ${ r._notification_rule_name } triggered by check: ${ r._check_name }: ${ r._message }"
  statusRules:
    - currentLevel: WARN
  associations:
    - kind: Label
      name: label-1
`),
					},
				},
				{
					kind: KindNotificationRule,
					resErr: testTemplateResourceError{
						name:      "label association dupe",
						valFields: []string{fieldSpec, fieldAssociations},
						templateStr: templateWithValidEndpint(`apiVersion: influxdata.com/v2alpha1
kind: Label
metadata:
  name: label-1
---
apiVersion: influxdata.com/v2alpha1
kind: NotificationRule
metadata:
  name: rule-0
spec:
  endpointName: endpoint-0
  every: 10m
  messageTemplate: "Notification Rule: ${ r._notification_rule_name } triggered by check: ${ r._check_name }: ${ r._message }"
  statusRules:
    - currentLevel: WARN
  associations:
    - kind: Label
      name: label-1
    - kind: Label
      name: label-1
`),
					},
				},
				{
					kind: KindNotificationRule,
					resErr: testTemplateResourceError{
						name:      "duplicate meta names",
						valFields: []string{fieldMetadata, fieldName},
						templateStr: templateWithValidEndpint(`
apiVersion: influxdata.com/v2alpha1
kind: NotificationRule
metadata:
  name: rule-0
spec:
  endpointName: endpoint-0
  every: 10m
  messageTemplate: "Notification Rule: ${ r._notification_rule_name } triggered by check: ${ r._check_name }: ${ r._message }"
  statusRules:
    - currentLevel: WARN
---
apiVersion: influxdata.com/v2alpha1
kind: NotificationRule
metadata:
  name: rule-0
spec:
  endpointName: endpoint-0
  every: 10m
  messageTemplate: "Notification Rule: ${ r._notification_rule_name } triggered by check: ${ r._check_name }: ${ r._message }"
  statusRules:
    - currentLevel: WARN
`),
					},
				},
				{
					kind: KindNotificationRule,
					resErr: testTemplateResourceError{
						name:      "missing endpoint association in template",
						valFields: []string{fieldSpec, fieldNotificationRuleEndpointName},
						templateStr: `
apiVersion: influxdata.com/v2alpha1
kind: NotificationRule
metadata:
  name: rule-0
spec:
  endpointName: RANDO_ENDPOINT_NAME
  every: 10m
  messageTemplate: "Notification Rule: ${ r._notification_rule_name } triggered by check: ${ r._check_name }: ${ r._message }"
  statusRules:
    - currentLevel: WARN
`,
					},
				},
			}

			for _, tt := range tests {
				testTemplateErrors(t, tt.kind, tt.resErr)
			}
		})
	})

	t.Run("template with tasks", func(t *testing.T) {
		t.Run("happy path", func(t *testing.T) {
			testfileRunner(t, "testdata/tasks", func(t *testing.T, template *Template) {
				sum := template.Summary()
				tasks := sum.Tasks
				require.Len(t, tasks, 2)

				for _, ta := range tasks {
					assert.Equal(t, KindTask, ta.Kind)
				}

				sort.Slice(tasks, func(i, j int) bool {
					return tasks[i].MetaName < tasks[j].MetaName
				})

				baseEqual := func(t *testing.T, i int, status influxdb.Status, actual SummaryTask) {
					t.Helper()

					assert.Equal(t, "task-"+strconv.Itoa(i), actual.Name)
					assert.Equal(t, "desc_"+strconv.Itoa(i), actual.Description)
					assert.Equal(t, status, actual.Status)

					expectedQuery := "from(bucket: \"rucket_1\")\n  |> range(start: -5d, stop: -1h)\n  |> filter(fn: (r) => r._measurement == \"cpu\")\n  |> filter(fn: (r) => r._field == \"usage_idle\")\n  |> aggregateWindow(every: 1m, fn: mean)\n  |> yield(name: \"mean\")"
					assert.Equal(t, expectedQuery, actual.Query)

					require.Len(t, actual.LabelAssociations, 1)
					assert.Equal(t, "label-1", actual.LabelAssociations[0].Name)
				}

				require.Len(t, sum.Labels, 1)

				task0 := tasks[0]
				baseEqual(t, 1, influxdb.Active, task0)
				assert.Equal(t, "15 * * * *", task0.Cron)

				task1 := tasks[1]
				baseEqual(t, 0, influxdb.Inactive, task1)
				assert.Equal(t, (25 * time.Hour).String(), task1.Every)
				assert.Equal(t, (15 * time.Second).String(), task1.Offset)
			})
		})

		t.Run("with params option should be parameterizable", func(t *testing.T) {
			testfileRunner(t, "testdata/tasks_params.yml", func(t *testing.T, template *Template) {
				sum := template.Summary()
				require.Len(t, sum.Tasks, 1)

				actual := sum.Tasks[0]
				assert.Equal(t, KindTask, actual.Kind)
				assert.Equal(t, "task-uuid", actual.MetaName)

				queryText := `option params = {
    bucket: "bar",
    start: -24h0m0s,
    stop: now(),
    name: "max",
    floatVal: 37.2,
    minVal: 10,
}

from(bucket: params.bucket)
    |> range(start: params.start, stop: params.stop)
    |> filter(fn: (r) => r._measurement == "processes")
    |> filter(fn: (r) => r.floater == params.floatVal)
    |> filter(fn: (r) => r._value > params.minVal)
    |> aggregateWindow(every: v.windowPeriod, fn: max)
    |> yield(name: params.name)
`

				assert.Equal(t, queryText, actual.Query)

				expectedRefs := []SummaryReference{
					{
						Field:        "spec.params.bucket",
						EnvRefKey:    `tasks[task-uuid].spec.params.bucket`,
						ValType:      "string",
						DefaultValue: "bar",
					},
					{
						Field:        "spec.params.floatVal",
						EnvRefKey:    `tasks[task-uuid].spec.params.floatVal`,
						ValType:      "float",
						DefaultValue: 37.2,
					},
					{
						Field:        "spec.params.minVal",
						EnvRefKey:    `tasks[task-uuid].spec.params.minVal`,
						ValType:      "integer",
						DefaultValue: int64(10),
					},
					{
						Field:        "spec.params.name",
						EnvRefKey:    `tasks[task-uuid].spec.params.name`,
						ValType:      "string",
						DefaultValue: "max",
					},
					{
						Field:        "spec.params.start",
						EnvRefKey:    `tasks[task-uuid].spec.params.start`,
						ValType:      "duration",
						DefaultValue: "-24h0m0s",
					},
					{
						Field:        "spec.params.stop",
						EnvRefKey:    `tasks[task-uuid].spec.params.stop`,
						ValType:      "time",
						DefaultValue: "now()",
					},
				}
				assert.Equal(t, expectedRefs, actual.EnvReferences)
			})
		})

		t.Run("with task option should be valid", func(t *testing.T) {
			testfileRunner(t, "testdata/task_v2.yml", func(t *testing.T, template *Template) {
				actual := template.Summary().Tasks
				require.Len(t, actual, 1)

				expectedEnvRefs := []SummaryReference{
					{
						Field:        "spec.task.every",
						EnvRefKey:    "tasks[task-1].spec.task.every",
						ValType:      "duration",
						DefaultValue: time.Minute,
					},
					{
						Field:        "spec.name",
						EnvRefKey:    "tasks[task-1].spec.task.name",
						ValType:      "string",
						DefaultValue: "bar",
					},
					{
						Field:        "spec.task.name",
						EnvRefKey:    "tasks[task-1].spec.task.name",
						ValType:      "string",
						DefaultValue: "bar",
					},
					{
						Field:        "spec.task.offset",
						EnvRefKey:    "tasks[task-1].spec.task.offset",
						ValType:      "duration",
						DefaultValue: time.Minute * 3,
					},
				}
				assert.Equal(t, expectedEnvRefs, actual[0].EnvReferences)
			})
		})

		t.Run("with task spec should be valid", func(t *testing.T) {
			testfileRunner(t, "testdata/task_v2_taskSpec.yml", func(t *testing.T, template *Template) {
				actual := template.Summary().Tasks
				require.Len(t, actual, 1)

				expectedEnvRefs := []SummaryReference{
					{
						Field:        "spec.task.every",
						EnvRefKey:    "tasks[task-1].spec.task.every",
						ValType:      "duration",
						DefaultValue: time.Minute,
					},
					{
						Field:        "spec.name",
						EnvRefKey:    "tasks[task-1].spec.task.name",
						ValType:      "string",
						DefaultValue: "foo",
					},
					{
						Field:        "spec.task.name",
						EnvRefKey:    "tasks[task-1].spec.task.name",
						ValType:      "string",
						DefaultValue: "foo",
					},
					{
						Field:        "spec.task.offset",
						EnvRefKey:    "tasks[task-1].spec.task.offset",
						ValType:      "duration",
						DefaultValue: time.Minute,
					},
				}

				queryText := `option task = {name: "foo", every: 1m0s, offset: 1m0s}

from(bucket: "rucket_1")
    |> range(start: -5d, stop: -1h)
    |> filter(fn: (r) => r._measurement == "cpu")
    |> filter(fn: (r) => r._field == "usage_idle")
    |> aggregateWindow(every: 1m, fn: mean)
    |> yield(name: "mean")
`

				assert.Equal(t, queryText, actual[0].Query)

				assert.Equal(t, expectedEnvRefs, actual[0].EnvReferences)
			})
		})

		t.Run("with params option should be valid", func(t *testing.T) {
			testfileRunner(t, "testdata/task_v2_params.yml", func(t *testing.T, template *Template) {
				actual := template.Summary().Tasks
				require.Len(t, actual, 1)

				expectedEnvRefs := []SummaryReference{
					{
						Field:        "spec.params.this",
						EnvRefKey:    "tasks[task-1].spec.params.this",
						ValType:      "string",
						DefaultValue: "foo",
					},
				}

				queryText := `option params = {this: "foo"}

from(bucket: "rucket_1")
    |> range(start: -5d, stop: -1h)
    |> filter(fn: (r) => r._measurement == params.this)
    |> filter(fn: (r) => r._field == "usage_idle")
    |> aggregateWindow(every: 1m, fn: mean)
    |> yield(name: "mean")
`

				assert.Equal(t, queryText, actual[0].Query)

				assert.Equal(t, expectedEnvRefs, actual[0].EnvReferences)
			})
		})

		t.Run("with env refs should be valid", func(t *testing.T) {
			testfileRunner(t, "testdata/task_ref.yml", func(t *testing.T, template *Template) {
				actual := template.Summary().Tasks
				require.Len(t, actual, 1)

				expectedEnvRefs := []SummaryReference{
					{
						Field:     "spec.associations[0].name",
						EnvRefKey: "label-meta-name",
					},
					{
						Field:        "metadata.name",
						EnvRefKey:    "meta-name",
						DefaultValue: "meta",
					},
					{
						Field:        "spec.name",
						EnvRefKey:    "spec-name",
						DefaultValue: "spectacles",
					},
				}
				assert.Equal(t, expectedEnvRefs, actual[0].EnvReferences)
			})
		})

		t.Run("handles bad config", func(t *testing.T) {
			tests := []struct {
				kind   Kind
				resErr testTemplateResourceError
			}{
				{
					kind: KindTask,
					resErr: testTemplateResourceError{
						name:           "missing name",
						validationErrs: 1,
						valFields:      []string{fieldMetadata, fieldName},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Task
metadata:
spec:
  description: desc_1
  cron: 15 * * * *
  query:  >
    from(bucket: "rucket_1") |> yield(name: "mean")
`,
					},
				},
				{
					kind: KindTask,
					resErr: testTemplateResourceError{
						name:           "invalid status",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldStatus},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Task
metadata:
  name: task-0
spec:
  cron: 15 * * * *
  query:  >
    from(bucket: "rucket_1") |> yield(name: "mean")
  status: RANDO WRONGO
`,
					},
				},
				{
					kind: KindTask,
					resErr: testTemplateResourceError{
						name:           "missing query",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldQuery},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Task
metadata:
  name: task-0
spec:
  description: desc_0
  every: 10m
  offset: 15s
`,
					},
				},
				{
					kind: KindTask,
					resErr: testTemplateResourceError{
						name:           "missing every and cron fields",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldEvery, fieldTaskCron},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Task
metadata:
  name: task-0
spec:
  description: desc_0
  offset: 15s
`,
					},
				},
				{
					kind: KindTask,
					resErr: testTemplateResourceError{
						name:           "invalid association",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldAssociations},
						templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Task
metadata:
  name: task-1
spec:
  cron: 15 * * * *
  query:  >
    from(bucket: "rucket_1") |> yield(name: "mean")
  associations:
    - kind: Label
      name: label-1
`,
					},
				},
				{
					kind: KindTask,
					resErr: testTemplateResourceError{
						name:           "duplicate association",
						validationErrs: 1,
						valFields:      []string{fieldSpec, fieldAssociations},
						templateStr: `---
apiVersion: influxdata.com/v2alpha1
kind: Label
metadata:
  name: label-1
---
apiVersion: influxdata.com/v2alpha1
kind: Task
metadata:
  name: task-0
spec:
  every: 10m
  offset: 15s
  query:  >
    from(bucket: "rucket_1") |> yield(name: "mean")
  status: inactive
  associations:
    - kind: Label
      name: label-1
    - kind: Label
      name: label-1
`,
					},
				},
				{
					kind: KindTask,
					resErr: testTemplateResourceError{
						name:           "duplicate meta names",
						validationErrs: 1,
						valFields:      []string{fieldMetadata, fieldName},
						templateStr: `
apiVersion: influxdata.com/v2alpha1
kind: Task
metadata:
  name: task-0
spec:
  every: 10m
  query:  >
    from(bucket: "rucket_1") |> yield(name: "mean")
---
apiVersion: influxdata.com/v2alpha1
kind: Task
metadata:
  name: task-0
spec:
  every: 10m
  query:  >
    from(bucket: "rucket_1") |> yield(name: "mean")
`,
					},
				},
			}

			for _, tt := range tests {
				testTemplateErrors(t, tt.kind, tt.resErr)
			}
		})
	})

	t.Run("template with telegraf config", func(t *testing.T) {
		t.Run("and associated labels should be successful", func(t *testing.T) {
			testfileRunner(t, "testdata/telegraf", func(t *testing.T, template *Template) {
				sum := template.Summary()
				require.Len(t, sum.TelegrafConfigs, 2)

				actual := sum.TelegrafConfigs[0]
				assert.Equal(t, KindTelegraf, actual.Kind)
				assert.Equal(t, "display name", actual.TelegrafConfig.Name)
				assert.Equal(t, "desc", actual.TelegrafConfig.Description)

				require.Len(t, actual.LabelAssociations, 2)
				assert.Equal(t, "label-1", actual.LabelAssociations[0].Name)
				assert.Equal(t, "label-2", actual.LabelAssociations[1].Name)

				actual = sum.TelegrafConfigs[1]
				assert.Equal(t, "tele-2", actual.TelegrafConfig.Name)
				assert.Empty(t, actual.LabelAssociations)

				require.Len(t, sum.LabelMappings, 2)
				expectedMapping := SummaryLabelMapping{
					Status:           StateStatusNew,
					ResourceMetaName: "first-tele-config",
					ResourceName:     "display name",
					LabelMetaName:    "label-1",
					LabelName:        "label-1",
					ResourceType:     influxdb.TelegrafsResourceType,
				}
				assert.Equal(t, expectedMapping, sum.LabelMappings[0])
				expectedMapping.LabelMetaName = "label-2"
				expectedMapping.LabelName = "label-2"
				assert.Equal(t, expectedMapping, sum.LabelMappings[1])
			})
		})

		t.Run("with env refs should be valid", func(t *testing.T) {
			testfileRunner(t, "testdata/telegraf_ref.yml", func(t *testing.T, template *Template) {
				actual := template.Summary().TelegrafConfigs
				require.Len(t, actual, 1)

				expectedEnvRefs := []SummaryReference{
					{
						Field:        "metadata.name",
						EnvRefKey:    "meta-name",
						DefaultValue: "meta",
					},
					{
						Field:        "spec.name",
						EnvRefKey:    "spec-name",
						DefaultValue: "spectacles",
					},
					{
						Field:     "spec.associations[0].name",
						EnvRefKey: "label-meta-name",
					},
				}
				assert.Equal(t, expectedEnvRefs, actual[0].EnvReferences)
			})
		})

		t.Run("handles bad config", func(t *testing.T) {
			tests := []testTemplateResourceError{
				{
					name:           "config missing",
					validationErrs: 1,
					valFields:      []string{fieldSpec, fieldTelegrafConfig},
					templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Telegraf
metadata:
  name: first-tele-config
spec:
`,
				},
				{
					name:           "duplicate metadata names",
					validationErrs: 1,
					valFields:      []string{fieldMetadata, fieldName},
					templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Telegraf
metadata:
  name: tele-0
spec:
  config: fake tele config
---
apiVersion: influxdata.com/v2alpha1
kind: Telegraf
metadata:
  name: tele-0
spec:
  config: fake tele config
`,
				},
			}

			for _, tt := range tests {
				testTemplateErrors(t, KindTelegraf, tt)
			}
		})
	})

	t.Run("template with a variable", func(t *testing.T) {
		t.Run("with valid fields should produce summary", func(t *testing.T) {
			testfileRunner(t, "testdata/variables", func(t *testing.T, template *Template) {
				sum := template.Summary()

				require.Len(t, sum.Variables, 4)
				for _, v := range sum.Variables {
					assert.Equal(t, KindVariable, v.Kind)
				}

				varEquals := func(t *testing.T, name, vType string, vals interface{}, selected []string, v SummaryVariable) {
					t.Helper()

					assert.Equal(t, name, v.Name)
					assert.Equal(t, name+" desc", v.Description)
					if selected == nil {
						selected = []string{}
					}
					assert.Equal(t, selected, v.Selected)
					require.NotNil(t, v.Arguments)
					assert.Equal(t, vType, v.Arguments.Type)
					assert.Equal(t, vals, v.Arguments.Values)
				}

				// validates we support all known variable types
				varEquals(t,
					"var-const-3",
					"constant",
					influxdb.VariableConstantValues([]string{"first val"}),
					nil,
					sum.Variables[0],
				)

				varEquals(t,
					"var-map-4",
					"map",
					influxdb.VariableMapValues{"k1": "v1"},
					nil,
					sum.Variables[1],
				)

				varEquals(t,
					"query var",
					"query",
					influxdb.VariableQueryValues{
						Query:    `buckets()  |> filter(fn: (r) => r.name !~ /^_/)  |> rename(columns: {name: "_value"})  |> keep(columns: ["_value"])`,
						Language: "flux",
					},
					[]string{"rucket"},
					sum.Variables[2],
				)

				varEquals(t,
					"var-query-2",
					"query",
					influxdb.VariableQueryValues{
						Query:    "an influxql query of sorts",
						Language: "influxql",
					},
					nil,
					sum.Variables[3],
				)
			})
		})

		t.Run("with env refs should be valid", func(t *testing.T) {
			testfileRunner(t, "testdata/variable_ref.yml", func(t *testing.T, template *Template) {
				actual := template.Summary().Variables
				require.Len(t, actual, 1)

				expectedEnvRefs := []SummaryReference{
					{
						Field:        "metadata.name",
						EnvRefKey:    "meta-name",
						DefaultValue: "meta",
					},
					{
						Field:        "spec.name",
						EnvRefKey:    "spec-name",
						DefaultValue: "spectacles",
					},
					{
						Field:     "spec.associations[0].name",
						EnvRefKey: "label-meta-name",
					},
					{
						Field:        "spec.selected[0]",
						EnvRefKey:    "the-selected",
						DefaultValue: "second val",
					},
					{
						Field:     "spec.selected[1]",
						EnvRefKey: "the-2nd",
					},
				}
				assert.Equal(t, expectedEnvRefs, actual[0].EnvReferences)
			})
		})

		t.Run("and labels associated", func(t *testing.T) {
			testfileRunner(t, "testdata/variable_associates_label.yml", func(t *testing.T, template *Template) {
				sum := template.Summary()
				require.Len(t, sum.Labels, 1)

				vars := sum.Variables
				require.Len(t, vars, 1)

				expectedLabelMappings := []struct {
					varName string
					labels  []string
				}{
					{
						varName: "var-1",
						labels:  []string{"label-1"},
					},
				}
				for i, expected := range expectedLabelMappings {
					v := vars[i]
					require.Len(t, v.LabelAssociations, len(expected.labels))

					for j, label := range expected.labels {
						assert.Equal(t, label, v.LabelAssociations[j].Name)
					}
				}

				expectedMappings := []SummaryLabelMapping{
					{
						Status:           StateStatusNew,
						ResourceMetaName: "var-1",
						ResourceName:     "var-1",
						LabelMetaName:    "label-1",
						LabelName:        "label-1",
					},
				}

				require.Len(t, sum.LabelMappings, len(expectedMappings))
				for i, expected := range expectedMappings {
					expected.ResourceType = influxdb.VariablesResourceType
					assert.Equal(t, expected, sum.LabelMappings[i])
				}
			})
		})

		t.Run("handles bad config", func(t *testing.T) {
			tests := []testTemplateResourceError{
				{
					name:           "name missing",
					validationErrs: 1,
					valFields:      []string{fieldMetadata, fieldName},
					templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Variable
metadata:
spec:
  description: var-map-4 desc
  type: map
  values:
    k1: v1
`,
				},
				{
					name:           "map var missing values",
					validationErrs: 1,
					valFields:      []string{fieldSpec, fieldValues},
					templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Variable
metadata:
  name:  var-map-4
spec:
  description: var-map-4 desc
  type: map
`,
				},
				{
					name:           "const var missing values",
					validationErrs: 1,
					valFields:      []string{fieldSpec, fieldValues},
					templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Variable
metadata:
  name:  var-const-3
spec:
  description: var-const-3 desc
  type: constant
`,
				},
				{
					name:           "query var missing query",
					validationErrs: 1,
					valFields:      []string{fieldSpec, fieldQuery},
					templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Variable
metadata:
  name:  var-query-2
spec:
  description: var-query-2 desc
  type: query
  language: influxql
`,
				},
				{
					name:           "query var missing query language",
					validationErrs: 1,
					valFields:      []string{fieldSpec, fieldLanguage},
					templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Variable
metadata:
  name:  var-query-2
spec:
  description: var-query-2 desc
  type: query
  query: an influxql query of sorts
`,
				},
				{
					name:           "query var provides incorrect query language",
					validationErrs: 1,
					valFields:      []string{fieldSpec, fieldLanguage},
					templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Variable
metadata:
  name:  var-query-2
spec:
  description: var-query-2 desc
  type: query
  query: an influxql query of sorts
  language: wrong Language
`,
				},
				{
					name:           "duplicate var names",
					validationErrs: 1,
					valFields:      []string{fieldMetadata, fieldName},
					templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Variable
metadata:
  name:  var-query-2
spec:
  description: var-query-2 desc
  type: query
  query: an influxql query of sorts
  language: influxql
---
apiVersion: influxdata.com/v2alpha1
kind: Variable
metadata:
  name:  var-query-2
spec:
  description: var-query-2 desc
  type: query
  query: an influxql query of sorts
  language: influxql
`,
				},
				{
					name:           "duplicate meta name and spec name",
					validationErrs: 1,
					valFields:      []string{fieldSpec, fieldName},
					templateStr: `apiVersion: influxdata.com/v2alpha1
kind: Variable
metadata:
  name:  var-query-2
spec:
  description: var-query-2 desc
  type: query
  query: an influxql query of sorts
  language: influxql
---
apiVersion: influxdata.com/v2alpha1
kind: Variable
metadata:
  name:  valid-query
spec:
  name: var-query-2
  description: var-query-2 desc
  type: query
  query: an influxql query of sorts
  language: influxql
`,
				},
			}

			for _, tt := range tests {
				testTemplateErrors(t, KindVariable, tt)
			}
		})
	})

	t.Run("referencing secrets", func(t *testing.T) {
		hasSecret := func(t *testing.T, refs map[string]bool, key string) {
			t.Helper()
			b, ok := refs[key]
			assert.True(t, ok)
			assert.False(t, b)
		}

		testfileRunner(t, "testdata/notification_endpoint_secrets.yml", func(t *testing.T, template *Template) {
			sum := template.Summary()

			endpoints := sum.NotificationEndpoints
			require.Len(t, endpoints, 1)

			expected := &endpoint.PagerDuty{
				Base: endpoint.Base{
					Name:   "pager-duty-notification-endpoint",
					Status: taskmodel.TaskStatusActive,
				},
				ClientURL:  "http://localhost:8080/orgs/7167eb6719fa34e5/alert-history",
				RoutingKey: influxdb.SecretField{Key: "-routing-key", Value: strPtr("not empty")},
			}
			actual, ok := endpoints[0].NotificationEndpoint.(*endpoint.PagerDuty)
			require.True(t, ok)
			assert.Equal(t, expected.Base.Name, actual.Name)
			require.Nil(t, actual.RoutingKey.Value)
			assert.Equal(t, "routing-key", actual.RoutingKey.Key)

			hasSecret(t, template.mSecrets, "routing-key")
		})
	})

	t.Run("referencing env", func(t *testing.T) {
		hasEnv := func(t *testing.T, refs map[string]bool, key string) {
			t.Helper()
			_, ok := refs[key]
			assert.True(t, ok)
		}

		testfileRunner(t, "testdata/env_refs.yml", func(t *testing.T, template *Template) {
			sum := template.Summary()

			require.Len(t, sum.Buckets, 1)
			assert.Equal(t, "env-bkt-1-name-ref", sum.Buckets[0].Name)
			assert.Len(t, sum.Buckets[0].LabelAssociations, 1)
			hasEnv(t, template.mEnv, "bkt-1-name-ref")

			require.Len(t, sum.Checks, 1)
			assert.Equal(t, "env-check-1-name-ref", sum.Checks[0].Check.GetName())
			assert.Len(t, sum.Checks[0].LabelAssociations, 1)
			hasEnv(t, template.mEnv, "check-1-name-ref")

			require.Len(t, sum.Dashboards, 1)
			assert.Equal(t, "env-dash-1-name-ref", sum.Dashboards[0].Name)
			assert.Len(t, sum.Dashboards[0].LabelAssociations, 1)
			hasEnv(t, template.mEnv, "dash-1-name-ref")

			require.Len(t, sum.NotificationEndpoints, 1)
			assert.Equal(t, "env-endpoint-1-name-ref", sum.NotificationEndpoints[0].NotificationEndpoint.GetName())
			hasEnv(t, template.mEnv, "endpoint-1-name-ref")

			require.Len(t, sum.Labels, 1)
			assert.Equal(t, "env-label-1-name-ref", sum.Labels[0].Name)
			hasEnv(t, template.mEnv, "label-1-name-ref")

			require.Len(t, sum.NotificationRules, 1)
			assert.Equal(t, "env-rule-1-name-ref", sum.NotificationRules[0].Name)
			assert.Equal(t, "env-endpoint-1-name-ref", sum.NotificationRules[0].EndpointMetaName)
			hasEnv(t, template.mEnv, "rule-1-name-ref")

			require.Len(t, sum.Tasks, 1)
			assert.Equal(t, "env-task-1-name-ref", sum.Tasks[0].Name)
			hasEnv(t, template.mEnv, "task-1-name-ref")

			require.Len(t, sum.TelegrafConfigs, 1)
			assert.Equal(t, "env-telegraf-1-name-ref", sum.TelegrafConfigs[0].TelegrafConfig.Name)
			hasEnv(t, template.mEnv, "telegraf-1-name-ref")

			require.Len(t, sum.Variables, 1)
			assert.Equal(t, "env-var-1-name-ref", sum.Variables[0].Name)
			hasEnv(t, template.mEnv, "var-1-name-ref")

			t.Log("applying env vars should populate env fields")
			{
				err := template.applyEnvRefs(map[string]interface{}{
					"bkt-1-name-ref":   "bucket-1",
					"label-1-name-ref": "label-1",
				})
				require.NoError(t, err)

				sum := template.Summary()

				require.Len(t, sum.Buckets, 1)
				assert.Equal(t, "bucket-1", sum.Buckets[0].Name)
				assert.Len(t, sum.Buckets[0].LabelAssociations, 1)
				hasEnv(t, template.mEnv, "bkt-1-name-ref")

				require.Len(t, sum.Labels, 1)
				assert.Equal(t, "label-1", sum.Labels[0].Name)
				hasEnv(t, template.mEnv, "label-1-name-ref")
			}
		})
	})

	t.Run("jsonnet support disabled by default", func(t *testing.T) {
		template := validParsedTemplateFromFile(t, "testdata/bucket_associates_labels.jsonnet", EncodingJsonnet)
		require.Equal(t, &Template{}, template)
	})

	t.Run("jsonnet support", func(t *testing.T) {
		template := validParsedTemplateFromFile(t, "testdata/bucket_associates_labels.jsonnet", EncodingJsonnet, EnableJsonnet())

		sum := template.Summary()

		labels := []SummaryLabel{
			sumLabelGen("label-1", "label-1", "#eee888", "desc_1"),
		}
		assert.Equal(t, labels, sum.Labels)

		bkts := []SummaryBucket{
			{
				SummaryIdentifier: SummaryIdentifier{
					Kind:          KindBucket,
					MetaName:      "rucket-1",
					EnvReferences: []SummaryReference{},
				},
				Name:              "rucket-1",
				Description:       "desc_1",
				RetentionPeriod:   10000 * time.Second,
				LabelAssociations: labels,
			},
			{
				SummaryIdentifier: SummaryIdentifier{
					Kind:          KindBucket,
					MetaName:      "rucket-2",
					EnvReferences: []SummaryReference{},
				},
				Name:              "rucket-2",
				Description:       "desc-2",
				RetentionPeriod:   20000 * time.Second,
				LabelAssociations: labels,
			},
			{
				SummaryIdentifier: SummaryIdentifier{
					Kind:          KindBucket,
					MetaName:      "rucket-3",
					EnvReferences: []SummaryReference{},
				},
				Name:              "rucket-3",
				Description:       "desc_3",
				RetentionPeriod:   30000 * time.Second,
				LabelAssociations: labels,
			},
		}
		assert.Equal(t, bkts, sum.Buckets)
	})
}

func TestCombine(t *testing.T) {
	newTemplateFromYmlStr := func(t *testing.T, templateStr string) *Template {
		t.Helper()
		return newParsedTemplate(t, FromString(templateStr), EncodingYAML, ValidSkipParseError())
	}

	associationsEqual := func(t *testing.T, summaryLabels []SummaryLabel, names ...string) {
		t.Helper()

		require.Len(t, summaryLabels, len(names))

		m := make(map[string]bool)
		for _, n := range names {
			m[n] = true
		}

		for _, l := range summaryLabels {
			if !m[l.Name] {
				assert.Fail(t, "did not find label: "+l.Name)
			}
			delete(m, l.Name)
		}

		if len(m) > 0 {
			var unexpectedLabels []string
			for name := range m {
				unexpectedLabels = append(unexpectedLabels, name)
			}
			assert.Failf(t, "additional labels found", "got: %v", unexpectedLabels)
		}
	}

	t.Run("multiple templates with associations across files", func(t *testing.T) {
		var templates []*Template
		numLabels := 5
		for i := 0; i < numLabels; i++ {
			template := newTemplateFromYmlStr(t, fmt.Sprintf(`
apiVersion: %[1]s
kind: Label
metadata:
  name: label-%d
`, APIVersion, i))
			templates = append(templates, template)
		}

		templates = append(templates, newTemplateFromYmlStr(t, fmt.Sprintf(`
apiVersion: %[1]s
kind: Bucket
metadata:
  name: rucket-1
spec:
  associations:
    - kind: Label
      name: label-1
`, APIVersion)))

		templates = append(templates, newTemplateFromYmlStr(t, fmt.Sprintf(`
apiVersion: %[1]s
kind: Bucket
metadata:
  name: rucket-2
spec:
  associations:
    - kind: Label
      name: label-2
`, APIVersion)))

		templates = append(templates, newTemplateFromYmlStr(t, fmt.Sprintf(`
apiVersion: %[1]s
kind: Bucket
metadata:
  name: rucket-3
spec:
  associations:
    - kind: Label
      name: label-1
    - kind: Label
      name: label-2
`, APIVersion)))

		combinedTemplate, err := Combine(templates)
		require.NoError(t, err)

		sum := combinedTemplate.Summary()

		require.Len(t, sum.Labels, numLabels)
		for i := 0; i < numLabels; i++ {
			assert.Equal(t, fmt.Sprintf("label-%d", i), sum.Labels[i].Name)
		}

		require.Len(t, sum.Labels, numLabels)
		for i := 0; i < numLabels; i++ {
			assert.Equal(t, fmt.Sprintf("label-%d", i), sum.Labels[i].Name)
		}

		require.Len(t, sum.Buckets, 3)
		assert.Equal(t, "rucket-1", sum.Buckets[0].Name)
		associationsEqual(t, sum.Buckets[0].LabelAssociations, "label-1")
		assert.Equal(t, "rucket-2", sum.Buckets[1].Name)
		associationsEqual(t, sum.Buckets[1].LabelAssociations, "label-2")
		assert.Equal(t, "rucket-3", sum.Buckets[2].Name)
		associationsEqual(t, sum.Buckets[2].LabelAssociations, "label-1", "label-2")
	})
}

func Test_normalizeGithubURLToContent(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "raw url passes untouched",
			input:    "https://raw.githubusercontent.com/influxdata/community-templates/master/github/github.yml",
			expected: "https://raw.githubusercontent.com/influxdata/community-templates/master/github/github.yml",
		},
		{
			name:     "URL that is to short is unchanged",
			input:    "https://github.com/influxdata/community-templates",
			expected: "https://github.com/influxdata/community-templates",
		},
		{
			name:     "URL that does not end in required extention is unchanged",
			input:    "https://github.com/influxdata/community-templates/master/github",
			expected: "https://github.com/influxdata/community-templates/master/github",
		},
		{
			name:     "converts base url with ext yaml to raw content url",
			input:    "https://github.com/influxdata/community-templates/blob/master/github/github.yaml",
			expected: "https://raw.githubusercontent.com/influxdata/community-templates/master/github/github.yaml",
		},
		{
			name:     "converts base url with ext yml to raw content url",
			input:    "https://github.com/influxdata/community-templates/blob/master/github/github.yml",
			expected: "https://raw.githubusercontent.com/influxdata/community-templates/master/github/github.yml",
		},
		{
			name:     "converts base url with ext json to raw content url",
			input:    "https://github.com/influxdata/community-templates/blob/master/github/github.json",
			expected: "https://raw.githubusercontent.com/influxdata/community-templates/master/github/github.json",
		},
		{
			name:     "converts base url with ext jsonnet to raw content url",
			input:    "https://github.com/influxdata/community-templates/blob/master/github/github.jsonnet",
			expected: "https://raw.githubusercontent.com/influxdata/community-templates/master/github/github.jsonnet",
		},
		{
			name:     "url with unexpected content type is unchanged 1",
			input:    "https://github.com/influxdata/community-templates/blob/master/github/github.jason",
			expected: "https://github.com/influxdata/community-templates/blob/master/github/github.jason",
		},
		{
			name:     "url with unexpected content type is unchanged 2",
			input:    "https://github.com/influxdata/community-templates/blob/master/github/github.rando",
			expected: "https://github.com/influxdata/community-templates/blob/master/github/github.rando",
		},
	}

	for _, tt := range tests {
		fn := func(t *testing.T) {
			actual := normalizeGithubURLToContent(tt.input)

			assert.Equal(t, tt.expected, actual)
		}

		t.Run(tt.name, fn)
	}
}

func Test_IsParseError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "base case",
			err:      &parseErr{},
			expected: true,
		},
		{
			name: "wrapped by influxdb error",
			err: &errors2.Error{
				Err: &parseErr{},
			},
			expected: true,
		},
		{
			name: "deeply nested in influxdb error",
			err: &errors2.Error{
				Err: &errors2.Error{
					Err: &errors2.Error{
						Err: &errors2.Error{
							Err: &parseErr{},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "influxdb error without nested parse err",
			err: &errors2.Error{
				Err: errors.New("nope"),
			},
			expected: false,
		},
		{
			name:     "plain error",
			err:      errors.New("nope"),
			expected: false,
		},
	}

	for _, tt := range tests {
		fn := func(t *testing.T) {
			isParseErr := IsParseErr(tt.err)
			assert.Equal(t, tt.expected, isParseErr)
		}
		t.Run(tt.name, fn)
	}
}

func Test_TemplateValidationErr(t *testing.T) {
	iPtr := func(i int) *int {
		return &i
	}

	compIntSlcs := func(t *testing.T, expected []int, actuals []*int) {
		t.Helper()

		if len(expected) >= len(actuals) {
			require.FailNow(t, "expected array is larger than actuals")
		}

		for i, actual := range actuals {
			if i == len(expected) {
				assert.Nil(t, actual)
				continue
			}
			assert.Equal(t, expected[i], *actual)
		}
	}

	pErr := &parseErr{
		Resources: []resourceErr{
			{
				Kind: KindDashboard.String(),
				Idx:  intPtr(0),
				ValidationErrs: []validationErr{
					{
						Field: "charts",
						Index: iPtr(1),
						Nested: []validationErr{
							{
								Field: "colors",
								Index: iPtr(0),
								Nested: []validationErr{
									{
										Field: "hex",
										Msg:   "hex value required",
									},
								},
							},
							{
								Field: "kind",
								Msg:   "chart kind must be provided",
							},
						},
					},
				},
			},
		},
	}

	errs := pErr.ValidationErrs()
	require.Len(t, errs, 2)
	assert.Equal(t, KindDashboard.String(), errs[0].Kind)
	assert.Equal(t, []string{"root", "charts", "colors", "hex"}, errs[0].Fields)
	compIntSlcs(t, []int{0, 1, 0}, errs[0].Indexes)
	assert.Equal(t, "hex value required", errs[0].Reason)

	assert.Equal(t, KindDashboard.String(), errs[1].Kind)
	assert.Equal(t, []string{"root", "charts", "kind"}, errs[1].Fields)
	compIntSlcs(t, []int{0, 1}, errs[1].Indexes)
	assert.Equal(t, "chart kind must be provided", errs[1].Reason)
}

func Test_validGeometry(t *testing.T) {
	tests := []struct {
		geom     string
		expected bool
	}{
		{
			geom: "line", expected: true,
		},
		{
			geom: "step", expected: true,
		},
		{
			geom: "stacked", expected: true,
		},
		{
			geom: "monotoneX", expected: true,
		},
		{
			geom: "bar", expected: true,
		},
		{
			geom: "rando", expected: false,
		},
		{
			geom: "not a valid geom", expected: false,
		},
	}

	for _, tt := range tests {
		fn := func(t *testing.T) {
			isValid := len(validGeometry(tt.geom)) == 0
			assert.Equal(t, tt.expected, isValid)
		}

		t.Run(tt.geom, fn)
	}
}

type testTemplateResourceError struct {
	name           string
	encoding       Encoding
	templateStr    string
	resourceErrs   int
	validationErrs int
	valFields      []string
	assErrs        int
	assIdxs        []int
}

// defaults to yaml encoding if encoding not provided
// defaults num resources to 1 if resource errs not provided.
func testTemplateErrors(t *testing.T, k Kind, tt testTemplateResourceError) {
	t.Helper()
	encoding := EncodingYAML
	if tt.encoding != EncodingUnknown {
		encoding = tt.encoding
	}

	resErrs := 1
	if tt.resourceErrs > 0 {
		resErrs = tt.resourceErrs
	}

	fn := func(t *testing.T) {
		t.Helper()

		_, err := Parse(encoding, FromString(tt.templateStr))
		require.Error(t, err)

		require.True(t, IsParseErr(err), err)

		pErr := err.(*parseErr)
		require.Len(t, pErr.Resources, resErrs)

		defer func() {
			if t.Failed() {
				t.Logf("received unexpected err: %s", pErr)
			}
		}()

		resErr := pErr.Resources[0]
		assert.Equal(t, k.String(), resErr.Kind)

		for i, vFail := range resErr.ValidationErrs {
			if len(tt.valFields) == i {
				break
			}
			expectedField := tt.valFields[i]
			findErr(t, expectedField, vFail)
		}

		if tt.assErrs == 0 {
			return
		}

		assFails := pErr.Resources[0].AssociationErrs
		for i, assFail := range assFails {
			if len(tt.valFields) == i {
				break
			}
			expectedField := tt.valFields[i]
			findErr(t, expectedField, assFail)
		}
	}
	t.Run(tt.name, fn)
}

func findErr(t *testing.T, expectedField string, vErr validationErr) validationErr {
	t.Helper()

	fields := strings.Split(expectedField, ".")
	if len(fields) == 1 {
		require.Equal(t, expectedField, vErr.Field)
		return vErr
	}

	currentFieldName, idx := nextField(t, fields[0])
	if idx > -1 {
		require.NotNil(t, vErr.Index)
		require.Equal(t, idx, *vErr.Index)
	}
	require.Equal(t, currentFieldName, vErr.Field)

	next := strings.Join(fields[1:], ".")
	nestedField, _ := nextField(t, next)
	for _, n := range vErr.Nested {
		if n.Field == nestedField {
			return findErr(t, next, n)
		}
	}
	assert.Fail(t, "did not find field: "+expectedField)

	return vErr
}

func nextField(t *testing.T, field string) (string, int) {
	t.Helper()

	fields := strings.Split(field, ".")
	if len(fields) == 1 && !strings.HasSuffix(fields[0], "]") {
		return field, -1
	}
	parts := strings.Split(fields[0], "[")
	if len(parts) == 1 {
		return parts[0], -1
	}
	fieldName := parts[0]

	if strIdx := strings.Index(parts[1], "]"); strIdx > -1 {
		idx, err := strconv.Atoi(parts[1][:strIdx])
		require.NoError(t, err)
		return fieldName, idx
	}
	return "", -1
}

func validParsedTemplateFromFile(t *testing.T, path string, encoding Encoding, opts ...ValidateOptFn) *Template {
	t.Helper()

	var readFn ReaderFn
	templateBytes, ok := availableTemplateFiles[path]
	if ok {
		readFn = FromReader(bytes.NewBuffer(templateBytes), "file://"+path)
	} else {
		readFn = FromFile(path)
		atomic.AddInt64(&missedTemplateCacheCounter, 1)
	}

	opt := &validateOpt{}
	for _, o := range opts {
		o(opt)
	}

	template := newParsedTemplate(t, readFn, encoding, opts...)
	if encoding == EncodingJsonnet && !opt.enableJsonnet {
		require.Equal(t, &Template{}, template)
		return template
	}

	u := url.URL{
		Scheme: "file",
		Path:   path,
	}
	require.Equal(t, []string{u.String()}, template.Sources())
	return template
}

func newParsedTemplate(t *testing.T, fn ReaderFn, encoding Encoding, opts ...ValidateOptFn) *Template {
	t.Helper()

	opt := &validateOpt{}
	for _, o := range opts {
		o(opt)
	}

	template, err := Parse(encoding, fn, opts...)
	if encoding == EncodingJsonnet && !opt.enableJsonnet {
		require.Error(t, err)
		return &Template{}
	}
	require.NoError(t, err)

	for _, k := range template.Objects {
		require.Contains(t, k.APIVersion, "influxdata.com/v2alpha")
	}

	require.True(t, template.isParsed)
	return template
}

func testfileRunner(t *testing.T, path string, testFn func(t *testing.T, template *Template)) {
	t.Helper()

	tests := []struct {
		name      string
		extension string
		encoding  Encoding
	}{
		{
			name:      "yaml",
			extension: ".yml",
			encoding:  EncodingYAML,
		},
		{
			name:      "json",
			extension: ".json",
			encoding:  EncodingJSON,
		},
	}

	ext := filepath.Ext(path)
	switch ext {
	case ".yml":
		tests = tests[:1]
	case ".json":
		tests = tests[1:]
	}

	path = strings.TrimSuffix(path, ext)

	for _, tt := range tests {
		fn := func(t *testing.T) {
			t.Helper()

			template := validParsedTemplateFromFile(t, path+tt.extension, tt.encoding)
			if testFn != nil {
				testFn(t, template)
			}
		}
		t.Run(tt.name, fn)
	}
}

func sumLabelGen(metaName, name, color, desc string, envRefs ...SummaryReference) SummaryLabel {
	if envRefs == nil {
		envRefs = make([]SummaryReference, 0)
	}
	return SummaryLabel{
		SummaryIdentifier: SummaryIdentifier{
			Kind:          KindLabel,
			MetaName:      metaName,
			EnvReferences: envRefs,
		},
		Name: name,
		Properties: struct {
			Color       string `json:"color"`
			Description string `json:"description"`
		}{
			Color:       color,
			Description: desc,
		},
	}
}

func strPtr(s string) *string {
	return &s
}

func mustDuration(t *testing.T, d time.Duration) *notification.Duration {
	t.Helper()
	dur, err := notification.FromTimeDuration(d)
	require.NoError(t, err)
	return &dur
}
