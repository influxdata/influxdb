package pkger

import (
	"strconv"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTemplate(t *testing.T) {
	t.Run("Summary", func(t *testing.T) {
		t.Run("buckets returned in asc order by name", func(t *testing.T) {
			pkg := Template{
				mBuckets: map[string]*bucket{
					"buck_2": {
						Description:    "desc2",
						identity:       identity{name: &references{val: "metaName2"}, displayName: &references{val: "name2"}},
						RetentionRules: retentionRules{newRetentionRule(2 * time.Hour)},
					},
					"buck_1": {
						identity:       identity{name: &references{val: "metaName1"}, displayName: &references{val: "name1"}},
						Description:    "desc1",
						RetentionRules: retentionRules{newRetentionRule(time.Hour)},
					},
				},
			}

			summary := pkg.Summary()
			require.Len(t, summary.Buckets, len(pkg.mBuckets))
			for i := 1; i <= len(summary.Buckets); i++ {
				buck := summary.Buckets[i-1]
				assert.Zero(t, buck.ID)
				assert.Zero(t, buck.OrgID)
				assert.Equal(t, "desc"+strconv.Itoa(i), buck.Description)
				assert.Equal(t, "metaName"+strconv.Itoa(i), buck.MetaName)
				assert.Equal(t, "name"+strconv.Itoa(i), buck.Name)
				assert.Equal(t, time.Duration(i)*time.Hour, buck.RetentionPeriod)
			}
		})

		t.Run("labels returned in asc order by name", func(t *testing.T) {
			pkg := Template{
				mLabels: map[string]*label{
					"2": {
						identity:    identity{name: &references{val: "pkgName2"}, displayName: &references{val: "name2"}},
						Description: "desc2",
						Color:       "blurple",
					},
					"1": {
						identity:    identity{name: &references{val: "pkgName1"}, displayName: &references{val: "name1"}},
						Description: "desc1",
						Color:       "peru",
					},
				},
			}

			summary := pkg.Summary()

			require.Len(t, summary.Labels, len(pkg.mLabels))
			label1 := summary.Labels[0]
			assert.Equal(t, "pkgName1", label1.MetaName)
			assert.Equal(t, "name1", label1.Name)
			assert.Equal(t, "desc1", label1.Properties.Description)
			assert.Equal(t, "peru", label1.Properties.Color)

			label2 := summary.Labels[1]
			assert.Equal(t, "pkgName2", label2.MetaName)
			assert.Equal(t, "name2", label2.Name)
			assert.Equal(t, "desc2", label2.Properties.Description)
			assert.Equal(t, "blurple", label2.Properties.Color)
		})

		t.Run("label mappings returned in asc order by name", func(t *testing.T) {
			bucket1 := &bucket{
				identity: identity{name: &references{val: "pkgBucket1"}, displayName: &references{val: "bd1"}},
			}
			label1 := &label{
				identity:    identity{name: &references{val: "pkgLabel2"}, displayName: &references{val: "name2"}},
				Description: "desc2",
				Color:       "blurple",
				associationMapping: associationMapping{
					mappings: map[assocMapKey][]assocMapVal{
						{
							resType: influxdb.BucketsResourceType,
							name:    bucket1.Name(),
						}: {{
							v: bucket1,
						}},
					},
				},
			}
			bucket1.labels = append(bucket1.labels, label1)

			pkg := Template{
				mBuckets: map[string]*bucket{bucket1.MetaName(): bucket1},
				mLabels:  map[string]*label{label1.MetaName(): label1},
			}

			summary := pkg.Summary()

			require.Len(t, summary.LabelMappings, 1)
			mapping1 := summary.LabelMappings[0]
			assert.Equal(t, bucket1.MetaName(), mapping1.ResourceMetaName)
			assert.Equal(t, bucket1.Name(), mapping1.ResourceName)
			assert.Equal(t, influxdb.BucketsResourceType, mapping1.ResourceType)
			assert.Equal(t, label1.MetaName(), mapping1.LabelMetaName)
			assert.Equal(t, label1.Name(), mapping1.LabelName)
		})
	})

	t.Run("Diff", func(t *testing.T) {
		t.Run("hasConflict", func(t *testing.T) {
			tests := []struct {
				name     string
				resource interface {
					hasConflict() bool
				}
				expected bool
			}{
				{
					name: "new bucket",
					resource: DiffBucket{
						DiffIdentifier: DiffIdentifier{
							MetaName: "new bucket",
						},
						New: DiffBucketValues{
							Description: "new desc",
						},
					},
					expected: false,
				},
				{
					name: "existing bucket with no changes",
					resource: DiffBucket{
						DiffIdentifier: DiffIdentifier{
							ID:       3,
							MetaName: "new bucket",
						},
						New: DiffBucketValues{
							Description: "new desc",
							RetentionRules: retentionRules{{
								Type:    "expire",
								Seconds: 3600,
							}},
						},
						Old: &DiffBucketValues{
							Description: "new desc",
							RetentionRules: retentionRules{{
								Type:    "expire",
								Seconds: 3600,
							}},
						},
					},
					expected: false,
				},
				{
					name: "existing bucket with desc changes",
					resource: DiffBucket{
						DiffIdentifier: DiffIdentifier{
							ID:       3,
							MetaName: "existing bucket",
						},
						New: DiffBucketValues{
							Description: "new desc",
							RetentionRules: retentionRules{{
								Type:    "expire",
								Seconds: 3600,
							}},
						},
						Old: &DiffBucketValues{
							Description: "newer desc",
							RetentionRules: retentionRules{{
								Type:    "expire",
								Seconds: 3600,
							}},
						},
					},
					expected: true,
				},
				{
					name: "existing bucket with retention changes",
					resource: DiffBucket{
						DiffIdentifier: DiffIdentifier{
							ID:       3,
							MetaName: "existing bucket",
						},
						New: DiffBucketValues{
							Description: "new desc",
							RetentionRules: retentionRules{{
								Type:    "expire",
								Seconds: 3600,
							}},
						},
						Old: &DiffBucketValues{
							Description: "new desc",
						},
					},
					expected: true,
				},
				{
					name: "existing bucket with retention changes",
					resource: DiffBucket{
						DiffIdentifier: DiffIdentifier{
							ID:       3,
							MetaName: "existing bucket",
						},
						New: DiffBucketValues{
							Description: "new desc",
							RetentionRules: retentionRules{{
								Type:    "expire",
								Seconds: 3600,
							}},
						},
						Old: &DiffBucketValues{
							Description: "new desc",
							RetentionRules: retentionRules{{
								Type:    "expire",
								Seconds: 360,
							}},
						},
					},
					expected: true,
				},
				{
					name: "existing bucket with retention changes",
					resource: DiffBucket{
						DiffIdentifier: DiffIdentifier{
							ID:       3,
							MetaName: "existing bucket",
						},
						New: DiffBucketValues{
							Description: "new desc",
							RetentionRules: retentionRules{{
								Type:    "expire",
								Seconds: 3600,
							}},
						},
						Old: &DiffBucketValues{
							Description: "new desc",
							RetentionRules: retentionRules{
								{
									Type:    "expire",
									Seconds: 360,
								},
								{
									Type:    "expire",
									Seconds: 36000,
								},
							},
						},
					},
					expected: true,
				},
				{
					name: "new label",
					resource: DiffLabel{
						DiffIdentifier: DiffIdentifier{
							MetaName: "new label",
						},
						New: DiffLabelValues{
							Name:        "new label",
							Color:       "new color",
							Description: "new desc",
						},
					},
					expected: false,
				},
				{
					name: "existing label with no changes",
					resource: DiffLabel{
						DiffIdentifier: DiffIdentifier{
							ID:       1,
							MetaName: "existing label",
						},
						New: DiffLabelValues{
							Name:        "existing label",
							Color:       "color",
							Description: "desc",
						},
						Old: &DiffLabelValues{
							Name:        "existing label",
							Color:       "color",
							Description: "desc",
						},
					},
					expected: false,
				},
				{
					name: "existing label with changes",
					resource: DiffLabel{
						DiffIdentifier: DiffIdentifier{
							ID:       1,
							MetaName: "existing label",
						},
						New: DiffLabelValues{
							Name:        "existing label",
							Color:       "color",
							Description: "desc",
						},
						Old: &DiffLabelValues{
							Name:        "existing label",
							Color:       "new color",
							Description: "new desc",
						},
					},
					expected: true,
				},
				{
					name: "new variable",
					resource: DiffVariable{
						DiffIdentifier: DiffIdentifier{
							MetaName: "new var",
						},
						New: DiffVariableValues{
							Name:        "new var",
							Description: "new desc",
							Args: &influxdb.VariableArguments{
								Type:   "constant",
								Values: &influxdb.VariableConstantValues{"1", "b"},
							},
						},
					},
					expected: false,
				},
				{
					name: "existing variable no changes",
					resource: DiffVariable{
						DiffIdentifier: DiffIdentifier{
							ID:       2,
							MetaName: "new var",
						},
						New: DiffVariableValues{
							Name:        "new var",
							Description: "new desc",
							Args: &influxdb.VariableArguments{
								Type:   "constant",
								Values: &influxdb.VariableConstantValues{"1", "b"},
							},
						},
						Old: &DiffVariableValues{
							Name:        "new var",
							Description: "new desc",
							Args: &influxdb.VariableArguments{
								Type:   "constant",
								Values: &influxdb.VariableConstantValues{"1", "b"},
							},
						},
					},
					expected: false,
				},
				{
					name: "existing variable with desc changes",
					resource: DiffVariable{
						DiffIdentifier: DiffIdentifier{
							ID:       3,
							MetaName: "new var",
						},
						New: DiffVariableValues{
							Name:        "new var",
							Description: "new desc",
							Args: &influxdb.VariableArguments{
								Type:   "constant",
								Values: &influxdb.VariableConstantValues{"1", "b"},
							},
						},
						Old: &DiffVariableValues{
							Description: "newer desc",
							Args: &influxdb.VariableArguments{
								Type:   "constant",
								Values: &influxdb.VariableConstantValues{"1", "b"},
							},
						},
					},
					expected: true,
				},
				{
					name: "existing variable with constant arg changes",
					resource: DiffVariable{
						DiffIdentifier: DiffIdentifier{
							ID:       3,
							MetaName: "new var",
						},
						New: DiffVariableValues{
							Name:        "new var",
							Description: "new desc",
							Args: &influxdb.VariableArguments{
								Type:   "constant",
								Values: &influxdb.VariableConstantValues{"1", "b"},
							},
						},
						Old: &DiffVariableValues{
							Description: "new desc",
							Args: &influxdb.VariableArguments{
								Type:   "constant",
								Values: &influxdb.VariableConstantValues{"1", "b", "new"},
							},
						},
					},
					expected: true,
				},
				{
					name: "existing variable with map arg changes",
					resource: DiffVariable{
						DiffIdentifier: DiffIdentifier{
							ID:       3,
							MetaName: "new var",
						},
						New: DiffVariableValues{
							Name:        "new var",
							Description: "new desc",
							Args: &influxdb.VariableArguments{
								Type:   "map",
								Values: &influxdb.VariableMapValues{"1": "b"},
							},
						},
						Old: &DiffVariableValues{
							Description: "new desc",
							Args: &influxdb.VariableArguments{
								Type:   "map",
								Values: &influxdb.VariableMapValues{"1": "b", "2": "new"},
							},
						},
					},
					expected: true,
				},
				{
					name: "existing variable with query arg changes",
					resource: DiffVariable{
						DiffIdentifier: DiffIdentifier{
							ID:       3,
							MetaName: "new var",
						},
						New: DiffVariableValues{
							Name:        "new var",
							Description: "new desc",
							Args: &influxdb.VariableArguments{
								Type: "query",
								Values: &influxdb.VariableQueryValues{
									Query:    "from(bucket: rucket)",
									Language: "flux",
								},
							},
						},
						Old: &DiffVariableValues{
							Name:        "new var",
							Description: "new desc",
							Args: &influxdb.VariableArguments{
								Type: "query",
								Values: &influxdb.VariableQueryValues{
									Query:    "from(bucket: rucket) |> yield(name: threeve)",
									Language: "flux",
								},
							},
						},
					},
					expected: true,
				},
			}

			for _, tt := range tests {
				fn := func(t *testing.T) {
					assert.Equal(t, tt.expected, tt.resource.hasConflict())
				}

				t.Run(tt.name, fn)
			}
		})
	})

	t.Run("Contains", func(t *testing.T) {
		tests := []struct {
			pkgFile   string
			kind      Kind
			validName string
		}{
			{
				pkgFile:   "testdata/label.yml",
				kind:      KindLabel,
				validName: "label-1",
			},
			{
				pkgFile:   "testdata/notification_rule.yml",
				kind:      KindNotificationRule,
				validName: "rule-uuid",
			},
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				testfileRunner(t, tt.pkgFile, func(t *testing.T, pkg *Template) {
					contained := pkg.Contains(tt.kind, tt.validName)
					assert.True(t, contained)

					contained = pkg.Contains(tt.kind, "RANdo Name_ not found anywhere")
					assert.False(t, contained)
				})
			}
			t.Run(tt.kind.String(), fn)
		}
	})
}
