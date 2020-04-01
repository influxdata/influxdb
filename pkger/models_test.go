package pkger

import (
	"strconv"
	"testing"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPkg(t *testing.T) {
	t.Run("Summary", func(t *testing.T) {
		t.Run("buckets returned in asc order by name", func(t *testing.T) {
			pkg := Pkg{
				mBuckets: map[string]*bucket{
					"buck_2": {
						id:             influxdb.ID(2),
						OrgID:          influxdb.ID(100),
						Description:    "desc2",
						identity:       identity{name: &references{val: "name2"}},
						RetentionRules: retentionRules{newRetentionRule(2 * time.Hour)},
					},
					"buck_1": {
						id:             influxdb.ID(1),
						OrgID:          influxdb.ID(100),
						identity:       identity{name: &references{val: "name1"}},
						Description:    "desc1",
						RetentionRules: retentionRules{newRetentionRule(time.Hour)},
					},
				},
			}

			summary := pkg.Summary()

			require.Len(t, summary.Buckets, len(pkg.mBuckets))
			for i := 1; i <= len(summary.Buckets); i++ {
				buck := summary.Buckets[i-1]
				assert.Equal(t, SafeID(i), buck.ID)
				assert.Equal(t, SafeID(100), buck.OrgID)
				assert.Equal(t, "desc"+strconv.Itoa(i), buck.Description)
				assert.Equal(t, "name"+strconv.Itoa(i), buck.Name)
				assert.Equal(t, time.Duration(i)*time.Hour, buck.RetentionPeriod)
			}
		})

		t.Run("labels returned in asc order by name", func(t *testing.T) {
			pkg := Pkg{
				mLabels: map[string]*label{
					"2": {
						id:          influxdb.ID(2),
						OrgID:       influxdb.ID(100),
						identity:    identity{name: &references{val: "name2"}},
						Description: "desc2",
						Color:       "blurple",
					},
					"1": {
						id:          influxdb.ID(1),
						OrgID:       influxdb.ID(100),
						identity:    identity{name: &references{val: "name1"}},
						Description: "desc1",
						Color:       "peru",
					},
				},
			}

			summary := pkg.Summary()

			require.Len(t, summary.Labels, len(pkg.mLabels))
			label1 := summary.Labels[0]
			assert.Equal(t, SafeID(1), label1.ID)
			assert.Equal(t, SafeID(100), label1.OrgID)
			assert.Equal(t, "name1", label1.Name)
			assert.Equal(t, "desc1", label1.Properties.Description)
			assert.Equal(t, "peru", label1.Properties.Color)

			label2 := summary.Labels[1]
			assert.Equal(t, SafeID(2), label2.ID)
			assert.Equal(t, SafeID(100), label2.OrgID)
			assert.Equal(t, "desc2", label2.Properties.Description)
			assert.Equal(t, "name2", label2.Name)
			assert.Equal(t, "blurple", label2.Properties.Color)
		})

		t.Run("label mappings returned in asc order by name", func(t *testing.T) {
			bucket1 := &bucket{
				id:       influxdb.ID(20),
				identity: identity{name: &references{val: "b1"}},
			}
			label1 := &label{
				id:          influxdb.ID(2),
				OrgID:       influxdb.ID(100),
				identity:    identity{name: &references{val: "name2"}},
				Description: "desc2",
				Color:       "blurple",
				associationMapping: associationMapping{
					mappings: map[assocMapKey][]assocMapVal{
						assocMapKey{
							resType: influxdb.BucketsResourceType,
							name:    bucket1.Name(),
						}: {{
							v: bucket1,
						}},
					},
				},
			}
			bucket1.labels = append(bucket1.labels, label1)

			pkg := Pkg{
				mBuckets: map[string]*bucket{bucket1.Name(): bucket1},
				mLabels:  map[string]*label{label1.Name(): label1},
			}

			summary := pkg.Summary()

			require.Len(t, summary.LabelMappings, 1)
			mapping1 := summary.LabelMappings[0]
			assert.Equal(t, SafeID(bucket1.id), mapping1.ResourceID)
			assert.Equal(t, bucket1.Name(), mapping1.ResourceName)
			assert.Equal(t, influxdb.BucketsResourceType, mapping1.ResourceType)
			assert.Equal(t, SafeID(label1.id), mapping1.LabelID)
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
						PkgName: "new bucket",
						New: DiffBucketValues{
							Description: "new desc",
						},
					},
					expected: false,
				},
				{
					name: "existing bucket with no changes",
					resource: DiffBucket{
						ID:      3,
						PkgName: "new bucket",
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
						ID:      3,
						PkgName: "existing bucket",
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
						ID:      3,
						PkgName: "existing bucket",
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
						ID:      3,
						PkgName: "existing bucket",
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
						ID:      3,
						PkgName: "existing bucket",
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
						Name: "new label",
						New: DiffLabelValues{
							Color:       "new color",
							Description: "new desc",
						},
					},
					expected: false,
				},
				{
					name: "existing label with no changes",
					resource: DiffLabel{
						ID:   1,
						Name: "existing label",
						New: DiffLabelValues{
							Color:       "color",
							Description: "desc",
						},
						Old: &DiffLabelValues{
							Color:       "color",
							Description: "desc",
						},
					},
					expected: false,
				},
				{
					name: "existing label with changes",
					resource: DiffLabel{
						ID:   1,
						Name: "existing label",
						New: DiffLabelValues{
							Color:       "color",
							Description: "desc",
						},
						Old: &DiffLabelValues{
							Color:       "new color",
							Description: "new desc",
						},
					},
					expected: true,
				},
				{
					name: "new variable",
					resource: DiffVariable{
						Name: "new var",
						New: DiffVariableValues{
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
						ID:   2,
						Name: "new var",
						New: DiffVariableValues{
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
								Values: &influxdb.VariableConstantValues{"1", "b"},
							},
						},
					},
					expected: false,
				},
				{
					name: "existing variable with desc changes",
					resource: DiffVariable{
						ID:   3,
						Name: "new var",
						New: DiffVariableValues{
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
						ID:   3,
						Name: "new var",
						New: DiffVariableValues{
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
						ID:   3,
						Name: "new var",
						New: DiffVariableValues{
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
						ID:   3,
						Name: "new var",
						New: DiffVariableValues{
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
				pkgFile:   "testdata/bucket.yml",
				kind:      KindBucket,
				validName: "rucket_11",
			},
			{
				pkgFile:   "testdata/checks.yml",
				kind:      KindCheck,
				validName: "check_0",
			},
			{
				pkgFile:   "testdata/dashboard.yml",
				kind:      KindDashboard,
				validName: "dash_1",
			},
			{
				pkgFile:   "testdata/label.yml",
				kind:      KindLabel,
				validName: "label_1",
			},
			{
				pkgFile:   "testdata/notification_endpoint.yml",
				kind:      KindNotificationEndpoint,
				validName: "slack_notification_endpoint",
			},
			{
				pkgFile:   "testdata/notification_rule.yml",
				kind:      KindNotificationRule,
				validName: "rule_UUID",
			},
			{
				pkgFile:   "testdata/tasks.yml",
				kind:      KindTask,
				validName: "task_UUID",
			},
			{
				pkgFile:   "testdata/telegraf.yml",
				kind:      KindTelegraf,
				validName: "first_tele_config",
			},
			{
				pkgFile:   "testdata/variables.yml",
				kind:      KindVariable,
				validName: "var_query_1",
			},
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				testfileRunner(t, tt.pkgFile, func(t *testing.T, pkg *Pkg) {
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
