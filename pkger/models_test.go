package pkger

import (
	"strconv"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
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
						name:           "name2",
						RetentionRules: retentionRules{newRetentionRule(2 * time.Hour)},
					},
					"buck_1": {
						id:             influxdb.ID(1),
						OrgID:          influxdb.ID(100),
						name:           "name1",
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
						name:        "name2",
						Description: "desc2",
						Color:       "blurple",
					},
					"1": {
						id:          influxdb.ID(1),
						OrgID:       influxdb.ID(100),
						name:        "name1",
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
				id:   influxdb.ID(20),
				name: "b1",
			}
			label1 := &label{
				id:          influxdb.ID(2),
				OrgID:       influxdb.ID(100),
				name:        "name2",
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
}
