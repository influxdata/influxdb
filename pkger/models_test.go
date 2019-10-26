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
						ID:              influxdb.ID(2),
						OrgID:           influxdb.ID(100),
						Description:     "desc2",
						Name:            "name2",
						RetentionPeriod: 2 * time.Hour,
					},
					"buck_1": {
						ID:              influxdb.ID(1),
						OrgID:           influxdb.ID(100),
						Name:            "name1",
						Description:     "desc1",
						RetentionPeriod: time.Hour,
					},
				},
			}

			summary := pkg.Summary()

			require.Len(t, summary.Buckets, len(pkg.mBuckets))
			for i := 1; i <= len(summary.Buckets); i++ {
				buck := summary.Buckets[i-1]
				assert.Equal(t, influxdb.ID(i), buck.ID)
				assert.Equal(t, influxdb.ID(100), buck.OrgID)
				assert.Equal(t, "desc"+strconv.Itoa(i), buck.Description)
				assert.Equal(t, "name"+strconv.Itoa(i), buck.Name)
				assert.Equal(t, time.Duration(i)*time.Hour, buck.RetentionPeriod)
			}
		})

		t.Run("labels returned in asc order by name", func(t *testing.T) {
			pkg := Pkg{
				mLabels: map[string]*label{
					"2": {
						ID:          influxdb.ID(2),
						OrgID:       influxdb.ID(100),
						Name:        "name2",
						Description: "desc2",
						Color:       "blurple",
					},
					"1": {
						ID:          influxdb.ID(1),
						OrgID:       influxdb.ID(100),
						Name:        "name1",
						Description: "desc1",
						Color:       "peru",
					},
				},
			}

			summary := pkg.Summary()

			require.Len(t, summary.Labels, len(pkg.mLabels))
			label1 := summary.Labels[0]
			assert.Equal(t, influxdb.ID(1), label1.ID)
			assert.Equal(t, influxdb.ID(100), label1.OrgID)
			assert.Equal(t, "desc1", label1.Properties["description"])
			assert.Equal(t, "name1", label1.Name)
			assert.Equal(t, "peru", label1.Properties["color"])

			label2 := summary.Labels[1]
			assert.Equal(t, influxdb.ID(2), label2.ID)
			assert.Equal(t, influxdb.ID(100), label2.OrgID)
			assert.Equal(t, "desc2", label2.Properties["description"])
			assert.Equal(t, "name2", label2.Name)
			assert.Equal(t, "blurple", label2.Properties["color"])
		})
	})
}
