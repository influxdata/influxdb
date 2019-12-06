package influxql

import (
	"time"
)

// Config modifies the behavior of the Transpiler.
type Config struct {
	DefaultDatabase        string
	DefaultRetentionPolicy string
	Now                    time.Time
	Cluster                string
	// FallbackToDBRP if true will use the naming convention of `db/rp`
	// for a bucket name when an mapping is not found
	FallbackToDBRP bool
}
