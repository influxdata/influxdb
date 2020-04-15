package influxql

import (
	"time"
)

// Config modifies the behavior of the Transpiler.
type Config struct {
	// Bucket is the name of a bucket to use instead of the db/rp from the query.
	// If bucket is empty then the dbrp mapping is used.
	Bucket                 string
	DefaultDatabase        string
	DefaultRetentionPolicy string
	Cluster                string
	Now                    time.Time
	// FallbackToDBRP if true will use the naming convention of `db/rp`
	// for a bucket name when an mapping is not found
	FallbackToDBRP bool
}
