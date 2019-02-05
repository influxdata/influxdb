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
}
