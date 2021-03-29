package testing

import (
	"github.com/influxdata/influxdb/v2/kit/platform"
)

// IDPtr returns a pointer to an influxdb.ID.
func IDPtr(id platform.ID) *platform.ID { return &id }
