package testing

import "github.com/influxdata/influxdb"

// IDPtr returns a pointer to an influxdb.ID.
func IDPtr(id influxdb.ID) *influxdb.ID { return &id }
