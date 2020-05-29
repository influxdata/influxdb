package testing

import "github.com/influxdata/influxdb/v2"

// IDPtr returns a pointer to an influxdb.ID.
func IDPtr(id influxdb.ID) *influxdb.ID { return &id }
