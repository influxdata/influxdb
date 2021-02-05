package kv

import (
	"fmt"

	"github.com/influxdata/influxdb/v2"
)

// UnexpectedIndexError is used when the error comes from an internal system.
func UnexpectedIndexError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  fmt.Sprintf("unexpected error retrieving index; Err: %v", err),
		Op:   "kv/index",
	}
}

// NotUniqueError is used when attempting to create a resource that already
// exists.
var NotUniqueError = &influxdb.Error{
	Code: influxdb.EConflict,
	Msg:  "name already exists",
}
