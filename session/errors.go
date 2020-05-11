package session

import (
	"github.com/influxdata/influxdb/v2"
)

var (
	// ErrUnauthorized when a session request is unauthorized
	// usually due to password missmatch
	ErrUnauthorized = &influxdb.Error{
		Code: influxdb.EUnauthorized,
		Msg:  "unauthorized access",
	}
)
