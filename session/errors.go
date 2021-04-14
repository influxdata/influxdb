package session

import (
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

var (
	// ErrUnauthorized when a session request is unauthorized
	// usually due to password mismatch
	ErrUnauthorized = &errors.Error{
		Code: errors.EUnauthorized,
		Msg:  "unauthorized access",
	}
)
