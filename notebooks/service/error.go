package service

import (
	"fmt"

	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

var (
	ErrOrgIDRequired  = fieldRequiredError("OrgID")
	ErrNameRequired   = fieldRequiredError("Name")
	ErrSpecRequired   = fieldRequiredError("Spec")
	ErrOffsetNegative = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "offset cannot be negative",
	}
	ErrLimitLTEZero = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "limit cannot be less-than or equal-to zero",
	}
)

func fieldRequiredError(field string) error {
	return &errors.Error{
		Code: errors.EInvalid,
		Msg:  fmt.Sprintf("%s required", field),
	}
}
