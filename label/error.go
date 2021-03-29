package label

import (
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

var (
	// NotUniqueIDError occurs when attempting to create a Label with an ID that already belongs to another one
	NotUniqueIDError = &errors.Error{
		Code: errors.EConflict,
		Msg:  "ID already exists",
	}

	// ErrFailureGeneratingID occurs ony when the random number generator
	// cannot generate an ID in MaxIDGenerationN times.
	ErrFailureGeneratingID = &errors.Error{
		Code: errors.EInternal,
		Msg:  "unable to generate valid id",
	}

	// ErrLabelNotFound occurs when a label cannot be found by its ID
	ErrLabelNotFound = &errors.Error{
		Code: errors.ENotFound,
		Msg:  "label not found",
	}
)

// ErrInternalServiceError is used when the error comes from an internal system.
func ErrInternalServiceError(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EInternal,
		Err:  err,
	}
}
