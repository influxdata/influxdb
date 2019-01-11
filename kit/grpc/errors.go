package grpc

import (
	"encoding/json"
	"errors"

	platform "github.com/influxdata/influxdb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ToStatus converts a platform.Error to a gRPC status message.
func ToStatus(err *platform.Error) (*status.Status, error) {
	if err == nil {
		return status.New(codes.OK, ""), nil
	}

	c := codes.Unknown
	switch err.Code {
	case platform.EInternal:
		c = codes.Internal
	case platform.ENotFound, platform.EMethodNotAllowed:
		c = codes.NotFound
	case platform.EConflict, platform.EInvalid, platform.EEmptyValue: // not really sure how to express this as gRPC only has Invalid
		c = codes.InvalidArgument
	case platform.EUnavailable:
		c = codes.Unavailable
	}

	buf, jerr := json.Marshal(err)
	if jerr != nil {
		return nil, jerr
	}

	return status.New(c, string(buf)), nil
}

// FromStatus converts a gRPC status message to a platform.Error.
func FromStatus(s *status.Status) *platform.Error {
	if s == nil || s.Code() == codes.OK {
		return nil
	}

	msg := s.Message()
	var perr platform.Error
	if err := json.Unmarshal([]byte(msg), &perr); err != nil {
		return &platform.Error{
			Err:  errors.New(msg),
			Code: platform.EInternal,
		}
	}

	return &perr
}
