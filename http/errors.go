package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	platform "github.com/influxdata/influxdb"
)

const (
	// PlatformErrorCodeHeader shows the error code of platform error.
	PlatformErrorCodeHeader = "X-Platform-Error-Code"
)

// AuthzError is returned for authorization errors. When this error type is returned,
// the user can be presented with a generic "authorization failed" error, but
// the system can log the underlying AuthzError() so that operators have insight
// into what actually failed with authorization.
type AuthzError interface {
	error
	AuthzError() error
}

// CheckErrorStatus for status and any error in the response.
func CheckErrorStatus(code int, res *http.Response) error {
	err := CheckError(res)
	if err != nil {
		return err
	}

	if res.StatusCode != code {
		return fmt.Errorf("unexpected status code: %s", res.Status)
	}

	return nil
}

// CheckError reads the http.Response and returns an error if one exists.
// It will automatically recognize the errors returned by Influx services
// and decode the error into an internal error type. If the error cannot
// be determined in that way, it will create a generic error message.
//
// If there is no error, then this returns nil.
func CheckError(resp *http.Response) (err error) {
	switch resp.StatusCode / 100 {
	case 4, 5:
		// We will attempt to parse this error outside of this block.
	case 2:
		return nil
	default:
		// TODO(jsternberg): Figure out what to do here?
		return &platform.Error{
			Code: platform.EInternal,
			Msg:  fmt.Sprintf("unexpected status code: %d %s", resp.StatusCode, resp.Status),
		}
	}
	pe := new(platform.Error)
	parseErr := json.NewDecoder(resp.Body).Decode(pe)
	if parseErr != nil {
		return parseErr
	}
	return pe
}

// EncodeError encodes err with the appropriate status code and format,
// sets the X-Platform-Error-Code headers on the response.
// We're no longer using X-Influx-Error and X-Influx-Reference.
// and sets the response status to the corresponding status code.
func EncodeError(ctx context.Context, err error, w http.ResponseWriter) {
	if err == nil {
		return
	}

	code := platform.ErrorCode(err)
	httpCode, ok := statusCodePlatformError[code]
	if !ok {
		httpCode = http.StatusBadRequest
	}
	w.Header().Set(PlatformErrorCodeHeader, code)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(httpCode)
	var e error
	if pe, ok := err.(*platform.Error); ok {
		e = &platform.Error{
			Code: code,
			Op:   platform.ErrorOp(err),
			Msg:  platform.ErrorMessage(err),
			Err:  pe.Err,
		}
	} else {
		e = &platform.Error{
			Code: platform.EInternal,
			Err:  err,
		}
	}
	b, _ := json.Marshal(e)
	_, _ = w.Write(b)
}

// UnauthorizedError encodes a error message and status code for unauthorized access.
func UnauthorizedError(ctx context.Context, w http.ResponseWriter) {
	EncodeError(ctx, &platform.Error{
		Code: platform.EUnauthorized,
		Msg:  "unauthorized access",
	}, w)
}

// statusCodePlatformError is the map convert platform.Error to error
var statusCodePlatformError = map[string]int{
	platform.EInternal:            http.StatusInternalServerError,
	platform.EInvalid:             http.StatusBadRequest,
	platform.EUnprocessableEntity: http.StatusUnprocessableEntity,
	platform.EEmptyValue:          http.StatusBadRequest,
	platform.EConflict:            http.StatusUnprocessableEntity,
	platform.ENotFound:            http.StatusNotFound,
	platform.EUnavailable:         http.StatusServiceUnavailable,
	platform.EForbidden:           http.StatusForbidden,
	platform.EUnauthorized:        http.StatusUnauthorized,
	platform.EMethodNotAllowed:    http.StatusMethodNotAllowed,
}
