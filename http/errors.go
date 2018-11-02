package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/influxdata/platform"
	kerrors "github.com/influxdata/platform/kit/errors"
	"github.com/pkg/errors"
)

const (
	// ErrorHeader is the standard location for influx errors to be reported.
	ErrorHeader = "X-Influx-Error"
	// ReferenceHeader is the header for the reference error reference code.
	ReferenceHeader = "X-Influx-Reference"

	errorHeaderMaxLength = 256
)

// ErrNotFound is returned when a request for a resource returns no results.
var ErrNotFound = errors.New("no results found")

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
	if err := CheckError(res); err != nil {
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
// Add an optional isPlatformError, to do decode with platform.Error
func CheckError(resp *http.Response, isPlatformError ...bool) (err error) {
	switch resp.StatusCode / 100 {
	case 4, 5:
		// We will attempt to parse this error outside of this block.
	case 2:
		return nil
	default:
		// TODO(jsternberg): Figure out what to do here?
		return kerrors.InternalErrorf("unexpected status code: %d %s", resp.StatusCode, resp.Status)
	}
	if len(isPlatformError) > 0 && isPlatformError[0] {
		pe := new(platform.Error)
		parseErr := json.NewDecoder(resp.Body).Decode(pe)
		if parseErr != nil {
			return parseErr
		}
		err = pe
		return err
	}
	// Attempt to read the X-Influx-Error header with the message.
	if errMsg := resp.Header.Get(ErrorHeader); errMsg != "" {
		// Parse the reference number as an integer. If we cannot parse it,
		// return the error message by itself.
		ref, err := strconv.Atoi(resp.Header.Get(ReferenceHeader))
		if err != nil {
			// We cannot parse the reference number so just use internal.
			ref = kerrors.InternalError
		}
		return &kerrors.Error{
			Reference: ref,
			Code:      resp.StatusCode,
			Err:       errMsg,
		}
	}

	// There is no influx error so we need to report that we have some kind
	// of error from somewhere.
	// TODO(jsternberg): Try to make this more advance by reading the response
	// and either decoding a possible json message or just reading the text itself.
	// This might be good enough though.
	msg := "unknown server error"
	if resp.StatusCode/100 == 4 {
		msg = "client error"
	}
	return errors.Wrap(errors.New(resp.Status), msg)
}

// EncodeError encodes err with the appropriate status code and format,
// sets the X-Influx-Error and X-Influx-Reference headers on the response,
// and sets the response status to the corresponding status code.
func EncodeError(ctx context.Context, err error, w http.ResponseWriter) {
	if err == nil {
		return
	}

	if pe, ok := err.(*platform.Error); ok {
		httpCode, ok := statusCodePlatformError[platform.ErrorCode(pe)]
		if !ok {
			httpCode = http.StatusBadRequest
		}
		w.WriteHeader(httpCode)
		b, _ := json.Marshal(pe)
		_, _ = w.Write(b)
		return
	}
	e, ok := err.(kerrors.Error)
	if !ok {
		e = kerrors.Error{
			Reference: kerrors.InternalError,
			Err:       err.Error(),
		}
	}
	encodeKError(e, w)
}

func encodeKError(e kerrors.Error, w http.ResponseWriter) {
	if e.Reference == 0 {
		e.Reference = kerrors.InternalError
	}
	if len(e.Err) > errorHeaderMaxLength {
		e.Err = e.Err[0:errorHeaderMaxLength]
	}
	code := statusCode(e)

	w.Header().Set(ErrorHeader, e.Err)
	w.Header().Set(ReferenceHeader, strconv.Itoa(e.Reference))
	w.WriteHeader(code)
}

// ForbiddenError encodes error with a forbidden status code.
func ForbiddenError(ctx context.Context, err error, w http.ResponseWriter) {
	EncodeError(ctx, kerrors.Forbiddenf(err.Error()), w)
}

// statusCode returns the http status code for an error.
func statusCode(e kerrors.Error) int {
	if e.Code > 0 {
		return e.Code
	}
	switch e.Reference {
	case kerrors.InternalError:
		return http.StatusInternalServerError
	case kerrors.InvalidData:
		return http.StatusUnprocessableEntity
	case kerrors.MalformedData:
		return http.StatusBadRequest
	case kerrors.Forbidden:
		return http.StatusForbidden
	case kerrors.NotFound:
		return http.StatusNotFound
	default:
		return http.StatusInternalServerError
	}
}

// statusCodePlatformError is the map convert platform.Error to error
var statusCodePlatformError = map[string]int{
	platform.EInternal:   http.StatusInternalServerError,
	platform.EInvalid:    http.StatusBadRequest,
	platform.EEmptyValue: http.StatusBadRequest,
	platform.EConflict:   http.StatusUnprocessableEntity,
	platform.ENotFound:   http.StatusNotFound,
}
