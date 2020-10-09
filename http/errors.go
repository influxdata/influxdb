package http

import (
	"bytes"
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	platform "github.com/influxdata/influxdb/v2"
	khttp "github.com/influxdata/influxdb/v2/kit/transport/http"
)

// AuthzError is returned for authorization errors. When this error type is returned,
// the user can be presented with a generic "authorization failed" error, but
// the system can log the underlying AuthzError() so that operators have insight
// into what actually failed with authorization.
type AuthzError interface {
	error
	AuthzError() error
}

// RetriableError is an error that contains a duration to wait before the next retry.
type RetriableError interface {
	error
	// RetryAfter returns a non-negative duration for the next retry
	RetryAfter() time.Duration
}

// retriableErrorImpl is an implementation of RetriableError
type retriableErrorImpl struct {
	error      error
	retryAfter time.Duration
}

// Error prints out the nested error
func (e *retriableErrorImpl) Error() string {
	if e.error == nil {
		return ""
	}
	return e.error.Error()
}

// RetryAfter returns a non-negative duration for the next retry
func (e *retriableErrorImpl) RetryAfter() time.Duration {
	return e.retryAfter
}

// NewRetriableError wraps existing error to create RetriableError
func NewRetriableError(err error, retryAfter time.Duration) RetriableError {
	if retryAfter < 0 {
		retryAfter = 0
	}
	return &retriableErrorImpl{
		error:      err,
		retryAfter: retryAfter,
	}
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

	perr := &platform.Error{
		Code: khttp.StatusCodeToErrorCode(resp.StatusCode),
	}

	if resp.StatusCode == http.StatusUnsupportedMediaType {
		perr.Msg = fmt.Sprintf("invalid media type: %q", resp.Header.Get("Content-Type"))
		return perr
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		// Assume JSON if there is no content-type.
		contentType = "application/json"
	}
	mediatype, _, _ := mime.ParseMediaType(contentType)

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, resp.Body); err != nil {
		perr.Msg = "failed to read error response"
		perr.Err = err
		return perr
	}

	switch mediatype {
	case "application/json":
		if err := json.Unmarshal(buf.Bytes(), perr); err != nil {
			perr.Msg = fmt.Sprintf("attempted to unmarshal error as JSON but failed: %q", err)
			perr.Err = firstLineAsError(buf)
		}
	default:
		perr.Err = firstLineAsError(buf)
	}

	if perr.Code == "" {
		// given it was unset during attempt to unmarshal as JSON
		perr.Code = khttp.StatusCodeToErrorCode(resp.StatusCode)
	}

	if retryAfter := resp.Header.Get("Retry-After"); len(retryAfter) > 0 {
		if seconds, err := strconv.Atoi(retryAfter); err == nil {
			perr.Err = NewRetriableError(perr.Err, time.Duration(seconds)*time.Second)
		}
		if exactTime, err := http.ParseTime(retryAfter); err == nil {
			perr.Err = NewRetriableError(perr.Err, time.Until(exactTime))
		}
	}

	return perr
}

func firstLineAsError(buf bytes.Buffer) error {
	line, _ := buf.ReadString('\n')
	return stderrors.New(strings.TrimSuffix(line, "\n"))
}

// UnauthorizedError encodes a error message and status code for unauthorized access.
func UnauthorizedError(ctx context.Context, h platform.HTTPErrorHandler, w http.ResponseWriter) {
	h.HandleHTTPError(ctx, &platform.Error{
		Code: platform.EUnauthorized,
		Msg:  "unauthorized access",
	}, w)
}

// InactiveUserError encode a error message and status code for inactive users.
func InactiveUserError(ctx context.Context, h platform.HTTPErrorHandler, w http.ResponseWriter) {
	h.HandleHTTPError(ctx, &platform.Error{
		Code: platform.EForbidden,
		Msg:  "User is inactive",
	}, w)
}

// RetryAfter returns duration for an HTTP request to be retried. A zero value value indicates
// that retry is not possible, a negative value indicates that this information is not known.
// Use with an error that is returned by CheckedError function.
func RetryAfter(err error) time.Duration {
	if pErr, ok := err.(*platform.Error); ok {
		return RetryAfter(pErr.Err)
	}
	if retryErr, ok := err.(RetriableError); ok {
		return retryErr.RetryAfter()
	}
	return -1
}
