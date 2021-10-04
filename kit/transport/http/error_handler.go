package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/influxdata/influxdb/v2"
)

// ErrorHandler is the error handler in http package.
type ErrorHandler struct {
	logger *zap.Logger
}

func NewErrorHandler(logger *zap.Logger) ErrorHandler {
	return ErrorHandler{logger: logger}
}

// HandleHTTPError encodes err with the appropriate status code and format,
// sets the X-Platform-Error-Code headers on the response.
// We're no longer using X-Influx-Error and X-Influx-Reference.
// and sets the response status to the corresponding status code.
func (h ErrorHandler) HandleHTTPError(ctx context.Context, err error, w http.ResponseWriter) {
	if err == nil {
		return
	}

	code := influxdb.ErrorCode(err)
	var msg string
	if _, ok := err.(*influxdb.Error); ok {
		msg = err.Error()
	} else {
		msg = "An internal error has occurred"
		h.logger.Warn("internal error not returned to client", zap.Error(err))
	}

	WriteErrorResponse(ctx, w, code, msg)
}

func WriteErrorResponse(ctx context.Context, w http.ResponseWriter, code string, msg string) {
	w.Header().Set(PlatformErrorCodeHeader, code)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(ErrorCodeToStatusCode(ctx, code))
	e := struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}{
		Code:    code,
		Message: msg,
	}
	b, _ := json.Marshal(e)
	_, _ = w.Write(b)
}

// StatusCodeToErrorCode maps a http status code integer to an
// influxdb error code string.
func StatusCodeToErrorCode(statusCode int) string {
	errorCode, ok := httpStatusCodeToInfluxDBError[statusCode]
	if ok {
		return errorCode
	}

	return influxdb.EInternal
}

// ErrorCodeToStatusCode maps an influxdb error code string to a
// http status code integer.
func ErrorCodeToStatusCode(ctx context.Context, code string) int {
	// If the client disconnects early or times out then return a different
	// error than the passed in error code. Client timeouts return a 408
	// while disconnections return a non-standard Nginx HTTP 499 code.
	if err := ctx.Err(); err == context.DeadlineExceeded {
		return http.StatusRequestTimeout
	} else if err == context.Canceled {
		return 499 // https://httpstatuses.com/499
	}

	// Otherwise map internal error codes to HTTP status codes.
	statusCode, ok := influxDBErrorToStatusCode[code]
	if ok {
		return statusCode
	}
	return http.StatusInternalServerError
}

// influxDBErrorToStatusCode is a mapping of ErrorCode to http status code.
var influxDBErrorToStatusCode = map[string]int{
	influxdb.EInternal:            http.StatusInternalServerError,
	influxdb.ENotImplemented:      http.StatusNotImplemented,
	influxdb.EInvalid:             http.StatusBadRequest,
	influxdb.EUnprocessableEntity: http.StatusUnprocessableEntity,
	influxdb.EEmptyValue:          http.StatusBadRequest,
	influxdb.EConflict:            http.StatusUnprocessableEntity,
	influxdb.ENotFound:            http.StatusNotFound,
	influxdb.EUnavailable:         http.StatusServiceUnavailable,
	influxdb.EForbidden:           http.StatusForbidden,
	influxdb.ETooManyRequests:     http.StatusTooManyRequests,
	influxdb.EUnauthorized:        http.StatusUnauthorized,
	influxdb.EMethodNotAllowed:    http.StatusMethodNotAllowed,
	influxdb.ETooLarge:            http.StatusRequestEntityTooLarge,
}

var httpStatusCodeToInfluxDBError = map[int]string{}

func init() {
	for k, v := range influxDBErrorToStatusCode {
		httpStatusCodeToInfluxDBError[v] = k
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
		return &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  fmt.Sprintf("unexpected status code: %d %s", resp.StatusCode, resp.Status),
		}
	}

	perr := &influxdb.Error{
		Code: StatusCodeToErrorCode(resp.StatusCode),
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
		perr.Code = StatusCodeToErrorCode(resp.StatusCode)
	}

	return perr
}
func firstLineAsError(buf bytes.Buffer) error {
	line, _ := buf.ReadString('\n')
	return errors.New(strings.TrimSuffix(line, "\n"))
}
