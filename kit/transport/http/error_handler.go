package http

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/influxdata/influxdb/v2"
)

// ErrorHandler is the error handler in http package.
type ErrorHandler int

// HandleHTTPError encodes err with the appropriate status code and format,
// sets the X-Platform-Error-Code headers on the response.
// We're no longer using X-Influx-Error and X-Influx-Reference.
// and sets the response status to the corresponding status code.
func (h ErrorHandler) HandleHTTPError(ctx context.Context, err error, w http.ResponseWriter) {
	if err == nil {
		return
	}

	code := influxdb.ErrorCode(err)
	w.Header().Set(PlatformErrorCodeHeader, code)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(ErrorCodeToStatusCode(ctx, code))
	var e struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	e.Code = influxdb.ErrorCode(err)
	if err, ok := err.(*influxdb.Error); ok {
		e.Message = err.Error()
	} else {
		e.Message = "An internal error has occurred"
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
