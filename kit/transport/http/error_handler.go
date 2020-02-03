package http

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/influxdata/influxdb"
)

// PlatformErrorCodeHeader shows the error code of platform error.
const PlatformErrorCodeHeader = "X-Platform-Error-Code"

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
	httpCode, ok := statusCodePlatformError[code]
	if !ok {
		httpCode = http.StatusBadRequest
	}
	w.Header().Set(PlatformErrorCodeHeader, code)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(httpCode)
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

// statusCodePlatformError is the map convert platform.Error to error
var statusCodePlatformError = map[string]int{
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
