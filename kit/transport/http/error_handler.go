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
