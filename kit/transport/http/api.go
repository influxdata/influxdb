package http

import (
	"compress/gzip"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/influxdata/influxdb/v2"
	"go.uber.org/zap"
)

// PlatformErrorCodeHeader shows the error code of platform error.
const PlatformErrorCodeHeader = "X-Platform-Error-Code"

// API provides a consolidated means for handling API interface concerns.
// Concerns such as decoding/encoding request and response bodies as well
// as adding headers for content type and content encoding.
type API struct {
	logger *zap.Logger

	prettyJSON bool
	encodeGZIP bool

	unmarshalErrFn func(encoding string, err error) error
	okErrFn        func(err error) error
	errFn          func(err error) (interface{}, int, error)
}

// APIOptFn is a functional option for setting fields on the API type.
type APIOptFn func(*API)

// WithLog sets the logger.
func WithLog(logger *zap.Logger) APIOptFn {
	return func(api *API) {
		api.logger = logger
	}
}

// WithErrFn sets the err handling func for issues when writing to the response body.
func WithErrFn(fn func(err error) (interface{}, int, error)) APIOptFn {
	return func(api *API) {
		api.errFn = fn
	}
}

// WithOKErrFn is an error handler for failing validation for request bodies.
func WithOKErrFn(fn func(err error) error) APIOptFn {
	return func(api *API) {
		api.okErrFn = fn
	}
}

// WithPrettyJSON sets the json encoder to marshal indent or not.
func WithPrettyJSON(b bool) APIOptFn {
	return func(api *API) {
		api.prettyJSON = b
	}
}

// WithEncodeGZIP sets the encoder to gzip contents.
func WithEncodeGZIP() APIOptFn {
	return func(api *API) {
		api.encodeGZIP = true
	}
}

// WithUnmarshalErrFn sets the error handler for errors that occur when unmarshaling
// the request body.
func WithUnmarshalErrFn(fn func(encoding string, err error) error) APIOptFn {
	return func(api *API) {
		api.unmarshalErrFn = fn
	}
}

// NewAPI creates a new API type.
func NewAPI(opts ...APIOptFn) *API {
	api := API{
		logger:     zap.NewNop(),
		prettyJSON: true,
		unmarshalErrFn: func(encoding string, err error) error {
			return &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  fmt.Sprintf("failed to unmarshal %s: %s", encoding, err),
			}
		},
		errFn: func(err error) (interface{}, int, error) {
			code := influxdb.ErrorCode(err)
			httpStatusCode, ok := statusCodePlatformError[code]
			if !ok {
				httpStatusCode = http.StatusBadRequest
			}
			msg := err.Error()
			if msg == "" {
				msg = "an internal error has occurred"
			}
			return ErrBody{
				Code: code,
				Msg:  msg,
			}, httpStatusCode, nil
		},
	}
	for _, o := range opts {
		o(&api)
	}
	return &api
}

// DecodeJSON decodes reader with json.
func (a *API) DecodeJSON(r io.Reader, v interface{}) error {
	return a.decode("json", json.NewDecoder(r), v)
}

// DecodeGob decodes reader with gob.
func (a *API) DecodeGob(r io.Reader, v interface{}) error {
	return a.decode("gob", gob.NewDecoder(r), v)
}

type (
	decoder interface {
		Decode(interface{}) error
	}

	oker interface {
		OK() error
	}
)

func (a *API) decode(encoding string, dec decoder, v interface{}) error {
	if err := dec.Decode(v); err != nil {
		if a != nil && a.unmarshalErrFn != nil {
			return a.unmarshalErrFn(encoding, err)
		}
		return err
	}

	if vv, ok := v.(oker); ok {
		err := vv.OK()
		if a != nil && a.okErrFn != nil {
			return a.okErrFn(err)
		}
		return err
	}

	return nil
}

// Respond writes to the response writer, handling all errors in writing.
func (a *API) Respond(w http.ResponseWriter, status int, v interface{}) {
	if status == http.StatusNoContent {
		w.WriteHeader(status)
		return
	}

	var writer io.WriteCloser = noopCloser{Writer: w}
	// we'll double close to make sure its always closed even
	//on issues before the write
	defer writer.Close()

	if a != nil && a.encodeGZIP {
		w.Header().Set("Content-Encoding", "gzip")
		writer = gzip.NewWriter(w)
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(writer)
	if a == nil || a.prettyJSON {
		enc.SetIndent("", "\t")
	}

	w.WriteHeader(status)
	if err := enc.Encode(v); err != nil {
		a.Err(w, err)
		return
	}

	if err := writer.Close(); err != nil {
		a.Err(w, err)
		return
	}
}

// Err is used for writing an error to the response.
func (a *API) Err(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}

	a.logErr("api error encountered", zap.Error(err))

	v, status, err := a.errFn(err)
	if err != nil {
		a.logErr("failed to write err to response writer", zap.Error(err))
		a.Respond(w, http.StatusInternalServerError, ErrBody{
			Code: "internal error",
			Msg:  "an unexpected error occured",
		})
		return
	}

	if eb, ok := v.(ErrBody); ok {
		w.Header().Set(PlatformErrorCodeHeader, eb.Code)
	}

	a.Respond(w, status, v)
}

func (a *API) logErr(msg string, fields ...zap.Field) {
	if a == nil || a.logger == nil {
		return
	}
	a.logger.Error(msg, fields...)
}

type noopCloser struct {
	io.Writer
}

func (n noopCloser) Close() error {
	return nil
}

// ErrBody is an err response body.
type ErrBody struct {
	Code string `json:"code"`
	Msg  string `json:"message"`
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
