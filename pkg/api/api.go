package api

import (
	"compress/gzip"
	"encoding/gob"
	"encoding/json"
	"io"
	"net/http"

	"go.uber.org/zap"
)

type oker interface {
	OK() error
}

type APIOptFn func(*API)

func WithLog(logger *zap.Logger) APIOptFn {
	return func(api *API) {
		api.logger = logger
	}
}

func WithErrFn(fn func(err error) (interface{}, int, error)) APIOptFn {
	return func(api *API) {
		api.errFn = fn
	}
}

func WithOKErrFn(fn func(err error) error) APIOptFn {
	return func(api *API) {
		api.okErrFn = fn
	}
}

func WithPrettyJSON(b bool) APIOptFn {
	return func(api *API) {
		api.prettyJSON = b
	}
}

func WithEncodeGZIP() APIOptFn {
	return func(api *API) {
		api.encodeGZIP = true
	}
}

func WithUnmarshalErrFn(fn func(encoding string, err error) error) APIOptFn {
	return func(api *API) {
		api.unmarshalErrFn = fn
	}
}

type API struct {
	logger *zap.Logger

	prettyJSON bool
	encodeGZIP bool

	unmarshalErrFn func(encoding string, err error) error
	okErrFn        func(err error) error
	errFn          func(err error) (interface{}, int, error)
}

func New(opts ...APIOptFn) *API {
	api := API{
		logger:     zap.NewNop(),
		prettyJSON: true,
		errFn: func(err error) (interface{}, int, error) {
			return ErrBody{
				Code: "internal error",
				Msg:  err.Error(),
			}, http.StatusInternalServerError, nil
		},
	}
	for _, o := range opts {
		o(&api)
	}
	return &api
}

func (a *API) DecodeJSON(r io.Reader, v interface{}) error {
	return a.decode("json", json.NewDecoder(r), v)
}

func (a *API) DecodeGob(r io.Reader, v interface{}) error {
	return a.decode("gob", gob.NewDecoder(r), v)
}

type decoder interface {
	Decode(interface{}) error
}

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

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(writer)
	if a != nil && a.prettyJSON {
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

func (a *API) Err(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}

	v, status, err := a.errFn(err)
	if err != nil {
		a.logger.Error("failed to write err to response writer", zap.Error(err))
		a.Respond(w, http.StatusInternalServerError, ErrBody{
			Code: "internal error",
			Msg:  "an unexpected error occured",
		})
		return
	}
	a.logger.Error("api error encountered", zap.Error(err))
	a.Respond(w, status, v)
}

type noopCloser struct {
	io.Writer
}

func (n noopCloser) Close() error {
	return nil
}

type ErrBody struct {
	Code string `json:"code"`
	Msg  string `json:"message"`
}
