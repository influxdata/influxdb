package api

import (
	"encoding/gob"
	"encoding/json"
	"io"
)

type (
	oker interface {
		OK() error
	}

	decoder interface {
		Decode(interface{}) error
	}
)

type APIOptFn func(*API)

func WithUnmarshalErrFn(fn func(encoding string, err error) error) APIOptFn {
	return func(api *API) {
		api.unmarshalErrFn = fn
	}
}

func WithOKErrFn(fn func(err error) error) APIOptFn {
	return func(api *API) {
		api.okErrFn = fn
	}
}

type API struct {
	unmarshalErrFn func(encoding string, err error) error
	okErrFn        func(err error) error
}

func New(opts ...APIOptFn) *API {
	var api API
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
