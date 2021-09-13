package http_test

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/pkg/testttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_API(t *testing.T) {
	t.Run("Decode", func(t *testing.T) {
		t.Run("valid foo no errors", func(t *testing.T) {
			expected := validatFoo{
				Foo: "valid",
				Bar: 10,
			}

			t.Run("json", func(t *testing.T) {
				var api *kithttp.API // shows it is safe to use a nil value

				var out validatFoo
				err := api.DecodeJSON(encodeJSON(t, expected), &out)
				if err != nil {
					t.Fatalf("unexpected err: %v", err)
				}

				if expected != out {
					t.Fatalf("unexpected vals:\n\texpected: %#v\n\tgot: %#v", expected, out)
				}
			})

			t.Run("gob", func(t *testing.T) {
				var out validatFoo
				err := kithttp.NewAPI().DecodeGob(encodeGob(t, expected), &out)
				if err != nil {
					t.Fatalf("unexpected err: %v", err)
				}

				if expected != out {
					t.Fatalf("unexpected vals:\n\texpected: %#v\n\tgot: %#v", expected, out)
				}
			})
		})

		t.Run("unmarshals fine with ok error", func(t *testing.T) {
			badFoo := validatFoo{
				Foo: "",
				Bar: 0,
			}

			t.Run("json", func(t *testing.T) {
				var out validatFoo
				err := kithttp.NewAPI().DecodeJSON(encodeJSON(t, badFoo), &out)
				if err == nil {
					t.Fatal("expected an err")
				}
			})

			t.Run("gob", func(t *testing.T) {
				var out validatFoo
				err := kithttp.NewAPI().DecodeGob(encodeGob(t, badFoo), &out)
				if err == nil {
					t.Fatal("expected an err")
				}
			})
		})

		t.Run("unmarshal error", func(t *testing.T) {
			invalidBody := []byte("[}-{]")

			var out validatFoo
			err := kithttp.NewAPI().DecodeJSON(bytes.NewReader(invalidBody), &out)
			if err == nil {
				t.Fatal("expected an error")
			}
		})

		t.Run("unmarshal err fn wraps unmarshalling error", func(t *testing.T) {
			t.Run("json", func(t *testing.T) {
				invalidBody := []byte("[}-{]")

				api := kithttp.NewAPI(kithttp.WithUnmarshalErrFn(unmarshalErrFn))

				var out validatFoo
				err := api.DecodeJSON(bytes.NewReader(invalidBody), &out)
				expectInfluxdbError(t, errors.EInvalid, err)
			})

			t.Run("gob", func(t *testing.T) {
				invalidBody := []byte("[}-{]")

				api := kithttp.NewAPI(kithttp.WithUnmarshalErrFn(unmarshalErrFn))

				var out validatFoo
				err := api.DecodeGob(bytes.NewReader(invalidBody), &out)
				expectInfluxdbError(t, errors.EInvalid, err)
			})
		})

		t.Run("ok error fn wraps ok error", func(t *testing.T) {
			badFoo := validatFoo{Foo: ""}

			t.Run("json", func(t *testing.T) {
				api := kithttp.NewAPI(kithttp.WithOKErrFn(okErrFn))

				var out validatFoo
				err := api.DecodeJSON(encodeJSON(t, badFoo), &out)
				expectInfluxdbError(t, errors.EUnprocessableEntity, err)
			})

			t.Run("gob", func(t *testing.T) {
				api := kithttp.NewAPI(kithttp.WithOKErrFn(okErrFn))

				var out validatFoo
				err := api.DecodeGob(encodeGob(t, badFoo), &out)
				expectInfluxdbError(t, errors.EUnprocessableEntity, err)
			})
		})
	})

	t.Run("Respond", func(t *testing.T) {
		tests := []int{
			http.StatusCreated,
			http.StatusOK,
			http.StatusNoContent,
			http.StatusForbidden,
			http.StatusInternalServerError,
		}

		for _, statusCode := range tests {
			fn := func(t *testing.T) {
				responder := kithttp.NewAPI()

				svr := func(w http.ResponseWriter, r *http.Request) {
					responder.Respond(w, r, statusCode, map[string]string{
						"foo": "bar",
					})
				}

				expectedBodyFn := func(body *bytes.Buffer) {
					var resp map[string]string
					require.NoError(t, json.NewDecoder(body).Decode(&resp))
					assert.Equal(t, "bar", resp["foo"])
				}
				if statusCode == http.StatusNoContent {
					expectedBodyFn = func(body *bytes.Buffer) {
						require.Zero(t, body.Len())
					}
				}

				testttp.
					Get(t, "/foo").
					Do(http.HandlerFunc(svr)).
					ExpectStatus(statusCode).
					ExpectBody(expectedBodyFn)
			}
			t.Run(http.StatusText(statusCode), fn)
		}
	})

	t.Run("Err", func(t *testing.T) {
		tests := []struct {
			statusCode  int
			expectedErr *errors.Error
		}{
			{
				statusCode: http.StatusBadRequest,
				expectedErr: &errors.Error{
					Code: errors.EInvalid,
					Msg:  "failed to unmarshal",
				},
			},
			{
				statusCode: http.StatusForbidden,
				expectedErr: &errors.Error{
					Code: errors.EForbidden,
					Msg:  "forbidden",
				},
			},
			{
				statusCode: http.StatusUnprocessableEntity,
				expectedErr: &errors.Error{
					Code: errors.EUnprocessableEntity,
					Msg:  "failed validation",
				},
			},
			{
				statusCode: http.StatusInternalServerError,
				expectedErr: &errors.Error{
					Code: errors.EInternal,
					Msg:  "internal error",
				},
			},
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				responder := kithttp.NewAPI()

				svr := func(w http.ResponseWriter, r *http.Request) {
					responder.Err(w, r, tt.expectedErr)
				}

				testttp.
					Get(t, "/foo").
					Do(http.HandlerFunc(svr)).
					ExpectStatus(tt.statusCode).
					ExpectBody(func(body *bytes.Buffer) {
						var err kithttp.ErrBody
						require.NoError(t, json.NewDecoder(body).Decode(&err))
						assert.Equal(t, tt.expectedErr.Msg, err.Msg)
						assert.Equal(t, tt.expectedErr.Code, err.Code)
					})
			}
			t.Run(http.StatusText(tt.statusCode), fn)
		}
	})
}

func expectInfluxdbError(t *testing.T, expectedCode string, err error) {
	t.Helper()

	if err == nil {
		t.Fatal("expected an error")
	}
	iErr, ok := err.(*errors.Error)
	if !ok {
		t.Fatalf("expected an influxdb error; got=%#v", err)
	}

	if got := iErr.Code; expectedCode != got {
		t.Fatalf("unexpected error code; expected=%s  got=%s", expectedCode, got)
	}
}

func encodeGob(t *testing.T, v interface{}) io.Reader {
	t.Helper()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		t.Fatal(err)
	}
	return &buf
}

func encodeJSON(t *testing.T, v interface{}) io.Reader {
	t.Helper()

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		t.Fatal(err)
	}
	return &buf
}

func okErrFn(err error) error {
	return &errors.Error{
		Code: errors.EUnprocessableEntity,
		Msg:  "failed validation",
		Err:  err,
	}
}

func unmarshalErrFn(encoding string, err error) error {
	return &errors.Error{
		Code: errors.EInvalid,
		Msg:  fmt.Sprintf("invalid %s request body", encoding),
		Err:  err,
	}
}

type validatFoo struct {
	Foo string `gob:"foo"`
	Bar int    `gob:"bar"`
}

func (v *validatFoo) OK() error {
	var errs multiErr
	if v.Foo == "" {
		errs = append(errs, "foo must be at least 1 char")
	}
	if v.Bar < 0 {
		errs = append(errs, "bar must be a positive real number")
	}
	return errs.toError()
}

type multiErr []string

func (m multiErr) toError() error {
	if len(m) > 0 {
		return m
	}
	return nil
}

func (m multiErr) Error() string {
	return strings.Join(m, "; ")
}
