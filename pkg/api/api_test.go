package api_test

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/pkg/api"
)

func Test_API(t *testing.T) {
	t.Run("valid foo no errors", func(t *testing.T) {
		expected := validatFoo{
			Foo: "valid",
			Bar: 10,
		}

		t.Run("json", func(t *testing.T) {
			var api *api.API // shows it is safe to use a nil value

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
			err := api.New().DecodeGob(encodeGob(t, expected), &out)
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
			err := api.New().DecodeJSON(encodeJSON(t, badFoo), &out)
			if err == nil {
				t.Fatal("expected an err")
			}
		})

		t.Run("gob", func(t *testing.T) {
			var out validatFoo
			err := api.New().DecodeGob(encodeGob(t, badFoo), &out)
			if err == nil {
				t.Fatal("expected an err")
			}
		})
	})

	t.Run("unmarshal error", func(t *testing.T) {
		invalidBody := []byte("[}-{]")

		var out validatFoo
		err := api.New().DecodeJSON(bytes.NewReader(invalidBody), &out)
		if err == nil {
			t.Fatal("expected an error")
		}
	})

	t.Run("unmarshal err fn wraps unmarshaling error", func(t *testing.T) {
		t.Run("json", func(t *testing.T) {
			invalidBody := []byte("[}-{]")

			api := api.New(api.WithUnmarshalErrFn(unmarshalErrFn))

			var out validatFoo
			err := api.DecodeJSON(bytes.NewReader(invalidBody), &out)
			expectInfluxdbError(t, influxdb.EInvalid, err)
		})

		t.Run("gob", func(t *testing.T) {
			invalidBody := []byte("[}-{]")

			api := api.New(api.WithUnmarshalErrFn(unmarshalErrFn))

			var out validatFoo
			err := api.DecodeGob(bytes.NewReader(invalidBody), &out)
			expectInfluxdbError(t, influxdb.EInvalid, err)
		})
	})

	t.Run("ok error fn wraps ok error", func(t *testing.T) {
		badFoo := validatFoo{Foo: ""}

		t.Run("json", func(t *testing.T) {
			api := api.New(api.WithOKErrFn(okErrFn))

			var out validatFoo
			err := api.DecodeJSON(encodeJSON(t, badFoo), &out)
			expectInfluxdbError(t, influxdb.EUnprocessableEntity, err)
		})

		t.Run("gob", func(t *testing.T) {
			api := api.New(api.WithOKErrFn(okErrFn))

			var out validatFoo
			err := api.DecodeGob(encodeGob(t, badFoo), &out)
			expectInfluxdbError(t, influxdb.EUnprocessableEntity, err)
		})
	})
}

func expectInfluxdbError(t *testing.T, expectedCode string, err error) {
	t.Helper()

	if err == nil {
		t.Fatal("expected an error")
	}

	iErr, ok := err.(*influxdb.Error)
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
	return &influxdb.Error{
		Code: influxdb.EUnprocessableEntity,
		Msg:  "failed validation",
		Err:  err,
	}
}

func unmarshalErrFn(encoding string, err error) error {
	return &influxdb.Error{
		Code: influxdb.EInvalid,
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
		errs = append(errs, "bar must be a postive real number")
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
