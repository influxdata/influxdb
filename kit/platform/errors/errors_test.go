package errors_test

import (
	"encoding/json"
	"errors"
	"fmt"
	errors2 "github.com/influxdata/influxdb/v2/kit/platform/errors"
	"testing"
)

const EFailedToGetStorageHost = "failed to get the storage host"

func TestErrorMsg(t *testing.T) {
	cases := []struct {
		name string
		err  error
		msg  string
	}{
		{
			name: "simple error",
			err:  &errors2.Error{Code: errors2.ENotFound},
			msg:  "<not found>",
		},
		{
			name: "with message",
			err: &errors2.Error{
				Code: errors2.ENotFound,
				Msg:  "bucket not found",
			},
			msg: "bucket not found",
		},
		{
			name: "with a third party error and no message",
			err: &errors2.Error{
				Code: EFailedToGetStorageHost,
				Err:  errors.New("empty value"),
			},
			msg: "empty value",
		},
		{
			name: "with a third party error and a message",
			err: &errors2.Error{
				Code: EFailedToGetStorageHost,
				Msg:  "failed to get storage hosts",
				Err:  errors.New("empty value"),
			},
			msg: "failed to get storage hosts: empty value",
		},
		{
			name: "with an internal error and no message",
			err: &errors2.Error{
				Code: EFailedToGetStorageHost,
				Err: &errors2.Error{
					Code: errors2.EEmptyValue,
					Msg:  "empty value",
				},
			},
			msg: "empty value",
		},
		{
			name: "with an internal error and a message",
			err: &errors2.Error{
				Code: EFailedToGetStorageHost,
				Msg:  "failed to get storage hosts",
				Err: &errors2.Error{
					Code: errors2.EEmptyValue,
					Msg:  "empty value",
				},
			},
			msg: "failed to get storage hosts: empty value",
		},
	}
	for _, c := range cases {
		if c.msg != c.err.Error() {
			t.Errorf("%s failed, want %s, got %s", c.name, c.msg, c.err.Error())
		}
	}
}

func TestErrorMessage(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "nil error",
		},
		{
			name: "nil error of type *platform.Error",
			err:  (*errors2.Error)(nil),
		},
		{
			name: "simple error",
			err:  &errors2.Error{Msg: "simple error"},
			want: "simple error",
		},
		{
			name: "embedded error",
			err:  &errors2.Error{Err: &errors2.Error{Msg: "embedded error"}},
			want: "embedded error",
		},
		{
			name: "default error",
			err:  errors.New("s"),
			want: "An internal error has occurred.",
		},
	}
	for _, c := range cases {
		if result := errors2.ErrorMessage(c.err); c.want != result {
			t.Errorf("%s failed, want %s, got %s", c.name, c.want, result)
		}
	}
}

func TestErrorOp(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "nil error",
		},
		{
			name: "nil error of type *platform.Error",
			err:  (*errors2.Error)(nil),
		},
		{
			name: "simple error",
			err:  &errors2.Error{Op: "op1"},
			want: "op1",
		},
		{
			name: "embedded error",
			err:  &errors2.Error{Op: "op1", Err: &errors2.Error{Code: errors2.EInvalid}},
			want: "op1",
		},
		{
			name: "embedded error without op in root level",
			err:  &errors2.Error{Err: &errors2.Error{Code: errors2.EInvalid, Op: "op2"}},
			want: "op2",
		},
		{
			name: "default error",
			err:  errors.New("s"),
			want: "",
		},
	}
	for _, c := range cases {
		if result := errors2.ErrorOp(c.err); c.want != result {
			t.Errorf("%s failed, want %s, got %s", c.name, c.want, result)
		}
	}
}
func TestErrorCode(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "nil error",
		},
		{
			name: "nil error of type *platform.Error",
			err:  (*errors2.Error)(nil),
		},
		{
			name: "simple error",
			err:  &errors2.Error{Code: errors2.ENotFound},
			want: errors2.ENotFound,
		},
		{
			name: "embedded error",
			err:  &errors2.Error{Code: errors2.ENotFound, Err: &errors2.Error{Code: errors2.EInvalid}},
			want: errors2.ENotFound,
		},
		{
			name: "embedded error with root level code",
			err:  &errors2.Error{Err: &errors2.Error{Code: errors2.EInvalid}},
			want: errors2.EInvalid,
		},
		{
			name: "default error",
			err:  errors.New("s"),
			want: errors2.EInternal,
		},
	}
	for _, c := range cases {
		if result := errors2.ErrorCode(c.err); c.want != result {
			t.Errorf("%s failed, want %s, got %s", c.name, c.want, result)
		}
	}
}

func TestJSON(t *testing.T) {
	cases := []struct {
		name    string
		err     *errors2.Error
		encoded string
	}{
		{
			name:    "simple error",
			err:     &errors2.Error{Code: errors2.ENotFound},
			encoded: `{"code":"not found"}`,
		},
		{
			name: "with op",
			err: &errors2.Error{
				Code: errors2.ENotFound,
				Op:   "bolt.FindAuthorizationByID",
			},
			encoded: `{"code":"not found","op":"bolt.FindAuthorizationByID"}`,
		},
		{
			name: "with op and value",
			err: &errors2.Error{
				Code: errors2.ENotFound,
				Op:   "bolt/FindAuthorizationByID",
				Msg:  fmt.Sprintf("with ID %d", 323),
			},
			encoded: `{"code":"not found","message":"with ID 323","op":"bolt/FindAuthorizationByID"}`,
		},
		{
			name: "with a third party error",
			err: &errors2.Error{
				Code: EFailedToGetStorageHost,
				Op:   "cmd/fluxd.injectDeps",
				Err:  errors.New("empty value"),
			},
			encoded: `{"code":"failed to get the storage host","op":"cmd/fluxd.injectDeps","error":"empty value"}`,
		},
		{
			name: "with a internal error",
			err: &errors2.Error{
				Code: EFailedToGetStorageHost,
				Op:   "cmd/fluxd.injectDeps",
				Err:  &errors2.Error{Code: errors2.EEmptyValue, Op: "cmd/fluxd.getStrList"},
			},
			encoded: `{"code":"failed to get the storage host","op":"cmd/fluxd.injectDeps","error":{"code":"empty value","op":"cmd/fluxd.getStrList"}}`,
		},
		{
			name: "with a deep internal error",
			err: &errors2.Error{
				Code: EFailedToGetStorageHost,
				Op:   "cmd/fluxd.injectDeps",
				Err: &errors2.Error{
					Code: errors2.EInvalid,
					Op:   "cmd/fluxd.getStrList",
					Err: &errors2.Error{
						Code: errors2.EEmptyValue,
						Err:  errors.New("an err"),
					},
				},
			},
			encoded: `{"code":"failed to get the storage host","op":"cmd/fluxd.injectDeps","error":{"code":"invalid","op":"cmd/fluxd.getStrList","error":{"code":"empty value","error":"an err"}}}`,
		},
	}
	for _, c := range cases {
		result, err := json.Marshal(c.err)
		// encode testing
		if err != nil {
			t.Errorf("%s encode failed, want err: %v, should be nil", c.name, err)
		}

		if string(result) != c.encoded {
			t.Errorf("%s encode failed, want result: %s, got %s", c.name, c.encoded, string(result))
		}
		// decode testing
		got := new(errors2.Error)
		err = json.Unmarshal(result, got)
		if err != nil {
			t.Errorf("%s decode failed, want err: %v, should be nil", c.name, err)
		}
		decodeEqual(t, c.err, got, "decode: "+c.name)
	}
}

func decodeEqual(t *testing.T, want, result *errors2.Error, caseName string) {
	if want.Code != result.Code {
		t.Errorf("%s code failed, want %s, got %s", caseName, want.Code, result.Code)
	}
	if want.Op != result.Op {
		t.Errorf("%s op failed, want %s, got %s", caseName, want.Op, result.Op)
	}
	if want.Msg != result.Msg {
		t.Errorf("%s msg failed, want %s, got %s", caseName, want.Msg, result.Msg)
	}
	if want.Err != nil {
		if _, ok := want.Err.(*errors2.Error); ok {
			decodeEqual(t, want.Err.(*errors2.Error), result.Err.(*errors2.Error), caseName)
		} else {
			if want.Err.Error() != result.Err.Error() {
				t.Errorf("%s Err failed, want %s, got %s", caseName, want.Err.Error(), result.Err.Error())
			}
		}
	}
}
