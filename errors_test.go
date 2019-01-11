package influxdb_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	platform "github.com/influxdata/influxdb"
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
			err:  &platform.Error{Code: platform.ENotFound},
			msg:  "<not found>",
		},
		{
			name: "with op",
			err: &platform.Error{
				Code: platform.ENotFound,
				Op:   "bolt.FindAuthorizationByID",
			},
			msg: "bolt.FindAuthorizationByID: <not found>",
		},
		{
			name: "with op and value",
			err: &platform.Error{
				Code: platform.ENotFound,
				Op:   "bolt.FindAuthorizationByID",
				Msg:  fmt.Sprintf("with ID %d", 323),
			},
			msg: "bolt.FindAuthorizationByID: <not found> with ID 323",
		},
		{
			name: "with a third party error",
			err: &platform.Error{
				Code: EFailedToGetStorageHost,
				Op:   "cmd/fluxd.injectDeps",
				Err:  errors.New("empty value"),
			},
			msg: "cmd/fluxd.injectDeps: empty value",
		},
		{
			name: "with a internal error",
			err: &platform.Error{
				Code: EFailedToGetStorageHost,
				Op:   "cmd/fluxd.injectDeps",
				Err:  &platform.Error{Code: platform.EEmptyValue, Op: "cmd/fluxd.getStrList"},
			},
			msg: "cmd/fluxd.injectDeps: cmd/fluxd.getStrList: <empty value>",
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
			err:  (*platform.Error)(nil),
		},
		{
			name: "simple error",
			err:  &platform.Error{Msg: "simple error"},
			want: "simple error",
		},
		{
			name: "embeded error",
			err:  &platform.Error{Err: &platform.Error{Msg: "embeded error"}},
			want: "embeded error",
		},
		{
			name: "default error",
			err:  errors.New("s"),
			want: "An internal error has occurred.",
		},
	}
	for _, c := range cases {
		if result := platform.ErrorMessage(c.err); c.want != result {
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
			err:  (*platform.Error)(nil),
		},
		{
			name: "simple error",
			err:  &platform.Error{Op: "op1"},
			want: "op1",
		},
		{
			name: "embeded error",
			err:  &platform.Error{Op: "op1", Err: &platform.Error{Code: platform.EInvalid}},
			want: "op1",
		},
		{
			name: "embeded error without op in root level",
			err:  &platform.Error{Err: &platform.Error{Code: platform.EInvalid, Op: "op2"}},
			want: "op2",
		},
		{
			name: "default error",
			err:  errors.New("s"),
			want: "",
		},
	}
	for _, c := range cases {
		if result := platform.ErrorOp(c.err); c.want != result {
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
			err:  (*platform.Error)(nil),
		},
		{
			name: "simple error",
			err:  &platform.Error{Code: platform.ENotFound},
			want: platform.ENotFound,
		},
		{
			name: "embeded error",
			err:  &platform.Error{Code: platform.ENotFound, Err: &platform.Error{Code: platform.EInvalid}},
			want: platform.ENotFound,
		},
		{
			name: "embeded error with root level code",
			err:  &platform.Error{Err: &platform.Error{Code: platform.EInvalid}},
			want: platform.EInvalid,
		},
		{
			name: "default error",
			err:  errors.New("s"),
			want: platform.EInternal,
		},
	}
	for _, c := range cases {
		if result := platform.ErrorCode(c.err); c.want != result {
			t.Errorf("%s failed, want %s, got %s", c.name, c.want, result)
		}
	}
}

func TestJSON(t *testing.T) {
	cases := []struct {
		name    string
		err     *platform.Error
		encoded string
	}{
		{
			name:    "simple error",
			err:     &platform.Error{Code: platform.ENotFound},
			encoded: `{"code":"not found"}`,
		},
		{
			name: "with op",
			err: &platform.Error{
				Code: platform.ENotFound,
				Op:   "bolt.FindAuthorizationByID",
			},
			encoded: `{"code":"not found","op":"bolt.FindAuthorizationByID"}`,
		},
		{
			name: "with op and value",
			err: &platform.Error{
				Code: platform.ENotFound,
				Op:   "bolt/FindAuthorizationByID",
				Msg:  fmt.Sprintf("with ID %d", 323),
			},
			encoded: `{"code":"not found","message":"with ID 323","op":"bolt/FindAuthorizationByID"}`,
		},
		{
			name: "with a third party error",
			err: &platform.Error{
				Code: EFailedToGetStorageHost,
				Op:   "cmd/fluxd.injectDeps",
				Err:  errors.New("empty value"),
			},
			encoded: `{"code":"failed to get the storage host","op":"cmd/fluxd.injectDeps","error":"empty value"}`,
		},
		{
			name: "with a internal error",
			err: &platform.Error{
				Code: EFailedToGetStorageHost,
				Op:   "cmd/fluxd.injectDeps",
				Err:  &platform.Error{Code: platform.EEmptyValue, Op: "cmd/fluxd.getStrList"},
			},
			encoded: `{"code":"failed to get the storage host","op":"cmd/fluxd.injectDeps","error":{"code":"empty value","op":"cmd/fluxd.getStrList"}}`,
		},
		{
			name: "with a deep internal error",
			err: &platform.Error{
				Code: EFailedToGetStorageHost,
				Op:   "cmd/fluxd.injectDeps",
				Err: &platform.Error{
					Code: platform.EInvalid,
					Op:   "cmd/fluxd.getStrList",
					Err: &platform.Error{
						Code: platform.EEmptyValue,
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
		got := new(platform.Error)
		err = json.Unmarshal(result, got)
		if err != nil {
			t.Errorf("%s decode failed, want err: %v, should be nil", c.name, err)
		}
		decodeEqual(t, c.err, got, "decode: "+c.name)
	}
}

func decodeEqual(t *testing.T, want, result *platform.Error, caseName string) {
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
		if _, ok := want.Err.(*platform.Error); ok {
			decodeEqual(t, want.Err.(*platform.Error), result.Err.(*platform.Error), caseName)
		} else {
			if want.Err.Error() != result.Err.Error() {
				t.Errorf("%s Err failed, want %s, got %s", caseName, want.Err.Error(), result.Err.Error())
			}
		}
	}
}
