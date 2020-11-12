package pipeline

import (
	"fmt"
	"strings"
	"testing"

	"github.com/influxdata/influxdb/v2"
)

func check(t *testing.T, gotErr error, want string, cmpErrFn func() error) {
	t.Helper()

	if len(want) > 0 {
		if gotErr == nil {
			t.Errorf("expected error got none")
		} else {
			if err := cmpErrFn(); err != nil {
				t.Error(err)
			}
		}
	} else {
		if gotErr != nil {
			t.Errorf("unexpected error: %v", gotErr)
		}
	}
}

// CheckErr checks if `errorMsgPart` is in the error message of `gotErr`.
// If `errorMsgPart` is an empty string, it fails if `gotErr != nil`.
func CheckErr(t *testing.T, gotErr error, errorMsgPart string) {
	t.Helper()

	check(t, gotErr, errorMsgPart, func() error {
		if !strings.Contains(gotErr.Error(), errorMsgPart) {
			return fmt.Errorf("unexpected error message: %v", gotErr)
		}
		return nil
	})
}

// CheckHTTPErr checks if `gotErr` has the specified HTTP `code`.
// If `code` is an empty string, it fails if `gotErr != nil`.
func CheckHTTPErr(t *testing.T, gotErr error, code string) {
	t.Helper()

	check(t, gotErr, code, func() error {
		if gotCode := influxdb.ErrorCode(gotErr); gotCode != code {
			t.Errorf("unexpected error code: %v\n\tcomplete error: %v", gotCode, gotErr)
		}
		return nil
	})
}
