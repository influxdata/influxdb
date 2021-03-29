package http_test

import (
	"context"
	"fmt"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/influxdb/v2/http"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
)

func TestEncodeError(t *testing.T) {
	ctx := context.TODO()

	w := httptest.NewRecorder()

	kithttp.ErrorHandler(0).HandleHTTPError(ctx, nil, w)

	if w.Code != 200 {
		t.Errorf("expected status code 200, got: %d", w.Code)
	}
}

func TestEncodeErrorWithError(t *testing.T) {
	ctx := context.TODO()
	err := &errors.Error{
		Code: errors.EInternal,
		Msg:  "an error occurred",
		Err:  fmt.Errorf("there's an error here, be aware"),
	}

	w := httptest.NewRecorder()

	kithttp.ErrorHandler(0).HandleHTTPError(ctx, err, w)

	if w.Code != 500 {
		t.Errorf("expected status code 500, got: %d", w.Code)
	}

	errHeader := w.Header().Get("X-Platform-Error-Code")
	if errHeader != errors.EInternal {
		t.Errorf("expected X-Platform-Error-Code: %s, got: %s", errors.EInternal, errHeader)
	}

	// The http handler will flatten the message and it will not
	// have an error property, so reading the serialization results
	// in a different error.
	pe := http.CheckError(w.Result()).(*errors.Error)
	if want, got := errors.EInternal, pe.Code; want != got {
		t.Errorf("unexpected code -want/+got:\n\t- %q\n\t+ %q", want, got)
	}
	if want, got := "an error occurred: there's an error here, be aware", pe.Msg; want != got {
		t.Errorf("unexpected message -want/+got:\n\t- %q\n\t+ %q", want, got)
	}
}
