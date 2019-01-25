package http_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/http"
)

func TestEncodeError(t *testing.T) {
	ctx := context.TODO()

	w := httptest.NewRecorder()

	http.EncodeError(ctx, nil, w)

	if w.Code != 200 {
		t.Errorf("expected status code 200, got: %d", w.Code)
	}
}

func TestEncodeErrorWithError(t *testing.T) {
	ctx := context.TODO()
	err := fmt.Errorf("there's an error here, be aware")

	w := httptest.NewRecorder()

	http.EncodeError(ctx, err, w)

	if w.Code != 500 {
		t.Errorf("expected status code 500, got: %d", w.Code)
	}

	errHeader := w.Header().Get("X-Platform-Error-Code")
	if errHeader != influxdb.EInternal {
		t.Errorf("expected X-Platform-Error-Code: %s, got: %s", influxdb.EInternal, errHeader)
	}

	expected := &influxdb.Error{
		Code: influxdb.EInternal,
		Err:  err,
	}

	pe := http.CheckError(w.Result())
	if pe.(*influxdb.Error).Err.Error() != expected.Err.Error() {
		t.Errorf("errors encode err: got %s", w.Body.String())
	}
}
