package http_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/platform/http"
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

	errHeader := w.Header().Get("X-Influx-Error")
	if errHeader != err.Error() {
		t.Errorf("expected X-Influx-Error: %s, got: %s", err.Error(), errHeader)
	}

	err = fmt.Errorf("there's a very long error here, it will be truncated so that we don't break webserver's pagesize")
	expected := "there's a very long error here, it will be truncated so that we "
	http.EncodeError(ctx, err, w)
	errHeader = w.Header().Get("X-Influx-Error")
	if errHeader != expected {
		t.Errorf("Expected a truncated X-Influx-Error header content: %s, got: %s", expected, errHeader)
	}
}
