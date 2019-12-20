package http_test

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/http"
	"github.com/pkg/errors"
)

func TestEncodeError(t *testing.T) {
	ctx := context.TODO()

	w := httptest.NewRecorder()

	http.ErrorHandler(0).HandleHTTPError(ctx, nil, w)

	if w.Code != 200 {
		t.Errorf("expected status code 200, got: %d", w.Code)
	}
}

func TestEncodeErrorWithError(t *testing.T) {
	ctx := context.TODO()
	err := &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  "an error occurred",
		Err:  fmt.Errorf("there's an error here, be aware"),
	}

	w := httptest.NewRecorder()

	http.ErrorHandler(0).HandleHTTPError(ctx, err, w)

	if w.Code != 500 {
		t.Errorf("expected status code 500, got: %d", w.Code)
	}

	errHeader := w.Header().Get("X-Platform-Error-Code")
	if errHeader != influxdb.EInternal {
		t.Errorf("expected X-Platform-Error-Code: %s, got: %s", influxdb.EInternal, errHeader)
	}

	// The http handler will flatten the message and it will not
	// have an error property, so reading the serialization results
	// in a different error.
	pe := http.CheckError(w.Result()).(*influxdb.Error)
	if want, got := influxdb.EInternal, pe.Code; want != got {
		t.Errorf("unexpected code -want/+got:\n\t- %q\n\t+ %q", want, got)
	}
	if want, got := "an error occurred: there's an error here, be aware", pe.Msg; want != got {
		t.Errorf("unexpected message -want/+got:\n\t- %q\n\t+ %q", want, got)
	}
}

func TestCheckError(t *testing.T) {
	for _, tt := range []struct {
		name  string
		write func(w *httptest.ResponseRecorder)
		want  error
	}{
		{
			name: "platform error",
			write: func(w *httptest.ResponseRecorder) {
				h := http.ErrorHandler(0)
				err := &influxdb.Error{
					Msg:  "expected",
					Code: influxdb.EInvalid,
				}
				h.HandleHTTPError(context.Background(), err, w)
			},
			want: &influxdb.Error{
				Msg:  "expected",
				Code: influxdb.EInvalid,
			},
		},
		{
			name: "text error",
			write: func(w *httptest.ResponseRecorder) {
				w.Header().Set("Content-Type", "text/plain")
				w.WriteHeader(500)
				_, _ = io.WriteString(w, "upstream timeout\n")
			},
			want: stderrors.New("upstream timeout"),
		},
		{
			name: "error with bad json",
			write: func(w *httptest.ResponseRecorder) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(500)
				_, _ = io.WriteString(w, "upstream timeout\n")
			},
			want: errors.Wrap(stderrors.New("upstream timeout"), "invalid character 'u' looking for beginning of value"),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			tt.write(w)

			resp := w.Result()
			cmpopt := cmp.Transformer("error", func(e error) string {
				if e, ok := e.(*influxdb.Error); ok {
					out, _ := json.Marshal(e)
					return string(out)
				}
				return e.Error()
			})
			if got, want := http.CheckError(resp), tt.want; !cmp.Equal(want, got, cmpopt) {
				t.Fatalf("unexpected error -want/+got:\n%s", cmp.Diff(want, got, cmpopt))
			}
		})
	}
}
