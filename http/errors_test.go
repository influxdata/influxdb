package http_test

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"go.uber.org/zap/zaptest"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/http"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
)

func TestCheckError(t *testing.T) {
	for _, tt := range []struct {
		name  string
		write func(w *httptest.ResponseRecorder)
		want  error
	}{
		{
			name: "platform error",
			write: func(w *httptest.ResponseRecorder) {
				h := kithttp.NewErrorHandler(zaptest.NewLogger(t))
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
			want: &influxdb.Error{
				Code: influxdb.EInternal,
				Err:  stderrors.New("upstream timeout"),
			},
		},
		{
			name: "error with bad json",
			write: func(w *httptest.ResponseRecorder) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(500)
				_, _ = io.WriteString(w, "upstream timeout\n")
			},
			want: &influxdb.Error{
				Code: influxdb.EInternal,
				Msg:  `attempted to unmarshal error as JSON but failed: "invalid character 'u' looking for beginning of value"`,
				Err:  stderrors.New("upstream timeout"),
			},
		},
		{
			name: "error with no content-type (encoded as json - with code)",
			write: func(w *httptest.ResponseRecorder) {
				w.WriteHeader(500)
				_, _ = io.WriteString(w, `{"error": "service unavailable", "code": "unavailable"}`)
			},
			want: &influxdb.Error{
				Code: influxdb.EUnavailable,
				Err:  stderrors.New("service unavailable"),
			},
		},
		{
			name: "error with no content-type (encoded as json - no code)",
			write: func(w *httptest.ResponseRecorder) {
				w.WriteHeader(503)
				_, _ = io.WriteString(w, `{"error": "service unavailable"}`)
			},
			want: &influxdb.Error{
				Code: influxdb.EUnavailable,
				Err:  stderrors.New("service unavailable"),
			},
		},
		{
			name: "error with no content-type (not json encoded)",
			write: func(w *httptest.ResponseRecorder) {
				w.WriteHeader(503)
			},
			want: &influxdb.Error{
				Code: influxdb.EUnavailable,
				Msg:  `attempted to unmarshal error as JSON but failed: "unexpected end of JSON input"`,
				Err:  stderrors.New(""),
			},
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
