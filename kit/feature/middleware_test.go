package feature_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/feature"
	"go.uber.org/zap/zaptest"
)

func Test_Handler(t *testing.T) {
	var (
		w = &httptest.ResponseRecorder{}
		r = httptest.NewRequest(http.MethodGet, "http://nowhere.test", new(bytes.Buffer)).
			WithContext(context.Background())

		original = r.Context()
	)

	handler := &checkHandler{t: t, f: func(t *testing.T, r *http.Request) {
		if r.Context() == original {
			t.Error("expected annotated context")
		}
	}}

	subject := feature.NewHandler(zaptest.NewLogger(t), feature.DefaultFlagger(), feature.Flags(), handler)

	subject.ServeHTTP(w, r)

	if !handler.called {
		t.Error("expected handler to be called")
	}
}

type checkHandler struct {
	t      *testing.T
	f      func(t *testing.T, r *http.Request)
	called bool
}

func (h *checkHandler) ServeHTTP(_ http.ResponseWriter, r *http.Request) {
	h.called = true
	h.f(h.t, r)
}
