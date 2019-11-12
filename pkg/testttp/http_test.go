package testttp_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/influxdata/influxdb/pkg/testttp"
)

func TestHTTP(t *testing.T) {
	svr := newMux()
	t.Run("Get", func(t *testing.T) {
		testttp.Get("/").
			Do(svr).
			ExpectStatus(t, http.StatusOK).
			ExpectBody(assertBody(t, http.MethodGet))
	})

	t.Run("Post", func(t *testing.T) {
		testttp.Post("/", nil).Do(svr).
			ExpectStatus(t, http.StatusCreated).
			ExpectBody(assertBody(t, http.MethodPost))
	})

	t.Run("Put", func(t *testing.T) {
		testttp.Put("/", nil).
			Do(svr).
			ExpectStatus(t, http.StatusAccepted).
			ExpectBody(assertBody(t, http.MethodPut))
	})

	t.Run("Patch", func(t *testing.T) {
		testttp.Patch("/", nil).
			Do(svr).
			ExpectStatus(t, http.StatusPartialContent).
			ExpectBody(assertBody(t, http.MethodPatch))
	})

	t.Run("Delete", func(t *testing.T) {
		testttp.Delete("/").
			Do(svr).
			ExpectStatus(t, http.StatusNoContent)
	})

	t.Run("Headers", func(t *testing.T) {
		testttp.Post("/", strings.NewReader(`a: foo`)).
			Headers("Content-Type", "text/yml").
			Do(svr).
			Expect(func(resp *testttp.Resp) {
				equals(t, "text/yml", resp.Req.Header.Get("Content-Type"))
			})
	})
}

type foo struct {
	Name, Thing, Method string
}

func newMux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodGet:
			writeFn(w, req.Method, http.StatusOK)
		case http.MethodPost:
			writeFn(w, req.Method, http.StatusCreated)
		case http.MethodPut:
			writeFn(w, req.Method, http.StatusAccepted)
		case http.MethodPatch:
			writeFn(w, req.Method, http.StatusPartialContent)
		case http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		}
	})
	return mux
}

func assertBody(t *testing.T, method string) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		var f foo
		if err := json.NewDecoder(buf).Decode(&f); err != nil {
			t.Fatal(err)
		}
		expected := foo{Name: "name", Thing: "thing", Method: method}
		equals(t, expected, f)
	}
}

func writeFn(w http.ResponseWriter, method string, statusCode int) {
	f := foo{Name: "name", Thing: "thing", Method: method}
	r, err := encodeBuf(f)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(statusCode)
	if _, err := io.Copy(w, r); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func equals(t *testing.T, expected, actual interface{}) {
	t.Helper()
	if expected == actual {
		return
	}
	t.Errorf("expected: %v\tactual: %v", expected, actual)
}

func encodeBuf(v interface{}) (io.Reader, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	return &buf, nil
}
