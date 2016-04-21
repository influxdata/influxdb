package ponyExpress

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
)

// TODO: Actually do this
func newTestServerHandlers() *mux.Router {
	r := mux.NewRouter()
	writeHandler := func(w http.ResponseWriter, r *http.Request) {
		body, _ := ioutil.ReadAll(r.Body)
		switch string(body) {
		case "foo":
			w.WriteHeader(204)
		case "bar":
			w.WriteHeader(500)
		}
	}
	readHandler := func(w http.ResponseWriter, r *http.Request) {
		body, _ := ioutil.ReadAll(r.Body)
		switch string(body) {
		case "foo":
			w.WriteHeader(204)
		case "bar":
			w.WriteHeader(500)
		}
	}
	r.HandleFunc("/write", writeHandler).Methods("POST")
	r.HandleFunc("/query", readHandler).Methods("GET")

	return r
}

func makeTestServer() *httptest.Server {
	return httptest.NewServer(newTestServerHandlers())
}

func TestMakePost(t *testing.T) {
	ts := makeTestServer()
	writeURL := fmt.Sprintf("%v/write", ts.URL)
	defer ts.Close()
	// Test bad url sent in
	resp, err := makePost("localhost:2049", bytes.NewBuffer([]byte("foo")))
	if err == nil {
		t.Fail()
	}
	// Test happy path
	resp, err = makePost(writeURL, bytes.NewBuffer([]byte("foo")))
	if err != nil {
		t.Error(err)
	}
	if resp.StatusCode != 204 {
		t.Errorf("expected: 204\ngot: %v\n", resp.StatusCode)
	}
	// Test error returned
	resp, err = makePost(writeURL, bytes.NewBuffer([]byte("bar")))
	if err == nil {
		t.Error("no error returned")
	}
	if resp.StatusCode != 500 {
		t.Errorf("expected: 500\ngot: %v\n", resp.StatusCode)
	}
}

func TestPrepareWrite(t *testing.T) {
	ts := makeTestServer()
	defer ts.Close()
	pe, _, _ := newTestPonyExpress(ts.URL)
	_, dur, err := pe.prepareWrite([]byte("foo"))

	if dur.Nanoseconds() > 10000000 {
		t.Skipped()
	}
	if err != nil {
		t.Skipped()
	}
}
