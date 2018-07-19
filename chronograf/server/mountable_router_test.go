package server_test

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bouk/httprouter"
	"github.com/influxdata/platform/chronograf/server"
)

func Test_MountableRouter_MountsRoutesUnderPrefix(t *testing.T) {
	t.Parallel()

	mr := &server.MountableRouter{
		Prefix:   "/chronograf",
		Delegate: httprouter.New(),
	}

	expected := "Hello?! McFly?! Anybody in there?!"
	mr.GET("/biff", http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(rw, expected)
	}))

	ts := httptest.NewServer(mr)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/chronograf/biff")
	if err != nil {
		t.Fatal("Unexpected error fetching from mounted router: err:", err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal("Unexpected error decoding response body: err:", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatal("Expected 200 but received", resp.StatusCode)
	}

	if string(body) != expected {
		t.Fatalf("Unexpected response body: Want: \"%s\". Got: \"%s\"", expected, string(body))
	}
}

func Test_MountableRouter_PrefixesPosts(t *testing.T) {
	t.Parallel()

	mr := &server.MountableRouter{
		Prefix:   "/chronograf",
		Delegate: httprouter.New(),
	}

	expected := "Great Scott!"
	actual := make([]byte, len(expected))
	mr.POST("/doc", http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		if _, err := io.ReadFull(r.Body, actual); err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
		} else {
			rw.WriteHeader(http.StatusOK)
		}
	}))

	ts := httptest.NewServer(mr)
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/chronograf/doc", "text/plain", strings.NewReader(expected))
	if err != nil {
		t.Fatal("Unexpected error posting to mounted router: err:", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatal("Expected 200 but received", resp.StatusCode)
	}

	if string(actual) != expected {
		t.Fatalf("Unexpected request body: Want: \"%s\". Got: \"%s\"", expected, string(actual))
	}
}

func Test_MountableRouter_PrefixesPuts(t *testing.T) {
	t.Parallel()

	mr := &server.MountableRouter{
		Prefix:   "/chronograf",
		Delegate: httprouter.New(),
	}

	expected := "Great Scott!"
	actual := make([]byte, len(expected))
	mr.PUT("/doc", http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		if _, err := io.ReadFull(r.Body, actual); err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
		} else {
			rw.WriteHeader(http.StatusOK)
		}
	}))

	ts := httptest.NewServer(mr)
	defer ts.Close()

	req := httptest.NewRequest(http.MethodPut, ts.URL+"/chronograf/doc", strings.NewReader(expected))
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	req.Header.Set("Content-Length", fmt.Sprintf("%d", len(expected)))
	req.RequestURI = ""

	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal("Unexpected error posting to mounted router: err:", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatal("Expected 200 but received", resp.StatusCode)
	}

	if string(actual) != expected {
		t.Fatalf("Unexpected request body: Want: \"%s\". Got: \"%s\"", expected, string(actual))
	}
}

func Test_MountableRouter_PrefixesDeletes(t *testing.T) {
	t.Parallel()

	mr := &server.MountableRouter{
		Prefix:   "/chronograf",
		Delegate: httprouter.New(),
	}

	mr.DELETE("/proto1985", http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusNoContent)
	}))

	ts := httptest.NewServer(mr)
	defer ts.Close()

	req := httptest.NewRequest(http.MethodDelete, ts.URL+"/chronograf/proto1985", nil)
	req.RequestURI = ""

	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal("Unexpected error sending request to mounted router: err:", err)
	}

	if resp.StatusCode != http.StatusNoContent {
		t.Fatal("Expected 204 but received", resp.StatusCode)
	}
}

func Test_MountableRouter_PrefixesPatches(t *testing.T) {
	t.Parallel()

	type Character struct {
		Name  string
		Items []string
	}

	mr := &server.MountableRouter{
		Prefix:   "/chronograf",
		Delegate: httprouter.New(),
	}

	biff := Character{"biff", []string{"sports almanac"}}
	mr.PATCH("/1955", http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		c := Character{}
		err := json.NewDecoder(r.Body).Decode(&c)
		if err != nil {
			rw.WriteHeader(http.StatusBadRequest)
		} else {
			biff.Items = c.Items
			rw.WriteHeader(http.StatusOK)
		}
	}))

	ts := httptest.NewServer(mr)
	defer ts.Close()

	r, w := io.Pipe()
	go func() {
		_ = json.NewEncoder(w).Encode(Character{"biff", []string{}})
		w.Close()
	}()

	req := httptest.NewRequest(http.MethodPatch, ts.URL+"/chronograf/1955", r)
	req.RequestURI = ""

	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal("Unexpected error sending request to mounted router: err:", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatal("Expected 200 but received", resp.StatusCode)
	}

	if len(biff.Items) != 0 {
		t.Fatal("Failed to alter history, biff still has the sports almanac")
	}
}

func Test_MountableRouter_PrefixesHandler(t *testing.T) {
	t.Parallel()

	mr := &server.MountableRouter{
		Prefix:   "/chronograf",
		Delegate: httprouter.New(),
	}

	mr.Handler(http.MethodGet, "/recklessAmountOfPower", http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte("1.21 Gigawatts!"))
	}))

	ts := httptest.NewServer(mr)
	defer ts.Close()

	req := httptest.NewRequest(http.MethodGet, ts.URL+"/chronograf/recklessAmountOfPower", nil)
	req.RequestURI = ""

	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal("Unexpected error sending request to mounted router: err:", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatal("Expected 200 but received", resp.StatusCode)
	}
}
