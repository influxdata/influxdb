package admin_test

import (
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/influxdb/influxdb/admin"
)

func Test_ServesIndexByDefault(t *testing.T) {
	s := admin.NewServer(":8083")
	go func() { s.ListenAndServe() }()
	defer s.Close()

	resp, err := http.Get("http://localhost:8083/")
	if err != nil {
		t.Fatalf("couldn't complete GET to / on port 8083")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("didn't get a 200 OK response from server, got %d instead", resp.StatusCode)
	}

	_, err = ioutil.ReadAll(resp.Body)

	if err != nil {
		t.Fatalf("couldn't read body")
	}
}
