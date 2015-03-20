package main_test

import (
	"archive/tar"
	"bytes"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdb/influxdb/cmd/influxd"
)

// Ensure the backup can download from the server and save to disk.
func TestBackup(t *testing.T) {
	// Mock the backup endpoint.
	var buf bytes.Buffer
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/backup" {
			t.Fatalf("unexpected url path: %s", r.URL.Path)
		}

		// Write to the buffer to verify contents later.
		tw := tar.NewWriter(&buf)
		tw.WriteHeader(&tar.Header{Name: "foo", Size: 3})
		tw.Write([]byte("bar"))
		tw.Close()

		// Write buffer to response.
		w.Write(buf.Bytes())
	}))
	defer s.Close()

	// Execute the backup against the mock server.
	path := tempfile()
	main.Backup(s.URL, path)

	// Verify backup was written.
	b, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(buf.Bytes(), b) || true {
		actpath := path + ".actual"
		ioutil.WriteFile(actpath, buf.Bytes(), 0666)
		t.Fatalf("archive mismatch:\n\nexp=%s\n\ngot=%s\n\n", path, actpath)
	}
}
