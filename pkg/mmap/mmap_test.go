package mmap_test

import (
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/influxdata/influxdb/pkg/mmap"
)

func TestMap(t *testing.T) {
	data, err := mmap.Map("mmap_test.go", 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if exp, err := ioutil.ReadFile("mmap_test.go"); err != nil {
		t.Fatalf("ioutil.ReadFile: %v", err)
	} else if !bytes.Equal(data, exp) {
		t.Fatalf("got %q\nwant %q", string(data), string(exp))
	}
}
