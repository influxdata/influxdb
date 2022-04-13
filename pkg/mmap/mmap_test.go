package mmap_test

import (
	"bytes"
	"os"
	"testing"

	"github.com/influxdata/influxdb/v2/pkg/mmap"
)

func TestMap(t *testing.T) {
	data, err := mmap.Map("mmap_test.go", 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if exp, err := os.ReadFile("mmap_test.go"); err != nil {
		t.Fatalf("os.ReadFile: %v", err)
	} else if !bytes.Equal(data, exp) {
		t.Fatalf("got %q\nwant %q", string(data), string(exp))
	}
}
