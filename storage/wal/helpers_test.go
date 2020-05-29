package wal

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func MustTempDir() string {
	dir, err := ioutil.TempDir("", "tsm1-test")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp dir: %v", err))
	}
	return dir
}

func MustTempFile(dir string) *os.File {
	f, err := ioutil.TempFile(dir, "tsm1test")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp file: %v", err))
	}
	return f
}

func fatal(t *testing.T, msg string, err error) {
	t.Fatalf("unexpected error %v: %v", msg, err)
}
