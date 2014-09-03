package raft_test

import (
	"io/ioutil"
	"log"
	"os"
)

func init() {
	log.SetFlags(0)
}

// tempfile returns the path to a non-existent file in the temp directory.
func tempfile() string {
	f, _ := ioutil.TempFile("", "raft-")
	path := f.Name()
	f.Close()
	os.Remove(path)
	return path
}
