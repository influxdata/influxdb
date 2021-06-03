package sqlite

import (
	"io/ioutil"
	"os"
	"testing"

	"go.uber.org/zap"
)

func NewTestStore(t *testing.T) (*SqlStore, func(t *testing.T)) {
	tempDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("unable to create temporary test directory %v", err)
	}

	cleanUpFn := func(t *testing.T) {
		if err := os.RemoveAll(tempDir); err != nil {
			t.Fatalf("unable to delete temporary test directory %s: %v", tempDir, err)
		}
	}

	s, err := NewSqlStore(tempDir+"/"+DefaultFilename, zap.NewNop())
	if err != nil {
		t.Fatal("unable to open testing database")
	}

	return s, cleanUpFn
}
