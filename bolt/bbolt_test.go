package bolt_test

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/influxdata/platform/bolt"
	"golang.org/x/crypto/bcrypt"
)

func NewTestClient() (*bolt.Client, func(), error) {
	c := bolt.NewClient()
	bolt.HashCost = bcrypt.MinCost

	f, err := ioutil.TempFile("", "influxdata-platform-bolt-")
	if err != nil {
		return nil, nil, errors.New("unable to open temporary boltdb file")
	}
	f.Close()

	c.Path = f.Name()

	if err := c.Open(context.TODO()); err != nil {
		return nil, nil, err
	}

	close := func() {
		c.Close()
		os.Remove(c.Path)
	}

	return c, close, nil
}

func TestClientOpen(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("unable to create temporary test directory %v", err)
	}

	defer func() {
		if err := os.RemoveAll(tempDir); err != nil {
			t.Fatalf("unable to delete temporary test directory %s: %v", tempDir, err)
		}
	}()

	boltFile := filepath.Join(tempDir, "test", "bolt.db")

	c := bolt.NewClient()
	c.Path = boltFile

	if err := c.Open(context.Background()); err != nil {
		t.Fatalf("unable to create database %s: %v", boltFile, err)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("unable to close database %s: %v", boltFile, err)
	}
}
