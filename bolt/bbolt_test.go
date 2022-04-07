package bolt_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/influxdata/influxdb/v2/bolt"
	"go.uber.org/zap/zaptest"
)

func NewTestClient(t *testing.T) (*bolt.Client, func(), error) {
	c, closeFn, err := newTestClient(t)
	if err != nil {
		return nil, nil, err
	}
	if err := c.Open(context.Background()); err != nil {
		return nil, nil, err
	}

	return c, closeFn, nil
}

func newTestClient(t *testing.T) (*bolt.Client, func(), error) {
	c := bolt.NewClient(zaptest.NewLogger(t))

	f, err := os.CreateTemp("", "influxdata-platform-bolt-")
	if err != nil {
		return nil, nil, errors.New("unable to open temporary boltdb file")
	}
	f.Close()

	c.Path = f.Name()

	close := func() {
		c.Close()
		os.Remove(c.Path)
	}

	return c, close, nil
}

func TestClientOpen(t *testing.T) {
	tempDir := t.TempDir()

	boltFile := filepath.Join(tempDir, "test", "bolt.db")

	c := bolt.NewClient(zaptest.NewLogger(t))
	c.Path = boltFile

	if err := c.Open(context.Background()); err != nil {
		t.Fatalf("unable to create database %s: %v", boltFile, err)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("unable to close database %s: %v", boltFile, err)
	}
}

func NewTestKVStore(t *testing.T) (*bolt.KVStore, func(), error) {
	f, err := os.CreateTemp("", "influxdata-platform-bolt-")
	if err != nil {
		return nil, nil, errors.New("unable to open temporary boltdb file")
	}
	f.Close()

	path := f.Name()
	s := bolt.NewKVStore(zaptest.NewLogger(t), path, bolt.WithNoSync)
	if err := s.Open(context.TODO()); err != nil {
		return nil, nil, err
	}

	close := func() {
		s.Close()
		os.Remove(path)
	}

	return s, close, nil
}
