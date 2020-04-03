package kv_test

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"testing"

	"github.com/influxdata/influxdb/bolt"
	"github.com/influxdata/influxdb/inmem"
	influxdbtesting "github.com/influxdata/influxdb/testing"
	"go.uber.org/zap/zaptest"
)

func Test_Inmem_Index(t *testing.T) {
	influxdbtesting.TestIndex(t, inmem.NewKVStore())
}

func Test_Bolt_Index(t *testing.T) {
	s, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}
	defer closeBolt()

	influxdbtesting.TestIndex(t, s)
}

func Benchmark_Inmem_Index_Walk(b *testing.B) {
	influxdbtesting.BenchmarkIndexWalk(b, inmem.NewKVStore(), 1000, 200)
}

func Benchmark_Bolt_Index_Walk(b *testing.B) {
	f, err := ioutil.TempFile("", "influxdata-bolt-")
	if err != nil {
		b.Fatal(errors.New("unable to open temporary boltdb file"))
	}
	f.Close()

	path := f.Name()
	s := bolt.NewKVStore(zaptest.NewLogger(b), path)
	if err := s.Open(context.Background()); err != nil {
		b.Fatal(err)
	}

	defer func() {
		s.Close()
		os.Remove(path)
	}()

	influxdbtesting.BenchmarkIndexWalk(b, s, 1000, 200)
}
