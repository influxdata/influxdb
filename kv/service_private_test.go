package kv

import (
	"context"
	"reflect"
	"sync"
	"testing"

	influxdb "github.com/influxdata/influxdb"
)

func ServiceConfigForTest() (conf ServiceConfig) {
	conf.SessionLength = influxdb.DefaultSessionLength
	conf.indexer = &spyIndexer{}
	return
}

// AuthSkipIndexOnPut configures calls to auth put to skip indexing by user
func AuthSkipIndexOnPut(ctx context.Context) context.Context {
	return context.WithValue(ctx, authSkipIndexOnPutContextKey{}, struct{}{})
}

func AssertIndexesWereCreated(t *testing.T, service *Service, calls ...AddToIndexCall) {
	idx, ok := service.indexer.(*spyIndexer)
	if !ok {
		t.Fatal("indexer was not configured as spy")
	}

	if !reflect.DeepEqual(calls, idx.calls) {
		t.Errorf("expected %#v, found %#v", calls, idx.calls)
	}
}

type spyIndexer struct {
	calls []AddToIndexCall
	mu    sync.Mutex
}

func (m *spyIndexer) AddToIndex(bkt []byte, keys map[string][]byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.calls = append(m.calls, AddToIndexCall{bkt, keys})
}

type AddToIndexCall struct {
	bucket []byte
	keys   map[string][]byte
}
