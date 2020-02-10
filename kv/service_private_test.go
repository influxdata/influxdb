package kv

import (
	"reflect"
	"sync"
	"testing"

	influxdb "github.com/influxdata/influxdb"
)

type ServiceConfigOption func(*ServiceConfig)

// WithoutIndexingOnPut skips indexing authorizations by user ID when putting authorizations.
// This is a test only option as it is used to validate auth by user ID lookup
// when the index has yet to be populated.
func WithoutIndexingOnPut(c *ServiceConfig) {
	c.authsSkipIndexOnPut = true
}

func ServiceConfigForTest(opts ...ServiceConfigOption) (conf ServiceConfig) {
	conf.SessionLength = influxdb.DefaultSessionLength
	conf.indexer = &spyIndexer{}
	for _, opt := range opts {
		opt(&conf)
	}
	return
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
