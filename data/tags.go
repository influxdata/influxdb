package data

import (
	"hash/fnv"
	"sort"
	"sync"
)

var tagCache = NewTagCache()

type TagCache struct {
	cache map[string]uint64
	mu    sync.RWMutex
}

func NewTagCache() *TagCache {
	var t TagCache
	t.cache = make(map[string]uint64)
	return &t
}

func (tc *TagCache) Get(k string) (uint64, bool) {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	v, ok := tc.cache[k]
	return v, ok
}

func (tc *TagCache) Set(k string, v uint64) {
	tc.mu.Lock()
	tc.cache[k] = v
	tc.mu.Unlock()
}

type Tags map[string]string

func (t Tags) Marshal() []byte {
	// Empty maps marshal to empty bytes.
	if len(t) == 0 {
		return nil
	}

	// Extract keys and determine final size.
	sz := (len(t) * 2) - 1 // separators
	keys := make([]string, 0, len(t))
	for k, v := range t {
		keys = append(keys, k)
		sz += len(k) + len(v)
	}
	sort.Strings(keys)

	// Generate marshaled bytes.
	b := make([]byte, sz)
	buf := b
	for _, k := range keys {
		copy(buf, k)
		buf[len(k)] = '|'
		buf = buf[len(k)+1:]
	}
	for i, k := range keys {
		v := t[k]
		copy(buf, v)
		if i < len(keys)-1 {
			buf[len(v)] = '|'
			buf = buf[len(v)+1:]
		}
	}
	return b
}

func (t Tags) Hash() uint64 {
	b := t.Marshal()
	if len(b) == 0 {
		return 0
	}
	if sum, ok := tagCache.Get(string(b)); ok {
		// we had a cache hit
		return sum
	}
	h := fnv.New64a()
	h.Write(b)
	sum := h.Sum64()
	tagCache.Set(string(b), sum)
	return sum
}
