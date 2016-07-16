package tsm1

import (
	"fmt"
	"strings"
)

type (
	// seriesKey is a typed string that holds the identity of a series.
	// format: [measurement],[canonical tags]
	// example: cpu,hostname=host0,region=us-east
	seriesKey string

	// fieldKey is a typed string that holds the identity of a field.
	// format: [name]
	// example: usage_user
	fieldKey string
)

// CompositeKey stores namespaced strings that fully identify a series.
type CompositeKey struct {
	SeriesKey seriesKey
	FieldKey  fieldKey
}

// NewCompositeKey makes a composite key from normal strings.
func NewCompositeKey(l, v string) CompositeKey {
	return CompositeKey{
		SeriesKey: seriesKey(l),
		FieldKey:  fieldKey(v),
	}
}

// StringToCompositeKey is a convenience function that parses a CompositeKey
// out of an untyped string. It assumes that the string has the following
// format (an example):
// "measurement_name,tag0=key0" + keyFieldSeparator + "field_name"
// It is a utility to help migrate existing code to use the CacheStore.
func StringToCompositeKey(s string) CompositeKey {
	sepStart := strings.Index(s, keyFieldSeparator)
	if sepStart == -1 {
		panic("logic error: bad StringToCompositeKey input")
	}

	l := s[:sepStart]
	v := s[sepStart+len(keyFieldSeparator):]
	return NewCompositeKey(l, v)
}

// StringKey makes a plain string version of a CompositeKey. It uses the
// magic `keyFieldSeparator` value defined elsewhere in the tsm1 code.
func (ck CompositeKey) StringKey() string {
	return fmt.Sprintf("%s%s%s", ck.SeriesKey, keyFieldSeparator, ck.FieldKey)
}

// CacheStore is a sharded map used for storing series data in a *tsm1.Cache.
// It breaks away from previous tsm1.Cache designs by namespacing the keys into
// parts, using CompositeKey, which allows for less contention on the root
// map instance. Using this type is a speed improvement over the previous
// map[string]*entry type that the tsm1.Cache used.
type CacheStore map[seriesKey]*fieldData

// fieldData stores field-related data. An instance of this type makes up a
// 'shard' in a CacheStore.
type fieldData struct {
	// TODO(rw): explore using a lock to implement finer-grained
	// concurrency control.
	data map[fieldKey]*entry
}

// NewCacheStore creates a new CacheStore.
func NewCacheStore() CacheStore {
	return make(CacheStore)
}

// Get fetches the value associated with the CacheStore, if any. It is
// equivalent to the one-variable form of a Go map access.
func (cs CacheStore) Get(ck CompositeKey) *entry {
	e, ok := cs.GetChecked(ck)
	if ok {
		return e
	}
	return nil
}

// Get fetches the value associated with the CacheStore. It is equivalent to
// the two-variable form of a Go map access.
func (cs CacheStore) GetChecked(ck CompositeKey) (*entry, bool) {
	sub, ok := cs[ck.SeriesKey]
	if sub == nil || !ok {
		return nil, false
	}
	e, ok2 := sub.data[ck.FieldKey]
	if e == nil || !ok2 {
		return e, false
	}
	return e, true
}

// Put puts the given value into the CacheStore.
func (cs CacheStore) Put(ck CompositeKey, e *entry) {
	sub, ok := cs[ck.SeriesKey]
	if sub == nil || !ok {
		sub = &fieldData{data: map[fieldKey]*entry{}}
		cs[ck.SeriesKey] = sub
	}
	sub.data[ck.FieldKey] = e
}

// Delete deletes the given key from the CacheStore, if applicable.
func (cs CacheStore) Delete(ck CompositeKey) {
	sub, ok := cs[ck.SeriesKey]
	if sub == nil || !ok {
		return
	}
	delete(sub.data, ck.FieldKey)
	if len(sub.data) == 0 {
		delete(cs, ck.SeriesKey)
	}
}

// Iter iterates over (key, value) pairs in the CacheStore. It takes a
// callback function that acts upon each (key, value) pair, and aborts if that
// callback returns an error. It is equivalent to the two-variable range
// statement with the normal Go map.
func (cs CacheStore) Iter(f func(CompositeKey, *entry) error) error {
	for seriesKey, sub := range cs {
		for fieldKey, e := range sub.data {
			ck := CompositeKey{
				SeriesKey: seriesKey,
				FieldKey:  fieldKey,
			}
			err := f(ck, e)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
