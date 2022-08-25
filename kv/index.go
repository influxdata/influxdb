package kv

import (
	"bytes"
	"context"
	"errors"
)

// Index is used to define and manage an index for a source bucket.
//
// When using the index you must provide it with an IndexMapping.
// The IndexMapping provides the index with the contract it needs to populate
// the entire index and traverse a populated index correctly.
// The IndexMapping provides a way to retrieve the key on which to index with
// when provided with the value from the source.
// It also provides the way to access the source bucket.
//
// The following is an illustration of its use:
//
//	byUserID := func(v []byte) ([]byte, error) {
//	    auth := &influxdb.Authorization{}
//
//	    if err := json.Unmarshal(v, auth); err != nil {
//	        return err
//	    }
//
//	    return auth.UserID.Encode()
//	}
//
//	// configure a write only index
//	indexByUser := NewIndex(NewSource([]byte(`authorizationsbyuserv1/), byUserID))
//
//	indexByUser.Insert(tx, someUserID, someAuthID)
//
//	indexByUser.Delete(tx, someUserID, someAuthID)
//
//	indexByUser.Walk(tx, someUserID, func(k, v []byte) error {
//	    auth := &influxdb.Authorization{}
//	    if err := json.Unmarshal(v, auth); err != nil {
//	        return err
//	    }
//
//	    // do something with auth
//
//	    return nil
//	})
//
//	// verify the current index against the source and return the differences
//	// found in each
//	diff, err := indexByUser.Verify(ctx, tx)
type Index struct {
	IndexMapping

	// canRead configures whether or not Walk accesses the index at all
	// or skips the index altogether and returns nothing.
	// This is used when you want to integrate only the write path before
	// releasing the read path.
	canRead bool
}

// IndexOption is a function which configures an index
type IndexOption func(*Index)

// WithIndexReadPathEnabled enables the read paths of the index (Walk)
// This should be enabled once the index has been fully populated and
// the Insert and Delete paths are correctly integrated.
func WithIndexReadPathEnabled(i *Index) {
	i.canRead = true
}

// IndexMapping is a type which configures and Index to map items
// from a source bucket to an index bucket via a mapping known as
// IndexSourceOn. This function is called on the values in the source
// to derive the foreign key on which to index each item.
type IndexMapping interface {
	SourceBucket() []byte
	IndexBucket() []byte
	IndexSourceOn(value []byte) (foreignKey []byte, err error)
}

// IndexSourceOnFunc is a function which can be used to derive the foreign key
// of a value in a source bucket.
type IndexSourceOnFunc func([]byte) ([]byte, error)

type indexMapping struct {
	source []byte
	index  []byte
	fn     IndexSourceOnFunc
}

func (i indexMapping) SourceBucket() []byte { return i.source }

func (i indexMapping) IndexBucket() []byte { return i.index }

func (i indexMapping) IndexSourceOn(v []byte) ([]byte, error) {
	return i.fn(v)
}

// NewIndexMapping creates an implementation of IndexMapping for the provided source bucket
// to a destination index bucket.
func NewIndexMapping(sourceBucket, indexBucket []byte, fn IndexSourceOnFunc) IndexMapping {
	return indexMapping{
		source: sourceBucket,
		index:  indexBucket,
		fn:     fn,
	}
}

// NewIndex configures and returns a new *Index for a given index mapping.
// By default the read path (Walk) is disabled. This is because the index needs to
// be fully populated before depending upon the read path.
// The read path can be enabled using WithIndexReadPathEnabled option.
func NewIndex(mapping IndexMapping, opts ...IndexOption) *Index {
	index := &Index{IndexMapping: mapping}

	for _, opt := range opts {
		opt(index)
	}

	return index
}

func (i *Index) indexBucket(tx Tx) (Bucket, error) {
	return tx.Bucket(i.IndexBucket())
}

func (i *Index) sourceBucket(tx Tx) (Bucket, error) {
	return tx.Bucket(i.SourceBucket())
}

var (
	// ErrKeyInvalidCharacters is returned when a foreignKey or primaryKey contains
	//
	ErrKeyInvalidCharacters = errors.New("key: contains invalid characters")
)

// IndexKey returns a value suitable for use as the key component
// when storing values in the index. IndexKey returns an
// ErrKeyInvalidCharacters error if either the foreignKey or primaryKey contains a /.
func IndexKey(foreignKey, primaryKey []byte) (newKey []byte, err error) {
	if bytes.IndexByte(foreignKey, '/') != -1 {
		return nil, ErrKeyInvalidCharacters
	}
	if bytes.IndexByte(primaryKey, '/') != -1 {
		return nil, ErrKeyInvalidCharacters
	}

	newKey = make([]byte, len(primaryKey)+len(foreignKey)+1)
	copy(newKey, foreignKey)
	newKey[len(foreignKey)] = '/'
	copy(newKey[len(foreignKey)+1:], primaryKey)

	return
}

func indexKeyParts(indexKey []byte) (fk, pk []byte, err error) {
	// this function is called with items missing in index
	fk, pk, ok := bytes.Cut(indexKey, []byte("/"))
	if !ok {
		return nil, nil, errors.New("malformed index key")
	}
	return
}

// Insert creates a single index entry for the provided primary key on the foreign key.
func (i *Index) Insert(tx Tx, foreignKey, primaryKey []byte) error {
	bkt, err := i.indexBucket(tx)
	if err != nil {
		return err
	}

	key, err := IndexKey(foreignKey, primaryKey)
	if err != nil {
		return err
	}

	return bkt.Put(key, primaryKey)
}

// Delete removes the foreignKey and primaryKey mapping from the underlying index.
func (i *Index) Delete(tx Tx, foreignKey, primaryKey []byte) error {
	bkt, err := i.indexBucket(tx)
	if err != nil {
		return err
	}

	key, err := IndexKey(foreignKey, primaryKey)
	if err != nil {
		return err
	}

	return bkt.Delete(key)
}

// Walk walks the source bucket using keys found in the index using the provided foreign key
// given the index has been fully populated.
func (i *Index) Walk(ctx context.Context, tx Tx, foreignKey []byte, visitFn VisitFunc) error {
	// skip walking if configured to do so as the index
	// is currently being used purely to write the index
	if !i.canRead {
		return nil
	}

	sourceBucket, err := i.sourceBucket(tx)
	if err != nil {
		return err
	}

	indexBucket, err := i.indexBucket(tx)
	if err != nil {
		return err
	}

	cursor, err := indexBucket.ForwardCursor(foreignKey,
		WithCursorPrefix(foreignKey))
	if err != nil {
		return err
	}

	return indexWalk(foreignKey, cursor, sourceBucket, visitFn)
}

// indexWalk consumes the indexKey and primaryKey pairs in the index bucket and looks up their
// associated primaryKey's value in the provided source bucket.
// When an item is located in the source, the provided visit function is called with primary key and associated value.
func indexWalk(foreignKey []byte, indexCursor ForwardCursor, sourceBucket Bucket, visit VisitFunc) (err error) {
	var keys [][]byte
	for ik, pk := indexCursor.Next(); ik != nil; ik, pk = indexCursor.Next() {
		if fk, _, err := indexKeyParts(ik); err != nil {
			return err
		} else if string(fk) == string(foreignKey) {
			keys = append(keys, pk)
		}
	}

	if err := indexCursor.Err(); err != nil {
		return err
	}

	if err := indexCursor.Close(); err != nil {
		return err
	}

	values, err := sourceBucket.GetBatch(keys...)
	if err != nil {
		return err
	}

	for i, value := range values {
		if value != nil {
			if cont, err := visit(keys[i], value); !cont || err != nil {
				return err
			}
		}
	}

	return nil
}

// IndexDiff contains a set of items present in the source not in index,
// along with a set of things in the index which are not in the source.
type IndexDiff struct {
	// PresentInIndex is a map of foreign key to primary keys
	// present in the index.
	PresentInIndex map[string]map[string]struct{}
	// MissingFromIndex is a map of foreign key to associated primary keys
	// missing from the index given the source bucket.
	// These items could be due to the fact an index populate migration has
	// not yet occurred, the index populate code is incorrect or the write path
	// for your resource type does not yet insert into the index as well (Create actions).
	MissingFromIndex map[string]map[string]struct{}
	// MissingFromSource is a map of foreign key to associated primary keys
	// missing from the source but accounted for in the index.
	// This happens when index items are not properly removed from the index
	// when an item is removed from the source (Delete actions).
	MissingFromSource map[string]map[string]struct{}
}

func (i *IndexDiff) addMissingSource(fk, pk []byte) {
	if i.MissingFromSource == nil {
		i.MissingFromSource = map[string]map[string]struct{}{}
	}

	if _, ok := i.MissingFromSource[string(fk)]; !ok {
		i.MissingFromSource[string(fk)] = map[string]struct{}{}
	}

	i.MissingFromSource[string(fk)][string(pk)] = struct{}{}
}

func (i *IndexDiff) addMissingIndex(fk, pk []byte) {
	if i.MissingFromIndex == nil {
		i.MissingFromIndex = map[string]map[string]struct{}{}
	}

	if _, ok := i.MissingFromIndex[string(fk)]; !ok {
		i.MissingFromIndex[string(fk)] = map[string]struct{}{}
	}

	i.MissingFromIndex[string(fk)][string(pk)] = struct{}{}
}

// Corrupt returns a list of foreign keys which have corrupted indexes (partial)
// These are foreign keys which map to a subset of the primary keys which they should
// be associated with.
func (i *IndexDiff) Corrupt() (corrupt []string) {
	for fk := range i.MissingFromIndex {
		if _, ok := i.PresentInIndex[fk]; ok {
			corrupt = append(corrupt, fk)
		}
	}
	return
}

// Verify returns the difference between a source and its index
// The difference contains items in the source that are not in the index
// and vice-versa.
func (i *Index) Verify(ctx context.Context, store Store) (diff IndexDiff, err error) {
	return indexVerify(ctx, i, store, true)
}

func indexVerify(ctx context.Context, mapping IndexMapping, store Store, includeMissingSource bool) (diff IndexDiff, err error) {
	diff.PresentInIndex, err = indexReadAll(ctx, store, func(tx Tx) (Bucket, error) {
		return tx.Bucket(mapping.IndexBucket())
	})
	if err != nil {
		return diff, err
	}

	sourceKVs, err := consumeBucket(ctx, store, func(tx Tx) (Bucket, error) {
		return tx.Bucket(mapping.SourceBucket())
	})
	if err != nil {
		return diff, err
	}

	// pks is a map of primary keys in source
	pks := map[string]struct{}{}

	// look for items missing from index
	for _, kv := range sourceKVs {
		pk, v := kv[0], kv[1]

		if includeMissingSource {
			// this is only useful for missing source
			pks[string(pk)] = struct{}{}
		}

		fk, err := mapping.IndexSourceOn(v)
		if err != nil {
			return diff, err
		}

		fkm, ok := diff.PresentInIndex[string(fk)]
		if ok {
			_, ok = fkm[string(pk)]
		}

		if !ok {
			diff.addMissingIndex(fk, pk)
		}
	}

	if includeMissingSource {
		// look for items missing from source
		for fk, fkm := range diff.PresentInIndex {
			for pk := range fkm {
				if _, ok := pks[pk]; !ok {
					diff.addMissingSource([]byte(fk), []byte(pk))
				}
			}
		}
	}

	return
}

// indexReadAll returns the entire current state of the index
func indexReadAll(ctx context.Context, store Store, indexBucket func(Tx) (Bucket, error)) (map[string]map[string]struct{}, error) {
	kvs, err := consumeBucket(ctx, store, indexBucket)
	if err != nil {
		return nil, err
	}

	index := map[string]map[string]struct{}{}
	for _, kv := range kvs {
		fk, pk, err := indexKeyParts(kv[0])
		if err != nil {
			return nil, err
		}

		if fkm, ok := index[string(fk)]; ok {
			fkm[string(pk)] = struct{}{}
			continue
		}

		index[string(fk)] = map[string]struct{}{string(pk): {}}
	}

	return index, nil
}

type kvSlice [][2][]byte

// consumeBucket consumes the entire k/v space for the provided bucket function
// applied to the provided store
func consumeBucket(ctx context.Context, store Store, fn func(tx Tx) (Bucket, error)) (kvs kvSlice, err error) {
	return kvs, store.View(ctx, func(tx Tx) error {
		bkt, err := fn(tx)
		if err != nil {
			return err
		}

		cursor, err := bkt.ForwardCursor(nil)
		if err != nil {
			return err
		}

		return WalkCursor(ctx, cursor, func(k, v []byte) (bool, error) {
			kvs = append(kvs, [2][]byte{k, v})
			return true, nil
		})
	})
}
