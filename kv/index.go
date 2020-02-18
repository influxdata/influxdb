package kv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
)

// Index is used to define and manage an index for a source bucket.
//
// When using the index you must provide it with an IndexSource.
// The IndexSource provides the index with the contract it needs to populate
// the entire index and traverse a populated index correctly.
// The IndexSource provides a way to retrieve the key on which to index with
// when provided with the value from the source.
// It also provides the way to access the source bucket.
//
// The following is an illustration of its use:
//
//  byUserID := func(v []byte) (influxdb.ID, error) {
//      auth := &influxdb.Authorization{}
//
//      if err := json.Unmarshal(v, auth); err != nil {
//          return err
//      }
//
//      return auth.UserID, nil
//  }
//
//  // configure a write only index
//  indexByUser := NewIndex(NewSource([]byte(`authorizationsbyuserv1/), byUserID), false)
//
//  indexByUser.Insert(tx, someUserID, someAuthID)
//
//  indexByUser.Delete(tx, someUserID, someAuthID)
//
//  indexByUser.Walk(tx, someUserID, func(k, v []byte) error {
//      auth := &influxdb.Authorization{}
//      if err := json.Unmarshal(v, auth); err != nil {
//          return err
//      }
//
//      // do something with auth
//
//      return nil
//  })
//
//  // populate entire index from source
//  indexedCount, err := indexByUser.Populate(ctx, store)
//
//  // verify the current index against the source and return the differences
//  // found in each
//  err := indexByUser.Verify(ctx, tx)
type Index struct {
	IndexMapping

	// populateBatchSize configures the size of the batch used for insertion
	populateBatchSize int
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
// to a destination index bucket in the form <source>by<relation>v<version>.
// For example: source = organization, relation = user, version = 2 -> organizationbyuserv2 bucket
func NewIndexMapping(sourceBucket []byte, relation string, version int, fn IndexSourceOnFunc) IndexMapping {
	return indexMapping{
		source: sourceBucket,
		index: []byte(fmt.Sprintf("%sby%sv%d",
			sourceBucket,
			relation,
			version,
		)),
		fn: fn,
	}
}

// NewIndex configures and returns a new *Index for a given index mapping.
// By default the read path (Walk) is disabled. This is because the index needs to
// be fully populated before depending upon the read path.
// The read path can be enabled using WithIndexReadPathEnabled option.
func NewIndex(mapping IndexMapping, opts ...IndexOption) *Index {
	index := &Index{
		IndexMapping:      mapping,
		populateBatchSize: 100,
	}

	for _, opt := range opts {
		opt(index)
	}

	return index
}

// IndexPopulatorStore is a store which also has a AutoPopulateIndex method
// The method returns true when the store support auto population of index
// on initialize.
type IndexPopulatorStore interface {
	Store
	AutoPopulateIndex() bool
}

// Initialize creates the index bucket on the provided store
func (i *Index) Initialize(ctx context.Context, store Store) error {
	if err := store.Update(ctx, func(tx Tx) error {
		_, err := i.indexBucket(tx)
		return err
	}); err != nil {
		return err
	}

	if store, ok := store.(IndexPopulatorStore); ok && store.AutoPopulateIndex() {
		_, err := i.Populate(ctx, store)
		return err
	}

	return nil
}

func (i *Index) indexBucket(tx Tx) (Bucket, error) {
	return tx.Bucket(i.IndexBucket())
}

func (i *Index) sourceBucket(tx Tx) (Bucket, error) {
	return tx.Bucket(i.SourceBucket())
}

func indexKey(foreignKey, primaryKey []byte) (newKey []byte, _ error) {
	newKey = make([]byte, len(primaryKey)+len(foreignKey)+1)
	copy(newKey, foreignKey)
	newKey[len(foreignKey)] = '/'
	copy(newKey[len(foreignKey)+1:], primaryKey)

	return
}

func indexKeyParts(indexKey []byte) (fk, pk []byte, err error) {
	// this function is called with items missing in index
	parts := bytes.SplitN(indexKey, []byte("/"), 2)
	if len(parts) < 2 {
		return nil, nil, errors.New("malformed index key")
	}

	// parts are fk/pk
	fk, pk = parts[0], parts[1]

	return
}

// Insert creates a single index entry for the provided primary key on the foreign key.
func (i *Index) Insert(tx Tx, foreignKey, primaryKey []byte) error {
	newKey, err := indexKey(foreignKey, primaryKey)
	if err != nil {
		return err
	}

	bkt, err := i.indexBucket(tx)
	if err != nil {
		return err
	}

	return bkt.Put(newKey, primaryKey)
}

// Delete removes the foreignKey and primaryKey mapping from the underlying index.
func (i *Index) Delete(tx Tx, foreignKey, primaryKey []byte) error {
	newKey, err := indexKey(foreignKey, primaryKey)
	if err != nil {
		return err
	}

	bkt, err := i.indexBucket(tx)
	if err != nil {
		return err
	}

	return bkt.Delete(newKey)
}

// VisitFunc is called for each k, v byte slice pair from the underlying source bucket
// which are found in the index bucket for a provided foreign key.
type VisitFunc func(k, v []byte) error

// Walk walks the source bucket using keys found in the index using the provided foreign key
// given the index has been fully populated.
func (i *Index) Walk(tx Tx, foreignKey []byte, visitFn VisitFunc) error {
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

	return indexWalk(cursor, sourceBucket, visitFn, func(fk, pk []byte) error {
		// fail iteration when key not found for item in index
		return fmt.Errorf("for key %v indexed by %v: %w", pk, fk, ErrKeyNotFound)
	})
}

// Populate does a full population of the index using the provided IndexOnFunc.
// Once completed it marks the index as ready for use.
// It return a nil error on success and the count of inserted items.
func (i *Index) Populate(ctx context.Context, store Store) (n int, err error) {
	var missing [][2][]byte

	if err = store.View(ctx, func(tx Tx) error {
		sourceBucket, err := i.sourceBucket(tx)
		if err != nil {
			return err
		}

		cursor, err := sourceBucket.ForwardCursor(nil)
		if err != nil {
			return err
		}

		indexBucket, err := i.indexBucket(tx)
		if err != nil {
			return err
		}

		if err = i.missingIndexWalk(cursor, indexBucket, func(fk, pk []byte) error {
			missing = append(missing, [2][]byte{fk, pk})
			return nil
		}); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return
	}

	for len(missing) > 0 {
		var (
			end   = i.populateBatchSize
			batch [][2][]byte
		)

		if end > len(missing) {
			end = len(missing)
		}

		batch, missing = missing[:end], missing[end:]

		if err = store.Update(ctx, func(tx Tx) error {
			indexBucket, err := i.indexBucket(tx)
			if err != nil {
				return err
			}

			for _, pair := range batch {
				// insert missing item into index
				if err := indexBucket.Put(pair[0], pair[1]); err != nil {
					return err
				}

				n++
			}

			return nil
		}); err != nil {
			return
		}
	}

	return
}

// IndexDiff contains a set of items present in the source not in index,
// along with a set of things in the index which are not in the source.
type IndexDiff struct {
	// Source is a set of primary key to foreign key mappings which
	// are present in the index, but have no associated source in the
	// source bucket. This can happen when items are removed from the source
	// but they have not be correctly removed from the Index (you need to wire
	// index.Delete into your source resource Delete action).
	Source map[string]string
	// Index is a set of index key (fk/pk) to primary key mappings which
	// are missing from the index. These are items which should be present because
	// there are source entries which are not accounted for.
	// This happens when creating a new index on an existing source which has not
	// yet been populated and when the creation of new index entries has not properly
	// been configured on source item creation (you need to wire index.Create() into your
	// source resource Create action).
	Index map[string]string
}

func (i *IndexDiff) addMissingSource(pk, fk []byte) {
	if i.Source == nil {
		i.Source = map[string]string{}
	}

	i.Source[string(pk)] = string(fk)
}

func (i *IndexDiff) addMissingIndex(indexKey, primaryKey []byte) {
	if i.Index == nil {
		i.Index = map[string]string{}
	}

	i.Index[string(indexKey)] = string(primaryKey)
}

// Verify returns returns difference between a source and its index
// The difference contains items in the source that are not in the index
// and vice-versa.
func (i *Index) Verify(ctx context.Context, tx Tx) (diff IndexDiff, err error) {
	sourceBucket, err := i.sourceBucket(tx)
	if err != nil {
		return
	}

	indexBucket, err := i.indexBucket(tx)
	if err != nil {
		return
	}

	// create cursor for entire index
	cursor, err := indexBucket.ForwardCursor(nil)
	if err != nil {
		return
	}

	if err = indexWalk(cursor, sourceBucket, func(k, v []byte) error {
		// we're only interested in indexed items not in the source bucket
		return nil
	}, func(fk, pk []byte) error {
		diff.addMissingSource(pk, fk)

		// continue iterating over index
		return nil
	}); err != nil {
		return
	}

	// create a new cursor over the source and look for items
	// missing from the index
	cursor, err = sourceBucket.ForwardCursor(nil)
	if err != nil {
		return
	}

	if err = i.missingIndexWalk(cursor, indexBucket, func(indexKey, pk []byte) error {
		// add missing item from source which is not indexed
		diff.addMissingIndex(indexKey, pk)

		return nil
	}); err != nil {
		return
	}

	return
}

type notFoundFunc func(from, to []byte) error

// indexWalk consumes the indexKey and primaryKey pairs in the index bucket and looks up their
// associated primaryKey's value in the provided source bucket.
// When an item is found in the index which has no associated pair in the source, the provided not found function
// is called with the foreign key to primary key expect mapping.
// When an item is located in the source, the provided visit function is called with primary key and associated value.
func indexWalk(indexCursor ForwardCursor, sourceBucket Bucket, visit VisitFunc, notFound notFoundFunc) error {
	return crossReference(indexCursor, sourceBucket, func(indexKey, primaryKey []byte) ([]byte, func([]byte, error) error, error) {
		return primaryKey, func(sourceValue []byte, err error) error {
			if err != nil {
				if IsNotFound(err) {
					fk, pk, err := indexKeyParts(indexKey)
					if err != nil {
						return err
					}

					// hand off primary key to foreign key mapping to is not found
					// function
					return notFound(fk, pk)
				}

				return err
			}

			// else visit the primary key and associated value from the source bucket
			return visit(primaryKey, sourceValue)
		}, nil
	})
}

// missingIndexWalk consumers the source cursor key value pairs and looks up the expected index
// for each item in the provided index bucket.
// When an item is missing from the index, the provided notFoundFunc is called with the expected
// foreignKey to primaryKey mapping.
func (i *Index) missingIndexWalk(srcCursor ForwardCursor, indexBucket Bucket, notFound notFoundFunc) error {
	return crossReference(srcCursor, indexBucket, func(primaryKey, body []byte) ([]byte, func([]byte, error) error, error) {
		foreignKey, err := i.IndexSourceOn(body)
		if err != nil {
			return nil, nil, err
		}

		indexKey, err := indexKey(foreignKey, primaryKey)
		if err != nil {
			return nil, nil, err
		}

		return indexKey, func(value []byte, err error) error {
			if err != nil {
				if IsNotFound(err) {
					return notFound(indexKey, primaryKey)
				}

				return err
			}

			return nil
		}, nil
	})
}

type mappingFunc func(k, v []byte) (toK []byte, _ func(toV []byte, err error) error, _ error)

// crossReference consumes a provided cursor abd maps each found k / v pair into
// a key which is used to lookup in the provided bucket.
// The derived value and or error found looking up the derived key in the bucket is
// passed the the value function returned by the mapping function.
func crossReference(cursor ForwardCursor, bucket Bucket, fn mappingFunc) (err error) {
	defer func() {
		if cerr := cursor.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	for k, v := cursor.Next(); k != nil; k, v = cursor.Next() {
		key, valFn, err := fn(k, v)
		if err != nil {
			return err
		}

		value, err := bucket.Get(key)
		if err := valFn(value, err); err != nil {
			return err
		}
	}

	return cursor.Err()
}
