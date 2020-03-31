package kv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
)

const (
	defaultPopulateBatchSize = 100
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
//  byUserID := func(v []byte) ([]byte, error) {
//      auth := &influxdb.Authorization{}
//
//      if err := json.Unmarshal(v, auth); err != nil {
//          return err
//      }
//
//      return auth.UserID.Encode()
//  }
//
//  // configure a write only index
//  indexByUser := NewIndex(NewSource([]byte(`authorizationsbyuserv1/), byUserID))
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
//  diff, err := indexByUser.Verify(ctx, tx)
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

// WithIndexPopulateBatchSize configures the size of each batch
// used when fully populating an index. (number of puts per tx)
func WithIndexPopulateBatchSize(n int) IndexOption {
	return func(i *Index) {
		i.populateBatchSize = n
	}
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
	index := &Index{
		IndexMapping:      mapping,
		populateBatchSize: defaultPopulateBatchSize,
	}

	for _, opt := range opts {
		opt(index)
	}

	return index
}

func (i *Index) initialize(ctx context.Context, store Store) error {
	return store.Update(ctx, func(tx Tx) error {
		// create bucket if not exist
		_, err := tx.Bucket(i.IndexBucket())
		return err
	})
}

func (i *Index) indexBucket(tx Tx) (Bucket, error) {
	return tx.Bucket(i.IndexBucket())
}

func (i *Index) sourceBucket(tx Tx) (Bucket, error) {
	return tx.Bucket(i.SourceBucket())
}

func indexKey(foreignKey, primaryKey []byte) (newKey []byte) {
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

// ensure IndexMigration implements MigrationSpec
var _ MigrationSpec = (*IndexMigration)(nil)

// IndexMigration is a migration for adding and removing an index.
// These are constructed via the Index.Migration function.
type IndexMigration struct {
	*Index
	opts []PopulateOption
}

// Name returns a readable name for the index migration.
func (i *IndexMigration) MigrationName() string {
	return fmt.Sprintf("add index %q", string(i.IndexBucket()))
}

// Up initializes the index bucket and populates the index.
func (i *IndexMigration) Up(ctx context.Context, store Store) (err error) {
	wrapErr := func(err error) error {
		if err == nil {
			return nil
		}

		return fmt.Errorf("migration (up) %s: %w", i.MigrationName(), err)
	}

	if err = i.initialize(ctx, store); err != nil {
		return wrapErr(err)
	}

	_, err = i.Populate(ctx, store, i.opts...)
	return wrapErr(err)
}

// Down deletes all entries from the index.
func (i *IndexMigration) Down(ctx context.Context, store Store) error {
	if err := i.DeleteAll(ctx, store); err != nil {
		return fmt.Errorf("migration (down) %s: %w", i.MigrationName(), err)
	}

	return nil
}

// Migration creates an IndexMigration for the underlying Index.
func (i *Index) Migration(opts ...PopulateOption) *IndexMigration {
	return &IndexMigration{
		Index: i,
		opts:  opts,
	}
}

// Insert creates a single index entry for the provided primary key on the foreign key.
func (i *Index) Insert(tx Tx, foreignKey, primaryKey []byte) error {
	bkt, err := i.indexBucket(tx)
	if err != nil {
		return err
	}

	return bkt.Put(indexKey(foreignKey, primaryKey), primaryKey)
}

// Delete removes the foreignKey and primaryKey mapping from the underlying index.
func (i *Index) Delete(tx Tx, foreignKey, primaryKey []byte) error {
	bkt, err := i.indexBucket(tx)
	if err != nil {
		return err
	}

	return bkt.Delete(indexKey(foreignKey, primaryKey))
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

	return indexWalk(ctx, cursor, sourceBucket, visitFn)
}

// PopulateConfig configures a call to Populate
type PopulateConfig struct {
	RemoveDanglingForeignKeys bool
}

// PopulateOption is a functional option for the Populate call
type PopulateOption func(*PopulateConfig)

// WithPopulateRemoveDanglingForeignKeys removes index entries which point to
// missing items in the source bucket.
func WithPopulateRemoveDanglingForeignKeys(c *PopulateConfig) {
	c.RemoveDanglingForeignKeys = true
}

// Populate does a full population of the index using the IndexSourceOn IndexMapping function.
// Once completed it marks the index as ready for use.
// It return a nil error on success and the count of inserted items.
func (i *Index) Populate(ctx context.Context, store Store, opts ...PopulateOption) (n int, err error) {
	var config PopulateConfig

	for _, opt := range opts {
		opt(&config)
	}

	// verify the index to derive missing index
	// we can skip missing source lookup as we're
	// only interested in populating the missing index
	diff, err := i.verify(ctx, store, config.RemoveDanglingForeignKeys)
	if err != nil {
		return 0, fmt.Errorf("looking up missing indexes: %w", err)
	}

	flush := func(batch kvSlice) error {
		if len(batch) == 0 {
			return nil
		}

		if err := store.Update(ctx, func(tx Tx) error {
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
			return fmt.Errorf("updating index: %w", err)
		}

		return nil
	}

	var batch kvSlice

	for fk, fkm := range diff.MissingFromIndex {
		for pk := range fkm {
			batch = append(batch, [2][]byte{indexKey([]byte(fk), []byte(pk)), []byte(pk)})

			if len(batch) >= i.populateBatchSize {
				if err := flush(batch); err != nil {
					return n, err
				}

				batch = batch[:0]
			}
		}
	}

	if err := flush(batch); err != nil {
		return n, err
	}

	if config.RemoveDanglingForeignKeys {
		return n, i.remove(ctx, store, diff.MissingFromSource)
	}

	return n, nil
}

// DeleteAll removes the entire index in batches
func (i *Index) DeleteAll(ctx context.Context, store Store) error {
	diff, err := i.verify(ctx, store, true)
	if err != nil {
		return err
	}

	for k, v := range diff.MissingFromSource {
		if fkm, ok := diff.PresentInIndex[k]; ok {
			for pk := range v {
				fkm[pk] = struct{}{}
			}
			continue
		}

		diff.PresentInIndex[k] = v
	}

	return i.remove(ctx, store, diff.PresentInIndex)
}

func (i *Index) remove(ctx context.Context, store Store, mappings map[string]map[string]struct{}) error {
	var (
		batch [][]byte
		flush = func(batch [][]byte) error {
			if len(batch) == 0 {
				return nil
			}

			if err := store.Update(ctx, func(tx Tx) error {
				indexBucket, err := i.indexBucket(tx)
				if err != nil {
					return err
				}

				for _, indexKey := range batch {
					// delete dangling foreign key
					if err := indexBucket.Delete(indexKey); err != nil {
						return err
					}
				}

				return nil
			}); err != nil {
				return fmt.Errorf("removing dangling foreign keys: %w", err)
			}

			return nil
		}
	)

	for fk, fkm := range mappings {
		for pk := range fkm {
			batch = append(batch, indexKey([]byte(fk), []byte(pk)))

			if len(batch) >= i.populateBatchSize {
				if err := flush(batch); err != nil {
					return err
				}

				batch = batch[:0]
			}
		}
	}

	return flush(batch)
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
	// not yet occured, the index populate code is incorrect or the write path
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
	return i.verify(ctx, store, true)
}

func (i *Index) verify(ctx context.Context, store Store, includeMissingSource bool) (diff IndexDiff, err error) {
	diff.PresentInIndex, err = i.readEntireIndex(ctx, store)
	if err != nil {
		return diff, err
	}

	sourceKVs, err := consumeBucket(ctx, store, i.sourceBucket)
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

		fk, err := i.IndexSourceOn(v)
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

// indexWalk consumes the indexKey and primaryKey pairs in the index bucket and looks up their
// associated primaryKey's value in the provided source bucket.
// When an item is located in the source, the provided visit function is called with primary key and associated value.
func indexWalk(ctx context.Context, indexCursor ForwardCursor, sourceBucket Bucket, visit VisitFunc) (err error) {
	defer func() {
		if cerr := indexCursor.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	for ik, pk := indexCursor.Next(); ik != nil; ik, pk = indexCursor.Next() {
		// TODO(george): this is a work-around as lots of calls to Get()
		// on a transaction causes issues with a particular implementation
		// of kv.Store.
		// The use of a cursor on this store bypasses the transaction
		// and gives us the access pattern we desire.
		// Please do not change back to a bucket.Get().
		v, err := getKeyUsingRange(ctx, sourceBucket, pk)
		if err != nil {
			return err
		}

		if err := visit(pk, v); err != nil {
			return fmt.Errorf("for index entry %q: %w", string(ik), err)
		}
	}

	return indexCursor.Err()
}

// getKeyUsingRange is a work around to for a particular implementation of kv.Store
// which needs to lookup using cursors instead of individual get operations.
func getKeyUsingRange(ctx context.Context, bucket Bucket, key []byte) ([]byte, error) {
	cursor, err := bucket.ForwardCursor(key,
		WithCursorPrefix(key))
	if err != nil {
		return nil, err
	}

	_, value := cursor.Next()
	if value == nil {
		return nil, ErrKeyNotFound
	}

	if err := cursor.Err(); err != nil {
		return nil, err
	}

	return value, cursor.Close()
}

// readEntireIndex returns the entire current state of the index
func (i *Index) readEntireIndex(ctx context.Context, store Store) (map[string]map[string]struct{}, error) {
	kvs, err := consumeBucket(ctx, store, i.indexBucket)
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

		return WalkCursor(ctx, cursor, func(k, v []byte) error {
			kvs = append(kvs, [2][]byte{k, v})
			return nil
		})
	})
}
