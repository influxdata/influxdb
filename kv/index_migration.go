package kv

import (
	"context"
	"fmt"
)

const (
	// defaultIndexMigrationOpBatchSize configures the size of batch operations
	// done by the index migration when populating or removing items from an
	// entire index
	defaultIndexMigrationOpBatchSize = 100
)

// IndexMigration is a migration for adding and removing an index.
// These are constructed via the Index.Migration function.
type IndexMigration struct {
	IndexMapping

	operationBatchSize        int
	removeDanglingForeignKeys bool
}

// IndexMigrationOption is a functional option for the IndexMigration type
type IndexMigrationOption func(*IndexMigration)

// WithIndexMigationBatchSize configures the size of the batches when committing
// changes to entire index during migration (e.g. size of put batch on index populate).
func WithIndexMigationBatchSize(n int) IndexMigrationOption {
	return func(m *IndexMigration) {
		m.operationBatchSize = n
	}
}

// WithIndexMigrationCleanup removes index entries which point to
// missing items in the source bucket.
func WithIndexMigrationCleanup(m *IndexMigration) {
	m.removeDanglingForeignKeys = true
}

// NewIndexMigration construct a migration for creating and populating an index
func NewIndexMigration(mapping IndexMapping, opts ...IndexMigrationOption) *IndexMigration {
	m := &IndexMigration{
		IndexMapping:       mapping,
		operationBatchSize: defaultIndexMigrationOpBatchSize,
	}

	for _, opt := range opts {
		opt(m)
	}

	return m
}

// Name returns a readable name for the index migration.
func (i *IndexMigration) MigrationName() string {
	return fmt.Sprintf("add index %q", string(i.IndexBucket()))
}

// Up initializes the index bucket and populates the index.
func (i *IndexMigration) Up(ctx context.Context, store SchemaStore) (err error) {
	wrapErr := func(err error) error {
		if err == nil {
			return nil
		}

		return fmt.Errorf("migration (up) %s: %w", i.MigrationName(), err)
	}

	if err = store.CreateBucket(ctx, i.IndexBucket()); err != nil {
		return wrapErr(err)
	}

	_, err = i.Populate(ctx, store)
	return wrapErr(err)
}

// Down deletes all entries from the index.
func (i *IndexMigration) Down(ctx context.Context, store SchemaStore) error {
	if err := store.DeleteBucket(ctx, i.IndexBucket()); err != nil {
		return fmt.Errorf("migration (down) %s: %w", i.MigrationName(), err)
	}

	return nil
}

// Populate does a full population of the index using the IndexSourceOn IndexMapping function.
// Once completed it marks the index as ready for use.
// It return a nil error on success and the count of inserted items.
func (i *IndexMigration) Populate(ctx context.Context, store Store) (n int, err error) {
	// verify the index to derive missing index
	// we can skip missing source lookup as we're
	// only interested in populating the missing index
	diff, err := indexVerify(ctx, i, store, i.removeDanglingForeignKeys)
	if err != nil {
		return 0, fmt.Errorf("looking up missing indexes: %w", err)
	}

	flush := func(batch kvSlice) error {
		if len(batch) == 0 {
			return nil
		}

		if err := store.Update(ctx, func(tx Tx) error {
			indexBucket, err := tx.Bucket(i.IndexBucket())
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
			key, err := IndexKey([]byte(fk), []byte(pk))
			if err != nil {
				return n, err
			}
			batch = append(batch, [2][]byte{key, []byte(pk)})

			if len(batch) >= i.operationBatchSize {
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

	if i.removeDanglingForeignKeys {
		return n, i.remove(ctx, store, diff.MissingFromSource)
	}

	return n, nil
}

func (i *IndexMigration) remove(ctx context.Context, store Store, mappings map[string]map[string]struct{}) error {
	var (
		batch [][]byte
		flush = func(batch [][]byte) error {
			if len(batch) == 0 {
				return nil
			}

			if err := store.Update(ctx, func(tx Tx) error {
				indexBucket, err := tx.Bucket(i.IndexBucket())
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
			key, err := IndexKey([]byte(fk), []byte(pk))
			if err != nil {
				return err
			}

			batch = append(batch, key)

			if len(batch) >= i.operationBatchSize {
				if err := flush(batch); err != nil {
					return err
				}

				batch = batch[:0]
			}
		}
	}

	return flush(batch)
}
