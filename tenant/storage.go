package tenant

import (
	"context"

	"github.com/influxdata/influxdb/kv"
)

type Store struct {
	kvStore kv.Store
}

func NewStore(kvStore kv.Store) (*Store, error) {
	st := &Store{
		kvStore: kvStore,
	}
	return st, st.setup()
}

// View opens up a transaction that will not write to any data. Implementing interfaces
// should take care to ensure that all view transactions do not mutate any data.
func (s *Store) View(ctx context.Context, fn func(kv.Tx) error) error {
	return s.kvStore.View(ctx, fn)
}

// Update opens up a transaction that will mutate data.
func (s *Store) Update(ctx context.Context, fn func(kv.Tx) error) error {
	return s.kvStore.Update(ctx, fn)
}

func (s *Store) setup() error {
	return s.Update(context.Background(), func(tx kv.Tx) error {
		if _, err := tx.Bucket(userBucket); err != nil {
			return err
		}

		if _, err := tx.Bucket(userIndex); err != nil {
			return err
		}

		if _, err := tx.Bucket(urmBucket); err != nil {
			return err
		}
		if _, err := tx.Bucket(organizationBucket); err != nil {
			return err
		}

		if _, err := tx.Bucket(organizationIndex); err != nil {
			return err
		}

		if _, err := tx.Bucket(bucketBucket); err != nil {
			return err
		}

		if _, err := tx.Bucket(bucketIndex); err != nil {
			return err
		}

		return nil
	})
}
