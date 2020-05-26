package tenant

import (
	"context"
	"encoding/json"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/snowflake"
)

const MaxIDGenerationN = 100
const ReservedIDs = 1000

type Store struct {
	kvStore        kv.Store
	IDGen          influxdb.IDGenerator
	urmByUserIndex *kv.Index
}

func NewStore(kvStore kv.Store) (*Store, error) {
	st := &Store{
		kvStore: kvStore,
		IDGen:   snowflake.NewDefaultIDGenerator(),
		urmByUserIndex: kv.NewIndex(kv.NewIndexMapping(
			urmBucket,
			urmByUserIndexBucket,
			func(v []byte) ([]byte, error) {
				var urm influxdb.UserResourceMapping
				if err := json.Unmarshal(v, &urm); err != nil {
					return nil, err
				}

				id, _ := urm.UserID.Encode()
				return id, nil
			},
		), kv.WithIndexReadPathEnabled),
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

		if _, err := tx.Bucket(userpasswordBucket); err != nil {
			return err
		}

		if _, err := tx.Bucket(urmBucket); err != nil {
			return err
		}

		if _, err := tx.Bucket(urmByUserIndexBucket); err != nil {
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

// generateSafeID attempts to create ids for buckets
// and orgs that are without backslash, commas, and spaces, BUT ALSO do not already exist.
func (s *Store) generateSafeID(ctx context.Context, tx kv.Tx, bucket []byte) (influxdb.ID, error) {
	for i := 0; i < MaxIDGenerationN; i++ {
		id := s.IDGen.ID()

		// TODO: this is probably unnecessary but for testing we need to keep it in.
		// After KV is cleaned out we can update the tests and remove this.
		if id < ReservedIDs {
			continue
		}

		err := s.uniqueID(ctx, tx, bucket, id)
		if err == nil {
			return id, nil
		}

		if err == NotUniqueIDError {
			continue
		}

		return influxdb.InvalidID(), err
	}
	return influxdb.InvalidID(), ErrFailureGeneratingID
}

func (s *Store) uniqueID(ctx context.Context, tx kv.Tx, bucket []byte, id influxdb.ID) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	encodedID, err := id.Encode()
	if err != nil {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	b, err := tx.Bucket(bucket)
	if err != nil {
		return err
	}

	_, err = b.Get(encodedID)
	if kv.IsNotFound(err) {
		return nil
	}

	return NotUniqueIDError
}
