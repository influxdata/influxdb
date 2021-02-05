package tenant

import (
	"context"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/rand"
	"github.com/influxdata/influxdb/v2/snowflake"
	"github.com/influxdata/influxdb/v2/tenant/index"
)

const MaxIDGenerationN = 100

type Store struct {
	kvStore     kv.Store
	IDGen       influxdb.IDGenerator
	OrgIDGen    influxdb.IDGenerator
	BucketIDGen influxdb.IDGenerator

	now func() time.Time

	urmByUserIndex *kv.Index
}

type StoreOption func(*Store)

func NewStore(kvStore kv.Store, opts ...StoreOption) *Store {
	store := &Store{
		kvStore:     kvStore,
		IDGen:       snowflake.NewDefaultIDGenerator(),
		OrgIDGen:    rand.NewOrgBucketID(time.Now().UnixNano()),
		BucketIDGen: rand.NewOrgBucketID(time.Now().UnixNano()),
		now: func() time.Time {
			return time.Now().UTC()
		},
		urmByUserIndex: kv.NewIndex(index.URMByUserIndexMapping, kv.WithIndexReadPathEnabled),
	}

	for _, opt := range opts {
		opt(store)
	}

	return store
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

// generateSafeID attempts to create ids for buckets
// and orgs that are without backslash, commas, and spaces, BUT ALSO do not already exist.
func (s *Store) generateSafeID(ctx context.Context, tx kv.Tx, bucket []byte, gen influxdb.IDGenerator) (influxdb.ID, error) {
	for i := 0; i < MaxIDGenerationN; i++ {
		id := gen.ID()

		err := s.uniqueID(ctx, tx, bucket, id)
		if err == nil {
			return id, nil
		}

		if err == ErrIDNotUnique {
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

	return ErrIDNotUnique
}
