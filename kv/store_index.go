package kv

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/tracing"
)

// IndexStore provides a entity store that uses an index lookup.
// The index store manages deleting and creating indexes for the
// caller. The index is automatically used if the FindEnt entity
// entity does not have the primary key.
type IndexStore struct {
	Resource   string
	EntStore   *StoreBase
	IndexStore *StoreBase
}

// Init creates the entity and index buckets.
func (s *IndexStore) Init(ctx context.Context, tx Tx) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	initFns := []func(context.Context, Tx) error{
		s.EntStore.Init,
		s.IndexStore.Init,
	}
	for _, fn := range initFns {
		if err := fn(ctx, tx); err != nil {
			return err
		}
	}
	return nil
}

// Delete deletes entities and associated indexes.
func (s *IndexStore) Delete(ctx context.Context, tx Tx, opts DeleteOpts) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	deleteIndexedRelationFn := func(k []byte, v interface{}) error {
		ent, err := s.EntStore.DecodeToEntFn(k, v)
		if err != nil {
			return err
		}
		return s.IndexStore.DeleteEnt(ctx, tx, ent)
	}
	opts.DeleteRelationFns = append(opts.DeleteRelationFns, deleteIndexedRelationFn)
	return s.EntStore.Delete(ctx, tx, opts)
}

// DeleteEnt deletes an entity and associated index.
func (s *IndexStore) DeleteEnt(ctx context.Context, tx Tx, ent Entity) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	existing, err := s.FindEnt(ctx, tx, ent)
	if err != nil {
		return err
	}

	if err := s.EntStore.DeleteEnt(ctx, tx, ent); err != nil {
		return err
	}

	decodedEnt, err := s.EntStore.DecodeToEntFn(nil, existing)
	if err != nil {
		return err
	}

	return s.IndexStore.DeleteEnt(ctx, tx, decodedEnt)
}

// Find provides a mechanism for looking through the bucket via
// the set options.
func (s *IndexStore) Find(ctx context.Context, tx Tx, opts FindOpts) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if len(opts.Prefix) == 0 {
		return s.EntStore.Find(ctx, tx, opts)
	}

	entCaptureFn := opts.CaptureFn
	kvStream, filterFn := s.indexFilterStream(ctx, tx, opts.FilterFn)
	opts.FilterFn = filterFn
	opts.CaptureFn = func(key []byte, indexVal interface{}) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case fromFilter, ok := <-kvStream:
			if !ok {
				return nil
			}
			return entCaptureFn(fromFilter.k, fromFilter.v)
		}
	}
	return s.IndexStore.Find(ctx, tx, opts)

}

func (s *IndexStore) indexFilterStream(ctx context.Context, tx Tx, entFilterFn FilterFn) (<-chan struct {
	k []byte
	v interface{}
}, func([]byte, interface{}) bool) {
	kvStream := make(chan struct {
		k []byte
		v interface{}
	}, 1)

	return kvStream, func(key []byte, indexVal interface{}) (isValid bool) {
		defer func() {
			if !isValid && kvStream != nil {
				close(kvStream)
			}
			kvStream = nil
		}()
		ent, err := s.IndexStore.DecodeToEntFn(key, indexVal)
		if err != nil {
			return false
		}

		entVal, err := s.EntStore.FindEnt(ctx, tx, ent)
		if err != nil {
			return false
		}

		entKey, err := s.EntStore.encodeEnt(ctx, ent, s.EntStore.EncodeEntKeyFn)
		if err != nil {
			return false
		}

		matches := entFilterFn(entKey, entVal)
		if matches {
			select {
			case <-ctx.Done():
				return false
			case kvStream <- struct {
				k []byte
				v interface{}
			}{k: entKey, v: entVal}:
			}
		}
		return matches
	}
}

// FindEnt returns the decoded entity body via teh provided entity.
// An example entity should not include a Body, but rather the ID,
// Name, or OrgID. If no ID is provided, then the algorithm assumes
// you are looking up the entity by the index.
func (s *IndexStore) FindEnt(ctx context.Context, tx Tx, ent Entity) (interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if ent.ID == 0 && ent.OrgID == 0 && ent.Name == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  fmt.Sprintf("no key was provided for %s", s.Resource),
		}
	}

	if ent.ID == 0 {
		return s.findByIndex(ctx, tx, ent)
	}
	return s.EntStore.FindEnt(ctx, tx, ent)
}

// Put will put the entity into both the entity store and the index store.
func (s *IndexStore) Put(ctx context.Context, tx Tx, ent Entity) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if err := s.IndexStore.Put(ctx, tx, ent); err != nil {
		return err
	}
	return s.EntStore.Put(ctx, tx, ent)
}

func (s *IndexStore) findByIndex(ctx context.Context, tx Tx, ent Entity) (interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	idxEncodedID, err := s.IndexStore.FindEnt(ctx, tx, ent)
	if err != nil {
		return nil, err
	}

	indexEnt, err := s.IndexStore.DecodeToEntFn([]byte{}, idxEncodedID)
	if err != nil {
		return nil, err
	}

	return s.EntStore.FindEnt(ctx, tx, indexEnt)
}
