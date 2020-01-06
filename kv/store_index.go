package kv

import (
	"context"

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
		ent, err := s.EntStore.ConvertValToEntFn(k, v)
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

	decodedEnt, err := s.EntStore.ConvertValToEntFn(nil, existing)
	if err != nil {
		return err
	}

	return s.IndexStore.DeleteEnt(ctx, tx, decodedEnt)
}

// Find provides a mechanism for looking through the bucket via
// the set options. When a prefix is provided, the prefix is used with
// the index, and not the entity bucket. If you wish to look at the entity
// bucket and seek, then nest into the EntStore here and do that instead.
func (s *IndexStore) Find(ctx context.Context, tx Tx, opts FindOpts) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if len(opts.Prefix) == 0 {
		return s.EntStore.Find(ctx, tx, opts)
	}

	entCaptureFn := opts.CaptureFn
	kvStream, filterFn := s.indexFilterStream(ctx, tx, opts.FilterEntFn)
	opts.FilterEntFn = filterFn
	opts.CaptureFn = func(key []byte, indexVal interface{}) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case fromFilter, ok := <-kvStream:
			if !ok {
				return nil
			}
			if fromFilter.err != nil {
				return fromFilter.err
			}
			return entCaptureFn(fromFilter.k, fromFilter.v)
		}
	}
	return s.IndexStore.Find(ctx, tx, opts)

}

func (s *IndexStore) indexFilterStream(ctx context.Context, tx Tx, entFilterFn FilterFn) (<-chan struct {
	k   []byte
	v   interface{}
	err error
}, func([]byte, interface{}) bool) {

	kvStream := make(chan struct {
		k   []byte
		v   interface{}
		err error
	}, 1)

	type kve struct {
		k   []byte
		v   interface{}
		err error
	}

	send := func(key []byte, v interface{}, err error) bool {
		select {
		case <-ctx.Done():
		case kvStream <- kve{k: key, v: v, err: err}:
		}
		return true
	}

	return kvStream, func(key []byte, indexVal interface{}) (isValid bool) {
		defer func() {
			if !isValid {
				close(kvStream)
			}
		}()
		ent, err := s.IndexStore.ConvertValToEntFn(key, indexVal)
		if err != nil {
			return send(nil, nil, err)
		}

		entVal, err := s.EntStore.FindEnt(ctx, tx, ent)
		if err != nil {
			return send(nil, nil, err)
		}

		entKey, err := s.EntStore.EntKey(ctx, ent)
		if err != nil {
			return send(nil, nil, err)
		}

		if entFilterFn == nil {
			return send(entKey, entVal, nil)
		}

		if matches := entFilterFn(entKey, entVal); matches {
			return send(entKey, entVal, nil)
		}
		return false
	}
}

// FindEnt returns the decoded entity body via teh provided entity.
// An example entity should not include a Body, but rather the ID,
// Name, or OrgID. If no ID is provided, then the algorithm assumes
// you are looking up the entity by the index.
func (s *IndexStore) FindEnt(ctx context.Context, tx Tx, ent Entity) (interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	_, err := s.EntStore.EntKey(ctx, ent)
	if err != nil {
		if _, idxErr := s.IndexStore.EntKey(ctx, ent); idxErr != nil {
			return nil, &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "no key was provided for " + s.Resource,
			}
		}
	}

	if err != nil {
		return s.findByIndex(ctx, tx, ent)
	}
	return s.EntStore.FindEnt(ctx, tx, ent)
}

// Put will persist the entity into both the entity store and the index store.
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

	indexKey, err := s.IndexStore.EntKey(ctx, ent)
	if err != nil {
		return nil, err
	}

	indexEnt, err := s.IndexStore.ConvertValToEntFn(indexKey, idxEncodedID)
	if err != nil {
		return nil, err
	}

	return s.EntStore.FindEnt(ctx, tx, indexEnt)
}
