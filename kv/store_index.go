package kv

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	ierrors "github.com/influxdata/influxdb/v2/kit/errors"
	errors2 "github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kit/tracing"
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
// the set options. When a prefix is provided, it will be used within
// the entity store. If you would like to search the index store, then
// you can by calling the index store directly.
func (s *IndexStore) Find(ctx context.Context, tx Tx, opts FindOpts) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	return s.EntStore.Find(ctx, tx, opts)
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
			return nil, &errors2.Error{
				Code: errors2.EInvalid,
				Msg:  "no key was provided for " + s.Resource,
			}
		}
	}
	if err != nil {
		return s.findByIndex(ctx, tx, ent)
	}
	return s.EntStore.FindEnt(ctx, tx, ent)
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

// Put will persist the entity into both the entity store and the index store.
func (s *IndexStore) Put(ctx context.Context, tx Tx, ent Entity, opts ...PutOptionFn) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var opt putOption
	for _, o := range opts {
		if err := o(&opt); err != nil {
			return &errors2.Error{
				Code: errors2.EConflict,
				Err:  err,
			}
		}
	}

	if err := s.putValidate(ctx, tx, ent, opt); err != nil {
		return err
	}

	if err := s.IndexStore.Put(ctx, tx, ent); err != nil {
		return err
	}

	return s.EntStore.Put(ctx, tx, ent)
}

func (s *IndexStore) putValidate(ctx context.Context, tx Tx, ent Entity, opt putOption) error {
	if opt.isNew {
		return s.validNew(ctx, tx, ent)
	}
	if opt.isUpdate {
		return s.validUpdate(ctx, tx, ent)
	}
	return nil
}

func (s *IndexStore) validNew(ctx context.Context, tx Tx, ent Entity) error {
	_, err := s.IndexStore.FindEnt(ctx, tx, ent)
	if err == nil || errors2.ErrorCode(err) != errors2.ENotFound {
		key, _ := s.IndexStore.EntKey(ctx, ent)
		return &errors2.Error{
			Code: errors2.EConflict,
			Msg:  fmt.Sprintf("%s is not unique for key %s", s.Resource, string(key)),
			Err:  err,
		}
	}

	_, err = s.EntStore.FindEnt(ctx, tx, ent)
	if err == nil || errors2.ErrorCode(err) != errors2.ENotFound {
		return &errors2.Error{Code: errors2.EConflict, Err: err}
	}
	return nil
}

func (s *IndexStore) validUpdate(ctx context.Context, tx Tx, ent Entity) (e error) {
	// first check to make sure the existing entity exists in the ent store
	existingVal, err := s.EntStore.FindEnt(ctx, tx, Entity{PK: ent.PK})
	if err != nil {
		return err
	}

	defer func() {
		if e != nil {
			return
		}
		// we need to cleanup the unique key entry when this is deemed
		// a valid update
		pk, err := ent.PK()
		if err != nil {
			e = ierrors.Wrap(err, "failed to encode PK")
			return
		}
		existingEnt, err := s.EntStore.ConvertValToEntFn(pk, existingVal)
		if err != nil {
			e = ierrors.Wrap(err, "failed to convert value")
			return
		}
		e = s.IndexStore.DeleteEnt(ctx, tx, existingEnt)
	}()

	idxVal, err := s.IndexStore.FindEnt(ctx, tx, ent)
	if err != nil {
		if errors2.ErrorCode(err) == errors2.ENotFound {
			return nil
		}
		return err
	}

	idxKey, err := s.IndexStore.EntKey(ctx, ent)
	if err != nil {
		return err
	}

	indexEnt, err := s.IndexStore.ConvertValToEntFn(idxKey, idxVal)
	if err != nil {
		return err
	}

	if err := sameKeys(ent.PK, indexEnt.PK); err != nil {
		if _, err := s.EntStore.FindEnt(ctx, tx, ent); errors2.ErrorCode(err) == errors2.ENotFound {
			key, _ := ent.PK()
			return &errors2.Error{
				Code: errors2.ENotFound,
				Msg:  fmt.Sprintf("%s does not exist for key %s", s.Resource, string(key)),
				Err:  err,
			}
		}
		key, _ := indexEnt.UniqueKey()
		return &errors2.Error{
			Code: errors2.EConflict,
			Msg:  fmt.Sprintf("%s entity update conflicts with an existing entity for key %s", s.Resource, string(key)),
		}
	}

	return nil
}

func sameKeys(key1, key2 EncodeFn) error {
	pk1, err := key1()
	if err != nil {
		return err
	}
	pk2, err := key2()
	if err != nil {
		return err
	}

	if !bytes.Equal(pk1, pk2) {
		return errors.New("keys differ")
	}
	return nil
}
