package kv

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/tracing"
)

type (
	PrimaryKey interface {
		PrimaryKey() ([]byte, error)
	}

	EntityInt interface {
		PrimaryKey
		Encode() ([]byte, error)
	}

	UniqueIdentifiers interface {
		PrimaryKey
		UniqueKey() ([]byte, error)
	}

	EntityUnique interface {
		UniqueIdentifiers
		Encode() ([]byte, error)
	}

	DecodeBucketValFn2 func(key, val []byte) (ent EntityInt, err error)
)

// StoreBase2 is the base behavior for accessing buckets in kv. It provides mechanisms that can
// be used in composing stores together (i.e. IndexStore).
type StoreBase2 struct {
	Resource string
	BktName  []byte
	DecodeFn DecodeBucketValFn2
}

// NewStoreBase2 creates a new store base.
func NewStoreBase2(resource string, bktName []byte, decFn DecodeBucketValFn2) *StoreBase2 {
	return &StoreBase2{
		Resource: resource,
		BktName:  bktName,
		DecodeFn: decFn,
	}
}

// Init creates the buckets.
func (s *StoreBase2) Init(ctx context.Context, tx Tx) error {
	span, ctx := tracing.StartSpanFromContextWithOperationName(ctx, "bucket_"+string(s.BktName))
	defer span.Finish()

	if _, err := s.bucket(ctx, tx); err != nil {
		return &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  fmt.Sprintf("failed to create bucket: %s", string(s.BktName)),
			Err:  err,
		}
	}
	return nil
}

// Delete deletes entities by the provided options.
func (s *StoreBase2) Delete(ctx context.Context, tx Tx, opts DeleteOpts) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if opts.FilterFn == nil {
		return nil
	}

	findOpts := FindOpts{
		CaptureFn: func(k []byte, v interface{}) error {
			for _, deleteFn := range opts.DeleteRelationFns {
				if err := deleteFn(k, v); err != nil {
					return err
				}
			}
			return s.bucketDelete(ctx, tx, k)
		},
		FilterEntFn: opts.FilterFn,
	}
	return s.Find(ctx, tx, findOpts)
}

// DeleteEnt deletes an entity.
func (s *StoreBase2) DeleteEnt(ctx context.Context, tx Tx, ent PrimaryKey) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	pk, err := s.encode(ctx, ent.PrimaryKey, "primary key")
	if err != nil {
		return err
	}
	return s.bucketDelete(ctx, tx, pk)
}

func (s *StoreBase2) Find(ctx context.Context, tx Tx, opts FindOpts) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	cur, err := s.bucketCursor(ctx, tx)
	if err != nil {
		return err
	}

	iter := &iterator{
		cursor:     cur,
		descending: opts.Descending,
		limit:      opts.Limit,
		offset:     opts.Offset,
		prefix:     opts.Prefix,
		decodeFn: func(key, val []byte) ([]byte, interface{}, error) {
			v, err := s.DecodeFn(key, val)
			return key, v, err
		},
		filterFn: opts.FilterEntFn,
	}

	for k, v, err := iter.Next(ctx); k != nil; k, v, err = iter.Next(ctx) {
		if err != nil {
			return err
		}
		if err := opts.CaptureFn(k, v); err != nil {
			return err
		}
	}
	return nil
}

// FindEnt returns the decoded entity body via the provided entity.
// An example entity should not include a Body, but rather the ID,
// Name, or OrgID.
func (s *StoreBase2) FindEnt(ctx context.Context, tx Tx, entDec PrimaryKey) (EntityInt, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	pk, err := s.encode(ctx, entDec.PrimaryKey, "primary key")
	if err != nil {
		return nil, err
	}

	body, err := s.bucketGet(ctx, tx, pk)
	if err != nil {
		return nil, err
	}

	return s.DecodeFn(pk, body)
}

func (s *StoreBase2) Put(ctx context.Context, tx Tx, ent EntityInt) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	pk, err := s.encode(ctx, ent.PrimaryKey, "primary key")
	if err != nil {
		return err
	}

	body, err := s.encode(ctx, ent.Encode, "body")
	if err != nil {
		return err
	}

	return s.bucketPut(ctx, tx, pk, body)
}

func (s *StoreBase2) bucket(ctx context.Context, tx Tx) (Bucket, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	bkt, err := tx.Bucket(s.BktName)
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  fmt.Sprintf("unexpected error retrieving bucket %q; Err %v", string(s.BktName), err),
			Err:  err,
		}
	}
	return bkt, nil
}

func (s *StoreBase2) bucketCursor(ctx context.Context, tx Tx) (Cursor, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	b, err := s.bucket(ctx, tx)
	if err != nil {
		return nil, err
	}

	cur, err := b.Cursor()
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  fmt.Sprintf("failed to retrieve cursor"),
			Err:  err,
		}
	}
	return cur, nil
}

func (s *StoreBase2) bucketDelete(ctx context.Context, tx Tx, key []byte) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	b, err := s.bucket(ctx, tx)
	if err != nil {
		return err
	}

	err = b.Delete(key)
	if err == nil {
		return nil
	}

	iErr := &influxdb.Error{
		Code: influxdb.EInternal,
		Err:  err,
	}
	if IsNotFound(err) {
		iErr.Code = influxdb.ENotFound
		iErr.Msg = fmt.Sprintf("%s does exist for key: %q", s.Resource, string(key))
	}
	return iErr
}

func (s *StoreBase2) bucketGet(ctx context.Context, tx Tx, key []byte) ([]byte, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	b, err := s.bucket(ctx, tx)
	if err != nil {
		return nil, err
	}

	body, err := b.Get(key)
	if IsNotFound(err) {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  fmt.Sprintf("%s not found for key %q", s.Resource, string(key)),
		}
	}
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInternal,
			Err:  err,
		}
	}

	return body, nil
}

func (s *StoreBase2) bucketPut(ctx context.Context, tx Tx, key, body []byte) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	b, err := s.bucket(ctx, tx)
	if err != nil {
		return err
	}

	if err := b.Put(key, body); err != nil {
		return &influxdb.Error{
			Code: influxdb.EInternal,
			Err:  err,
		}
	}
	return nil
}

func (s *StoreBase2) decode(ctx context.Context, key, body []byte) (EntityInt, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	ent, err := s.DecodeFn(key, body)
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  fmt.Sprintf("failed to decode %s body", s.Resource),
			Err:  err,
		}
	}
	return ent, nil
}

func (s *StoreBase2) encode(ctx context.Context, fn EncodeFn, field string) ([]byte, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if fn == nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  fmt.Sprintf("no key was provided for %s", s.Resource),
		}
	}

	encoded, err := fn()
	if err != nil {
		return encoded, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  fmt.Sprintf("provided %s %s is an invalid format", s.Resource, field),
			Err:  err,
		}
	}
	return encoded, nil
}
