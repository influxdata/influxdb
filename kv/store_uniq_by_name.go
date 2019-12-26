package kv

import (
	"context"
	"fmt"
	"strings"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/tracing"
)

type Entity struct {
	ID    influxdb.ID
	Name  string
	OrgID influxdb.ID
	Body  []byte
}

func (e Entity) indexKey(caseInsensitive bool) ([]byte, error) {
	return indexByOrgNameKey(e.OrgID, e.Name, caseInsensitive)
}

type uniqByNameStore struct {
	kv              Store
	caseInsensitive bool
	resource        string
	bktName         []byte
	indexName       []byte

	decodeBucketEntFn func(key, val []byte) ([]byte, interface{}, error)
	decodeOrgNameFn   func(body []byte) (orgID influxdb.ID, name string, err error)
}

func (s *uniqByNameStore) Init(ctx context.Context) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	return s.kv.Update(ctx, func(tx Tx) error {
		return s.initBuckets(ctx, tx)
	})
}

func (s *uniqByNameStore) initBuckets(ctx context.Context, tx Tx) error {
	bktFn := func(ctx context.Context, tx Tx, name []byte) error {
		span, _ := tracing.StartSpanFromContextWithOperationName(ctx, string(name)+"_bucket")
		defer span.Finish()

		_, err := tx.Bucket(name)
		return err
	}

	for _, name := range [][]byte{s.bktName, s.indexName} {
		if err := bktFn(ctx, tx, name); err != nil {
			return &influxdb.Error{
				Code: influxdb.EInternal,
				Msg:  fmt.Sprintf("failed to create index: %s", string(name)),
				Err:  err,
			}
		}
	}
	return nil
}

func (s *uniqByNameStore) Delete(ctx context.Context, tx Tx, id influxdb.ID) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	existing, err := s.FindByID(ctx, tx, id)
	if err != nil {
		return err
	}

	orgID, name, err := s.decodeOrgNameFn(existing)
	if err != nil {
		return &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  "failed to decoded body",
			Err:  err,
		}
	}

	if err := s.deleteEnt(ctx, tx, id); err != nil {
		return err
	}
	return s.deleteInIndex(ctx, tx, orgID, name)
}

func (s *uniqByNameStore) deleteEnt(ctx context.Context, tx Tx, id influxdb.ID) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	bkt, err := s.bucket(ctx, tx, s.bktName)
	if err != nil {
		return err
	}

	encodedID, err := id.Encode()
	if err != nil {
		return err
	}
	return bkt.Delete(encodedID)
}

func (s *uniqByNameStore) deleteInIndex(ctx context.Context, tx Tx, orgID influxdb.ID, name string) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	bkt, err := s.bucket(ctx, tx, s.indexName)
	if err != nil {
		return err
	}

	encodedID, err := indexByOrgNameKey(orgID, name, s.caseInsensitive)
	if err != nil {
		return err
	}

	return s.bucketDelete(ctx, bkt, encodedID)
}

func (s *uniqByNameStore) Find(ctx context.Context, tx Tx, opt influxdb.FindOptions, filterFn func(k []byte, v interface{}) bool, captureFn func(k []byte, v interface{})) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	bkt, err := s.bucket(ctx, tx, s.bktName)
	if err != nil {
		return err
	}

	cur, err := s.bucketCursor(ctx, bkt)
	if err != nil {
		return err
	}

	iter := &Iter{
		cursor:     cur,
		descending: opt.Descending,
		offset:     opt.Offset,
		limit:      opt.Limit,
		decodeFn:   s.decodeBucketEntFn,
		filterFn:   filterFn,
	}

	for k, v := iter.Next(ctx); k != nil; k, v = iter.Next(ctx) {
		captureFn(k, v)
	}
	return nil
}

func (s *uniqByNameStore) FindByID(ctx context.Context, tx Tx, id influxdb.ID) ([]byte, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	encodedID, err := id.Encode()
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  fmt.Sprintf("provided %s ID is an invalid format", s.resource),
		}
	}

	return s.findByKey(ctx, tx, s.bktName, encodedID)
}

func (s *uniqByNameStore) FindByName(ctx context.Context, tx Tx, orgID influxdb.ID, name string) ([]byte, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	indexKey, err := indexByOrgNameKey(orgID, name, s.caseInsensitive)
	if err != nil {
		return nil, err
	}

	encodedID, err := s.findByKey(ctx, tx, s.indexName, indexKey)
	if err != nil {
		return nil, err
	}

	return s.findByKey(ctx, tx, s.bktName, encodedID)
}

func (s *uniqByNameStore) findByKey(ctx context.Context, tx Tx, bktName, key []byte) ([]byte, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	bkt, err := s.bucket(ctx, tx, bktName)
	if err != nil {
		return nil, err
	}

	return s.bucketGet(ctx, bkt, key)
}

func (s *uniqByNameStore) Put(ctx context.Context, tx Tx, ent Entity) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if err := s.putIndexKey(ctx, tx, ent); err != nil {
		return err
	}

	return s.putEnt(ctx, tx, ent)
}

func (s *uniqByNameStore) putEnt(ctx context.Context, tx Tx, ent Entity) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	encodedID, err := ent.ID.Encode()
	if err != nil {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  fmt.Sprintf("invalid %s ID provided: %q", s.resource, ent.ID.String()),
			Err:  err,
		}
	}

	bkt, err := s.bucket(ctx, tx, s.bktName)
	if err != nil {
		return err
	}

	return s.bucketPut(ctx, bkt, encodedID, ent.Body)
}

func (s *uniqByNameStore) putIndexKey(ctx context.Context, tx Tx, ent Entity) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	indexKey, err := ent.indexKey(s.caseInsensitive)
	if err != nil {
		return err
	}

	encodedID, err := ent.ID.Encode()
	if err != nil {
		return err
	}

	indexBucket, err := s.bucket(ctx, tx, s.indexName)
	if err != nil {
		return err
	}

	return s.bucketPut(ctx, indexBucket, indexKey, encodedID)
}

func (s *uniqByNameStore) bucketCursor(ctx context.Context, b Bucket) (Cursor, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

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

func (s *uniqByNameStore) bucketDelete(ctx context.Context, b Bucket, key []byte) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if err := b.Delete(key); err != nil {
		return &influxdb.Error{
			Code: influxdb.EInternal,
			Err:  err,
		}
	}
	return nil
}

func (s *uniqByNameStore) bucketGet(ctx context.Context, b Bucket, key []byte) ([]byte, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	body, err := b.Get(key)
	if IsNotFound(err) {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  fmt.Sprintf("%s not found for key %q", s.resource, string(key)),
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

func (s *uniqByNameStore) bucketPut(ctx context.Context, b Bucket, key, body []byte) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if err := b.Put(key, body); err != nil {
		return &influxdb.Error{
			Code: influxdb.EInternal,
			Err:  err,
		}
	}
	return nil
}

func (s *uniqByNameStore) bucket(ctx context.Context, tx Tx, bktName []byte) (Bucket, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	bkt, err := tx.Bucket(bktName)
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  fmt.Sprintf("unexpected error retrieving bucket %q; Err %v", string(bktName), err),
			Err:  err,
		}
	}
	return bkt, nil
}

type Iter struct {
	cursor Cursor

	counter    int
	descending bool
	limit      int
	offset     int

	decodeFn func(key, val []byte) (k []byte, decodedVal interface{}, err error)
	filterFn func(key []byte, decodedVal interface{}) bool
}

func (i *Iter) Next(ctx context.Context) (key []byte, val interface{}) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()
	if i.limit > 0 && i.counter >= i.limit+i.offset {
		return nil, nil
	}

	var (
		k, vRaw []byte
		nextFn  func() ([]byte, []byte)
	)
	switch {
	case i.counter == 0 && i.descending:
		k, vRaw = i.cursor.Last()
		nextFn = i.cursor.Prev
	case i.counter == 0:
		k, vRaw = i.cursor.First()
		nextFn = i.cursor.Next
	case i.descending:
		k, vRaw = i.cursor.Prev()
		nextFn = i.cursor.Prev
	default:
		k, vRaw = i.cursor.Next()
		nextFn = i.cursor.Next
	}

	k, decodedVal, err := i.decodeFn(k, vRaw)
	for ; err == nil && len(k) > 0; k, decodedVal, err = i.decodeFn(nextFn()) {
		if i.isNext(k, decodedVal) {
			break
		}
	}
	return k, decodedVal
}

func (i *Iter) isNext(k []byte, v interface{}) bool {
	if len(k) == 0 {
		return true
	}

	if i.filterFn != nil && !i.filterFn(k, v) {
		return false
	}

	// increase counter here since the entity is a valid ent
	// and counts towards the total the user is looking for
	// 	i.e. limit = 5 => 5 valid ents
	//	i.e. offset = 5 => return valid ents after seeing 5 valid ents
	i.counter++

	if i.limit > 0 && i.counter >= i.limit+i.offset {
		return true
	}
	if i.offset > 0 && i.counter <= i.offset {
		return false
	}
	return true
}

func indexByOrgNameKey(orgID influxdb.ID, name string, caseInsensitive bool) ([]byte, error) {
	if name == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "name must be provided",
		}
	}

	orgIDEncoded, err := orgID.Encode()
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  fmt.Sprintf("invalid org ID provided: %q", orgID.String()),
			Err:  err,
		}
	}
	k := make([]byte, influxdb.IDLength+len(name))
	copy(k, orgIDEncoded)
	if caseInsensitive {
		name = strings.ToLower(name)
	}
	copy(k[influxdb.IDLength:], name)
	return k, nil
}
