package kv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/tracing"
)

type Entity struct {
	ID    influxdb.ID
	Name  string
	OrgID influxdb.ID
	Body  interface{}
}

type EncodeEntFn func(ent Entity) ([]byte, string, error)

func EncIDKey(ent Entity) ([]byte, string, error) {
	id, err := ent.ID.Encode()
	return id, "ID", err
}

func EncOrgNameKeyFn(caseSensitive bool) func(ent Entity) ([]byte, string, error) {
	return func(ent Entity) ([]byte, string, error) {
		key, err := indexByOrgNameKey(ent.OrgID, ent.Name, caseSensitive)
		return key, "organization ID and name", err
	}
}

func EncBodyJSON(ent Entity) ([]byte, string, error) {
	v, err := json.Marshal(ent.Body)
	return v, "entity body", err
}

type DecodeBucketEntFn func(key, val []byte) (keyRepeat []byte, decodedVal interface{}, err error)

func DecIndexEntFn(key, val []byte) ([]byte, interface{}, error) {
	var i influxdb.ID
	return key, i, i.Decode(val)
}

type DecodedValToEntFn func(k []byte, v interface{}) (Entity, error)

func DecodeOrgNameKey(k []byte) (influxdb.ID, string, error) {
	var orgID influxdb.ID
	if err := orgID.Decode(k[:influxdb.IDLength]); err != nil {
		return 0, "", err
	}
	return orgID, string(k[influxdb.IDLength:]), nil
}

func NewOrgNameKeyStore(resource string, bktName []byte, caseSensitive bool) *StoreBase {
	var decValToEntFn DecodedValToEntFn = func(k []byte, v interface{}) (Entity, error) {
		id, ok := v.(influxdb.ID)
		if err := errUnexpectedDecodeVal(ok); err != nil {
			return Entity{}, err
		}

		ent := Entity{ID: id}
		if len(k) == 0 {
			return ent, nil
		}

		orgID, name, err := DecodeOrgNameKey(k)
		if err != nil {
			return Entity{}, err
		}
		ent.OrgID = orgID
		ent.Name = name
		return ent, nil
	}

	return NewStoreBase(resource, bktName, EncOrgNameKeyFn(caseSensitive), EncIDKey, DecIndexEntFn, decValToEntFn)
}

type StoreBase struct {
	Resource string
	BktName  []byte

	EncodeEntKeyFn  EncodeEntFn
	EncodeEntBodyFn EncodeEntFn
	DecodeEntFn     DecodeBucketEntFn
	DecodeToEntFn   DecodedValToEntFn
}

func NewStoreBase(resource string, bktName []byte, encKeyFn, encBodyFn EncodeEntFn, decFn DecodeBucketEntFn, decToEntFn DecodedValToEntFn) *StoreBase {
	return &StoreBase{
		Resource:        resource,
		BktName:         bktName,
		EncodeEntKeyFn:  encKeyFn,
		EncodeEntBodyFn: encBodyFn,
		DecodeEntFn:     decFn,
		DecodeToEntFn:   decToEntFn,
	}
}

func (s *StoreBase) EntKey(ctx context.Context, ent Entity) ([]byte, error) {
	return s.encodeEnt(ctx, ent, s.EncodeEntKeyFn)
}

func (s *StoreBase) Init(ctx context.Context, tx Tx) error {
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

type (
	DeleteOpts struct {
		DeleteRelationFns []DeleteRelationsFn
		FilterFn          FilterFn
	}

	DeleteRelationsFn func(key []byte, decodedVal interface{}) error
)

func (s *StoreBase) Delete(ctx context.Context, tx Tx, opts DeleteOpts) error {
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
		FilterFn: opts.FilterFn,
	}
	return s.Find(ctx, tx, findOpts)
}

func (s *StoreBase) DeleteEnt(ctx context.Context, tx Tx, ent Entity) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	encodedID, err := s.EntKey(ctx, ent)
	if err != nil {
		return err
	}
	return s.bucketDelete(ctx, tx, encodedID)
}

type (
	FindOpts struct {
		Descending bool
		Offset     int
		Limit      int
		Prefix     []byte
		CaptureFn  FindCaptureFn
		FilterFn   FilterFn
	}

	FindCaptureFn func(key []byte, decodedVal interface{}) error
	FilterFn      func(k []byte, v interface{}) bool
)

func (s *StoreBase) Find(ctx context.Context, tx Tx, opts FindOpts) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	cur, err := s.bucketCursor(ctx, tx)
	if err != nil {
		return err
	}

	iter := &Iter{
		cursor:     cur,
		descending: opts.Descending,
		limit:      opts.Limit,
		offset:     opts.Offset,
		prefix:     opts.Prefix,
		decodeFn:   s.DecodeEntFn,
		filterFn:   opts.FilterFn,
	}

	for k, v := iter.Next(ctx); k != nil; k, v = iter.Next(ctx) {
		if err := opts.CaptureFn(k, v); err != nil {
			return err
		}
	}
	return nil
}

func (s *StoreBase) FindEnt(ctx context.Context, tx Tx, ent Entity) (interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	encodedID, err := s.EntKey(ctx, ent)
	if err != nil {
		// TODO: fix this error up
		return nil, err
	}

	body, err := s.bucketGet(ctx, tx, encodedID)
	if err != nil {
		return nil, err
	}

	return s.decodeEnt(ctx, body)
}

func (s *StoreBase) Put(ctx context.Context, tx Tx, ent Entity) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	encodedID, err := s.EntKey(ctx, ent)
	if err != nil {
		return err
	}

	body, err := s.encodeEnt(ctx, ent, s.EncodeEntBodyFn)
	if err != nil {
		return err
	}

	return s.bucketPut(ctx, tx, encodedID, body)
}

func (s *StoreBase) bucket(ctx context.Context, tx Tx) (Bucket, error) {
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

func (s *StoreBase) bucketCursor(ctx context.Context, tx Tx) (Cursor, error) {
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

func (s *StoreBase) bucketDelete(ctx context.Context, tx Tx, key []byte) error {
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

func (s *StoreBase) bucketGet(ctx context.Context, tx Tx, key []byte) ([]byte, error) {
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

func (s *StoreBase) bucketPut(ctx context.Context, tx Tx, key, body []byte) error {
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

func (s *StoreBase) decodeEnt(ctx context.Context, body []byte) (interface{}, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	_, v, err := s.DecodeEntFn([]byte{}, body) // ignore key here
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  fmt.Sprintf("failed to decode %s body", s.Resource),
			Err:  err,
		}
	}
	return v, nil
}

func (s *StoreBase) encodeEnt(ctx context.Context, ent Entity, fn EncodeEntFn) ([]byte, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	encoded, field, err := fn(ent)
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  fmt.Sprintf("provided %s %s is an invalid format", s.Resource, field),
			Err:  err,
		}
	}
	return encoded, nil
}

type Iter struct {
	cursor Cursor

	counter    int
	descending bool
	limit      int
	offset     int
	prefix     []byte

	seekChan <-chan struct{ k, v []byte }

	decodeFn func(key, val []byte) (k []byte, decodedVal interface{}, err error)
	filterFn FilterFn
}

func (i *Iter) Next(ctx context.Context) (key []byte, val interface{}) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if i.limit > 0 && i.counter >= i.limit+i.offset {
		return nil, nil
	}

	var (
		k, vRaw []byte
		nextFn  func() ([]byte, []byte)
	)
	switch {
	case len(i.prefix) > 0:
		i.seek(ctx)
		nextFn = func() ([]byte, []byte) {
			kv := <-i.seekChan
			return kv.k, kv.v
		}
		k, vRaw = nextFn()
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

func (i *Iter) seek(ctx context.Context) {
	if i.seekChan != nil || len(i.prefix) == 0 {
		return
	}
	out := make(chan struct{ k, v []byte })
	i.seekChan = out

	go func() {
		defer close(out)

		for k, v := i.cursor.Seek(i.prefix); bytes.HasPrefix(k, i.prefix); k, v = i.cursor.Next() {
			if len(k) == 0 {
				return
			}

			select {
			case <-ctx.Done():
				return
			case out <- struct{ k, v []byte }{k: k, v: v}:
			}
		}
	}()

}

func indexByOrgNameKey(orgID influxdb.ID, name string, caseSensitive bool) ([]byte, error) {
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
	if !caseSensitive {
		name = strings.ToLower(name)
	}
	copy(k[influxdb.IDLength:], name)
	return k, nil
}

func errUnexpectedDecodeVal(ok bool) error {
	if ok {
		return nil
	}
	return errors.New("unexpected value decoded")
}
