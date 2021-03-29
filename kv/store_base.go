package kv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/influxdata/influxdb/v2/kit/platform"
	errors2 "github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/opentracing/opentracing-go"
)

type Entity struct {
	PK        EncodeFn
	UniqueKey EncodeFn

	Body interface{}
}

// EncodeEntFn encodes the entity. This is used both for the key and vals in the store base.
type EncodeEntFn func(ent Entity) ([]byte, string, error)

// EncIDKey encodes an entity into a key that represents the encoded ID provided.
func EncIDKey(ent Entity) ([]byte, string, error) {
	if ent.PK == nil {
		return nil, "ID", errors.New("no ID provided")
	}
	key, err := ent.PK()
	return key, "ID", err
}

// EncUniqKey encodes the unique key.
func EncUniqKey(ent Entity) ([]byte, string, error) {
	if ent.UniqueKey == nil {
		return nil, "Unique Key", errors.New("no unique key provided")
	}
	key, err := ent.UniqueKey()
	return key, "Unique Key", err
}

// EncBodyJSON JSON encodes the entity body and returns the raw bytes and indicates
// that it uses the entity body.
func EncBodyJSON(ent Entity) ([]byte, string, error) {
	v, err := json.Marshal(ent.Body)
	return v, "entity body", err
}

// DecodeBucketValFn decodes the raw []byte.
type DecodeBucketValFn func(key, val []byte) (keyRepeat []byte, decodedVal interface{}, err error)

// DecIndexID decodes the bucket val into an influxdb.ID.
func DecIndexID(key, val []byte) ([]byte, interface{}, error) {
	var i platform.ID
	return key, i, i.Decode(val)
}

// ConvertValToEntFn converts a key and decoded bucket value to an entity.
type ConvertValToEntFn func(k []byte, v interface{}) (Entity, error)

// DecodeOrgNameKey decodes a raw bucket key into the organization id and name
// used to create it.
func DecodeOrgNameKey(k []byte) (platform.ID, string, error) {
	var orgID platform.ID
	if err := orgID.Decode(k[:platform.IDLength]); err != nil {
		return 0, "", err
	}
	return orgID, string(k[platform.IDLength:]), nil
}

// NewOrgNameKeyStore creates a store for an entity's unique index on organization id and name.
// This is used throughout the kv pkg here to provide an entity uniquness by name within an org.
func NewOrgNameKeyStore(resource string, bktName []byte, caseSensitive bool) *StoreBase {
	var decValToEntFn ConvertValToEntFn = func(k []byte, v interface{}) (Entity, error) {
		id, ok := v.(platform.ID)
		if err := IsErrUnexpectedDecodeVal(ok); err != nil {
			return Entity{}, err
		}

		ent := Entity{PK: EncID(id)}
		if len(k) == 0 {
			return ent, nil
		}

		orgID, name, err := DecodeOrgNameKey(k)
		if err != nil {
			return Entity{}, err
		}
		nameEnc := EncString(name)
		if !caseSensitive {
			nameEnc = EncStringCaseInsensitive(name)
		}
		ent.UniqueKey = Encode(EncID(orgID), nameEnc)
		return ent, nil
	}

	return NewStoreBase(resource, bktName, EncUniqKey, EncIDKey, DecIndexID, decValToEntFn)
}

// StoreBase is the base behavior for accessing buckets in kv. It provides mechanisms that can
// be used in composing stores together (i.e. IndexStore).
type StoreBase struct {
	Resource string
	BktName  []byte

	EncodeEntKeyFn    EncodeEntFn
	EncodeEntBodyFn   EncodeEntFn
	DecodeEntFn       DecodeBucketValFn
	ConvertValToEntFn ConvertValToEntFn
}

// NewStoreBase creates a new store base.
func NewStoreBase(resource string, bktName []byte, encKeyFn, encBodyFn EncodeEntFn, decFn DecodeBucketValFn, decToEntFn ConvertValToEntFn) *StoreBase {
	return &StoreBase{
		Resource:          resource,
		BktName:           bktName,
		EncodeEntKeyFn:    encKeyFn,
		EncodeEntBodyFn:   encBodyFn,
		DecodeEntFn:       decFn,
		ConvertValToEntFn: decToEntFn,
	}
}

// EntKey returns the key for the entity provided. This is a shortcut for grabbing the EntKey without
// having to juggle the encoding funcs.
func (s *StoreBase) EntKey(ctx context.Context, ent Entity) ([]byte, error) {
	span, ctx := s.startSpan(ctx)
	defer span.Finish()
	return s.encodeEnt(ctx, ent, s.EncodeEntKeyFn)
}

type (
	// DeleteOpts provides indicators to the store.Delete call for deleting a given
	// entity. The FilterFn indicates the current value should be deleted when returning
	// true.
	DeleteOpts struct {
		DeleteRelationFns []DeleteRelationsFn
		FilterFn          FilterFn
	}

	// DeleteRelationsFn is a hook that a store that composes other stores can use to
	// delete an entity and any relations it may share. An example would be deleting an
	// an entity and its associated index.
	DeleteRelationsFn func(key []byte, decodedVal interface{}) error
)

// Delete deletes entities by the provided options.
func (s *StoreBase) Delete(ctx context.Context, tx Tx, opts DeleteOpts) error {
	span, ctx := s.startSpan(ctx)
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
func (s *StoreBase) DeleteEnt(ctx context.Context, tx Tx, ent Entity) error {
	span, ctx := s.startSpan(ctx)
	defer span.Finish()

	encodedID, err := s.EntKey(ctx, ent)
	if err != nil {
		return err
	}
	return s.bucketDelete(ctx, tx, encodedID)
}

type (
	// FindOpts provided a means to search through the bucket. When a filter func
	// is provided, that will run against the entity and if the filter responds true,
	// will count it towards the number of entries seen and the capture func will be
	// run with it provided to it.
	FindOpts struct {
		Descending  bool
		Offset      int
		Limit       int
		Prefix      []byte
		CaptureFn   FindCaptureFn
		FilterEntFn FilterFn
	}

	// FindCaptureFn is the mechanism for closing over the key and decoded value pair
	// for adding results to the call sites collection. This generic implementation allows
	// it to be reused. The returned decodedVal should always satisfy whatever decoding
	// of the bucket value was set on the storeo that calls Find.
	FindCaptureFn func(key []byte, decodedVal interface{}) error

	// FilterFn will provide an indicator to the Find or Delete calls that the entity that
	// was seen is one that is valid and should be either captured or deleted (depending on
	// the caller of the filter func).
	FilterFn func(key []byte, decodedVal interface{}) bool
)

// Find provides a mechanism for looking through the bucket via
// the set options. When a prefix is provided, the prefix is used to
// seek the bucket.
func (s *StoreBase) Find(ctx context.Context, tx Tx, opts FindOpts) error {
	span, ctx := s.startSpan(ctx)
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
		decodeFn:   s.DecodeEntFn,
		filterFn:   opts.FilterEntFn,
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
func (s *StoreBase) FindEnt(ctx context.Context, tx Tx, ent Entity) (interface{}, error) {
	span, ctx := s.startSpan(ctx)
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

type (
	putOption struct {
		isNew    bool
		isUpdate bool
	}

	// PutOptionFn provides a hint to the store to make some guarantees about the
	// put action. I.e. If it is new, then will validate there is no existing entity
	// by the given PK.
	PutOptionFn func(o *putOption) error
)

// PutNew will create an entity that is not does not already exist. Guarantees uniqueness
// by the store's uniqueness guarantees.
func PutNew() PutOptionFn {
	return func(o *putOption) error {
		o.isNew = true
		return nil
	}
}

// PutUpdate will update an entity that must already exist.
func PutUpdate() PutOptionFn {
	return func(o *putOption) error {
		o.isUpdate = true
		return nil
	}
}

// Put will persist the entity.
func (s *StoreBase) Put(ctx context.Context, tx Tx, ent Entity, opts ...PutOptionFn) error {
	span, ctx := s.startSpan(ctx)
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

func (s *StoreBase) putValidate(ctx context.Context, tx Tx, ent Entity, opt putOption) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if !opt.isUpdate && !opt.isNew {
		return nil
	}

	_, err := s.FindEnt(ctx, tx, ent)
	if opt.isNew {
		if err == nil || errors2.ErrorCode(err) != errors2.ENotFound {
			return &errors2.Error{
				Code: errors2.EConflict,
				Msg:  fmt.Sprintf("%s is not unique", s.Resource),
				Err:  err,
			}
		}
		return nil
	}
	return err
}

func (s *StoreBase) bucket(ctx context.Context, tx Tx) (Bucket, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	bkt, err := tx.Bucket(s.BktName)
	if err != nil {
		return nil, &errors2.Error{
			Code: errors2.EInternal,
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
		return nil, &errors2.Error{
			Code: errors2.EInternal,
			Msg:  "failed to retrieve cursor",
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

	iErr := &errors2.Error{
		Code: errors2.EInternal,
		Err:  err,
	}
	if IsNotFound(err) {
		iErr.Code = errors2.ENotFound
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
		return nil, &errors2.Error{
			Code: errors2.ENotFound,
			Msg:  fmt.Sprintf("%s not found for key %q", s.Resource, string(key)),
		}
	}
	if err != nil {
		return nil, &errors2.Error{
			Code: errors2.EInternal,
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
		return &errors2.Error{
			Code: errors2.EInternal,
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
		return nil, &errors2.Error{
			Code: errors2.EInternal,
			Msg:  fmt.Sprintf("failed to decode %s body", s.Resource),
			Err:  err,
		}
	}
	return v, nil
}

func (s *StoreBase) encodeEnt(ctx context.Context, ent Entity, fn EncodeEntFn) ([]byte, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if fn == nil {
		return nil, &errors2.Error{
			Code: errors2.EInvalid,
			Msg:  fmt.Sprintf("no key was provided for %s", s.Resource),
		}
	}

	encoded, field, err := fn(ent)
	if err != nil {
		return encoded, &errors2.Error{
			Code: errors2.EInvalid,
			Msg:  fmt.Sprintf("provided %s %s is an invalid format", s.Resource, field),
			Err:  err,
		}
	}
	return encoded, nil
}

func (s *StoreBase) startSpan(ctx context.Context) (opentracing.Span, context.Context) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	span.SetTag("Bucket", string(s.BktName))
	span.SetTag("Resource", s.Resource)
	return span, ctx
}

type iterator struct {
	cursor Cursor

	counter    int
	descending bool
	limit      int
	offset     int
	prefix     []byte

	nextFn func() (key, val []byte)

	decodeFn func(key, val []byte) (k []byte, decodedVal interface{}, err error)
	filterFn FilterFn
}

func (i *iterator) Next(ctx context.Context) (key []byte, val interface{}, err error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if i.limit > 0 && i.counter >= i.limit+i.offset {
		return nil, nil, nil
	}

	var k, vRaw []byte
	switch {
	case i.nextFn != nil:
		k, vRaw = i.nextFn()
	case len(i.prefix) > 0:
		k, vRaw = i.cursor.Seek(i.prefix)
		i.nextFn = i.cursor.Next
	case i.descending:
		k, vRaw = i.cursor.Last()
		i.nextFn = i.cursor.Prev
	default:
		k, vRaw = i.cursor.First()
		i.nextFn = i.cursor.Next
	}

	k, decodedVal, err := i.decodeFn(k, vRaw)
	for ; ; k, decodedVal, err = i.decodeFn(i.nextFn()) {
		if err != nil {
			return nil, nil, err
		}
		if i.isNext(k, decodedVal) {
			break
		}
	}
	return k, decodedVal, nil
}

func (i *iterator) isNext(k []byte, v interface{}) bool {
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

func IsErrUnexpectedDecodeVal(ok bool) error {
	if ok {
		return nil
	}
	return errors.New("unexpected value decoded")
}
