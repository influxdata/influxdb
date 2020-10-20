package label

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
)

func (s *Store) CreateLabel(ctx context.Context, tx kv.Tx, l *influxdb.Label) error {
	// if the provided ID is invalid, or already maps to an existing Auth, then generate a new one
	id, err := s.generateSafeID(ctx, tx, labelBucket)
	if err != nil {
		return nil
	}
	l.ID = id

	v, err := json.Marshal(l)
	if err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	encodedID, err := l.ID.Encode()
	if err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	idx, err := tx.Bucket(labelIndex)
	if err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	key, err := labelIndexKey(l)
	if err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	if err := idx.Put([]byte(key), encodedID); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	b, err := tx.Bucket(labelBucket)
	if err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	if err := b.Put(encodedID, v); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	return nil
}

func (s *Store) ListLabels(ctx context.Context, tx kv.Tx, filter influxdb.LabelFilter) ([]*influxdb.Label, error) {
	ls := []*influxdb.Label{}
	filterFn := filterLabelsFn(filter)
	err := forEachLabel(ctx, tx, func(l *influxdb.Label) bool {
		if filterFn(l) {
			ls = append(ls, l)
		}
		return true
	})

	if err != nil {
		return nil, err
	}

	return ls, nil
}

func (s *Store) GetLabel(ctx context.Context, tx kv.Tx, id influxdb.ID) (*influxdb.Label, error) {
	encodedID, err := id.Encode()
	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	b, err := tx.Bucket(labelBucket)
	if err != nil {
		return nil, err
	}

	v, err := b.Get(encodedID)
	if kv.IsNotFound(err) {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  influxdb.ErrLabelNotFound,
		}
	}

	if err != nil {
		return nil, err
	}

	var l influxdb.Label
	if err := json.Unmarshal(v, &l); err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	return &l, nil
}

func (s *Store) UpdateLabel(ctx context.Context, tx kv.Tx, id influxdb.ID, upd influxdb.LabelUpdate) (*influxdb.Label, error) {
	label, err := s.GetLabel(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	if len(upd.Properties) > 0 && label.Properties == nil {
		label.Properties = make(map[string]string)
	}

	for k, v := range upd.Properties {
		if v == "" {
			delete(label.Properties, k)
		} else {
			label.Properties[k] = v
		}
	}

	if upd.Name != "" {
		upd.Name = strings.TrimSpace(upd.Name)

		idx, err := tx.Bucket(labelIndex)
		if err != nil {
			return nil, &influxdb.Error{
				Err: err,
			}
		}

		key, err := labelIndexKey(label)
		if err != nil {
			return nil, &influxdb.Error{
				Err: err,
			}
		}

		if err := idx.Delete(key); err != nil {
			return nil, &influxdb.Error{
				Err: err,
			}
		}

		label.Name = upd.Name
		if err := uniqueLabelName(ctx, tx, label); err != nil {
			return nil, &influxdb.Error{
				Err: err,
			}
		}
	}

	if err := label.Validate(); err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	v, err := json.Marshal(label)
	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	encodedID, err := label.ID.Encode()
	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	idx, err := tx.Bucket(labelIndex)
	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	key, err := labelIndexKey(label)
	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	if err := idx.Put([]byte(key), encodedID); err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	b, err := tx.Bucket(labelBucket)
	if err != nil {
		return nil, err
	}

	if err := b.Put(encodedID, v); err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	return label, nil
}

func (s *Store) DeleteLabel(ctx context.Context, tx kv.Tx, id influxdb.ID) error {
	label, err := s.GetLabel(ctx, tx, id)
	if err != nil {
		return ErrLabelNotFound
	}
	encodedID, idErr := id.Encode()
	if idErr != nil {
		return &influxdb.Error{
			Err: idErr,
		}
	}

	b, err := tx.Bucket(labelBucket)
	if err != nil {
		return err
	}

	if err := b.Delete(encodedID); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	idx, err := tx.Bucket(labelIndex)
	if err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	key, err := labelIndexKey(label)
	if err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	if err := idx.Delete(key); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	return nil
}

//********* Label Mappings *********//

func (s *Store) CreateLabelMapping(ctx context.Context, tx kv.Tx, m *influxdb.LabelMapping) error {
	v, err := json.Marshal(m)
	if err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	key, err := labelMappingKey(m)
	if err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	idx, err := tx.Bucket(labelMappingBucket)
	if err != nil {
		return err
	}

	if err := idx.Put(key, v); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	return nil
}

func (s *Store) FindResourceLabels(ctx context.Context, tx kv.Tx, filter influxdb.LabelMappingFilter, ls *[]*influxdb.Label) error {
	if !filter.ResourceID.Valid() {
		return &influxdb.Error{Code: influxdb.EInvalid, Msg: "filter requires a valid resource id", Err: influxdb.ErrInvalidID}
	}
	idx, err := tx.Bucket(labelMappingBucket)
	if err != nil {
		return err
	}

	prefix, err := filter.ResourceID.Encode()
	if err != nil {
		return err
	}

	cur, err := idx.ForwardCursor(prefix, kv.WithCursorPrefix(prefix))
	if err != nil {
		return err
	}

	for k, _ := cur.Next(); k != nil; k, _ = cur.Next() {
		_, id, err := decodeLabelMappingKey(k)
		if err != nil {
			return err
		}

		l, err := s.GetLabel(ctx, tx, id)
		if l == nil && err != nil {
			// TODO(jm): return error instead of continuing once orphaned mappings are fixed
			// (see https://github.com/influxdata/influxdb/issues/11278)
			continue
		}

		*ls = append(*ls, l)
	}

	if err := cur.Err(); err != nil {
		return err
	}

	return cur.Close()
}

func (s *Store) DeleteLabelMapping(ctx context.Context, tx kv.Tx, m *influxdb.LabelMapping) error {
	key, err := labelMappingKey(m)
	if err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	idx, err := tx.Bucket(labelMappingBucket)
	if err != nil {
		return err
	}

	if err := idx.Delete(key); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	return nil
}

//********* helper functions *********//

func labelMappingKey(m *influxdb.LabelMapping) ([]byte, error) {
	lid, err := m.LabelID.Encode()
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	rid, err := m.ResourceID.Encode()
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	key := make([]byte, influxdb.IDLength+influxdb.IDLength) // len(rid) + len(lid)
	copy(key, rid)
	copy(key[len(rid):], lid)

	return key, nil
}

// labelAlreadyExistsError is used when creating a new label with
// a name that has already been used. Label names must be unique.
func labelAlreadyExistsError(lbl *influxdb.Label) error {
	return &influxdb.Error{
		Code: influxdb.EConflict,
		Msg:  fmt.Sprintf("label with name %s already exists", lbl.Name),
	}
}

func labelIndexKey(l *influxdb.Label) ([]byte, error) {
	orgID, err := l.OrgID.Encode()
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	k := make([]byte, influxdb.IDLength+len(l.Name))
	copy(k, orgID)
	copy(k[influxdb.IDLength:], []byte(strings.ToLower((l.Name))))
	return k, nil
}

func filterLabelsFn(filter influxdb.LabelFilter) func(l *influxdb.Label) bool {
	return func(label *influxdb.Label) bool {
		return (filter.Name == "" || (strings.EqualFold(filter.Name, label.Name))) &&
			((filter.OrgID == nil) || (filter.OrgID != nil && *filter.OrgID == label.OrgID))
	}
}

func decodeLabelMappingKey(key []byte) (resourceID influxdb.ID, labelID influxdb.ID, err error) {
	if len(key) != 2*influxdb.IDLength {
		return 0, 0, &influxdb.Error{Code: influxdb.EInvalid, Msg: "malformed label mapping key (please report this error)"}
	}

	if err := (&resourceID).Decode(key[:influxdb.IDLength]); err != nil {
		return 0, 0, &influxdb.Error{Code: influxdb.EInvalid, Msg: "bad resource id", Err: influxdb.ErrInvalidID}
	}

	if err := (&labelID).Decode(key[influxdb.IDLength:]); err != nil {
		return 0, 0, &influxdb.Error{Code: influxdb.EInvalid, Msg: "bad label id", Err: influxdb.ErrInvalidID}
	}

	return resourceID, labelID, nil
}

func forEachLabel(ctx context.Context, tx kv.Tx, fn func(*influxdb.Label) bool) error {
	b, err := tx.Bucket(labelBucket)
	if err != nil {
		return err
	}

	cur, err := b.ForwardCursor(nil)
	if err != nil {
		return err
	}

	for k, v := cur.Next(); k != nil; k, v = cur.Next() {
		l := &influxdb.Label{}
		if err := json.Unmarshal(v, l); err != nil {
			return err
		}
		if !fn(l) {
			break
		}
	}

	if err := cur.Err(); err != nil {
		return err
	}

	return cur.Close()
}
