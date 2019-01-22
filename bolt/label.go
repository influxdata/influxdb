package bolt

import (
	"bytes"
	"context"
	"encoding/json"

	bolt "github.com/coreos/bbolt"
	influxdb "github.com/influxdata/influxdb"
)

var (
	labelBucket        = []byte("labelsv1")
	labelMappingBucket = []byte("labelmappingsv1")
)

func (c *Client) initializeLabels(ctx context.Context, tx *bolt.Tx) error {
	if _, err := tx.CreateBucketIfNotExists([]byte(labelBucket)); err != nil {
		return err
	}

	if _, err := tx.CreateBucketIfNotExists([]byte(labelMappingBucket)); err != nil {
		return err
	}

	return nil
}

// FindLabelByID finds a label by its ID
func (c *Client) FindLabelByID(ctx context.Context, id influxdb.ID) (*influxdb.Label, error) {
	var l *influxdb.Label

	err := c.db.View(func(tx *bolt.Tx) error {
		label, pe := c.findLabelByID(ctx, tx, id)
		if pe != nil {
			return pe
		}
		l = label
		return nil
	})

	if err != nil {
		return nil, &influxdb.Error{
			Op:  getOp(influxdb.OpFindLabelByID),
			Err: err,
		}
	}

	return l, nil
}

func (c *Client) findLabelByID(ctx context.Context, tx *bolt.Tx, id influxdb.ID) (*influxdb.Label, *influxdb.Error) {
	encodedID, err := id.Encode()
	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	var l influxdb.Label
	v := tx.Bucket(labelBucket).Get(encodedID)

	if len(v) == 0 {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Err:  influxdb.ErrLabelNotFound,
		}
	}

	if err := json.Unmarshal(v, &l); err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	return &l, nil
}

func filterLabelsFn(filter influxdb.LabelFilter) func(l *influxdb.Label) bool {
	return func(label *influxdb.Label) bool {
		return (filter.Name == "" || (filter.Name == label.Name))
	}
}

// FindLabels returns a list of labels that match a filter.
func (c *Client) FindLabels(ctx context.Context, filter influxdb.LabelFilter, opt ...influxdb.FindOptions) ([]*influxdb.Label, error) {
	ls := []*influxdb.Label{}
	err := c.db.View(func(tx *bolt.Tx) error {
		labels, err := c.findLabels(ctx, tx, filter)
		if err != nil {
			return err
		}
		ls = labels
		return nil
	})

	if err != nil {
		return nil, err
	}

	return ls, nil
}

func (c *Client) findLabels(ctx context.Context, tx *bolt.Tx, filter influxdb.LabelFilter) ([]*influxdb.Label, error) {
	ls := []*influxdb.Label{}
	filterFn := filterLabelsFn(filter)
	err := c.forEachLabel(ctx, tx, func(l *influxdb.Label) bool {
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

// func filterMappingsFn(filter influxdb.LabelMappingFilter) func(m *influxdb.LabelMapping) bool {
// 	return func(mapping *influxdb.LabelMapping) bool {
// 		return (filter.ResourceID.String() == mapping.ResourceID.String()) &&
// 			(filter.LabelID == nil || filter.LabelID == mapping.LabelID)
// 	}
// }

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

func (c *Client) FindResourceLabels(ctx context.Context, filter influxdb.LabelMappingFilter) ([]*influxdb.Label, error) {
	if !filter.ResourceID.Valid() {
		return nil, &influxdb.Error{Code: influxdb.EInvalid, Msg: "filter requires a valid resource id", Err: influxdb.ErrInvalidID}
	}

	ls := []*influxdb.Label{}
	err := c.db.View(func(tx *bolt.Tx) error {
		cur := tx.Bucket(labelMappingBucket).Cursor()
		prefix, err := filter.ResourceID.Encode()
		if err != nil {
			return err
		}

		for k, _ := cur.Seek(prefix); bytes.HasPrefix(k, prefix); k, _ = cur.Next() {
			_, id, err := decodeLabelMappingKey(k)
			if err != nil {
				return err
			}

			l, err := c.findLabelByID(ctx, tx, id)
			if l == nil && err != nil {
				// TODO(jm): return error instead of continuing once orphaned mappings are fixed
				// (see https://github.com/influxdata/influxdb/issues/11278)
				continue
			}

			ls = append(ls, l)
		}
		return nil
	})

	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
			Op:  getOp(influxdb.OpFindLabelMapping),
		}
	}

	return ls, nil
}

// CreateLabelMapping creates a new mapping between a resource and a label.
func (c *Client) CreateLabelMapping(ctx context.Context, m *influxdb.LabelMapping) error {
	_, err := c.FindLabelByID(ctx, *m.LabelID)
	if err != nil {
		return &influxdb.Error{
			Err: err,
			Op:  getOp(influxdb.OpCreateLabel),
		}
	}

	err = c.db.Update(func(tx *bolt.Tx) error {
		return c.putLabelMapping(ctx, tx, m)
	})

	if err != nil {
		return &influxdb.Error{
			Err: err,
			Op:  getOp(influxdb.OpCreateLabel),
		}
	}

	return nil
}

// DeleteLabelMapping deletes a label mapping.
func (c *Client) DeleteLabelMapping(ctx context.Context, m *influxdb.LabelMapping) error {
	err := c.db.Update(func(tx *bolt.Tx) error {
		return c.deleteLabelMapping(ctx, tx, m)
	})
	if err != nil {
		return &influxdb.Error{
			Op:  getOp(influxdb.OpDeleteLabelMapping),
			Err: err,
		}
	}
	return nil
}

func (c *Client) deleteLabelMapping(ctx context.Context, tx *bolt.Tx, m *influxdb.LabelMapping) error {
	key, err := labelMappingKey(m)
	if err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	if err := tx.Bucket(labelMappingBucket).Delete(key); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	return nil
}

// CreateLabel creates a new label.
func (c *Client) CreateLabel(ctx context.Context, l *influxdb.Label) error {
	err := c.db.Update(func(tx *bolt.Tx) error {
		l.ID = c.IDGenerator.ID()

		return c.putLabel(ctx, tx, l)
	})

	if err != nil {
		return &influxdb.Error{
			Err: err,
			Op:  getOp(influxdb.OpCreateLabel),
		}
	}
	return nil
}

// PutLabel creates a label from the provided struct, without generating a new ID.
func (c *Client) PutLabel(ctx context.Context, l *influxdb.Label) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		var err error
		pe := c.putLabel(ctx, tx, l)
		if pe != nil {
			err = pe
		}
		return err
	})
}

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

	key := make([]byte, len(rid)+len(lid))
	copy(key, rid)
	copy(key[len(rid):], lid)

	return key, nil
}

func (c *Client) forEachLabel(ctx context.Context, tx *bolt.Tx, fn func(*influxdb.Label) bool) error {
	cur := tx.Bucket(labelBucket).Cursor()
	for k, v := cur.First(); k != nil; k, v = cur.Next() {
		l := &influxdb.Label{}
		if err := json.Unmarshal(v, l); err != nil {
			return err
		}
		if !fn(l) {
			break
		}
	}

	return nil
}

// UpdateLabel updates a label.
func (c *Client) UpdateLabel(ctx context.Context, id influxdb.ID, upd influxdb.LabelUpdate) (*influxdb.Label, error) {
	var label *influxdb.Label
	err := c.db.Update(func(tx *bolt.Tx) error {
		labelResponse, pe := c.updateLabel(ctx, tx, id, upd)
		if pe != nil {
			return &influxdb.Error{
				Err: pe,
				Op:  getOp(influxdb.OpUpdateLabel),
			}
		}
		label = labelResponse
		return nil
	})

	return label, err
}

func (c *Client) updateLabel(ctx context.Context, tx *bolt.Tx, id influxdb.ID, upd influxdb.LabelUpdate) (*influxdb.Label, error) {
	label, err := c.findLabelByID(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	if label.Properties == nil {
		label.Properties = make(map[string]string)
	}

	for k, v := range upd.Properties {
		if v == "" {
			delete(label.Properties, k)
		} else {
			label.Properties[k] = v
		}
	}

	if err := label.Validate(); err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	if err := c.putLabel(ctx, tx, label); err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	return label, nil
}

// set a label and overwrite any existing label
func (c *Client) putLabel(ctx context.Context, tx *bolt.Tx, l *influxdb.Label) error {
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

	if err := tx.Bucket(labelBucket).Put(encodedID, v); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	return nil
}

// PutLabelMapping writes a label mapping to boltdb
func (c *Client) PutLabelMapping(ctx context.Context, m *influxdb.LabelMapping) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		var err error
		pe := c.putLabelMapping(ctx, tx, m)
		if pe != nil {
			err = pe
		}
		return err
	})
}

func (c *Client) putLabelMapping(ctx context.Context, tx *bolt.Tx, m *influxdb.LabelMapping) error {
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

	if err := tx.Bucket(labelMappingBucket).Put(key, v); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	return nil
}

// DeleteLabel deletes a label.
func (c *Client) DeleteLabel(ctx context.Context, id influxdb.ID) error {
	err := c.db.Update(func(tx *bolt.Tx) error {
		return c.deleteLabel(ctx, tx, id)
	})
	if err != nil {
		return &influxdb.Error{
			Op:  getOp(influxdb.OpDeleteLabel),
			Err: err,
		}
	}
	return nil
}

func (c *Client) deleteLabel(ctx context.Context, tx *bolt.Tx, id influxdb.ID) error {
	_, err := c.findLabelByID(ctx, tx, id)
	if err != nil {
		return err
	}
	encodedID, idErr := id.Encode()
	if idErr != nil {
		return &influxdb.Error{
			Err: idErr,
		}
	}

	return tx.Bucket(labelBucket).Delete(encodedID)
}

// func (c *Client) deleteLabels(ctx context.Context, tx *bolt.Tx, filter influxdb.LabelFilter) error {
// 	ls, err := c.findLabels(ctx, tx, filter)
// 	if err != nil {
// 		return err
// 	}
// 	for _, l := range ls {
// 		encodedID, idErr := l.ID.Encode()
// 		if idErr != nil {
// 			return &influxdb.Error{
// 				Err: idErr,
// 			}
// 		}
//
// 		if err = tx.Bucket(labelBucket).Delete(encodedID); err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }
