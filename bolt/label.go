package bolt

import (
	"context"
	"encoding/json"

	bolt "github.com/coreos/bbolt"
	platform "github.com/influxdata/influxdb"
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
func (c *Client) FindLabelByID(ctx context.Context, id platform.ID) (*platform.Label, error) {
	var l *platform.Label

	err := c.db.View(func(tx *bolt.Tx) error {
		label, pe := c.findLabelByID(ctx, tx, id)
		if pe != nil {
			return pe
		}
		l = label
		return nil
	})

	if err != nil {
		return nil, &platform.Error{
			Op:  getOp(platform.OpFindLabelByID),
			Err: err,
		}
	}

	return l, nil
}

func (c *Client) findLabelByID(ctx context.Context, tx *bolt.Tx, id platform.ID) (*platform.Label, *platform.Error) {
	encodedID, err := id.Encode()
	if err != nil {
		return nil, &platform.Error{
			Err: err,
		}
	}

	var l platform.Label
	v := tx.Bucket(labelBucket).Get(encodedID)

	if len(v) == 0 {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  "label not found",
		}
	}

	if err := json.Unmarshal(v, &l); err != nil {
		return nil, &platform.Error{
			Err: err,
		}
	}

	return &l, nil
}

func filterLabelsFn(filter platform.LabelFilter) func(l *platform.Label) bool {
	return func(label *platform.Label) bool {
		return (filter.Name == "" || (filter.Name == label.Name))
	}
}

// FindLabels returns a list of labels that match a filter.
func (c *Client) FindLabels(ctx context.Context, filter platform.LabelFilter, opt ...platform.FindOptions) ([]*platform.Label, error) {
	ls := []*platform.Label{}
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

func (c *Client) findLabels(ctx context.Context, tx *bolt.Tx, filter platform.LabelFilter) ([]*platform.Label, error) {
	ls := []*platform.Label{}
	filterFn := filterLabelsFn(filter)
	err := c.forEachLabel(ctx, tx, func(l *platform.Label) bool {
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

func (c *Client) FindResourceLabels(ctx context.Context, filter platform.LabelMappingFilter) ([]*platform.Label, error) {
	return nil, nil
}

func (c *Client) CreateLabelMapping(ctx context.Context, m *platform.LabelMapping) error {
	err := c.db.Update(func(tx *bolt.Tx) error {
		return c.putLabelMapping(ctx, tx, m)
	})

	if err != nil {
		return &platform.Error{
			Err: err,
			Op:  getOp(platform.OpCreateLabel),
		}
	}

	return nil
}

func (c *Client) DeleteLabelMapping(ctx context.Context, m *platform.LabelMapping) error {
	return nil
}

func (c *Client) CreateLabel(ctx context.Context, l *platform.Label) error {
	err := c.db.Update(func(tx *bolt.Tx) error {
		l.ID = c.IDGenerator.ID()

		return c.putLabel(ctx, tx, l)
	})

	if err != nil {
		return &platform.Error{
			Err: err,
			Op:  getOp(platform.OpCreateLabel),
		}
	}
	return nil
}

func (c *Client) PutLabel(ctx context.Context, l *platform.Label) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		var err error
		pe := c.putLabel(ctx, tx, l)
		if pe != nil {
			err = pe
		}
		return err
	})
}

func labelMappingKey(m *platform.LabelMapping) ([]byte, error) {
	lid, err := m.LabelID.Encode()
	if err != nil {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Err:  err,
		}
	}

	rid, err := m.ResourceID.Encode()
	if err != nil {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Err:  err,
		}
	}

	key := make([]byte, len(lid)+len(rid))
	copy(key, lid)
	copy(key[len(lid):], rid)

	return key, nil
}

func (c *Client) forEachLabel(ctx context.Context, tx *bolt.Tx, fn func(*platform.Label) bool) error {
	cur := tx.Bucket(labelBucket).Cursor()
	for k, v := cur.First(); k != nil; k, v = cur.Next() {
		l := &platform.Label{}
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
func (c *Client) UpdateLabel(ctx context.Context, id platform.ID, upd platform.LabelUpdate) (*platform.Label, error) {
	var label *platform.Label
	err := c.db.Update(func(tx *bolt.Tx) error {
		labelResponse, pe := c.updateLabel(ctx, tx, id, upd)
		if pe != nil {
			return &platform.Error{
				Err: pe,
				Op:  getOp(platform.OpUpdateLabel),
			}
		}
		label = labelResponse
		return nil
	})

	return label, err
}

func (c *Client) updateLabel(ctx context.Context, tx *bolt.Tx, id platform.ID, upd platform.LabelUpdate) (*platform.Label, error) {
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
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Err:  err,
		}
	}

	if err := c.putLabel(ctx, tx, label); err != nil {
		return nil, &platform.Error{
			Err: err,
		}
	}

	return label, nil
}

// set a label and overwrite any existing label
func (c *Client) putLabel(ctx context.Context, tx *bolt.Tx, l *platform.Label) error {
	v, err := json.Marshal(l)
	if err != nil {
		return &platform.Error{
			Err: err,
		}
	}

	encodedID, err := l.ID.Encode()
	if err != nil {
		return &platform.Error{
			Err: err,
		}
	}

	if err := tx.Bucket(labelBucket).Put(encodedID, v); err != nil {
		return &platform.Error{
			Err: err,
		}
	}

	return nil
}

func (c *Client) putLabelMapping(ctx context.Context, tx *bolt.Tx, m *platform.LabelMapping) error {
	v, err := json.Marshal(m)
	if err != nil {
		return &platform.Error{
			Err: err,
		}
	}

	key, err := labelMappingKey(m)
	if err != nil {
		return &platform.Error{
			Err: err,
		}
	}

	if err := tx.Bucket(labelBucket).Put(key, v); err != nil {
		return &platform.Error{
			Err: err,
		}
	}

	return nil
}

// DeleteLabel deletes a label.
func (c *Client) DeleteLabel(ctx context.Context, id platform.ID) error {
	err := c.db.Update(func(tx *bolt.Tx) error {
		return c.deleteLabel(ctx, tx, id)
	})
	if err != nil {
		return &platform.Error{
			Op:  getOp(platform.OpDeleteLabel),
			Err: err,
		}
	}
	return nil
}

func (c *Client) deleteLabel(ctx context.Context, tx *bolt.Tx, id platform.ID) error {
	_, err := c.findLabelByID(ctx, tx, id)
	if err != nil {
		return err
	}
	encodedID, idErr := id.Encode()
	if idErr != nil {
		return &platform.Error{
			Err: idErr,
		}
	}

	return tx.Bucket(labelBucket).Delete(encodedID)
}

// func (c *Client) deleteLabels(ctx context.Context, tx *bolt.Tx, filter platform.LabelFilter) error {
// 	ls, err := c.findLabels(ctx, tx, filter)
// 	if err != nil {
// 		return err
// 	}
// 	for _, l := range ls {
// 		encodedID, idErr := l.ID.Encode()
// 		if idErr != nil {
// 			return &platform.Error{
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
