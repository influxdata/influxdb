package bolt

import (
	"context"
	"encoding/json"
	"fmt"

	bolt "github.com/coreos/bbolt"
	platform "github.com/influxdata/influxdb"
)

var (
	labelBucket = []byte("labelsv1")
)

func (c *Client) initializeLabels(ctx context.Context, tx *bolt.Tx) error {
	if _, err := tx.CreateBucketIfNotExists([]byte(labelBucket)); err != nil {
		return err
	}
	return nil
}

func filterLabelsFn(filter platform.LabelFilter) func(l *platform.Label) bool {
	return func(label *platform.Label) bool {
		return (filter.Name == "" || (filter.Name == label.Name)) &&
			(!filter.ResourceID.Valid() || (filter.ResourceID == label.ResourceID))
	}
}

// func (c *Client) findLabel(ctx context.Context, tx *bolt.Tx, resourceID platform.ID, name string) (*platform.Label, error) {
// 	key, err := labelKey(&platform.Label{ResourceID: resourceID, Name: name})
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	l := &platform.Label{}
// 	v := tx.Bucket(labelBucket).Get(key)
// 	if err := json.Unmarshal(v, l); err != nil {
// 		return nil, err
// 	}
//
// 	return l, nil
// }

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

func (c *Client) CreateLabel(ctx context.Context, l *platform.Label) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		return c.createLabel(ctx, tx, l)
	})
}

func (c *Client) createLabel(ctx context.Context, tx *bolt.Tx, l *platform.Label) error {
	unique := c.uniqueLabel(ctx, tx, l)

	if !unique {
		return &platform.Error{
			Code: platform.EConflict,
			Op:   getOp(platform.OpCreateLabel),
			Msg:  fmt.Sprintf("label %s already exists", l.Name),
		}
	}

	v, err := json.Marshal(l)
	if err != nil {
		return err
	}

	key, err := labelKey(l)
	if err != nil {
		return err
	}

	if err := tx.Bucket(labelBucket).Put(key, v); err != nil {
		return err
	}

	return nil
}

func labelKey(l *platform.Label) ([]byte, error) {
	encodedResourceID, err := l.ResourceID.Encode()
	if err != nil {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Err:  err,
		}
	}

	key := make([]byte, len(encodedResourceID)+len(l.Name))
	copy(key, encodedResourceID)
	copy(key[len(encodedResourceID):], l.Name)

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

func (c *Client) uniqueLabel(ctx context.Context, tx *bolt.Tx, l *platform.Label) bool {
	key, err := labelKey(l)
	if err != nil {
		return false
	}

	v := tx.Bucket(labelBucket).Get(key)
	return len(v) == 0
}

// UpdateLabel updates a label.
func (c *Client) UpdateLabel(ctx context.Context, l *platform.Label, upd platform.LabelUpdate) (*platform.Label, error) {
	var label *platform.Label
	err := c.db.Update(func(tx *bolt.Tx) error {
		labelResponse, pe := c.updateLabel(ctx, tx, l, upd)
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

func (c *Client) updateLabel(ctx context.Context, tx *bolt.Tx, l *platform.Label, upd platform.LabelUpdate) (*platform.Label, error) {
	ls, err := c.findLabels(ctx, tx, platform.LabelFilter{Name: l.Name, ResourceID: l.ResourceID})
	if err != nil {
		return nil, err
	}
	if len(ls) == 0 {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Err:  platform.ErrLabelNotFound,
		}
	}

	label := ls[0]

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

	key, pe := labelKey(l)
	if pe != nil {
		return pe
	}

	if err := tx.Bucket(labelBucket).Put(key, v); err != nil {
		return &platform.Error{
			Err: err,
		}
	}

	return nil
}

// DeleteLabel deletes a label.
func (c *Client) DeleteLabel(ctx context.Context, l platform.Label) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		return c.deleteLabel(ctx, tx, platform.LabelFilter{Name: l.Name, ResourceID: l.ResourceID})
	})
}

func (c *Client) deleteLabel(ctx context.Context, tx *bolt.Tx, filter platform.LabelFilter) error {
	ls, err := c.findLabels(ctx, tx, filter)
	if err != nil {
		return err
	}
	if len(ls) == 0 {
		return &platform.Error{
			Code: platform.ENotFound,
			Op:   getOp(platform.OpDeleteLabel),
			Err:  platform.ErrLabelNotFound,
		}
	}

	key, err := labelKey(ls[0])
	if err != nil {
		return err
	}

	return tx.Bucket(labelBucket).Delete(key)
}

func (c *Client) deleteLabels(ctx context.Context, tx *bolt.Tx, filter platform.LabelFilter) error {
	ls, err := c.findLabels(ctx, tx, filter)
	if err != nil {
		return err
	}
	for _, l := range ls {
		key, err := labelKey(l)
		if err != nil {
			return err
		}
		if err = tx.Bucket(labelBucket).Delete(key); err != nil {
			return err
		}
	}
	return nil
}
