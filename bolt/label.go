package bolt

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/coreos/bbolt"
	"github.com/influxdata/platform"
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
		return fmt.Errorf("label %s already exists", l.Name)
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
		return nil, err
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

// DeleteLabel deletes a label.
func (c *Client) DeleteLabel(ctx context.Context, l platform.Label) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		return c.deleteLabel(ctx, tx, platform.LabelFilter(l))
	})
}

func (c *Client) deleteLabel(ctx context.Context, tx *bolt.Tx, filter platform.LabelFilter) error {
	ls, err := c.findLabels(ctx, tx, filter)
	if err != nil {
		return err
	}
	if len(ls) == 0 {
		return fmt.Errorf("label not found")
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
