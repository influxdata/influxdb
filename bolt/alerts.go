package bolt

import (
	"context"

	"github.com/boltdb/bolt"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/bolt/internal"
)

// Ensure AlertsStore implements chronograf.AlertsStore.
var _ chronograf.AlertRulesStore = &AlertsStore{}

var AlertsBucket = []byte("Alerts")

type AlertsStore struct {
	client *Client
}

// All returns all known alerts
func (s *AlertsStore) All(ctx context.Context) ([]chronograf.AlertRule, error) {
	var srcs []chronograf.AlertRule
	if err := s.client.db.View(func(tx *bolt.Tx) error {
		if err := tx.Bucket(AlertsBucket).ForEach(func(k, v []byte) error {
			var src chronograf.AlertRule
			if err := internal.UnmarshalAlertRule(v, &src); err != nil {
				return err
			}
			srcs = append(srcs, src)
			return nil
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return srcs, nil

}

// Add creates a new Alerts in the AlertsStore.
func (s *AlertsStore) Add(ctx context.Context, src chronograf.AlertRule) (chronograf.AlertRule, error) {
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(AlertsBucket)
		if v, err := internal.MarshalAlertRule(&src); err != nil {
			return err
		} else if err := b.Put([]byte(src.ID), v); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return chronograf.AlertRule{}, err
	}

	return src, nil
}

// Delete removes the Alerts from the AlertsStore
func (s *AlertsStore) Delete(ctx context.Context, src chronograf.AlertRule) error {
	_, err := s.Get(ctx, src.ID)
	if err != nil {
		return err
	}
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(AlertsBucket).Delete([]byte(src.ID)); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// Get returns a Alerts if the id exists.
func (s *AlertsStore) Get(ctx context.Context, id string) (chronograf.AlertRule, error) {
	var src chronograf.AlertRule
	if err := s.client.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket(AlertsBucket).Get([]byte(id)); v == nil {
			return chronograf.ErrAlertNotFound
		} else if err := internal.UnmarshalAlertRule(v, &src); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return chronograf.AlertRule{}, err
	}

	return src, nil
}

// Update a Alerts
func (s *AlertsStore) Update(ctx context.Context, src chronograf.AlertRule) error {
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		// Get an existing alerts with the same ID.
		b := tx.Bucket(AlertsBucket)
		if v := b.Get([]byte(src.ID)); v == nil {
			return chronograf.ErrAlertNotFound
		}

		if v, err := internal.MarshalAlertRule(&src); err != nil {
			return err
		} else if err := b.Put([]byte(src.ID), v); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}
