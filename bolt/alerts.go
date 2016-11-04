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
func (s *AlertsStore) All(ctx context.Context, sourceID, kapaID int) ([]chronograf.AlertRule, error) {
	var srcs []chronograf.AlertRule
	if err := s.client.db.View(func(tx *bolt.Tx) error {
		if err := tx.Bucket(AlertsBucket).ForEach(func(k, v []byte) error {
			var src internal.ScopedAlert
			if err := internal.UnmarshalAlertRule(v, &src); err != nil {
				return err
			}
			// filter by only those rules with these ids
			if src.KapaID == kapaID && src.SrcID == sourceID {
				srcs = append(srcs, src.AlertRule)
			}
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
func (s *AlertsStore) Add(ctx context.Context, sourceID, kapaID int, src chronograf.AlertRule) (chronograf.AlertRule, error) {
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(AlertsBucket)
		scoped := internal.ScopedAlert{
			AlertRule: src,
			SrcID:     sourceID,
			KapaID:    kapaID,
		}
		if v, err := internal.MarshalAlertRule(&scoped); err != nil {
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
func (s *AlertsStore) Delete(ctx context.Context, srcID, kapaID int, src chronograf.AlertRule) error {
	_, err := s.Get(ctx, srcID, kapaID, src.ID)
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

// scopedGet returns a Alerts if the id exists.
func (s *AlertsStore) scopedGet(ctx context.Context, id string) (internal.ScopedAlert, error) {
	var src internal.ScopedAlert
	if err := s.client.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket(AlertsBucket).Get([]byte(id)); v == nil {
			return chronograf.ErrAlertNotFound
		} else if err := internal.UnmarshalAlertRule(v, &src); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return internal.ScopedAlert{}, err
	}

	return src, nil
}

// Get returns a Alerts if the id exists.
func (s *AlertsStore) Get(ctx context.Context, srcID, kapaID int, id string) (chronograf.AlertRule, error) {
	scoped, err := s.scopedGet(ctx, id)
	if err != nil {
		return chronograf.AlertRule{}, err
	}

	if scoped.SrcID != srcID || scoped.KapaID != kapaID {
		return chronograf.AlertRule{}, chronograf.ErrAlertNotFound
	}
	return scoped.AlertRule, nil
}

// Update a Alerts
func (s *AlertsStore) Update(ctx context.Context, srcID, kapaID int, src chronograf.AlertRule) error {
	// Check if we have permissions to get this alert
	_, err := s.Get(ctx, srcID, kapaID, src.ID)
	if err != nil {
		return err
	}

	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		// Get an existing alerts with the same ID.
		b := tx.Bucket(AlertsBucket)
		scoped := internal.ScopedAlert{
			AlertRule: src,
			SrcID:     srcID,
			KapaID:    kapaID,
		}
		if v, err := internal.MarshalAlertRule(&scoped); err != nil {
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
