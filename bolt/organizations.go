package bolt

import (
	"context"

	"github.com/boltdb/bolt"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/bolt/internal"
)

// Ensure OrganizationsStore implements chronograf.OrganizationsStore.
var _ chronograf.OrganizationsStore = &OrganizationsStore{}

// OrganizationsStore is used to store organizations local to chronograf
var OrganizationsBucket = []byte("OrganizationsV1")

// OrganizationsStore uses bolt to store and retrieve Organizations
type OrganizationsStore struct {
	client *Client
}

func (s *OrganizationsStore) Add(ctx context.Context, o *chronograf.Organization) (*chronograf.Organization, error) {
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(OrganizationsBucket)
		seq, err := b.NextSequence()
		if err != nil {
			return err
		}
		if v, err := internal.MarshalOrganization(o); err != nil {
			return err
		} else if err := b.Put(u64tob(seq), v); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return o, nil
}

func (s *OrganizationsStore) All(context.Context) ([]chronograf.Organization, error) {
	panic("not implemented")
}

func (s *OrganizationsStore) Delete(context.Context, *chronograf.Organization) error {
	panic("not implemented")
}

func (s *OrganizationsStore) each(ctx context.Context, fn func(*chronograf.Organization)) error {
	return s.client.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(OrganizationsBucket).ForEach(func(k, v []byte) error {
			var org chronograf.Organization
			if err := internal.UnmarshalOrganization(v, &org); err != nil {
				return err
			}
			fn(&org)
			return nil
		})
	})
	return nil
}

func (s *OrganizationsStore) Get(ctx context.Context, name string) (*chronograf.Organization, error) {
	var org *chronograf.Organization
	err := s.each(ctx, func(o *chronograf.Organization) {
		if org != nil {
			return
		}

		if o.Name == name {
			org = o
		}
	})

	if err != nil {
		return nil, err
	}

	if org == nil {
		return nil, chronograf.ErrOrganizationNotFound
	}

	return org, nil
}

func (s *OrganizationsStore) Update(context.Context, *chronograf.Organization) error {
	panic("not implemented")
}
