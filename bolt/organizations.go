package bolt

import (
	"context"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/bolt/internal"
)

// Ensure OrganizationsStore implements chronograf.OrganizationsStore.
var _ chronograf.OrganizationsStore = &OrganizationsStore{}

// OrganizationsBucket is the bucket where organizations are stored.
var OrganizationsBucket = []byte("OrganizationsV1")

// OrganizationsStore uses bolt to store and retrieve Organizations
type OrganizationsStore struct {
	client *Client
}

// Migrate sets the default organization at runtime
func (s *OrganizationsStore) Migrate(ctx context.Context) error {
	o := chronograf.Organization{
		ID:   0,
		Name: "__default",
	}
	return s.client.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(OrganizationsBucket)
		v := b.Get(u64tob(o.ID))
		if v != nil {
			return nil
		}
		if v, err := internal.MarshalOrganization(&o); err != nil {
			return err
		} else if err := b.Put(u64tob(o.ID), v); err != nil {
			return err
		}

		return nil
	})
}

// Add creates a new Organization in the OrganizationsStore
func (s *OrganizationsStore) Add(ctx context.Context, o *chronograf.Organization) (*chronograf.Organization, error) {
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(OrganizationsBucket)
		seq, err := b.NextSequence()
		if err != nil {
			return err
		}
		o.ID = seq
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

// All returns all known organizations
func (s *OrganizationsStore) All(ctx context.Context) ([]chronograf.Organization, error) {
	var orgs []chronograf.Organization
	err := s.each(ctx, func(o *chronograf.Organization) {
		orgs = append(orgs, *o)
	})

	if err != nil {
		return nil, err
	}

	return orgs, nil
}

// Delete the organization from OrganizationsStore
func (s *OrganizationsStore) Delete(ctx context.Context, o *chronograf.Organization) error {
	_, err := s.get(ctx, o.ID)
	if err != nil {
		return err
	}
	return s.client.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(OrganizationsBucket).Delete(u64tob(o.ID))
	})
}

func (s *OrganizationsStore) get(ctx context.Context, id uint64) (*chronograf.Organization, error) {
	var o chronograf.Organization
	err := s.client.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(OrganizationsBucket).Get(u64tob(id))
		if v == nil {
			return chronograf.ErrOrganizationNotFound
		}
		return internal.UnmarshalOrganization(v, &o)
	})

	if err != nil {
		return nil, err
	}

	return &o, nil
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

// Get returns a Organization if the id exists.
// If an ID is provided in the query, the lookup time for an organization will be O(1).
// If Name is provided, the lookup time will be O(n).
// Get expects that only one of ID or Name will be specified, but will prefer ID over Name if both are specified.
func (s *OrganizationsStore) Get(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
	if q.ID != nil {
		return s.get(ctx, *q.ID)
	}

	if q.Name != nil {
		var org *chronograf.Organization
		err := s.each(ctx, func(o *chronograf.Organization) {
			if org != nil {
				return
			}

			if o.Name == *q.Name {
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
	return nil, fmt.Errorf("must specify either ID, or Name in OrganizationQuery")
}

// Update the organization in OrganizationsStore
func (s *OrganizationsStore) Update(ctx context.Context, o *chronograf.Organization) error {
	org, err := s.get(ctx, o.ID)
	if err != nil {
		return err
	}
	return s.client.db.Update(func(tx *bolt.Tx) error {
		org.Name = o.Name
		if v, err := internal.MarshalOrganization(org); err != nil {
			return err
		} else if err := tx.Bucket(OrganizationsBucket).Put(u64tob(org.ID), v); err != nil {
			return err
		}
		return nil
	})
}
