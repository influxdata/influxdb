package bolt

import (
	"context"
	"fmt"
	"path"
	"strconv"

	"github.com/boltdb/bolt"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/bolt/internal"
	"github.com/influxdata/chronograf/uuid"
)

// Ensure OrganizationsStore implements chronograf.OrganizationsStore.
var _ chronograf.OrganizationsStore = &OrganizationsStore{}

// OrganizationsStore is used to store organizations local to chronograf
var OrganizationsBucket = []byte("OrganizationsV1")

// OrganizationsStore uses bolt to store and retrieve Organizations
type OrganizationsStore struct {
	client *Client
}

func (s *OrganizationsStore) Open(tx *bolt.Tx) error {
	if s == nil {
		return fmt.Errorf("OrganizationsStore is nil")
	}
	// Always create Organizations bucket.
	if _, err := tx.CreateBucketIfNotExists(OrganizationsBucket); err != nil {
		return err
	}

	return tx.Bucket(OrganizationsBucket).ForEach(func(k, v []byte) error {
		var org chronograf.Organization
		if err := internal.UnmarshalOrganization(v, &org); err != nil {
			return err
		}

		return s.createResources(org.ID, tx)
	})
}

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
		} else if err := s.createResources(seq, tx); err != nil {
			return err
		}

		s.appendStores(o)

		return nil
	}); err != nil {
		return nil, err
	}

	return o, nil
}

func (s *OrganizationsStore) createResources(id uint64, tx *bolt.Tx) error {
	idStr := strconv.FormatUint(id, 10)
	// Always create Sources bucket.
	orgSourcesBucket := []byte(path.Join(string(SourcesBucket), idStr))
	if _, err := tx.CreateBucketIfNotExists(orgSourcesBucket); err != nil {
		return err
	}
	// Always create Servers bucket.
	orgServersBucket := []byte(path.Join(string(ServersBucket), idStr))
	if _, err := tx.CreateBucketIfNotExists(orgServersBucket); err != nil {
		return err
	}
	// Always create Layouts bucket.
	orgLayoutBucket := []byte(path.Join(string(LayoutBucket), idStr))
	if _, err := tx.CreateBucketIfNotExists(orgLayoutBucket); err != nil {
		return err
	}
	// Always create Dashboards bucket.
	orgDashboardBucket := []byte(path.Join(string(DashboardBucket), idStr))
	if _, err := tx.CreateBucketIfNotExists(orgDashboardBucket); err != nil {
		return err
	}
	return nil
}

func (s *OrganizationsStore) removeResources(id uint64, tx *bolt.Tx) error {
	idStr := strconv.FormatUint(id, 10)
	// Always create Sources bucket.
	orgSourcesBucket := []byte(path.Join(string(SourcesBucket), idStr))
	if err := tx.DeleteBucket(orgSourcesBucket); err != nil {
		return err
	}
	// Always create Servers bucket.
	orgServersBucket := []byte(path.Join(string(ServersBucket), idStr))
	if err := tx.DeleteBucket(orgServersBucket); err != nil {
		return err
	}
	// Always create Layouts bucket.
	orgLayoutBucket := []byte(path.Join(string(LayoutBucket), idStr))
	if err := tx.DeleteBucket(orgLayoutBucket); err != nil {
		return err
	}
	// Always create Dashboards bucket.
	orgDashboardBucket := []byte(path.Join(string(DashboardBucket), idStr))
	if err := tx.DeleteBucket(orgDashboardBucket); err != nil {
		return err
	}

	return nil
}

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

func (s *OrganizationsStore) Delete(ctx context.Context, o *chronograf.Organization) error {
	_, err := s.get(ctx, o.ID)
	if err != nil {
		return err
	}
	return s.client.db.Update(func(tx *bolt.Tx) error {
		if err := s.removeResources(o.ID, tx); err != nil {
			return err
		}
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

	s.appendStores(&o)

	if err != nil {
		return nil, err
	}

	return &o, nil
}

func (s *OrganizationsStore) appendStores(o *chronograf.Organization) {
	idStr := strconv.FormatUint(o.ID, 10)
	o.SourcesStore = &SourcesStore{
		client: s.client,
		Org:    idStr,
	}
	o.ServersStore = &ServersStore{
		client: s.client,
		Org:    idStr,
	}
	o.LayoutStore = &LayoutStore{
		client: s.client,
		Org:    idStr,
		IDs:    &uuid.V4{},
	}
	o.DashboardsStore = &DashboardsStore{
		client: s.client,
		Org:    idStr,
		IDs:    &uuid.V4{},
	}
}

func (s *OrganizationsStore) each(ctx context.Context, fn func(*chronograf.Organization)) error {
	return s.client.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(OrganizationsBucket).ForEach(func(k, v []byte) error {
			var org chronograf.Organization
			if err := internal.UnmarshalOrganization(v, &org); err != nil {
				return err
			}
			s.appendStores(&org)
			fn(&org)
			return nil
		})
	})
	return nil
}

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
