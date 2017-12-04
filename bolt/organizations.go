package bolt

import (
	"context"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/bolt/internal"
	"github.com/influxdata/chronograf/organizations"
)

// Ensure OrganizationsStore implements chronograf.OrganizationsStore.
var _ chronograf.OrganizationsStore = &OrganizationsStore{}

// OrganizationsBucket is the bucket where organizations are stored.
var OrganizationsBucket = []byte("OrganizationsV1")

const (
	// DefaultOrganizationID is the ID of the default organization.
	DefaultOrganizationID uint64 = 0
	// DefaultOrganizationName is the Name of the default organization
	DefaultOrganizationName string = "Default"
	// DefaultOrganizationRole is the DefaultRole for the Default organization
	DefaultOrganizationRole string = "member"
	// DefaultOrganizationPublic is the Public setting for the Default organization.
	DefaultOrganizationPublic bool = true
)

// OrganizationsStore uses bolt to store and retrieve Organizations
type OrganizationsStore struct {
	client *Client
}

// Migrate sets the default organization at runtime
func (s *OrganizationsStore) Migrate(ctx context.Context) error {
	return s.CreateDefault(ctx)
}

// CreateDefault does a findOrCreate on the default organization
func (s *OrganizationsStore) CreateDefault(ctx context.Context) error {
	o := chronograf.Organization{
		ID:          DefaultOrganizationID,
		Name:        DefaultOrganizationName,
		DefaultRole: DefaultOrganizationRole,
		Public:      DefaultOrganizationPublic,
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

func (s *OrganizationsStore) nameIsUnique(ctx context.Context, name string) bool {
	_, err := s.Get(ctx, chronograf.OrganizationQuery{Name: &name})
	switch err {
	case chronograf.ErrOrganizationNotFound:
		return true
	default:
		return false
	}
}

// DefaultOrganizationID returns the ID of the default organization
func (s *OrganizationsStore) DefaultOrganization(ctx context.Context) (*chronograf.Organization, error) {
	var org chronograf.Organization
	if err := s.client.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(OrganizationsBucket).Get(u64tob(DefaultOrganizationID))
		return internal.UnmarshalOrganization(v, &org)
	}); err != nil {
		return nil, err
	}

	return &org, nil
}

// Add creates a new Organization in the OrganizationsStore
func (s *OrganizationsStore) Add(ctx context.Context, o *chronograf.Organization) (*chronograf.Organization, error) {
	if !s.nameIsUnique(ctx, o.Name) {
		return nil, chronograf.ErrOrganizationAlreadyExists
	}
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
	if o.ID == DefaultOrganizationID {
		return chronograf.ErrCannotDeleteDefaultOrganization
	}
	_, err := s.get(ctx, o.ID)
	if err != nil {
		return err
	}
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(OrganizationsBucket).Delete(u64tob(o.ID))
	}); err != nil {
		return err
	}

	// Dependent Delete of all resources

	org := fmt.Sprintf("%d", o.ID)
	// Each of the associated organization stores expects organization to be
	// set on the context.
	ctx = context.WithValue(ctx, organizations.ContextKey, org)

	sourcesStore := organizations.NewSourcesStore(s.client.SourcesStore, org)
	sources, err := sourcesStore.All(ctx)
	if err != nil {
		return err
	}
	for _, source := range sources {
		if err := sourcesStore.Delete(ctx, source); err != nil {
			return err
		}
	}

	serversStore := organizations.NewServersStore(s.client.ServersStore, org)
	servers, err := serversStore.All(ctx)
	if err != nil {
		return err
	}
	for _, server := range servers {
		if err := serversStore.Delete(ctx, server); err != nil {
			return err
		}
	}

	layoutsStore := organizations.NewLayoutsStore(s.client.LayoutsStore, org)
	layouts, err := layoutsStore.All(ctx)
	if err != nil {
		return err
	}
	for _, layout := range layouts {
		if err := layoutsStore.Delete(ctx, layout); err != nil {
			return err
		}
	}

	dashboardsStore := organizations.NewDashboardsStore(s.client.DashboardsStore, org)
	dashboards, err := dashboardsStore.All(ctx)
	if err != nil {
		return err
	}
	for _, dashboard := range dashboards {
		if err := dashboardsStore.Delete(ctx, dashboard); err != nil {
			return err
		}
	}

	usersStore := organizations.NewUsersStore(s.client.UsersStore, org)
	users, err := usersStore.All(ctx)
	if err != nil {
		return err
	}
	for _, user := range users {
		if err := usersStore.Delete(ctx, &user); err != nil {
			return err
		}
	}

	return nil
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
	if o.Name != org.Name && !s.nameIsUnique(ctx, o.Name) {
		return chronograf.ErrOrganizationAlreadyExists
	}
	return s.client.db.Update(func(tx *bolt.Tx) error {
		if v, err := internal.MarshalOrganization(o); err != nil {
			return err
		} else if err := tx.Bucket(OrganizationsBucket).Put(u64tob(o.ID), v); err != nil {
			return err
		}
		return nil
	})
}
