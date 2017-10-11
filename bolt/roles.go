package bolt

import (
	"context"

	"github.com/boltdb/bolt"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/bolt/internal"
)

// Ensure RolesStore implements chronograf.RolesStore.
var _ chronograf.RolesStore = &RolesStore{}

// RolesBucket is used to store roles local to chronograf
var RolesBucket = []byte("RolesV1")

// RolesStore uses bolt to store and retrieve roles
type RolesStore struct {
	client *Client
}

// get searches the RolesStore for role with name and returns the chronograf representation
func (s *RolesStore) get(ctx context.Context, name string) (*chronograf.Role, error) {
	found := false
	var role *chronograf.Role
	err := s.client.db.View(func(tx *bolt.Tx) error {
		err := tx.Bucket(RolesBucket).ForEach(func(k, v []byte) error {
			var r chronograf.Role
			if err := internal.UnmarshalRole(v, &r); err != nil {
				return err
			} else if role.Name != name {
				return nil
			}
			found = true
			role = &r
			return nil
		})
		if err != nil {
			return err
		}
		if found == false {
			return chronograf.ErrRoleNotFound
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return role, nil
}

// Get searches the RolesStore for role with name
func (s *RolesStore) Get(ctx context.Context, name string) (*chronograf.Role, error) {
	r, err := s.get(ctx, name)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// Add a new Roles in the RolesStore.
func (s *RolesStore) Add(ctx context.Context, r *chronograf.Role) (*chronograf.Role, error) {
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(RolesBucket)
		if v, err := internal.MarshalRole(r); err != nil {
			return err
		} else if err := b.Put([]byte(r.Name), v); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return r, nil
}

// Delete the roles from the RolesStore
func (s *RolesStore) Delete(ctx context.Context, role *chronograf.Role) error {
	r, err := s.get(ctx, role.Name)
	if err != nil {
		return err
	}
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(RolesBucket).Delete([]byte(r.Name)); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// Update a role
func (s *RolesStore) Update(ctx context.Context, role *chronograf.Role) error {
	r, err := s.get(ctx, role.Name)
	if err != nil {
		return err
	}
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		r.Name = role.Name
		if v, err := internal.MarshalRole(r); err != nil {
			return err
		} else if err := tx.Bucket(RolesBucket).Put([]byte(r.Name), v); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// All returns all roles
func (s *RolesStore) All(ctx context.Context) ([]chronograf.Role, error) {
	var roles []chronograf.Role
	if err := s.client.db.View(func(tx *bolt.Tx) error {
		if err := tx.Bucket(RolesBucket).ForEach(func(k, v []byte) error {
			var role chronograf.Role
			if err := internal.UnmarshalRole(v, &role); err != nil {
				return err
			}
			roles = append(roles, role)
			return nil
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return roles, nil
}
