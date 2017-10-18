package bolt

import (
	"context"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/bolt/internal"
)

// Ensure UsersStore implements chronograf.UsersStore.
var _ chronograf.UsersStore = &UsersStore{}

// UsersBucket is used to store users local to chronograf
var UsersBucket = []byte("UsersV1")

// UsersStore uses bolt to store and retrieve users
type UsersStore struct {
	client *Client
}

// get searches the UsersStore for user with id and returns the bolt representation
func (s *UsersStore) get(ctx context.Context, id uint64) (*chronograf.User, error) {
	var u chronograf.User
	err := s.client.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(UsersBucket).Get(u64tob(id))
		if v == nil {
			return chronograf.ErrUserNotFound
		}
		return internal.UnmarshalUser(v, &u)
	})

	if err != nil {
		return nil, err
	}

	return &u, nil
}

func (s *UsersStore) each(ctx context.Context, fn func(*chronograf.User)) error {
	return s.client.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(UsersBucket).ForEach(func(k, v []byte) error {
			var user chronograf.User
			if err := internal.UnmarshalUser(v, &user); err != nil {
				return err
			}
			fn(&user)
			return nil
		})
	})
}

// Get searches the UsersStore for user with name
func (s *UsersStore) Get(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
	if q.ID != nil {
		return s.get(ctx, *q.ID)
	}

	if q.Name != nil && q.Provider != nil {
		var user *chronograf.User
		err := s.each(ctx, func(u *chronograf.User) {
			if user != nil {
				return
			}
			if u.Name == *q.Name && u.Provider == *q.Provider {
				user = u
			}
		})

		if err != nil {
			return nil, err
		}

		if user == nil {
			return nil, fmt.Errorf("user not found")
		}

		return user, nil
	}

	return nil, fmt.Errorf("must specify ID or Provider and Scheme in UserQuery")
}

// Add a new Users in the UsersStore.
func (s *UsersStore) Add(ctx context.Context, u *chronograf.User) (*chronograf.User, error) {
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(UsersBucket)
		seq, err := b.NextSequence()
		if err != nil {
			return err
		}
		u.ID = seq
		if v, err := internal.MarshalUser(u); err != nil {
			return err
		} else if err := b.Put(u64tob(seq), v); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return u, nil
}

// Delete the users from the UsersStore
func (s *UsersStore) Delete(ctx context.Context, usr *chronograf.User) error {
	_, err := s.get(ctx, usr.ID)
	if err != nil {
		return err
	}
	return s.client.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(UsersBucket).Delete(u64tob(usr.ID))
	})
}

// Update a user
func (s *UsersStore) Update(ctx context.Context, usr *chronograf.User) error {
	_, err := s.get(ctx, usr.ID)
	if err != nil {
		return err
	}
	return s.client.db.Update(func(tx *bolt.Tx) error {
		if v, err := internal.MarshalUser(usr); err != nil {
			return err
		} else if err := tx.Bucket(UsersBucket).Put(u64tob(usr.ID), v); err != nil {
			return err
		}
		return nil
	})
}

// All returns all users
func (s *UsersStore) All(ctx context.Context) ([]chronograf.User, error) {
	var users []chronograf.User
	if err := s.client.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(UsersBucket).ForEach(func(k, v []byte) error {
			var user chronograf.User
			if err := internal.UnmarshalUser(v, &user); err != nil {
				return err
			}
			users = append(users, user)
			return nil
		})
	}); err != nil {
		return nil, err
	}

	return users, nil
}
