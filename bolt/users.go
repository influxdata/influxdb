package bolt

import (
	"context"

	"github.com/boltdb/bolt"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/bolt/internal"
)

// Ensure UsersStore implements chronograf.UsersStore.
var _ chronograf.UsersStore = &UsersStore{}

var UsersBucket = []byte("Users")

type UsersStore struct {
	client *Client
}

// FindByEmail searches the UsersStore for all users owned with the email
func (s *UsersStore) FindByEmail(ctx context.Context, email string) (*chronograf.User, error) {
	var user *chronograf.User
	err := s.client.db.View(func(tx *bolt.Tx) error {
		err := tx.Bucket(UsersBucket).ForEach(func(k, v []byte) error {
			var u *chronograf.User
			if err := internal.UnmarshalUser(v, u); err != nil {
				return err
			} else if u.Email != email {
				return nil
			}
			user = u
			return nil
		})
		if err != nil {
			return err
		}
		if user == nil {
			return chronograf.ErrUserNotFound
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return user, nil
}

// Create a new Users in the UsersStore.
func (s *UsersStore) Add(ctx context.Context, u *chronograf.User) (*chronograf.User, error) {
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(UsersBucket)
		seq, err := b.NextSequence()
		if err != nil {
			return err
		}
		u.ID = chronograf.UserID(seq)

		if v, err := internal.MarshalUser(u); err != nil {
			return err
		} else if err := b.Put(itob(int(u.ID)), v); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return u, nil
}

// Delete the users from the UsersStore
func (s *UsersStore) Delete(ctx context.Context, u *chronograf.User) error {
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(UsersBucket).Delete(itob(int(u.ID))); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// Get retrieves a user by id.
func (s *UsersStore) Get(ctx context.Context, id chronograf.UserID) (*chronograf.User, error) {
	var u chronograf.User
	if err := s.client.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket(UsersBucket).Get(itob(int(id))); v == nil {
			return chronograf.ErrUserNotFound
		} else if err := internal.UnmarshalUser(v, &u); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return &u, nil
}

// Update a user
func (s *UsersStore) Update(ctx context.Context, usr *chronograf.User) error {
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		// Retrieve an existing user with the same ID.
		var u chronograf.User
		b := tx.Bucket(UsersBucket)
		if v := b.Get(itob(int(usr.ID))); v == nil {
			return chronograf.ErrUserNotFound
		} else if err := internal.UnmarshalUser(v, &u); err != nil {
			return err
		}

		u.Email = usr.Email

		if v, err := internal.MarshalUser(&u); err != nil {
			return err
		} else if err := b.Put(itob(int(u.ID)), v); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}
