package tenant

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	"golang.org/x/crypto/bcrypt"
)

type UserSvc struct {
	store *Store
	svc   *Service
}

func NewUserSvc(st *Store, svc *Service) *UserSvc {
	return &UserSvc{
		store: st,
		svc:   svc,
	}
}

// Returns a single user by ID.
func (s *UserSvc) FindUserByID(ctx context.Context, id platform.ID) (*influxdb.User, error) {
	var user *influxdb.User
	err := s.store.View(ctx, func(tx kv.Tx) error {
		u, err := s.store.GetUser(ctx, tx, id)
		if err != nil {
			return err
		}
		user = u
		return nil
	})

	if err != nil {
		return nil, err
	}

	return user, nil
}

// Returns the first user that matches filter.
func (s *UserSvc) FindUser(ctx context.Context, filter influxdb.UserFilter) (*influxdb.User, error) {
	// if im given no filters its not a valid find user request. (leaving it unchecked seems dangerous)
	if filter.ID == nil && filter.Name == nil {
		return nil, ErrUserNotFound
	}

	if filter.ID != nil {
		return s.FindUserByID(ctx, *filter.ID)
	}

	var user *influxdb.User
	err := s.store.View(ctx, func(tx kv.Tx) error {
		u, err := s.store.GetUserByName(ctx, tx, *filter.Name)
		if err != nil {
			return err
		}
		user = u
		return nil
	})
	if err != nil {
		return nil, err
	}

	return user, nil
}

// Returns a list of users that match filter and the total count of matching users.
// Additional options provide pagination & sorting. {
func (s *UserSvc) FindUsers(ctx context.Context, filter influxdb.UserFilter, opt ...influxdb.FindOptions) ([]*influxdb.User, int, error) {
	// if a id is provided we will reroute to findUserByID
	if filter.ID != nil {
		user, err := s.FindUserByID(ctx, *filter.ID)
		if err != nil {
			return nil, 0, err
		}
		return []*influxdb.User{user}, 1, nil
	}

	// if a name is provided we will reroute to findUser with a name filter
	if filter.Name != nil {
		user, err := s.FindUser(ctx, filter)
		if err != nil {
			return nil, 0, err
		}
		return []*influxdb.User{user}, 1, nil
	}

	var users []*influxdb.User
	err := s.store.View(ctx, func(tx kv.Tx) error {
		us, err := s.store.ListUsers(ctx, tx, opt...)
		if err != nil {
			return err
		}
		users = us
		return nil
	})
	if err != nil {
		return nil, 0, err
	}

	return users, len(users), nil
}

// Creates a new user and sets u.ID with the new identifier.
func (s *UserSvc) CreateUser(ctx context.Context, u *influxdb.User) error {
	err := s.store.Update(ctx, func(tx kv.Tx) error {
		return s.store.CreateUser(ctx, tx, u)
	})

	return err
}

// Updates a single user with changeset.
// Returns the new user state after update. {
func (s *UserSvc) UpdateUser(ctx context.Context, id platform.ID, upd influxdb.UserUpdate) (*influxdb.User, error) {
	var user *influxdb.User
	err := s.store.Update(ctx, func(tx kv.Tx) error {
		u, err := s.store.UpdateUser(ctx, tx, id, upd)
		if err != nil {
			return err
		}
		user = u
		return nil
	})
	if err != nil {
		return nil, err
	}

	return user, nil
}

// Removes a user by ID.
func (s *UserSvc) DeleteUser(ctx context.Context, id platform.ID) error {
	err := s.store.Update(ctx, func(tx kv.Tx) error {
		err := s.store.DeletePassword(ctx, tx, id)
		if err != nil {
			return err
		}
		return s.store.DeleteUser(ctx, tx, id)
	})
	return err
}

// FindPermissionForUser gets the full set of permission for a specified user id
func (s *UserSvc) FindPermissionForUser(ctx context.Context, uid platform.ID) (influxdb.PermissionSet, error) {
	mappings, _, err := s.svc.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{UserID: uid}, influxdb.FindOptions{Limit: 100})
	if err != nil {
		return nil, err
	}

	permissions, err := permissionFromMapping(mappings)
	if err != nil {
		return nil, err
	}

	if len(mappings) >= 100 {
		// if we got 100 mappings we probably need to pull more pages
		// account for paginated results
		for i := len(mappings); len(mappings) > 0; i += len(mappings) {
			mappings, _, err = s.svc.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{UserID: uid}, influxdb.FindOptions{Offset: i, Limit: 100})
			if err != nil {
				return nil, err
			}
			pms, err := permissionFromMapping(mappings)
			if err != nil {
				return nil, err
			}
			permissions = append(permissions, pms...)
		}
	}

	permissions = append(permissions, influxdb.MePermissions(uid)...)
	return permissions, nil
}

// SetPassword overrides the password of a known user.
func (s *UserSvc) SetPassword(ctx context.Context, userID platform.ID, password string) error {
	if len(password) < MinPasswordLen {
		return EShortPassword
	}
	passHash, err := encryptPassword(password)
	if err != nil {
		return err
	}
	// set password
	return s.store.Update(ctx, func(tx kv.Tx) error {
		_, err := s.store.GetUser(ctx, tx, userID)
		if err != nil {
			return EIncorrectUser
		}
		return s.store.SetPassword(ctx, tx, userID, passHash)
	})
}

// ComparePassword checks if the password matches the password recorded.
// Passwords that do not match return errors.
func (s *UserSvc) ComparePassword(ctx context.Context, userID platform.ID, password string) error {
	// get password
	var hash []byte
	err := s.store.View(ctx, func(tx kv.Tx) error {

		_, err := s.store.GetUser(ctx, tx, userID)
		if err != nil {
			return EIncorrectUser
		}
		h, err := s.store.GetPassword(ctx, tx, userID)
		if err != nil {
			if err == kv.ErrKeyNotFound {
				return EIncorrectPassword
			}
			return err
		}
		hash = []byte(h)
		return nil
	})
	if err != nil {
		return err
	}
	// compare password
	if err := bcrypt.CompareHashAndPassword(hash, []byte(password)); err != nil {
		return EIncorrectPassword
	}

	return nil
}

// CompareAndSetPassword checks the password and if they match
// updates to the new password.
func (s *UserSvc) CompareAndSetPassword(ctx context.Context, userID platform.ID, old, new string) error {
	err := s.ComparePassword(ctx, userID, old)
	if err != nil {
		return err
	}

	return s.SetPassword(ctx, userID, new)
}

func encryptPassword(password string) (string, error) {
	passHash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(passHash), nil
}

func permissionFromMapping(mappings []*influxdb.UserResourceMapping) ([]influxdb.Permission, error) {
	ps := make([]influxdb.Permission, 0, len(mappings))
	for _, m := range mappings {
		p, err := m.ToPermissions()
		if err != nil {
			return nil, &errors.Error{
				Err: err,
			}
		}

		ps = append(ps, p...)
	}

	return ps, nil
}
