package enterprise

import (
	"context"

	"github.com/influxdata/chronograf"
)

// UserStore uses a control client operate on Influx Enterprise users
type UserStore struct {
	Ctrl
	Logger chronograf.Logger
}

// Add creates a new User in Influx Enterprise
func (c *UserStore) Add(ctx context.Context, u *chronograf.User) (*chronograf.User, error) {
	if err := c.Ctrl.CreateUser(ctx, u.Name, u.Passwd); err != nil {
		return nil, err
	}
	return u, nil
}

// Delete the User from Influx Enterprise
func (c *UserStore) Delete(ctx context.Context, u *chronograf.User) error {
	return c.Ctrl.DeleteUser(ctx, u.Name)
}

// Get retrieves a user if name exists.
func (c *UserStore) Get(ctx context.Context, name string) (*chronograf.User, error) {
	u, err := c.Ctrl.User(ctx, name)
	if err != nil {
		return nil, err
	}
	return &chronograf.User{
		Name:        u.Name,
		Permissions: ToChronograf(u.Permissions),
	}, nil
}

// Update the user's permissions or roles
func (c *UserStore) Update(ctx context.Context, u *chronograf.User) error {
	// Only allow one type of change at a time. If it is a password
	// change then do it and return without any changes to permissions
	if u.Passwd != "" {
		return c.Ctrl.ChangePassword(ctx, u.Name, u.Passwd)
	}
	perms := ToEnterprise(u.Permissions)
	return c.Ctrl.SetUserPerms(ctx, u.Name, perms)
}

// All is all users in influx
func (c *UserStore) All(ctx context.Context) ([]chronograf.User, error) {
	all, err := c.Ctrl.Users(ctx, nil)
	if err != nil {
		return nil, err
	}

	res := make([]chronograf.User, len(all.Users))
	for i, user := range all.Users {
		res[i] = chronograf.User{
			Name:        user.Name,
			Permissions: ToChronograf(user.Permissions),
		}
	}
	return res, nil
}

// ToEnterprise converts chronograf permission shape to enterprise
func ToEnterprise(perms chronograf.Permissions) Permissions {
	res := Permissions{}
	for _, perm := range perms {
		if perm.Scope == chronograf.AllScope {
			// Enterprise uses empty string as the key for all databases
			res[""] = perm.Allowed
		} else {
			res[perm.Name] = perm.Allowed
		}
	}
	return res
}

// ToChronograf converts enterprise permissions shape to chronograf shape
func ToChronograf(perms Permissions) chronograf.Permissions {
	res := chronograf.Permissions{}
	for db, perm := range perms {
		// Enterprise uses empty string as the key for all databases
		if db == "" {
			res = append(res, chronograf.Permission{
				Scope:   chronograf.AllScope,
				Allowed: perm,
			})
		} else {
			res = append(res, chronograf.Permission{
				Scope:   chronograf.DBScope,
				Name:    db,
				Allowed: perm,
			})

		}
	}
	return res
}
