package enterprise

import (
	"context"

	"github.com/influxdata/chronograf"
)

// Create a new User in Influx Enterprise
func (c *Client) Add(ctx context.Context, u *chronograf.User) (*chronograf.User, error) {
	if err := c.Ctrl.CreateUser(ctx, u.Name, u.Passwd); err != nil {
		return nil, err
	}
	return u, nil
}

// Delete the User from Influx Enterprise
func (c *Client) Delete(ctx context.Context, u *chronograf.User) error {
	return c.Ctrl.DeleteUser(ctx, u.Name)
}

// Get retrieves a user if name exists.
func (c *Client) Get(ctx context.Context, name string) (*chronograf.User, error) {
	u, err := c.Ctrl.User(ctx, name)
	if err != nil {
		return nil, err
	}
	return &chronograf.User{
		Name:        u.Name,
		Permissions: toChronograf(u.Permissions),
	}, nil
}

// Update the user's permissions or roles
func (c *Client) Update(ctx context.Context, u *chronograf.User) error {
	// Only allow one type of change at a time. If it is a password
	// change then do it and return without any changes to permissions
	if u.Passwd != "" {
		return c.Ctrl.ChangePassword(ctx, u.Name, u.Passwd)
	}
	perms := toEnterprise(u.Permissions)
	return c.Ctrl.SetUserPerms(ctx, u.Name, perms)
}

// All is all users in influx
func (c *Client) All(ctx context.Context) ([]chronograf.User, error) {
	all, err := c.Ctrl.Users(ctx, nil)
	if err != nil {
		return nil, err
	}

	res := make([]chronograf.User, len(all.Users))
	for i, user := range all.Users {
		res[i] = chronograf.User{
			Name:        user.Name,
			Permissions: toChronograf(user.Permissions),
		}
	}
	return res, nil
}

func toEnterprise(perms chronograf.Permissions) Permissions {
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

func toChronograf(perms Permissions) chronograf.Permissions {
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
