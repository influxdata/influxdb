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
		Name: u.Name,
	}, nil
}

// Update the user's permissions or roles
func (c *Client) Update(ctx context.Context, u *chronograf.User) error {
	// TODO: Update permissions
	return c.Ctrl.ChangePassword(ctx, u.Name, u.Passwd)
}
