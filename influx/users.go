package influx

import (
	"context"

	"github.com/influxdata/chronograf"
)

// Create a new User in InfluxDB
func (c *Client) Add(context.Context, *chronograf.User) (*chronograf.User, error) { return nil, nil }

// Delete the User from InfluxDB
func (c *Client) Delete(context.Context, *chronograf.User) error { return nil }

// Get retrieves a user if name exists.
func (c *Client) Get(ctx context.Context, name string) (*chronograf.User, error) {
	return nil, nil
}

// Update the user's permissions or roles
func (c *Client) Update(context.Context, *chronograf.User) error { return nil }

// All is all users in influx
func (c *Client) All(context.Context) ([]chronograf.User, error) { return nil, nil }
