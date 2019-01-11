package influx

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/influxdata/influxdb/chronograf"
)

// Add a new User in InfluxDB
func (c *Client) Add(ctx context.Context, u *chronograf.User) (*chronograf.User, error) {
	_, err := c.Query(ctx, chronograf.Query{
		Command: fmt.Sprintf(`CREATE USER "%s" WITH PASSWORD '%s'`, u.Name, u.Passwd),
	})
	if err != nil {
		return nil, err
	}
	for _, p := range u.Permissions {
		if err := c.grantPermission(ctx, u.Name, p); err != nil {
			return nil, err
		}
	}
	return c.Get(ctx, chronograf.UserQuery{Name: &u.Name})
}

// Delete the User from InfluxDB
func (c *Client) Delete(ctx context.Context, u *chronograf.User) error {
	res, err := c.Query(ctx, chronograf.Query{
		Command: fmt.Sprintf(`DROP USER "%s"`, u.Name),
	})
	if err != nil {
		return err
	}
	// The DROP USER statement puts the error within the results itself
	// So, we have to crack open the results to see what happens
	octets, err := res.MarshalJSON()
	if err != nil {
		return err
	}

	results := make([]struct{ Error string }, 0)
	if err := json.Unmarshal(octets, &results); err != nil {
		return err
	}

	// At last, we can check if there are any error strings
	for _, r := range results {
		if r.Error != "" {
			return fmt.Errorf(r.Error)
		}
	}
	return nil
}

// Get retrieves a user if name exists.
func (c *Client) Get(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
	if q.Name == nil {
		return nil, fmt.Errorf("query must specify name")
	}

	users, err := c.showUsers(ctx)
	if err != nil {
		return nil, err
	}

	for _, user := range users {
		if user.Name == *q.Name {
			perms, err := c.userPermissions(ctx, user.Name)
			if err != nil {
				return nil, err
			}
			user.Permissions = append(user.Permissions, perms...)
			return &user, nil
		}
	}

	return nil, fmt.Errorf("user not found")
}

// Update the user's permissions or roles
func (c *Client) Update(ctx context.Context, u *chronograf.User) error {
	// Only allow one type of change at a time. If it is a password
	// change then do it and return without any changes to permissions
	if u.Passwd != "" {
		return c.updatePassword(ctx, u.Name, u.Passwd)
	}

	user, err := c.Get(ctx, chronograf.UserQuery{Name: &u.Name})
	if err != nil {
		return err
	}

	revoke, add := Difference(u.Permissions, user.Permissions)
	for _, a := range add {
		if err := c.grantPermission(ctx, u.Name, a); err != nil {
			return err
		}
	}

	for _, r := range revoke {
		if err := c.revokePermission(ctx, u.Name, r); err != nil {
			return err
		}
	}
	return nil
}

// All users in influx
func (c *Client) All(ctx context.Context) ([]chronograf.User, error) {
	users, err := c.showUsers(ctx)
	if err != nil {
		return nil, err
	}

	// For all users we need to look up permissions to add to the user.
	for i, user := range users {
		perms, err := c.userPermissions(ctx, user.Name)
		if err != nil {
			return nil, err
		}

		user.Permissions = append(user.Permissions, perms...)
		users[i] = user
	}
	return users, nil
}

// Num is the number of users in DB
func (c *Client) Num(ctx context.Context) (int, error) {
	all, err := c.All(ctx)
	if err != nil {
		return 0, err
	}

	return len(all), nil
}

// showUsers runs SHOW USERS InfluxQL command and returns chronograf users.
func (c *Client) showUsers(ctx context.Context) ([]chronograf.User, error) {
	res, err := c.Query(ctx, chronograf.Query{
		Command: `SHOW USERS`,
	})
	if err != nil {
		return nil, err
	}
	octets, err := res.MarshalJSON()
	if err != nil {
		return nil, err
	}

	results := showResults{}
	if err := json.Unmarshal(octets, &results); err != nil {
		return nil, err
	}

	return results.Users(), nil
}

func (c *Client) grantPermission(ctx context.Context, username string, perm chronograf.Permission) error {
	query := ToGrant(username, perm)
	if query == "" {
		return nil
	}

	_, err := c.Query(ctx, chronograf.Query{
		Command: query,
	})
	return err
}

func (c *Client) revokePermission(ctx context.Context, username string, perm chronograf.Permission) error {
	query := ToRevoke(username, perm)
	if query == "" {
		return nil
	}

	_, err := c.Query(ctx, chronograf.Query{
		Command: query,
	})
	return err
}

func (c *Client) userPermissions(ctx context.Context, name string) (chronograf.Permissions, error) {
	res, err := c.Query(ctx, chronograf.Query{
		Command: fmt.Sprintf(`SHOW GRANTS FOR "%s"`, name),
	})
	if err != nil {
		return nil, err
	}

	octets, err := res.MarshalJSON()
	if err != nil {
		return nil, err
	}

	results := showResults{}
	if err := json.Unmarshal(octets, &results); err != nil {
		return nil, err
	}
	return results.Permissions(), nil
}

func (c *Client) updatePassword(ctx context.Context, name, passwd string) error {
	res, err := c.Query(ctx, chronograf.Query{
		Command: fmt.Sprintf(`SET PASSWORD for "%s" = '%s'`, name, passwd),
	})
	if err != nil {
		return err
	}
	// The SET PASSWORD statements puts the error within the results itself
	// So, we have to crack open the results to see what happens
	octets, err := res.MarshalJSON()
	if err != nil {
		return err
	}

	results := make([]struct{ Error string }, 0)
	if err := json.Unmarshal(octets, &results); err != nil {
		return err
	}

	// At last, we can check if there are any error strings
	for _, r := range results {
		if r.Error != "" {
			return fmt.Errorf(r.Error)
		}
	}
	return nil
}
