package influx

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/influxdata/chronograf"
)

// Create a new User in InfluxDB
func (c *Client) Add(ctx context.Context, u *chronograf.User) (*chronograf.User, error) {
	_, err := c.Query(ctx, chronograf.Query{
		Command: fmt.Sprintf(`CREATE USER %s WITH PASSWORD '%s'`, u.Name, u.Passwd),
	})
	if err != nil {
		return nil, err
	}

	return u, nil
}

// Delete the User from InfluxDB
func (c *Client) Delete(ctx context.Context, u *chronograf.User) error {
	res, err := c.Query(ctx, chronograf.Query{
		Command: fmt.Sprintf(`DROP USER %s`, u.Name),
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
	if err := json.Unmarshal(octets, results); err != nil {
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
func (c *Client) Get(ctx context.Context, name string) (*chronograf.User, error) {
	users, err := c.All(ctx)
	if err != nil {
		return nil, err
	}

	for _, user := range users {
		if user.Name == name {
			return &user, nil
		}
	}

	return nil, fmt.Errorf("user not found")
}

// Update the user's permissions or roles
func (c *Client) Update(context.Context, *chronograf.User) error { return nil }

// All users in influx
func (c *Client) All(ctx context.Context) ([]chronograf.User, error) {
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

	users := results.Users()
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

type showResults []struct {
	Series []struct {
		Values [][]interface{} `json:"values"`
	} `json:"series"`
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

func (r *showResults) Users() []chronograf.User {
	res := []chronograf.User{}
	for _, u := range *r {
		for _, s := range u.Series {
			for _, v := range s.Values {
				if name, ok := v[0].(string); !ok {
					continue
				} else if admin, ok := v[1].(bool); !ok {
					continue
				} else {
					c := chronograf.User{
						Name: name,
					}
					if admin {
						c.Permissions = adminPerms()
					}
					res = append(res, c)
				}
			}
		}
	}
	return res
}

func (r *showResults) Permissions() chronograf.Permissions {
	res := []chronograf.Permission{}
	for _, u := range *r {
		for _, s := range u.Series {
			for _, v := range s.Values {
				if db, ok := v[0].(string); !ok {
					continue
				} else if priv, ok := v[1].(string); !ok {
					continue
				} else {
					c := chronograf.Permission{
						Name:    db,
						Allowed: []string{priv},
					}
					res = append(res, c)
				}
			}
		}
	}
	return res
}

func adminPerms() chronograf.Permissions {
	return []chronograf.Permission{
		{
			Scope:   chronograf.AllScope,
			Allowed: []string{"ALL"},
		},
	}
}
