package enterprise

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/influxdata/chronograf"
)

// MetaClient represents a Meta node in an Influx Enterprise cluster
type MetaClient struct {
	MetaHostPort string
	Username     string
	Password     string
}

// ShowCluster returns the cluster configuration (not health)
func (t *MetaClient) ShowCluster(ctx context.Context) (*Cluster, error) {
	res, err := t.Do(ctx, "GET", "/show-cluster", nil, nil)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	dec := json.NewDecoder(res.Body)
	out := &Cluster{}
	err = dec.Decode(out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Users gets all the users.  If name is not nil it filters for a single user
func (t *MetaClient) Users(ctx context.Context, name *string) (*Users, error) {
	params := map[string]string{}
	if name != nil {
		params["name"] = *name
	}
	res, err := t.Do(ctx, "GET", "/user", params, nil)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	dec := json.NewDecoder(res.Body)
	users := &Users{}
	err = dec.Decode(users)
	if err != nil {
		return nil, err
	}
	return users, nil
}

// User returns a single Influx Enterprise user
func (t *MetaClient) User(ctx context.Context, name string) (*User, error) {
	users, err := t.Users(ctx, &name)
	if err != nil {
		return nil, err
	}
	for _, user := range users.Users {
		return &user, nil
	}
	return nil, fmt.Errorf("No user found")
}

// CreateUser adds a user to Influx Enterprise
func (t *MetaClient) CreateUser(ctx context.Context, name, passwd string) error {
	return t.CreateUpdateUser(ctx, "create", name, passwd)
}

// ChangePassword updates a user's password in Influx Enterprise
func (t *MetaClient) ChangePassword(ctx context.Context, name, passwd string) error {
	return t.CreateUpdateUser(ctx, "change-password", name, passwd)
}

// CreateUpdateUser is a helper function to POST to the /user Influx Enterprise endpoint
func (t *MetaClient) CreateUpdateUser(ctx context.Context, action, name, passwd string) error {
	a := &UserAction{
		Action: action,
		User: &User{
			Name:     name,
			Password: passwd,
		},
	}
	return t.Post(ctx, "/user", a, nil)
}

// DeleteUser removes a user from Influx Enterprise
func (t *MetaClient) DeleteUser(ctx context.Context, name string) error {
	a := &UserAction{
		Action: "delete",
		User: &User{
			Name: name,
		},
	}

	return t.Post(ctx, "/user", a, nil)
}

// RemoveAllUserPerms revokes all permissions for a user in Influx Enterprise
func (t *MetaClient) RemoveAllUserPerms(ctx context.Context, name string) error {
	user, err := t.User(ctx, name)
	if err != nil {
		return err
	}

	// No permissions to remove
	if len(user.Permissions) == 0 {
		return nil
	}

	a := &UserAction{
		Action: "remove-permissions",
		User:   user,
	}
	return t.Post(ctx, "/user", a, nil)
}

// SetUserPerms removes all permissions and then adds the requested perms
func (t *MetaClient) SetUserPerms(ctx context.Context, name string, perms Permissions) error {
	err := t.RemoveAllUserPerms(ctx, name)
	if err != nil {
		return err
	}

	// No permissions to add, so, user is in the right state
	if len(perms) == 0 {
		return nil
	}

	a := &UserAction{
		Action: "add-permissions",
		User: &User{
			Name:        name,
			Permissions: perms,
		},
	}
	return t.Post(ctx, "/user", a, nil)
}

// Roles gets all the roles.  If name is not nil it filters for a single role
func (t *MetaClient) Roles(ctx context.Context, name *string) (*Roles, error) {
	params := map[string]string{}
	if name != nil {
		params["name"] = *name
	}
	res, err := t.Do(ctx, "GET", "/role", params, nil)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	dec := json.NewDecoder(res.Body)
	roles := &Roles{}
	err = dec.Decode(roles)
	if err != nil {
		return nil, err
	}
	return roles, nil
}

// Role returns a single named role
func (t *MetaClient) Role(ctx context.Context, name string) (*Role, error) {
	roles, err := t.Roles(ctx, &name)
	if err != nil {
		return nil, err
	}
	for _, role := range roles.Roles {
		return &role, nil
	}
	return nil, fmt.Errorf("No role found")
}

// CreateRole adds a role to Influx Enterprise
func (t *MetaClient) CreateRole(ctx context.Context, name string) error {
	a := &RoleAction{
		Action: "create",
		Role: &Role{
			Name: name,
		},
	}
	return t.Post(ctx, "/role", a, nil)
}

// DeleteRole removes a role from Influx Enterprise
func (t *MetaClient) DeleteRole(ctx context.Context, name string) error {
	a := &RoleAction{
		Action: "delete",
		Role: &Role{
			Name: name,
		},
	}
	return t.Post(ctx, "/role", a, nil)
}

// RemoveAllRolePerms removes all permissions from a role
func (t *MetaClient) RemoveAllRolePerms(ctx context.Context, name string) error {
	role, err := t.Role(ctx, name)
	if err != nil {
		return err
	}

	// No permissions to remove
	if len(role.Permissions) == 0 {
		return nil
	}

	a := &RoleAction{
		Action: "remove-permissions",
		Role:   role,
	}
	return t.Post(ctx, "/role", a, nil)
}

// SetRolePerms removes all permissions and then adds the requested perms to role
func (t *MetaClient) SetRolePerms(ctx context.Context, name string, perms Permissions) error {
	err := t.RemoveAllRolePerms(ctx, name)
	if err != nil {
		return err
	}

	// No permissions to add, so, role is in the right state
	if len(perms) == 0 {
		return nil
	}

	a := &RoleAction{
		Action: "add-permissions",
		Role: &Role{
			Name:        name,
			Permissions: perms,
		},
	}
	return t.Post(ctx, "/role", a, nil)
}

// RemoveAllRoleUsers removes all users from a role
func (t *MetaClient) RemoveAllRoleUsers(ctx context.Context, name string) error {
	role, err := t.Role(ctx, name)
	if err != nil {
		return err
	}

	// No users to remove
	if len(role.Users) == 0 {
		return nil
	}

	a := &RoleAction{
		Action: "remove-users",
		Role:   role,
	}
	return t.Post(ctx, "/role", a, nil)
}

// SetRoleUsers removes all users and then adds the requested users to role
func (t *MetaClient) SetRoleUsers(ctx context.Context, name string, users []string) error {
	err := t.RemoveAllRoleUsers(ctx, name)
	if err != nil {
		return err
	}

	// No permissions to add, so, role is in the right state
	if len(users) == 0 {
		return nil
	}

	a := &RoleAction{
		Action: "add-users",
		Role: &Role{
			Name:  name,
			Users: users,
		},
	}
	return t.Post(ctx, "/role", a, nil)
}

// Post is a helper function to POST to Influx Enterprise
func (t *MetaClient) Post(ctx context.Context, path string, action interface{}, params map[string]string) error {
	b, err := json.Marshal(action)
	if err != nil {
		return err
	}
	body := bytes.NewReader(b)
	_, err = t.Do(ctx, "POST", path, params, body)
	if err != nil {
		return err
	}
	return nil
}

// do is a helper function to interface with Influx Enterprise's Meta API
func (t *MetaClient) do(method, path string, params map[string]string, body io.Reader) (*http.Response, error) {
	p := url.Values{}
	p.Add("u", t.Username)
	p.Add("p", t.Password)
	for k, v := range params {
		p.Add(k, v)
	}
	URL := url.URL{
		Scheme:   "http",
		Host:     t.MetaHostPort,
		Path:     path,
		RawQuery: p.Encode(),
	}

	req, err := http.NewRequest(method, URL.String(), body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != http.StatusOK {
		defer res.Body.Close()
		dec := json.NewDecoder(res.Body)
		out := &Error{}
		err = dec.Decode(out)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(out.Error)
	}

	return res, nil

}

// Do is a cancelable function to interface with Influx Enterprise's Meta API
func (t *MetaClient) Do(ctx context.Context, method, path string, params map[string]string, body io.Reader) (*http.Response, error) {
	type result struct {
		Response *http.Response
		Err      error
	}
	resps := make(chan (result))
	go func() {
		resp, err := t.do(method, path, params, body)
		resps <- result{resp, err}
	}()

	select {
	case resp := <-resps:
		return resp.Response, resp.Err
	case <-ctx.Done():
		return nil, chronograf.ErrUpstreamTimeout
	}
}
