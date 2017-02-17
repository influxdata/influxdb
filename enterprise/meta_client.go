package enterprise

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

type MetaClient struct {
	MetaHostPort string
	Username     string
	Password     string
}

func (t *MetaClient) ShowCluster() (*Cluster, error) {
	res, err := t.Do("GET", "/show-cluster", nil, nil)
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
func (t *MetaClient) Users(name *string) (*Users, error) {
	params := map[string]string{}
	if name != nil {
		params["name"] = *name
	}
	res, err := t.Do("GET", "/user", params, nil)
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

func (t *MetaClient) User(name string) (*User, error) {
	users, err := t.Users(&name)
	if err != nil {
		return nil, err
	}
	for _, user := range users.Users {
		return &user, nil
	}
	return nil, fmt.Errorf("No user found")
}

func (t *MetaClient) CreateUser(name, passwd string) error {
	return t.CreateUpdateUser("create", name, passwd)
}

func (t *MetaClient) ChangePassword(name, passwd string) error {
	return t.CreateUpdateUser("change-password", name, passwd)
}

func (t *MetaClient) CreateUpdateUser(action, name, passwd string) error {
	a := &UserAction{
		Action: action,
		User: &User{
			Name:     name,
			Password: passwd,
		},
	}
	return t.Post("/user", a, nil)
}

func (t *MetaClient) DeleteUser(name string) error {
	a := &UserAction{
		Action: "delete",
		User: &User{
			Name: name,
		},
	}

	return t.Post("/user", a, nil)
}

func (t *MetaClient) RemoveAllUserPerms(name string) error {
	user, err := t.User(name)
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
	return t.Post("/user", a, nil)
}

// SetUserPerms removes all permissions and then adds the requested perms
func (t *MetaClient) SetUserPerms(name string, perms Permissions) error {
	err := t.RemoveAllUserPerms(name)
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
	return t.Post("/user", a, nil)
}

// Users gets all the roles.  If name is not nil it filters for a single role
func (t *MetaClient) Roles(name *string) (*Roles, error) {
	params := map[string]string{}
	if name != nil {
		params["name"] = *name
	}
	res, err := t.Do("GET", "/role", params, nil)
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

func (t *MetaClient) Role(name string) (*Role, error) {
	roles, err := t.Roles(&name)
	if err != nil {
		return nil, err
	}
	for _, role := range roles.Roles {
		return &role, nil
	}
	return nil, fmt.Errorf("No role found")
}

func (t *MetaClient) CreateRole(name string) error {
	a := &RoleAction{
		Action: "create",
		Role: &Role{
			Name: name,
		},
	}
	return t.Post("/role", a, nil)
}
func (t *MetaClient) DeleteRole(name string) error {
	a := &RoleAction{
		Action: "delete",
		Role: &Role{
			Name: name,
		},
	}
	return t.Post("/role", a, nil)
}

func (t *MetaClient) RemoveAllRolePerms(name string) error {
	role, err := t.Role(name)
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
	return t.Post("/role", a, nil)
}

// SetRolePerms removes all permissions and then adds the requested perms to role
func (t *MetaClient) SetRolePerms(name string, perms Permissions) error {
	err := t.RemoveAllRolePerms(name)
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
	return t.Post("/role", a, nil)
}

func (t *MetaClient) RemoveAllRoleUsers(name string) error {
	role, err := t.Role(name)
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
	return t.Post("/role", a, nil)
}

// SetRoleUsers removes all users and then adds the requested users to role
func (t *MetaClient) SetRoleUsers(name string, users []string) error {
	err := t.RemoveAllRoleUsers(name)
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
	return t.Post("/role", a, nil)
}

func (t *MetaClient) Post(path string, action interface{}, params map[string]string) error {
	b, err := json.Marshal(action)
	if err != nil {
		return err
	}
	body := bytes.NewReader(b)
	_, err = t.Do("POST", path, params, body)
	if err != nil {
		return err
	}
	return nil
}

func (t *MetaClient) Do(method, path string, params map[string]string, body io.Reader) (*http.Response, error) {
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
