package enterprise

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/influxdata/influxdb/chronograf"
	"github.com/influxdata/influxdb/chronograf/influx"
)

// Shared transports for all clients to prevent leaking connections
var (
	skipVerifyTransport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	defaultTransport = &http.Transport{}
)

type client interface {
	Do(URL *url.URL, path, method string, authorizer influx.Authorizer, params map[string]string, body io.Reader) (*http.Response, error)
}

// MetaClient represents a Meta node in an Influx Enterprise cluster
type MetaClient struct {
	URL        *url.URL
	client     client
	authorizer influx.Authorizer
}

// NewMetaClient represents a meta node in an Influx Enterprise cluster
func NewMetaClient(url *url.URL, InsecureSkipVerify bool, authorizer influx.Authorizer) *MetaClient {
	return &MetaClient{
		URL: url,
		client: &defaultClient{
			InsecureSkipVerify: InsecureSkipVerify,
		},
		authorizer: authorizer,
	}
}

type jsonLDAPConfig struct {
	Enabled bool `json:"enabled"`
}

// LDAPConfig represents the configuration for ldap from influxdb
type LDAPConfig struct {
	Structured jsonLDAPConfig `json:"structured"`
}

func (m *MetaClient) requestLDAPChannel(ctx context.Context, errors chan error) chan *http.Response {
	channel := make(chan *http.Response, 1)
	go (func() {
		res, err := m.Do(ctx, "/ldap/v1/config", "GET", m.authorizer, nil, nil)
		if err != nil {
			errors <- err
		} else {
			channel <- res
		}
	})()

	return channel
}

// GetLDAPConfig get the current ldap config response from influxdb enterprise
func (m *MetaClient) GetLDAPConfig(ctx context.Context) (*LDAPConfig, error) {
	ctxt, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	errorCh := make(chan error, 1)
	responseChannel := m.requestLDAPChannel(ctxt, errorCh)

	select {
	case res := <-responseChannel:
		result, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return nil, err
		}

		var config LDAPConfig
		err = json.Unmarshal(result, &config)
		if err != nil {
			return nil, err
		}

		return &config, nil
	case err := <-errorCh:
		return nil, err
	case <-ctxt.Done():
		return nil, ctxt.Err()
	}
}

// ShowCluster returns the cluster configuration (not health)
func (m *MetaClient) ShowCluster(ctx context.Context) (*Cluster, error) {
	res, err := m.Do(ctx, "/show-cluster", "GET", m.authorizer, nil, nil)
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
func (m *MetaClient) Users(ctx context.Context, name *string) (*Users, error) {
	params := map[string]string{}
	if name != nil {
		params["name"] = *name
	}
	res, err := m.Do(ctx, "/user", "GET", m.authorizer, params, nil)
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
func (m *MetaClient) User(ctx context.Context, name string) (*User, error) {
	users, err := m.Users(ctx, &name)
	if err != nil {
		return nil, err
	}

	for _, user := range users.Users {
		return &user, nil
	}
	return nil, fmt.Errorf("no user found")
}

// CreateUser adds a user to Influx Enterprise
func (m *MetaClient) CreateUser(ctx context.Context, name, passwd string) error {
	return m.CreateUpdateUser(ctx, "create", name, passwd)
}

// ChangePassword updates a user's password in Influx Enterprise
func (m *MetaClient) ChangePassword(ctx context.Context, name, passwd string) error {
	return m.CreateUpdateUser(ctx, "change-password", name, passwd)
}

// CreateUpdateUser is a helper function to POST to the /user Influx Enterprise endpoint
func (m *MetaClient) CreateUpdateUser(ctx context.Context, action, name, passwd string) error {
	a := &UserAction{
		Action: action,
		User: &User{
			Name:     name,
			Password: passwd,
		},
	}
	return m.Post(ctx, "/user", a, nil)
}

// DeleteUser removes a user from Influx Enterprise
func (m *MetaClient) DeleteUser(ctx context.Context, name string) error {
	a := &UserAction{
		Action: "delete",
		User: &User{
			Name: name,
		},
	}

	return m.Post(ctx, "/user", a, nil)
}

// RemoveUserPerms revokes permissions for a user in Influx Enterprise
func (m *MetaClient) RemoveUserPerms(ctx context.Context, name string, perms Permissions) error {
	a := &UserAction{
		Action: "remove-permissions",
		User: &User{
			Name:        name,
			Permissions: perms,
		},
	}
	return m.Post(ctx, "/user", a, nil)
}

// SetUserPerms removes permissions not in set and then adds the requested perms
func (m *MetaClient) SetUserPerms(ctx context.Context, name string, perms Permissions) error {
	user, err := m.User(ctx, name)
	if err != nil {
		return err
	}

	revoke, add := permissionsDifference(perms, user.Permissions)

	// first, revoke all the permissions the user currently has, but,
	// shouldn't...
	if len(revoke) > 0 {
		err := m.RemoveUserPerms(ctx, name, revoke)
		if err != nil {
			return err
		}
	}

	// ... next, add any permissions the user should have
	if len(add) > 0 {
		a := &UserAction{
			Action: "add-permissions",
			User: &User{
				Name:        name,
				Permissions: add,
			},
		}
		return m.Post(ctx, "/user", a, nil)
	}
	return nil
}

// UserRoles returns a map of users to all of their current roles
func (m *MetaClient) UserRoles(ctx context.Context) (map[string]Roles, error) {
	res, err := m.Roles(ctx, nil)
	if err != nil {
		return nil, err
	}

	userRoles := make(map[string]Roles)
	for _, role := range res.Roles {
		for _, u := range role.Users {
			ur, ok := userRoles[u]
			if !ok {
				ur = Roles{}
			}
			ur.Roles = append(ur.Roles, role)
			userRoles[u] = ur
		}
	}
	return userRoles, nil
}

// Roles gets all the roles.  If name is not nil it filters for a single role
func (m *MetaClient) Roles(ctx context.Context, name *string) (*Roles, error) {
	params := map[string]string{}
	if name != nil {
		params["name"] = *name
	}
	res, err := m.Do(ctx, "/role", "GET", m.authorizer, params, nil)
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
func (m *MetaClient) Role(ctx context.Context, name string) (*Role, error) {
	roles, err := m.Roles(ctx, &name)
	if err != nil {
		return nil, err
	}
	for _, role := range roles.Roles {
		return &role, nil
	}
	return nil, fmt.Errorf("no role found")
}

// CreateRole adds a role to Influx Enterprise
func (m *MetaClient) CreateRole(ctx context.Context, name string) error {
	a := &RoleAction{
		Action: "create",
		Role: &Role{
			Name: name,
		},
	}
	return m.Post(ctx, "/role", a, nil)
}

// DeleteRole removes a role from Influx Enterprise
func (m *MetaClient) DeleteRole(ctx context.Context, name string) error {
	a := &RoleAction{
		Action: "delete",
		Role: &Role{
			Name: name,
		},
	}
	return m.Post(ctx, "/role", a, nil)
}

// RemoveRolePerms revokes permissions from a role
func (m *MetaClient) RemoveRolePerms(ctx context.Context, name string, perms Permissions) error {
	a := &RoleAction{
		Action: "remove-permissions",
		Role: &Role{
			Name:        name,
			Permissions: perms,
		},
	}
	return m.Post(ctx, "/role", a, nil)
}

// SetRolePerms removes permissions not in set and then adds the requested perms to role
func (m *MetaClient) SetRolePerms(ctx context.Context, name string, perms Permissions) error {
	role, err := m.Role(ctx, name)
	if err != nil {
		return err
	}

	revoke, add := permissionsDifference(perms, role.Permissions)

	// first, revoke all the permissions the role currently has, but,
	// shouldn't...
	if len(revoke) > 0 {
		err := m.RemoveRolePerms(ctx, name, revoke)
		if err != nil {
			return err
		}
	}

	// ... next, add any permissions the role should have
	if len(add) > 0 {
		a := &RoleAction{
			Action: "add-permissions",
			Role: &Role{
				Name:        name,
				Permissions: add,
			},
		}
		return m.Post(ctx, "/role", a, nil)
	}
	return nil
}

// SetRoleUsers removes users not in role and then adds the requested users to role
func (m *MetaClient) SetRoleUsers(ctx context.Context, name string, users []string) error {
	role, err := m.Role(ctx, name)
	if err != nil {
		return err
	}
	revoke, add := Difference(users, role.Users)
	if err := m.RemoveRoleUsers(ctx, name, revoke); err != nil {
		return err
	}

	return m.AddRoleUsers(ctx, name, add)
}

// Difference compares two sets and returns a set to be removed and a set to be added
func Difference(wants []string, haves []string) (revoke []string, add []string) {
	for _, want := range wants {
		found := false
		for _, got := range haves {
			if want != got {
				continue
			}
			found = true
		}
		if !found {
			add = append(add, want)
		}
	}
	for _, got := range haves {
		found := false
		for _, want := range wants {
			if want != got {
				continue
			}
			found = true
			break
		}
		if !found {
			revoke = append(revoke, got)
		}
	}
	return
}

func permissionsDifference(wants Permissions, haves Permissions) (revoke Permissions, add Permissions) {
	revoke = make(Permissions)
	add = make(Permissions)
	for scope, want := range wants {
		have, ok := haves[scope]
		if ok {
			r, a := Difference(want, have)
			revoke[scope] = r
			add[scope] = a
		} else {
			add[scope] = want
		}
	}

	for scope, have := range haves {
		_, ok := wants[scope]
		if !ok {
			revoke[scope] = have
		}
	}
	return
}

// AddRoleUsers updates a role to have additional users.
func (m *MetaClient) AddRoleUsers(ctx context.Context, name string, users []string) error {
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
	return m.Post(ctx, "/role", a, nil)
}

// RemoveRoleUsers updates a role to remove some users.
func (m *MetaClient) RemoveRoleUsers(ctx context.Context, name string, users []string) error {
	// No permissions to add, so, role is in the right state
	if len(users) == 0 {
		return nil
	}

	a := &RoleAction{
		Action: "remove-users",
		Role: &Role{
			Name:  name,
			Users: users,
		},
	}
	return m.Post(ctx, "/role", a, nil)
}

// Post is a helper function to POST to Influx Enterprise
func (m *MetaClient) Post(ctx context.Context, path string, action interface{}, params map[string]string) error {
	b, err := json.Marshal(action)
	if err != nil {
		return err
	}
	body := bytes.NewReader(b)
	_, err = m.Do(ctx, path, "POST", m.authorizer, params, body)
	if err != nil {
		return err
	}
	return nil
}

type defaultClient struct {
	Leader             string
	InsecureSkipVerify bool
}

// Do is a helper function to interface with Influx Enterprise's Meta API
func (d *defaultClient) Do(URL *url.URL, path, method string, authorizer influx.Authorizer, params map[string]string, body io.Reader) (*http.Response, error) {
	p := url.Values{}
	for k, v := range params {
		p.Add(k, v)
	}

	URL.Path = path
	URL.RawQuery = p.Encode()
	if d.Leader == "" {
		d.Leader = URL.Host
	} else if d.Leader != URL.Host {
		URL.Host = d.Leader
	}

	req, err := http.NewRequest(method, URL.String(), body)
	if err != nil {
		return nil, err
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	if authorizer != nil {
		if err = authorizer.Set(req); err != nil {
			return nil, err
		}
	}

	// Meta servers will redirect (307) to leader. We need
	// special handling to preserve authentication headers.
	client := &http.Client{
		CheckRedirect: d.AuthedCheckRedirect,
	}

	if d.InsecureSkipVerify {
		client.Transport = skipVerifyTransport
	} else {
		client.Transport = defaultTransport
	}

	res, err := client.Do(req)
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

// AuthedCheckRedirect tries to follow the Influx Enterprise pattern of
// redirecting to the leader but preserving authentication headers.
func (d *defaultClient) AuthedCheckRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return errors.New("too many redirects")
	} else if len(via) == 0 {
		return nil
	}
	preserve := "Authorization"
	if auth, ok := via[0].Header[preserve]; ok {
		req.Header[preserve] = auth
	}
	d.Leader = req.URL.Host
	return nil
}

// Do is a cancelable function to interface with Influx Enterprise's Meta API
func (m *MetaClient) Do(ctx context.Context, path, method string, authorizer influx.Authorizer, params map[string]string, body io.Reader) (*http.Response, error) {
	type result struct {
		Response *http.Response
		Err      error
	}

	resps := make(chan (result))
	go func() {
		resp, err := m.client.Do(m.URL, path, method, authorizer, params, body)
		resps <- result{resp, err}
	}()

	select {
	case resp := <-resps:
		return resp.Response, resp.Err
	case <-ctx.Done():
		return nil, chronograf.ErrUpstreamTimeout
	}
}
