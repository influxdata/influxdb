package launcher_test

import (
	"encoding/json"
	"fmt"
	"io"
	nethttp "net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	th "github.com/influxdata/influxdb/v2/pkg/testing/helper"
	"github.com/influxdata/influxdb/v2/tenant"
)

// TestLauncher_SessionAccess signs in as an organization's owner and as one of
// its members, as the UI does, and checks what each session can do: the owner
// creates an all-access token and updates the organization, and the member reads
// the organization and reads and updates their own user.
func TestLauncher_SessionAccess(t *testing.T) {
	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
	defer l.ShutdownOrFail(t, ctx)

	org := l.Org
	users := &tenant.UserClientService{Client: l.HTTPClient(t)}
	urms := &tenant.UserResourceMappingClient{Client: l.HTTPClient(t)}

	// do sends req and returns the status code and response body.
	do := func(t *testing.T, req *nethttp.Request) (int, string) {
		t.Helper()
		resp, err := nethttp.DefaultClient.Do(req)
		require.NoError(t, err)
		defer th.CheckedClose(t, resp.Body)()
		b, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return resp.StatusCode, string(b)
	}

	// newUser creates a user with the given role in org and a password.
	newUser := func(t *testing.T, name string, role influxdb.UserType, password string) *influxdb.User {
		t.Helper()
		u := &influxdb.User{Name: name, Status: influxdb.Active}
		require.NoError(t, users.CreateUser(ctx, u))
		require.NoError(t, urms.CreateUserResourceMapping(ctx, &influxdb.UserResourceMapping{
			UserID:       u.ID,
			UserType:     role,
			ResourceType: influxdb.OrgsResourceType,
			ResourceID:   org.ID,
		}))
		body := fmt.Sprintf(`{"password":%q}`, password)
		code, resp := do(t, l.NewHTTPRequestOrFail(t, "POST", "/api/v2/users/"+u.ID.String()+"/password", l.Auth.Token, body))
		require.Equalf(t, nethttp.StatusNoContent, code, "setup: setting the password; body: %s", resp)
		return u
	}

	// signIn signs in as u and returns the session cookie.
	signIn := func(t *testing.T, u *influxdb.User, password string) *nethttp.Cookie {
		t.Helper()
		req, err := nethttp.NewRequest("POST", l.URL().String()+"/api/v2/signin", nil)
		require.NoError(t, err)
		req.SetBasicAuth(u.Name, password)
		resp, err := nethttp.DefaultClient.Do(req)
		require.NoError(t, err)
		defer th.CheckedClose(t, resp.Body)()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equalf(t, nethttp.StatusNoContent, resp.StatusCode, "sign in as %s; body: %s", u.Name, body)
		cookies := resp.Cookies()
		require.Len(t, cookies, 1)
		return cookies[0]
	}

	// withSession sends a request authenticated by the session cookie.
	withSession := func(t *testing.T, c *nethttp.Cookie, method, path, body string) (int, string) {
		t.Helper()
		req, err := nethttp.NewRequest(method, l.URL().String()+path, strings.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		req.AddCookie(c)
		return do(t, req)
	}

	t.Run("owner creates an all-access token and updates the organization", func(t *testing.T) {
		const password = "owner-password"
		owner := newUser(t, "session-owner", influxdb.Owner, password)
		session := signIn(t, owner, password)

		// The permissions the CLI and UI give an all-access token: the
		// organization and the user by ID with no organization, and every other
		// type across the organization.
		var perms []influxdb.Permission
		for _, rt := range influxdb.AllResourceTypes {
			for _, a := range []influxdb.Action{influxdb.ReadAction, influxdb.WriteAction} {
				switch rt {
				case influxdb.InstanceResourceType:
				case influxdb.OrgsResourceType:
					if a == influxdb.ReadAction {
						id := org.ID
						perms = append(perms, influxdb.Permission{Action: a, Resource: influxdb.Resource{Type: rt, ID: &id}})
					}
				case influxdb.UsersResourceType:
					id := owner.ID
					perms = append(perms, influxdb.Permission{Action: a, Resource: influxdb.Resource{Type: rt, ID: &id}})
				default:
					oid := org.ID
					perms = append(perms, influxdb.Permission{Action: a, Resource: influxdb.Resource{Type: rt, OrgID: &oid}})
				}
			}
		}
		req, err := json.Marshal(map[string]any{"orgID": org.ID.String(), "permissions": perms})
		require.NoError(t, err)

		code, body := withSession(t, session, "POST", "/api/v2/authorizations", string(req))
		require.Equalf(t, nethttp.StatusCreated, code, "body: %s", body)
		var created struct {
			Token       string                `json:"token"`
			Permissions []influxdb.Permission `json:"permissions"`
		}
		require.NoError(t, json.Unmarshal([]byte(body), &created))
		require.NotEmpty(t, created.Token)
		require.Len(t, created.Permissions, len(perms))

		code, body = withSession(t, session, "PATCH", "/api/v2/orgs/"+org.ID.String(), `{"description":"updated by its owner"}`)
		require.Equalf(t, nethttp.StatusOK, code, "body: %s", body)
		got, err := l.OrgService(t).FindOrganizationByID(ctx, org.ID)
		require.NoError(t, err)
		require.Equal(t, "updated by its owner", got.Description)
	})

	t.Run("member reads the organization and updates their own user", func(t *testing.T) {
		const password = "member-password"
		member := newUser(t, "session-member", influxdb.Member, password)
		session := signIn(t, member, password)

		code, body := withSession(t, session, "GET", "/api/v2/orgs/"+org.ID.String(), "")
		require.Equalf(t, nethttp.StatusOK, code, "body: %s", body)
		require.Contains(t, body, org.Name)

		code, body = withSession(t, session, "GET", "/api/v2/users/"+member.ID.String(), "")
		require.Equalf(t, nethttp.StatusOK, code, "body: %s", body)
		require.Contains(t, body, member.Name)

		code, body = withSession(t, session, "PATCH", "/api/v2/users/"+member.ID.String(), `{"name":"session-member-renamed"}`)
		require.Equalf(t, nethttp.StatusOK, code, "body: %s", body)
		got, err := users.FindUserByID(ctx, member.ID)
		require.NoError(t, err)
		require.Equal(t, "session-member-renamed", got.Name)
	})
}
