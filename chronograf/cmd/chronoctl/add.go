package main

import (
	"context"
	"strings"

	"github.com/influxdata/chronograf"
)

type AddCommand struct {
	BoltPath      string  `short:"b" long:"bolt-path" description:"Full path to boltDB file (e.g. './chronograf-v1.db')" env:"BOLT_PATH" default:"chronograf-v1.db"`
	ID            *uint64 `short:"i" long:"id" description:"Users ID. Must be id for existing user"`
	Username      string  `short:"n" long:"name" description:"Users name. Must be Oauth-able email address or username"`
	Provider      string  `short:"p" long:"provider" description:"Name of the Auth provider (e.g. google, github, auth0, or generic)"`
	Scheme        string  `short:"s" long:"scheme" description:"Authentication scheme that matches auth provider (e.g. oauth2)" default:"oauth2"`
	Organizations string  `short:"o" long:"orgs" description:"A comma separated list of organizations that the user should be added to" default:"default"`
}

var addCommand AddCommand

func (l *AddCommand) Execute(args []string) error {
	c, err := NewBoltClient(l.BoltPath)
	if err != nil {
		return err
	}
	defer c.Close()

	q := chronograf.UserQuery{
		Name:     &l.Username,
		Provider: &l.Provider,
		Scheme:   &l.Scheme,
	}

	if l.ID != nil {
		q.ID = l.ID
	}

	ctx := context.Background()

	user, err := c.UsersStore.Get(ctx, q)
	if err != nil && err != chronograf.ErrUserNotFound {
		return err
	} else if err == chronograf.ErrUserNotFound {
		user = &chronograf.User{
			Name:     l.Username,
			Provider: l.Provider,
			Scheme:   l.Scheme,
			Roles: []chronograf.Role{
				{
					Name:         "member",
					Organization: "default",
				},
			},
			SuperAdmin: true,
		}

		user, err = c.UsersStore.Add(ctx, user)
		if err != nil {
			return err
		}
	} else {
		user.SuperAdmin = true
		if len(user.Roles) == 0 {
			user.Roles = []chronograf.Role{
				{
					Name:         "member",
					Organization: "default",
				},
			}
		}
		if err = c.UsersStore.Update(ctx, user); err != nil {
			return err
		}
	}

	// TODO(desa): Apply mapping to user and update their roles
	roles := []chronograf.Role{}
OrgLoop:
	for _, org := range strings.Split(l.Organizations, ",") {
		// Check to see is user is already a part of the organization
		for _, r := range user.Roles {
			if r.Organization == org {
				continue OrgLoop
			}
		}

		orgQuery := chronograf.OrganizationQuery{
			ID: &org,
		}
		o, err := c.OrganizationsStore.Get(ctx, orgQuery)
		if err != nil {
			return err
		}

		role := chronograf.Role{
			Organization: org,
			Name:         o.DefaultRole,
		}
		roles = append(roles, role)
	}

	user.Roles = append(user.Roles, roles...)
	if err = c.UsersStore.Update(ctx, user); err != nil {
		return err
	}

	w := NewTabWriter()
	WriteHeaders(w)
	WriteUser(w, user)
	w.Flush()

	return nil
}

func init() {
	parser.AddCommand("add-superadmin",
		"Creates a new superadmin user",
		"The add-user command will create a new user with superadmin status",
		&addCommand)
}
