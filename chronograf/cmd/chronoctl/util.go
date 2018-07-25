package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/influxdata/platform/chronograf"
	"github.com/influxdata/platform/chronograf/bolt"
	"github.com/influxdata/platform/chronograf/mocks"
)

func NewBoltClient(path string) (*bolt.Client, error) {
	c := bolt.NewClient()
	c.Path = path

	ctx := context.Background()
	logger := mocks.NewLogger()
	var bi chronograf.BuildInfo
	if err := c.Open(ctx, logger, bi); err != nil {
		return nil, err
	}

	return c, nil
}

func NewTabWriter() *tabwriter.Writer {
	return tabwriter.NewWriter(os.Stdout, 0, 8, 1, '\t', 0)
}

func WriteHeaders(w io.Writer) {
	fmt.Fprintln(w, "ID\tName\tProvider\tScheme\tSuperAdmin\tOrganization(s)")
}

func WriteUser(w io.Writer, user *chronograf.User) {
	orgs := []string{}
	for _, role := range user.Roles {
		orgs = append(orgs, role.Organization)
	}
	fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%t\t%s\n", user.ID, user.Name, user.Provider, user.Scheme, user.SuperAdmin, strings.Join(orgs, ","))
}
