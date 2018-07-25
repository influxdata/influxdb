package enterprise

import (
	"container/ring"
	"net/url"
	"strings"

	"context"

	"github.com/influxdata/platform/chronograf"
	"github.com/influxdata/platform/chronograf/influx"
)

var _ chronograf.TimeSeries = &Client{}

// Ctrl represents administrative controls over an Influx Enterprise cluster
type Ctrl interface {
	ShowCluster(ctx context.Context) (*Cluster, error)

	Users(ctx context.Context, name *string) (*Users, error)
	User(ctx context.Context, name string) (*User, error)
	CreateUser(ctx context.Context, name, passwd string) error
	DeleteUser(ctx context.Context, name string) error
	ChangePassword(ctx context.Context, name, passwd string) error
	SetUserPerms(ctx context.Context, name string, perms Permissions) error

	UserRoles(ctx context.Context) (map[string]Roles, error)

	Roles(ctx context.Context, name *string) (*Roles, error)
	Role(ctx context.Context, name string) (*Role, error)
	CreateRole(ctx context.Context, name string) error
	DeleteRole(ctx context.Context, name string) error
	SetRolePerms(ctx context.Context, name string, perms Permissions) error
	SetRoleUsers(ctx context.Context, name string, users []string) error
	AddRoleUsers(ctx context.Context, name string, users []string) error
	RemoveRoleUsers(ctx context.Context, name string, users []string) error
}

// Client is a device for retrieving time series data from an Influx Enterprise
// cluster. It is configured using the addresses of one or more meta node URLs.
// Data node URLs are retrieved automatically from the meta nodes and queries
// are appropriately load balanced across the cluster.
type Client struct {
	Ctrl
	UsersStore chronograf.UsersStore
	RolesStore chronograf.RolesStore
	Logger     chronograf.Logger

	dataNodes *ring.Ring
	opened    bool
}

// NewClientWithTimeSeries initializes a Client with a known set of TimeSeries.
func NewClientWithTimeSeries(lg chronograf.Logger, mu string, authorizer influx.Authorizer, tls, insecure bool, series ...chronograf.TimeSeries) (*Client, error) {
	metaURL, err := parseMetaURL(mu, tls)
	if err != nil {
		return nil, err
	}

	ctrl := NewMetaClient(metaURL, insecure, authorizer)
	c := &Client{
		Ctrl: ctrl,
		UsersStore: &UserStore{
			Ctrl:   ctrl,
			Logger: lg,
		},
		RolesStore: &RolesStore{
			Ctrl:   ctrl,
			Logger: lg,
		},
	}

	c.dataNodes = ring.New(len(series))

	for _, s := range series {
		c.dataNodes.Value = s
		c.dataNodes = c.dataNodes.Next()
	}

	return c, nil
}

// NewClientWithURL initializes an Enterprise client with a URL to a Meta Node.
// Acceptable URLs include host:port combinations as well as scheme://host:port
// varieties. TLS is used when the URL contains "https" or when the TLS
// parameter is set.  authorizer will add the correct `Authorization` headers
// on the out-bound request.
func NewClientWithURL(mu string, authorizer influx.Authorizer, tls bool, insecure bool, lg chronograf.Logger) (*Client, error) {
	metaURL, err := parseMetaURL(mu, tls)
	if err != nil {
		return nil, err
	}

	ctrl := NewMetaClient(metaURL, insecure, authorizer)
	return &Client{
		Ctrl: ctrl,
		UsersStore: &UserStore{
			Ctrl:   ctrl,
			Logger: lg,
		},
		RolesStore: &RolesStore{
			Ctrl:   ctrl,
			Logger: lg,
		},
		Logger: lg,
	}, nil
}

// Connect prepares a Client to process queries. It must be called prior to calling Query
func (c *Client) Connect(ctx context.Context, src *chronograf.Source) error {
	c.opened = true
	// return early if we already have dataNodes
	if c.dataNodes != nil {
		return nil
	}
	cluster, err := c.Ctrl.ShowCluster(ctx)
	if err != nil {
		return err
	}

	c.dataNodes = ring.New(len(cluster.DataNodes))
	for _, dn := range cluster.DataNodes {
		cl := &influx.Client{
			Logger: c.Logger,
		}
		dataSrc := &chronograf.Source{}
		*dataSrc = *src
		dataSrc.URL = dn.HTTPAddr
		if err := cl.Connect(ctx, dataSrc); err != nil {
			continue
		}
		c.dataNodes.Value = cl
		c.dataNodes = c.dataNodes.Next()
	}
	return nil
}

// Query retrieves timeseries information pertaining to a specified query. It
// can be cancelled by using a provided context.
func (c *Client) Query(ctx context.Context, q chronograf.Query) (chronograf.Response, error) {
	if !c.opened {
		return nil, chronograf.ErrUninitialized
	}
	return c.nextDataNode().Query(ctx, q)
}

// Write records points into a time series
func (c *Client) Write(ctx context.Context, points []chronograf.Point) error {
	if !c.opened {
		return chronograf.ErrUninitialized
	}
	return c.nextDataNode().Write(ctx, points)
}

// Users is the interface to the users within Influx Enterprise
func (c *Client) Users(context.Context) chronograf.UsersStore {
	return c.UsersStore
}

// Roles provide a grouping of permissions given to a grouping of users
func (c *Client) Roles(ctx context.Context) (chronograf.RolesStore, error) {
	return c.RolesStore, nil
}

// Permissions returns all Influx Enterprise permission strings
func (c *Client) Permissions(context.Context) chronograf.Permissions {
	all := chronograf.Allowances{
		"NoPermissions",
		"ViewAdmin",
		"ViewChronograf",
		"CreateDatabase",
		"CreateUserAndRole",
		"AddRemoveNode",
		"DropDatabase",
		"DropData",
		"ReadData",
		"WriteData",
		"Rebalance",
		"ManageShard",
		"ManageContinuousQuery",
		"ManageQuery",
		"ManageSubscription",
		"Monitor",
		"CopyShard",
		"KapacitorAPI",
		"KapacitorConfigAPI",
	}

	return chronograf.Permissions{
		{
			Scope:   chronograf.AllScope,
			Allowed: all,
		},
		{
			Scope:   chronograf.DBScope,
			Allowed: all,
		},
	}
}

// nextDataNode retrieves the next available data node
func (c *Client) nextDataNode() chronograf.TimeSeries {
	c.dataNodes = c.dataNodes.Next()
	return c.dataNodes.Value.(chronograf.TimeSeries)
}

// parseMetaURL constructs a url from either a host:port combination or a
// scheme://host:port combo. The optional TLS parameter takes precedence over
// any TLS preference found in the provided URL
func parseMetaURL(mu string, tls bool) (metaURL *url.URL, err error) {
	if strings.Contains(mu, "http") {
		metaURL, err = url.Parse(mu)
	} else {
		metaURL = &url.URL{
			Scheme: "http",
			Host:   mu,
		}
	}

	if tls {
		metaURL.Scheme = "https"
	}

	return
}
