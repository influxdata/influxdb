package enterprise_test

import (
	"context"
	"encoding/json"
	"net/url"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/enterprise"
)

type ControlClient struct {
	Cluster            *enterprise.Cluster
	ShowClustersCalled bool
}

func NewMockControlClient(addr string) *ControlClient {
	_, err := url.Parse(addr)
	if err != nil {
		panic(err)
	}

	return &ControlClient{
		Cluster: &enterprise.Cluster{
			DataNodes: []enterprise.DataNode{
				enterprise.DataNode{
					HTTPAddr: addr,
				},
			},
		},
	}
}

func (cc *ControlClient) ShowCluster(context.Context) (*enterprise.Cluster, error) {
	cc.ShowClustersCalled = true
	return cc.Cluster, nil
}

func (cc *ControlClient) User(ctx context.Context, name string) (*enterprise.User, error) {
	return nil, nil
}

func (cc *ControlClient) CreateUser(ctx context.Context, name, passwd string) error {
	return nil
}

func (cc *ControlClient) DeleteUser(ctx context.Context, name string) error {
	return nil
}

func (cc *ControlClient) ChangePassword(ctx context.Context, name, passwd string) error {
	return nil
}

func (cc *ControlClient) Users(ctx context.Context, name *string) (*enterprise.Users, error) {
	return nil, nil
}

func (cc *ControlClient) SetUserPerms(ctx context.Context, name string, perms enterprise.Permissions) error {
	return nil
}

func (cc *ControlClient) CreateRole(ctx context.Context, name string) error {
	return nil
}

func (cc *ControlClient) Role(ctx context.Context, name string) (*enterprise.Role, error) {
	return nil, nil
}

func (ccm *ControlClient) UserRoles(ctx context.Context) (map[string]enterprise.Roles, error) {
	return nil, nil
}

func (ccm *ControlClient) Roles(ctx context.Context, name *string) (*enterprise.Roles, error) {
	return nil, nil
}

func (cc *ControlClient) DeleteRole(ctx context.Context, name string) error {
	return nil
}

func (cc *ControlClient) SetRolePerms(ctx context.Context, name string, perms enterprise.Permissions) error {
	return nil
}

func (cc *ControlClient) SetRoleUsers(ctx context.Context, name string, users []string) error {
	return nil
}

func (cc *ControlClient) AddRoleUsers(ctx context.Context, name string, users []string) error {
	return nil
}

func (cc *ControlClient) RemoveRoleUsers(ctx context.Context, name string, users []string) error {
	return nil
}

type TimeSeries struct {
	URLs     []string
	Response Response

	QueryCtr int
}

type Response struct{}

func (r *Response) MarshalJSON() ([]byte, error) {
	return json.Marshal(r)
}

func (ts *TimeSeries) Query(ctx context.Context, q chronograf.Query) (chronograf.Response, error) {
	ts.QueryCtr++
	return &Response{}, nil
}

func (ts *TimeSeries) Connect(ctx context.Context, src *chronograf.Source) error {
	return nil
}

func (ts *TimeSeries) Write(ctx context.Context, point *chronograf.Point) error {
	return nil
}

func (ts *TimeSeries) Users(ctx context.Context) chronograf.UsersStore {
	return nil
}

func (ts *TimeSeries) Roles(ctx context.Context) (chronograf.RolesStore, error) {
	return nil, nil
}

func (ts *TimeSeries) Permissions(ctx context.Context) chronograf.Permissions {
	return chronograf.Permissions{}
}

func NewMockTimeSeries(urls ...string) *TimeSeries {
	return &TimeSeries{
		URLs:     urls,
		Response: Response{},
	}
}
