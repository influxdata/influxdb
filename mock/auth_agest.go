package mock

import (
	"context"

	"github.com/influxdata/influxdb"
)

type AuthAgent struct {
	IsWritableFn    func(ctx context.Context, orgID influxdb.ID, resType influxdb.ResourceType) error
	OrgPermissionFn func(ctx context.Context, orgID influxdb.ID, action influxdb.Action) error
}

func NewAuthAgent() *AuthAgent {
	return &AuthAgent{
		IsWritableFn: func(ctx context.Context, orgID influxdb.ID, resType influxdb.ResourceType) error {
			return nil
		},
		OrgPermissionFn: func(ctx context.Context, orgID influxdb.ID, action influxdb.Action) error {
			return nil
		},
	}
}

func (a *AuthAgent) IsWritable(ctx context.Context, orgID influxdb.ID, resType influxdb.ResourceType) error {
	return a.IsWritableFn(ctx, orgID, resType)
}

func (a *AuthAgent) OrgPermission(ctx context.Context, orgID influxdb.ID, action influxdb.Action) error {
	return a.OrgPermissionFn(ctx, orgID, action)
}
