package authorizer

import (
	"context"

	"github.com/influxdata/influxdb"
)

type AuthAgent struct{}

func (a *AuthAgent) OrgPermission(ctx context.Context, orgID influxdb.ID, action influxdb.Action) error {
	switch action {
	case influxdb.ReadAction:
		return authorizeReadOrg(ctx, orgID)
	case influxdb.WriteAction:
		return authorizeWriteOrg(ctx, orgID)
	default:
		return &influxdb.Error{Code: influxdb.EConflict, Msg: "invalid action provided: " + string(action)}
	}
}

func (a *AuthAgent) IsWritable(ctx context.Context, orgID influxdb.ID, resType influxdb.ResourceType) error {
	p, err := influxdb.NewPermission(influxdb.WriteAction, resType, orgID)
	if err != nil {
		return err
	}

	pOrg, err := newOrgPermission(influxdb.WriteAction, orgID)
	if err != nil {
		return err
	}

	err0 := IsAllowed(ctx, *p)
	err1 := IsAllowed(ctx, *pOrg)

	if err0 != nil && err1 != nil {
		return &influxdb.Error{
			Code: influxdb.EUnauthorized,
			Msg:  "not authorized to create " + string(resType),
		}
	}

	return nil
}
