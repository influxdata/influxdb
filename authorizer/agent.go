package authorizer

import (
	"context"

	"github.com/influxdata/influxdb/v2"
)

// AuthAgent provides a means to authenticate users with resource and their associate actions. It
// makes for a clear dependency, to an auth middleware for instance.
type AuthAgent struct{}

// OrgPermissions identifies if a user has access to the org by the specified action.
func (a *AuthAgent) OrgPermissions(ctx context.Context, orgID influxdb.ID, action influxdb.Action, rest ...influxdb.Action) error {
	for _, action := range append(rest, action) {
		var err error
		switch action {
		case influxdb.ReadAction:
			_, _, err = AuthorizeReadOrg(ctx, orgID)
		case influxdb.WriteAction:
			_, _, err = AuthorizeWriteOrg(ctx, orgID)
		default:
			err = &influxdb.Error{Code: influxdb.EInvalid, Msg: "invalid action provided: " + string(action)}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *AuthAgent) IsWritable(ctx context.Context, orgID influxdb.ID, resType influxdb.ResourceType) error {
	_, _, resTypeErr := AuthorizeOrgWriteResource(ctx, resType, &orgID)
	_, _, orgErr := AuthorizeWriteOrg(ctx, orgID)

	if resTypeErr != nil && orgErr != nil {
		return &influxdb.Error{
			Code: influxdb.EUnauthorized,
			Msg:  "not authorized to create " + string(resType),
		}
	}

	return nil
}
