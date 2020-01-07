package endpoints

import (
	"context"

	"github.com/influxdata/influxdb"
)

type AuthAgent interface {
	IsWritable(ctx context.Context, orgID influxdb.ID, resType influxdb.ResourceType) error
	OrgPermission(ctx context.Context, orgID influxdb.ID, action influxdb.Action) error
}

type mwAuth struct {
	authAgent AuthAgent
	next      influxdb.NotificationEndpointService
}

var _ influxdb.NotificationEndpointService = (*mwAuth)(nil)

// MiddlewareAuth is an auth service middleware for the notification endpoint service.
func MiddlewareAuth(authAgent AuthAgent) ServiceMW {
	return func(svc influxdb.NotificationEndpointService) influxdb.NotificationEndpointService {
		return &mwAuth{
			authAgent: authAgent,
			next:      svc,
		}
	}
}

func (mw *mwAuth) Delete(ctx context.Context, id influxdb.ID) error {
	endpoint, err := mw.next.FindByID(ctx, id)
	if err != nil {
		return err
	}

	if err := mw.authAgent.OrgPermission(ctx, endpoint.GetOrgID(), influxdb.WriteAction); err != nil {
		return err
	}

	return mw.next.Delete(setEndpoint(ctx, endpoint), id)
}

func (mw *mwAuth) FindByID(ctx context.Context, id influxdb.ID) (influxdb.NotificationEndpoint, error) {
	endpoint, err := mw.next.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if err := mw.authAgent.OrgPermission(ctx, endpoint.GetOrgID(), influxdb.ReadAction); err != nil {
		return nil, err
	}

	return endpoint, nil
}

func (mw *mwAuth) Find(ctx context.Context, f influxdb.NotificationEndpointFilter, opt ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, error) {
	if !f.UserID.Valid() && f.OrgID == nil {
		return nil, &influxdb.Error{
			Code: influxdb.EUnauthorized,
			Msg:  "cannot process a request without a org or user filter",
		}
	}

	edps, err := mw.next.Find(ctx, f, opt...)
	if err != nil {
		return nil, err
	}

	endpoints := make([]influxdb.NotificationEndpoint, 0, len(edps))
	for _, endpoint := range edps {
		err := mw.authAgent.OrgPermission(ctx, endpoint.GetOrgID(), influxdb.ReadAction)
		if err == nil {
			endpoints = append(endpoints, endpoint)
			continue
		}
		if influxdb.ErrorCode(err) != influxdb.EUnauthorized {
			return nil, err
		}
	}

	return endpoints, nil
}

func (mw *mwAuth) Create(ctx context.Context, userID influxdb.ID, endpoint influxdb.NotificationEndpoint) error {
	err := mw.authAgent.IsWritable(ctx, endpoint.GetOrgID(), influxdb.NotificationEndpointResourceType)
	if err != nil {
		return err
	}
	return mw.next.Create(ctx, userID, endpoint)
}

func (mw *mwAuth) Update(ctx context.Context, update influxdb.EndpointUpdate) (influxdb.NotificationEndpoint, error) {
	endpoint, err := mw.next.FindByID(ctx, update.ID)
	if err != nil {
		return nil, err
	}

	if err := mw.authAgent.OrgPermission(ctx, endpoint.GetOrgID(), influxdb.WriteAction); err != nil {
		return nil, err
	}

	return mw.next.Update(setEndpoint(ctx, endpoint), update)
}
