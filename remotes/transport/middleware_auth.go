package transport

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

func newAuthCheckingService(underlying RemoteConnectionService) *authCheckingService {
	return &authCheckingService{underlying}
}

type authCheckingService struct {
	underlying RemoteConnectionService
}

var _ RemoteConnectionService = (*authCheckingService)(nil)

func (a authCheckingService) ListRemoteConnections(ctx context.Context, filter influxdb.RemoteConnectionListFilter) (*influxdb.RemoteConnections, error) {
	rs, err := a.underlying.ListRemoteConnections(ctx, filter)
	if err != nil {
		return nil, err
	}

	rrs := rs.Remotes[:0]
	for _, r := range rs.Remotes {
		_, _, err := authorizer.AuthorizeRead(ctx, influxdb.RemotesResourceType, r.ID, r.OrgID)
		if err != nil && errors.ErrorCode(err) != errors.EUnauthorized {
			return nil, err
		}
		if errors.ErrorCode(err) == errors.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return &influxdb.RemoteConnections{Remotes: rrs}, nil
}

func (a authCheckingService) CreateRemoteConnection(ctx context.Context, request influxdb.CreateRemoteConnectionRequest) (*influxdb.RemoteConnection, error) {
	if _, _, err := authorizer.AuthorizeCreate(ctx, influxdb.RemotesResourceType, request.OrgID); err != nil {
		return nil, err
	}

	return a.underlying.CreateRemoteConnection(ctx, request)
}

func (a authCheckingService) GetRemoteConnection(ctx context.Context, id platform.ID) (*influxdb.RemoteConnection, error) {
	r, err := a.underlying.GetRemoteConnection(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := authorizer.AuthorizeRead(ctx, influxdb.RemotesResourceType, id, r.OrgID); err != nil {
		return nil, err
	}
	return r, nil
}

func (a authCheckingService) UpdateRemoteConnection(ctx context.Context, id platform.ID, request influxdb.UpdateRemoteConnectionRequest) (*influxdb.RemoteConnection, error) {
	r, err := a.underlying.GetRemoteConnection(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := authorizer.AuthorizeWrite(ctx, influxdb.RemotesResourceType, id, r.OrgID); err != nil {
		return nil, err
	}
	return a.underlying.UpdateRemoteConnection(ctx, id, request)
}

func (a authCheckingService) DeleteRemoteConnection(ctx context.Context, id platform.ID) error {
	r, err := a.underlying.GetRemoteConnection(ctx, id)
	if err != nil {
		return err
	}
	if _, _, err := authorizer.AuthorizeWrite(ctx, influxdb.RemotesResourceType, id, r.OrgID); err != nil {
		return err
	}
	return a.underlying.DeleteRemoteConnection(ctx, id)
}
