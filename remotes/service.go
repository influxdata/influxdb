package remotes

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

var (
	errNotImplemented = &errors.Error{
		Code: errors.ENotImplemented,
		Msg:  "remote connection APIs not yet implemented",
	}
)

func NewService() *service {
	return &service{}
}

type service struct{}

var _ influxdb.RemoteConnectionService = (*service)(nil)

func (s service) ListRemoteConnections(ctx context.Context, filter influxdb.RemoteConnectionListFilter) (*influxdb.RemoteConnections, error) {
	return nil, errNotImplemented
}

func (s service) CreateRemoteConnection(ctx context.Context, request influxdb.CreateRemoteConnectionRequest) (*influxdb.RemoteConnection, error) {
	return nil, errNotImplemented
}

func (s service) ValidateNewRemoteConnection(ctx context.Context, request influxdb.CreateRemoteConnectionRequest) error {
	return errNotImplemented
}

func (s service) GetRemoteConnection(ctx context.Context, id platform.ID) (*influxdb.RemoteConnection, error) {
	return nil, errNotImplemented
}

func (s service) UpdateRemoteConnection(ctx context.Context, id platform.ID, request influxdb.UpdateRemoteConnectionRequest) (*influxdb.RemoteConnection, error) {
	return nil, errNotImplemented
}

func (s service) ValidateUpdatedRemoteConnection(ctx context.Context, id platform.ID, request influxdb.UpdateRemoteConnectionRequest) error {
	return errNotImplemented
}

func (s service) DeleteRemoteConnection(ctx context.Context, id platform.ID) error {
	return errNotImplemented
}

func (s service) ValidateRemoteConnection(ctx context.Context, id platform.ID) error {
	return errNotImplemented
}
