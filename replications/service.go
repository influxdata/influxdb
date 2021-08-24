package replications

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	ierrors "github.com/influxdata/influxdb/v2/kit/platform/errors"
)

var errNotImplemented = &ierrors.Error{
	Code: ierrors.ENotImplemented,
	Msg:  "replication APIs not yet implemented",
}

func NewService() *service {
	return &service{}
}

type service struct{}

var _ influxdb.ReplicationService = (*service)(nil)

func (s service) ListReplications(ctx context.Context, filter influxdb.ReplicationListFilter) (*influxdb.Replications, error) {
	return nil, errNotImplemented
}

func (s service) CreateReplication(ctx context.Context, request influxdb.CreateReplicationRequest) (*influxdb.Replication, error) {
	return nil, errNotImplemented
}

func (s service) ValidateNewReplication(ctx context.Context, request influxdb.CreateReplicationRequest) error {
	return errNotImplemented
}

func (s service) GetReplication(ctx context.Context, id platform.ID) (*influxdb.Replication, error) {
	return nil, errNotImplemented
}

func (s service) UpdateReplication(ctx context.Context, id platform.ID, request influxdb.UpdateReplicationRequest) (*influxdb.Replication, error) {
	return nil, errNotImplemented
}

func (s service) ValidateUpdatedReplication(ctx context.Context, id platform.ID, request influxdb.UpdateReplicationRequest) error {
	return errNotImplemented
}

func (s service) DeleteReplication(ctx context.Context, id platform.ID) error {
	return errNotImplemented
}

func (s service) ValidateReplication(ctx context.Context, id platform.ID) error {
	return errNotImplemented
}
