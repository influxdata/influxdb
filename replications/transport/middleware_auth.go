package transport

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

func newAuthCheckingService(underlying ReplicationService) *authCheckingService {
	return &authCheckingService{underlying}
}

type authCheckingService struct {
	underlying ReplicationService
}

var _ ReplicationService = (*authCheckingService)(nil)

func (a authCheckingService) ListReplications(ctx context.Context, filter influxdb.ReplicationListFilter) (*influxdb.Replications, error) {
	rs, err := a.underlying.ListReplications(ctx, filter)
	if err != nil {
		return nil, err
	}

	rrs := rs.Replications[:0]
	for _, r := range rs.Replications {
		_, _, err := authorizer.AuthorizeRead(ctx, influxdb.ReplicationsResourceType, r.ID, r.OrgID)
		if err != nil && errors.ErrorCode(err) != errors.EUnauthorized {
			return nil, err
		}
		if errors.ErrorCode(err) == errors.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return &influxdb.Replications{Replications: rrs}, nil
}

func (a authCheckingService) CreateReplication(ctx context.Context, request influxdb.CreateReplicationRequest) (*influxdb.Replication, error) {
	if err := a.authCreateReplication(ctx, request); err != nil {
		return nil, err
	}
	return a.underlying.CreateReplication(ctx, request)
}

func (a authCheckingService) ValidateNewReplication(ctx context.Context, request influxdb.CreateReplicationRequest) error {
	if err := a.authCreateReplication(ctx, request); err != nil {
		return err
	}
	return a.underlying.ValidateNewReplication(ctx, request)
}

func (a authCheckingService) authCreateReplication(ctx context.Context, request influxdb.CreateReplicationRequest) error {
	if _, _, err := authorizer.AuthorizeCreate(ctx, influxdb.ReplicationsResourceType, request.OrgID); err != nil {
		return err
	}
	// N.B. creating a replication requires read-access to both the source bucket and the target remote.
	if _, _, err := authorizer.AuthorizeRead(ctx, influxdb.BucketsResourceType, request.LocalBucketID, request.OrgID); err != nil {
		return err
	}
	if _, _, err := authorizer.AuthorizeRead(ctx, influxdb.RemotesResourceType, request.RemoteID, request.OrgID); err != nil {
		return err
	}
	return nil
}

func (a authCheckingService) GetReplication(ctx context.Context, id platform.ID) (*influxdb.Replication, error) {
	r, err := a.underlying.GetReplication(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := authorizer.AuthorizeRead(ctx, influxdb.ReplicationsResourceType, id, r.OrgID); err != nil {
		return nil, err
	}
	return r, nil
}

func (a authCheckingService) UpdateReplication(ctx context.Context, id platform.ID, request influxdb.UpdateReplicationRequest) (*influxdb.Replication, error) {
	if err := a.authUpdateReplication(ctx, id, request); err != nil {
		return nil, err
	}
	return a.underlying.UpdateReplication(ctx, id, request)
}

func (a authCheckingService) ValidateUpdatedReplication(ctx context.Context, id platform.ID, request influxdb.UpdateReplicationRequest) error {
	if err := a.authUpdateReplication(ctx, id, request); err != nil {
		return err
	}
	return a.underlying.ValidateUpdatedReplication(ctx, id, request)
}

func (a authCheckingService) authUpdateReplication(ctx context.Context, id platform.ID, request influxdb.UpdateReplicationRequest) error {
	r, err := a.underlying.GetReplication(ctx, id)
	if err != nil {
		return err
	}
	if _, _, err := authorizer.AuthorizeWrite(ctx, influxdb.ReplicationsResourceType, id, r.OrgID); err != nil {
		return err
	}
	if request.RemoteID != nil {
		if _, _, err := authorizer.AuthorizeRead(ctx, influxdb.RemotesResourceType, *request.RemoteID, r.OrgID); err != nil {
			return err
		}
	}
	return nil
}

func (a authCheckingService) DeleteReplication(ctx context.Context, id platform.ID) error {
	r, err := a.underlying.GetReplication(ctx, id)
	if err != nil {
		return err
	}
	if _, _, err := authorizer.AuthorizeWrite(ctx, influxdb.ReplicationsResourceType, id, r.OrgID); err != nil {
		return err
	}
	return a.underlying.DeleteReplication(ctx, id)
}

func (a authCheckingService) ValidateReplication(ctx context.Context, id platform.ID) error {
	r, err := a.underlying.GetReplication(ctx, id)
	if err != nil {
		return err
	}
	if _, _, err := authorizer.AuthorizeRead(ctx, influxdb.ReplicationsResourceType, id, r.OrgID); err != nil {
		return err
	}
	return a.underlying.ValidateReplication(ctx, id)
}
