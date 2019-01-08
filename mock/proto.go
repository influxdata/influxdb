package mock

import (
	"context"
	"errors"

	platform "github.com/influxdata/influxdb"
)

// ProtoService is a mock implementation of a retention.ProtoService, which
// also makes it a suitable mock to use wherever an platform.ProtoService is required.
type ProtoService struct {
	FindProtosFn                func(context.Context) ([]*platform.Proto, error)
	CreateDashboardsFromProtoFn func(context.Context, platform.ID, platform.ID) ([]*platform.Dashboard, error)
}

// FindProtos returns a list of protos.
func (s *ProtoService) FindProtos(ctx context.Context) ([]*platform.Proto, error) {
	if s.FindProtosFn == nil {
		return nil, errors.New("not implemented")
	}
	return s.FindProtosFn(ctx)
}

// CreateDashboardsFromProto creates a new set of dashboards for a proto
func (s *ProtoService) CreateDashboardsFromProto(ctx context.Context, protoID, orgID platform.ID) ([]*platform.Dashboard, error) {
	if s.CreateDashboardsFromProtoFn == nil {
		return nil, errors.New("not implemented")
	}
	return s.CreateDashboardsFromProtoFn(ctx, protoID, orgID)
}
