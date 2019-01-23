package influxdb

import (
	"context"
)

// Proto is templated resource.
type Proto struct {
	ID         ID               `json:"id"`
	Name       string           `json:"name"`
	Dashboards []ProtoDashboard `json:"dashboards,omitempty"`
}

// ProtoDashboard is a templated dashboard.
type ProtoDashboard struct {
	Dashboard Dashboard       `json:"dashboard"`
	Views     map[string]View `json:"views"`
}

// ProtoService is what dashboard.land will be.
type ProtoService interface {
	// TODO(desa): add pagination here eventually.
	FindProtos(ctx context.Context) ([]*Proto, error)
	CreateDashboardsFromProto(ctx context.Context, protoID ID, orgID ID) ([]*Dashboard, error)
}
