package mock

import (
	"context"

	"github.com/influxdata/influxdb"
)

// CheckService is a mock implementation of a retention.CheckService, which
// also makes it a suitable mock to use wherever an influxdb.CheckService is required.
type CheckService struct {
	OrganizationService
	UserResourceMappingService

	// Methods for an influxdb.CheckService
	FindCheckByIDFn func(context.Context, influxdb.ID) (influxdb.Check, error)
	FindCheckFn     func(context.Context, influxdb.CheckFilter) (influxdb.Check, error)
	FindChecksFn    func(context.Context, influxdb.CheckFilter, ...influxdb.FindOptions) ([]influxdb.Check, int, error)
	CreateCheckFn   func(context.Context, influxdb.Check) error
	UpdateCheckFn   func(context.Context, influxdb.ID, influxdb.Check) (influxdb.Check, error)
	PatchCheckFn    func(context.Context, influxdb.ID, influxdb.CheckUpdate) (influxdb.Check, error)
	DeleteCheckFn   func(context.Context, influxdb.ID) error
}

// NewCheckService returns a mock CheckService where its methods will return
// zero values.
func NewCheckService() *CheckService {
	return &CheckService{
		FindCheckByIDFn: func(context.Context, influxdb.ID) (influxdb.Check, error) { return nil, nil },
		FindCheckFn:     func(context.Context, influxdb.CheckFilter) (influxdb.Check, error) { return nil, nil },
		FindChecksFn: func(context.Context, influxdb.CheckFilter, ...influxdb.FindOptions) ([]influxdb.Check, int, error) {
			return nil, 0, nil
		},
		CreateCheckFn: func(context.Context, influxdb.Check) error { return nil },
		UpdateCheckFn: func(context.Context, influxdb.ID, influxdb.Check) (influxdb.Check, error) { return nil, nil },
		PatchCheckFn:  func(context.Context, influxdb.ID, influxdb.CheckUpdate) (influxdb.Check, error) { return nil, nil },
		DeleteCheckFn: func(context.Context, influxdb.ID) error { return nil },
	}
}

// FindCheckByID returns a single check by ID.
func (s *CheckService) FindCheckByID(ctx context.Context, id influxdb.ID) (influxdb.Check, error) {
	return s.FindCheckByIDFn(ctx, id)
}

// FindCheck returns the first check that matches filter.
func (s *CheckService) FindCheck(ctx context.Context, filter influxdb.CheckFilter) (influxdb.Check, error) {
	return s.FindCheckFn(ctx, filter)
}

// FindChecks returns a list of checks that match filter and the total count of matching checks.
func (s *CheckService) FindChecks(ctx context.Context, filter influxdb.CheckFilter, opts ...influxdb.FindOptions) ([]influxdb.Check, int, error) {
	return s.FindChecksFn(ctx, filter, opts...)
}

// CreateCheck creates a new check and sets b.ID with the new identifier.
func (s *CheckService) CreateCheck(ctx context.Context, check influxdb.Check) error {
	return s.CreateCheckFn(ctx, check)
}

// UpdateCheck updates everything except id orgID.
func (s *CheckService) UpdateCheck(ctx context.Context, id influxdb.ID, chk influxdb.Check) (influxdb.Check, error) {
	return s.UpdateCheckFn(ctx, id, chk)
}

// PatchCheck updates a single check with changeset.
func (s *CheckService) PatchCheck(ctx context.Context, id influxdb.ID, upd influxdb.CheckUpdate) (influxdb.Check, error) {
	return s.PatchCheckFn(ctx, id, upd)
}

// DeleteCheck removes a check by ID.
func (s *CheckService) DeleteCheck(ctx context.Context, id influxdb.ID) error {
	return s.DeleteCheckFn(ctx, id)
}
