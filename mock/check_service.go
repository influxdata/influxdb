package mock

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
)

// CheckService is a mock implementation of a retention.CheckService, which
// also makes it a suitable mock to use wherever an influxdb.CheckService is required.
type CheckService struct {
	OrganizationService
	UserResourceMappingService

	// Methods for an influxdb.CheckService
	FindCheckByIDFn    func(context.Context, platform.ID) (influxdb.Check, error)
	FindCheckByIDCalls SafeCount
	FindCheckFn        func(context.Context, influxdb.CheckFilter) (influxdb.Check, error)
	FindCheckCalls     SafeCount
	FindChecksFn       func(context.Context, influxdb.CheckFilter, ...influxdb.FindOptions) ([]influxdb.Check, int, error)
	FindChecksCalls    SafeCount
	CreateCheckFn      func(context.Context, influxdb.CheckCreate, platform.ID) error
	CreateCheckCalls   SafeCount
	UpdateCheckFn      func(context.Context, platform.ID, influxdb.CheckCreate) (influxdb.Check, error)
	UpdateCheckCalls   SafeCount
	PatchCheckFn       func(context.Context, platform.ID, influxdb.CheckUpdate) (influxdb.Check, error)
	PatchCheckCalls    SafeCount
	DeleteCheckFn      func(context.Context, platform.ID) error
	DeleteCheckCalls   SafeCount
}

// NewCheckService returns a mock CheckService where its methods will return
// zero values.
func NewCheckService() *CheckService {
	return &CheckService{
		FindCheckByIDFn: func(context.Context, platform.ID) (influxdb.Check, error) { return nil, nil },
		FindCheckFn:     func(context.Context, influxdb.CheckFilter) (influxdb.Check, error) { return nil, nil },
		FindChecksFn: func(context.Context, influxdb.CheckFilter, ...influxdb.FindOptions) ([]influxdb.Check, int, error) {
			return nil, 0, nil
		},
		CreateCheckFn: func(context.Context, influxdb.CheckCreate, platform.ID) error { return nil },
		UpdateCheckFn: func(context.Context, platform.ID, influxdb.CheckCreate) (influxdb.Check, error) { return nil, nil },
		PatchCheckFn:  func(context.Context, platform.ID, influxdb.CheckUpdate) (influxdb.Check, error) { return nil, nil },
		DeleteCheckFn: func(context.Context, platform.ID) error { return nil },
	}
}

// FindCheckByID returns a single check by ID.
func (s *CheckService) FindCheckByID(ctx context.Context, id platform.ID) (influxdb.Check, error) {
	defer s.FindCheckByIDCalls.IncrFn()()
	return s.FindCheckByIDFn(ctx, id)
}

// FindCheck returns the first check that matches filter.
func (s *CheckService) FindCheck(ctx context.Context, filter influxdb.CheckFilter) (influxdb.Check, error) {
	defer s.FindCheckCalls.IncrFn()()
	return s.FindCheckFn(ctx, filter)
}

// FindChecks returns a list of checks that match filter and the total count of matching checks.
func (s *CheckService) FindChecks(ctx context.Context, filter influxdb.CheckFilter, opts ...influxdb.FindOptions) ([]influxdb.Check, int, error) {
	defer s.FindChecksCalls.IncrFn()()
	return s.FindChecksFn(ctx, filter, opts...)
}

// CreateCheck creates a new check and sets b.ID with the new identifier.
func (s *CheckService) CreateCheck(ctx context.Context, check influxdb.CheckCreate, userID platform.ID) error {
	defer s.CreateCheckCalls.IncrFn()()
	return s.CreateCheckFn(ctx, check, userID)
}

// UpdateCheck updates everything except id orgID.
func (s *CheckService) UpdateCheck(ctx context.Context, id platform.ID, chk influxdb.CheckCreate) (influxdb.Check, error) {
	defer s.UpdateCheckCalls.IncrFn()()
	return s.UpdateCheckFn(ctx, id, chk)
}

// PatchCheck updates a single check with changeset.
func (s *CheckService) PatchCheck(ctx context.Context, id platform.ID, upd influxdb.CheckUpdate) (influxdb.Check, error) {
	defer s.PatchCheckCalls.IncrFn()()
	return s.PatchCheckFn(ctx, id, upd)
}

// DeleteCheck removes a check by ID.
func (s *CheckService) DeleteCheck(ctx context.Context, id platform.ID) error {
	defer s.DeleteCheckCalls.IncrFn()()
	return s.DeleteCheckFn(ctx, id)
}
