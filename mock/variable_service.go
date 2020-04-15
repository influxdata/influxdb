package mock

import (
	"context"

	platform "github.com/influxdata/influxdb/v2"
)

var _ platform.VariableService = &VariableService{}

type VariableService struct {
	CreateVariableF       func(context.Context, *platform.Variable) error
	CreateVariableCalls   SafeCount
	DeleteVariableF       func(context.Context, platform.ID) error
	DeleteVariableCalls   SafeCount
	FindVariableByIDF     func(context.Context, platform.ID) (*platform.Variable, error)
	FindVariableByIDCalls SafeCount
	FindVariablesF        func(context.Context, platform.VariableFilter, ...platform.FindOptions) ([]*platform.Variable, error)
	FindVariablesCalls    SafeCount
	ReplaceVariableF      func(context.Context, *platform.Variable) error
	ReplaceVariableCalls  SafeCount
	UpdateVariableF       func(ctx context.Context, id platform.ID, update *platform.VariableUpdate) (*platform.Variable, error)
	UpdateVariableCalls   SafeCount
}

// NewVariableService returns a mock of VariableService where its methods will return zero values.
func NewVariableService() *VariableService {
	return &VariableService{
		CreateVariableF:   func(context.Context, *platform.Variable) error { return nil },
		DeleteVariableF:   func(context.Context, platform.ID) error { return nil },
		FindVariableByIDF: func(context.Context, platform.ID) (*platform.Variable, error) { return nil, nil },
		FindVariablesF: func(context.Context, platform.VariableFilter, ...platform.FindOptions) ([]*platform.Variable, error) {
			return nil, nil
		},
		ReplaceVariableF: func(context.Context, *platform.Variable) error { return nil },
		UpdateVariableF: func(ctx context.Context, id platform.ID, update *platform.VariableUpdate) (*platform.Variable, error) {
			return nil, nil
		},
	}
}

func (s *VariableService) CreateVariable(ctx context.Context, variable *platform.Variable) error {
	defer s.CreateVariableCalls.IncrFn()()
	return s.CreateVariableF(ctx, variable)
}

func (s *VariableService) ReplaceVariable(ctx context.Context, variable *platform.Variable) error {
	defer s.ReplaceVariableCalls.IncrFn()()
	return s.ReplaceVariableF(ctx, variable)
}

func (s *VariableService) FindVariables(ctx context.Context, filter platform.VariableFilter, opts ...platform.FindOptions) ([]*platform.Variable, error) {
	defer s.FindVariablesCalls.IncrFn()()
	return s.FindVariablesF(ctx, filter, opts...)
}

func (s *VariableService) FindVariableByID(ctx context.Context, id platform.ID) (*platform.Variable, error) {
	defer s.FindVariableByIDCalls.IncrFn()()
	return s.FindVariableByIDF(ctx, id)
}

func (s *VariableService) DeleteVariable(ctx context.Context, id platform.ID) error {
	defer s.DeleteVariableCalls.IncrFn()()
	return s.DeleteVariableF(ctx, id)
}

func (s *VariableService) UpdateVariable(ctx context.Context, id platform.ID, update *platform.VariableUpdate) (*platform.Variable, error) {
	defer s.UpdateVariableCalls.IncrFn()()
	return s.UpdateVariableF(ctx, id, update)
}
