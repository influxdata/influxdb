package mock

import (
	"context"

	platform "github.com/influxdata/influxdb"
)

var _ platform.VariableService = &VariableService{}

type VariableService struct {
	FindVariablesF    func(context.Context, platform.VariableFilter, ...platform.FindOptions) ([]*platform.Variable, error)
	FindVariableByIDF func(context.Context, platform.ID) (*platform.Variable, error)
	CreateVariableF   func(context.Context, *platform.Variable) error
	UpdateVariableF   func(ctx context.Context, id platform.ID, update *platform.VariableUpdate) (*platform.Variable, error)
	ReplaceVariableF  func(context.Context, *platform.Variable) error
	DeleteVariableF   func(context.Context, platform.ID) error
}

// NewVariableService returns a mock of VariableService where its methods will return zero values.
func NewVariableService() *VariableService {
	return &VariableService{
		FindVariablesF: func(context.Context, platform.VariableFilter, ...platform.FindOptions) ([]*platform.Variable, error) {
			return nil, nil
		},
		FindVariableByIDF: func(context.Context, platform.ID) (*platform.Variable, error) { return nil, nil },
		CreateVariableF:   func(context.Context, *platform.Variable) error { return nil },
		UpdateVariableF: func(ctx context.Context, id platform.ID, update *platform.VariableUpdate) (*platform.Variable, error) {
			return nil, nil
		},
		ReplaceVariableF: func(context.Context, *platform.Variable) error { return nil },
		DeleteVariableF:  func(context.Context, platform.ID) error { return nil },
	}
}

func (s *VariableService) CreateVariable(ctx context.Context, variable *platform.Variable) error {
	return s.CreateVariableF(ctx, variable)
}

func (s *VariableService) ReplaceVariable(ctx context.Context, variable *platform.Variable) error {
	return s.ReplaceVariableF(ctx, variable)
}

func (s *VariableService) FindVariables(ctx context.Context, filter platform.VariableFilter, opts ...platform.FindOptions) ([]*platform.Variable, error) {
	return s.FindVariablesF(ctx, filter, opts...)
}

func (s *VariableService) FindVariableByID(ctx context.Context, id platform.ID) (*platform.Variable, error) {
	return s.FindVariableByIDF(ctx, id)
}

func (s *VariableService) DeleteVariable(ctx context.Context, id platform.ID) error {
	return s.DeleteVariableF(ctx, id)
}

func (s *VariableService) UpdateVariable(ctx context.Context, id platform.ID, update *platform.VariableUpdate) (*platform.Variable, error) {
	return s.UpdateVariableF(ctx, id, update)
}
