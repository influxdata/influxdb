package testing

import (
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
)

const (
	idA = "020f755c3c082000"
	idB = "020f755c3c082001"
	idC = "020f755c3c082002"
	idD = "020f755c3c082003"
)

var variableCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*platform.Variable) []*platform.Variable {
		out := append([]*platform.Variable(nil), in...)
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID.String() > out[j].ID.String()
		})
		return out
	}),
}

// VariableFields defines fields for a variable test
type VariableFields struct {
	Variables   []*platform.Variable
	IDGenerator platform.IDGenerator
}

// VariableService tests all the service functions.
func VariableService(
	init func(VariableFields, *testing.T) (platform.VariableService, string, func()), t *testing.T,
) {
	tests := []struct {
		name string
		fn   func(init func(VariableFields, *testing.T) (platform.VariableService, string, func()),
			t *testing.T)
	}{
		{
			name: "CreateVariable",
			fn:   CreateVariable,
		},
		{
			name: "FindVariableByID",
			fn:   FindVariableByID,
		},
		{
			name: "FindVariables",
			fn:   FindVariables,
		},
		{
			name: "UpdateVariable",
			fn:   UpdateVariable,
		},
		{
			name: "DeleteVariable",
			fn:   DeleteVariable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(init, t)
		})
	}
}

// CreateVariable tests platform.VariableService CreateVariable interface method
func CreateVariable(init func(VariableFields, *testing.T) (platform.VariableService, string, func()), t *testing.T) {
	type args struct {
		variable *platform.Variable
	}
	type wants struct {
		err       error
		variables []*platform.Variable
	}

	tests := []struct {
		name   string
		fields VariableFields
		args   args
		wants  wants
	}{
		{
			name: "basic create with missing id",
			fields: VariableFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return MustIDBase16(idD)
					},
				},
				Variables: []*platform.Variable{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(1),
						Name:           "already there",
					},
				},
			},
			args: args{
				variable: &platform.Variable{
					OrganizationID: platform.ID(3),
					Name:           "basic variable",
					Selected:       []string{"a"},
					Arguments: &platform.VariableArguments{
						Type:   "constant",
						Values: platform.VariableConstantValues{"a"},
					},
				},
			},
			wants: wants{
				variables: []*platform.Variable{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(1),
						Name:           "already there",
					},
					{
						ID:             MustIDBase16(idD),
						OrganizationID: platform.ID(3),
						Name:           "basic variable",
						Selected:       []string{"a"},
						Arguments: &platform.VariableArguments{
							Type:   "constant",
							Values: platform.VariableConstantValues{"a"},
						},
					},
				},
			},
		},
		{
			name: "creating a variable assigns the variable an id and adds it to the store",
			fields: VariableFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return MustIDBase16(idA)
					},
				},
				Variables: []*platform.Variable{
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(3),
						Name:           "existing-variable",
						Selected:       []string{"b"},
						Arguments: &platform.VariableArguments{
							Type:   "constant",
							Values: platform.VariableConstantValues{"b"},
						},
					},
				},
			},
			args: args{
				variable: &platform.Variable{
					ID:             MustIDBase16(idA),
					OrganizationID: platform.ID(3),
					Name:           "my-variable",
					Selected:       []string{"a"},
					Arguments: &platform.VariableArguments{
						Type:   "constant",
						Values: platform.VariableConstantValues{"a"},
					},
				},
			},
			wants: wants{
				err: nil,
				variables: []*platform.Variable{
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(3),
						Name:           "existing-variable",
						Selected:       []string{"b"},
						Arguments: &platform.VariableArguments{
							Type:   "constant",
							Values: platform.VariableConstantValues{"b"},
						},
					},
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(3),
						Name:           "my-variable",
						Selected:       []string{"a"},
						Arguments: &platform.VariableArguments{
							Type:   "constant",
							Values: platform.VariableConstantValues{"a"},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			err := s.CreateVariable(ctx, tt.args.variable)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			variables, err := s.FindVariables(ctx, platform.VariableFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve variables: %v", err)
			}
			if diff := cmp.Diff(variables, tt.wants.variables, variableCmpOptions...); diff != "" {
				t.Fatalf("found unexpected variables -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindVariableByID tests platform.VariableService FindVariableByID interface method
func FindVariableByID(init func(VariableFields, *testing.T) (platform.VariableService, string, func()), t *testing.T) {
	type args struct {
		id platform.ID
	}
	type wants struct {
		err      error
		variable *platform.Variable
	}

	tests := []struct {
		name   string
		fields VariableFields
		args   args
		wants  wants
	}{
		{
			name: "finding a variable that exists by id",
			fields: VariableFields{
				Variables: []*platform.Variable{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(5),
						Name:           "existing-variable-a",
						Arguments: &platform.VariableArguments{
							Type:   "constant",
							Values: platform.VariableConstantValues{},
						},
					},
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(5),
						Name:           "existing-variable-b",
						Arguments: &platform.VariableArguments{
							Type:   "constant",
							Values: platform.VariableConstantValues{},
						},
					},
				},
			},
			args: args{
				id: MustIDBase16(idB),
			},
			wants: wants{
				err: nil,
				variable: &platform.Variable{
					ID:             MustIDBase16(idB),
					OrganizationID: platform.ID(5),
					Name:           "existing-variable-b",
					Arguments: &platform.VariableArguments{
						Type:   "constant",
						Values: platform.VariableConstantValues{},
					},
				},
			},
		},
		{
			name: "finding a non-existant variable",
			fields: VariableFields{
				Variables: []*platform.Variable{},
			},
			args: args{
				id: MustIDBase16(idA),
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.ENotFound,
					Op:   platform.OpFindVariableByID,
					Msg:  platform.ErrVariableNotFound,
				},
				variable: nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			variable, err := s.FindVariableByID(ctx, tt.args.id)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(variable, tt.wants.variable); diff != "" {
				t.Fatalf("found unexpected variable -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindVariables tests platform.variableService FindVariables interface method
func FindVariables(init func(VariableFields, *testing.T) (platform.VariableService, string, func()), t *testing.T) {
	// todo(leodido)
	type args struct {
		// todo(leodido) > use VariableFilter as arg
		orgID    *platform.ID
		findOpts platform.FindOptions
	}
	type wants struct {
		variables []*platform.Variable
		err       error
	}

	tests := []struct {
		name   string
		fields VariableFields
		args   args
		wants  wants
	}{
		{
			name: "find nothing (empty set)",
			fields: VariableFields{
				Variables: []*platform.Variable{},
			},
			args: args{
				findOpts: platform.DefaultVariableFindOptions,
			},
			wants: wants{
				variables: []*platform.Variable{},
			},
		},
		{
			name: "find all variables",
			fields: VariableFields{
				Variables: []*platform.Variable{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(22),
						Name:           "a",
					},
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(22),
						Name:           "b",
					},
				},
			},
			args: args{
				findOpts: platform.DefaultVariableFindOptions,
			},
			wants: wants{
				variables: []*platform.Variable{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(22),
						Name:           "a",
					},
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(22),
						Name:           "b",
					},
				},
			},
		},
		{
			name: "find variables by wrong org id",
			fields: VariableFields{
				Variables: []*platform.Variable{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(22),
						Name:           "a",
					},
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(22),
						Name:           "b",
					},
				},
			},
			args: args{
				findOpts: platform.DefaultVariableFindOptions,
				orgID:    idPtr(platform.ID(1)),
			},
			wants: wants{
				variables: []*platform.Variable{},
			},
		},
		{
			name: "find all variables by org 22",
			fields: VariableFields{
				Variables: []*platform.Variable{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(1),
						Name:           "a",
					},
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(22),
						Name:           "b",
					},
					{
						ID:             MustIDBase16(idC),
						OrganizationID: platform.ID(2),
						Name:           "c",
					},
					{
						ID:             MustIDBase16(idD),
						OrganizationID: platform.ID(22),
						Name:           "d",
					},
				},
			},
			args: args{
				findOpts: platform.DefaultVariableFindOptions,
				orgID:    idPtr(platform.ID(22)),
			},
			wants: wants{
				variables: []*platform.Variable{
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(22),
						Name:           "b",
					},
					{
						ID:             MustIDBase16(idD),
						OrganizationID: platform.ID(22),
						Name:           "d",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			filter := platform.VariableFilter{}
			if tt.args.orgID != nil {
				filter.OrganizationID = tt.args.orgID
			}

			variables, err := s.FindVariables(ctx, filter, tt.args.findOpts)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(variables, tt.wants.variables, variableCmpOptions...); diff != "" {
				t.Errorf("variables are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// UpdateVariable tests platform.VariableService UpdateVariable interface method
func UpdateVariable(init func(VariableFields, *testing.T) (platform.VariableService, string, func()), t *testing.T) {
	type args struct {
		id     platform.ID
		update *platform.VariableUpdate
	}
	type wants struct {
		err       error
		variables []*platform.Variable
	}

	tests := []struct {
		name   string
		fields VariableFields
		args   args
		wants  wants
	}{
		{
			name: "updating a variable's name",
			fields: VariableFields{
				Variables: []*platform.Variable{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(7),
						Name:           "existing-variable-a",
						Arguments: &platform.VariableArguments{
							Type:   "constant",
							Values: platform.VariableConstantValues{},
						},
					},
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(7),
						Name:           "existing-variable-b",
						Arguments: &platform.VariableArguments{
							Type:   "constant",
							Values: platform.VariableConstantValues{},
						},
					},
				},
			},
			args: args{
				id: MustIDBase16(idB),
				update: &platform.VariableUpdate{
					Name: "new-variable-b-name",
				},
			},
			wants: wants{
				err: nil,
				variables: []*platform.Variable{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(7),
						Name:           "existing-variable-a",
						Arguments: &platform.VariableArguments{
							Type:   "constant",
							Values: platform.VariableConstantValues{},
						},
					},
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(7),
						Name:           "new-variable-b-name",
						Arguments: &platform.VariableArguments{
							Type:   "constant",
							Values: platform.VariableConstantValues{},
						},
					},
				},
			},
		},
		{
			name: "updating a non-existant variable fails",
			fields: VariableFields{
				Variables: []*platform.Variable{},
			},
			args: args{
				id: MustIDBase16(idA),
				update: &platform.VariableUpdate{
					Name: "howdy",
				},
			},
			wants: wants{
				err: &platform.Error{
					Op:   platform.OpUpdateVariable,
					Msg:  platform.ErrVariableNotFound,
					Code: platform.ENotFound,
				},
				variables: []*platform.Variable{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			variable, err := s.UpdateVariable(ctx, tt.args.id, tt.args.update)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if variable != nil {
				if tt.args.update.Name != "" && variable.Name != tt.args.update.Name {
					t.Fatalf("variable name not updated")
				}
			}

			variables, err := s.FindVariables(ctx, platform.VariableFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve variables: %v", err)
			}
			if diff := cmp.Diff(variables, tt.wants.variables, variableCmpOptions...); diff != "" {
				t.Fatalf("found unexpected variables -got/+want\ndiff %s", diff)
			}
		})
	}
}

// DeleteVariable tests platform.VariableService DeleteVariable interface method
func DeleteVariable(init func(VariableFields, *testing.T) (platform.VariableService, string, func()), t *testing.T) {
	type args struct {
		id platform.ID
	}
	type wants struct {
		err       error
		variables []*platform.Variable
	}

	tests := []struct {
		name   string
		fields VariableFields
		args   args
		wants  wants
	}{
		{
			name: "deleting a variable",
			fields: VariableFields{
				Variables: []*platform.Variable{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(9),
						Name:           "m",
						Arguments: &platform.VariableArguments{
							Type:   "constant",
							Values: platform.VariableConstantValues{},
						},
					},
				},
			},
			args: args{
				id: MustIDBase16(idA),
			},
			wants: wants{
				err:       nil,
				variables: []*platform.Variable{},
			},
		},
		{
			name: "deleting a variable that doesn't exist",
			fields: VariableFields{
				Variables: []*platform.Variable{
					{
						ID:             MustIDBase16(idD),
						OrganizationID: platform.ID(1),
						Name:           "existing-variable",
						Arguments: &platform.VariableArguments{
							Type:   "constant",
							Values: platform.VariableConstantValues{},
						},
					},
				},
			},
			args: args{
				id: MustIDBase16(idB),
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.ENotFound,
					Op:   platform.OpDeleteVariable,
					Msg:  platform.ErrVariableNotFound,
				},
				variables: []*platform.Variable{
					{
						ID:             MustIDBase16(idD),
						OrganizationID: platform.ID(1),
						Name:           "existing-variable",
						Arguments: &platform.VariableArguments{
							Type:   "constant",
							Values: platform.VariableConstantValues{},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			err := s.DeleteVariable(ctx, tt.args.id)
			defer s.ReplaceVariable(ctx, &platform.Variable{
				ID:             tt.args.id,
				OrganizationID: platform.ID(1),
			})
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			variables, err := s.FindVariables(ctx, platform.VariableFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve variables: %v", err)
			}
			if diff := cmp.Diff(variables, tt.wants.variables, variableCmpOptions...); diff != "" {
				t.Fatalf("found unexpected variables -got/+want\ndiff %s", diff)
			}
		})
	}
}
