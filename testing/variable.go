package testing

import (
	"bytes"
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/pkg/testing/assert"
	"github.com/stretchr/testify/require"
)

const (
	idA = "020f755c3c082000"
	idB = "020f755c3c082001"
	idC = "020f755c3c082002"
	idD = "020f755c3c082003"
)

var oldFakeDate = time.Date(2002, 8, 5, 2, 2, 3, 0, time.UTC)
var fakeDate = time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)
var fakeGenerator = mock.TimeGenerator{FakeValue: fakeDate}

var variableCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*influxdb.Variable) []*influxdb.Variable {
		out := append([]*influxdb.Variable(nil), in...)
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID.String() > out[j].ID.String()
		})
		return out
	}),
}

// VariableFields defines fields for a variable test
type VariableFields struct {
	Variables     []*influxdb.Variable
	IDGenerator   platform.IDGenerator
	TimeGenerator influxdb.TimeGenerator
}

type VariableSvcOpts struct {
	HTTPValidation bool
}

func WithHTTPValidation() VariableSvcOpts {
	return VariableSvcOpts{
		HTTPValidation: true,
	}
}

// VariableService tests all the service functions.
func VariableService(
	init func(VariableFields, *testing.T) (influxdb.VariableService, string, func()), t *testing.T, opts ...VariableSvcOpts,
) {
	tests := []struct {
		name string
		fn   func(init func(VariableFields, *testing.T) (influxdb.VariableService, string, func()),
			t *testing.T)
	}{
		{
			name: "CreateVariable",
			fn:   CreateVariable,
		},
		{
			name: "TrimWhitespace",
			fn:   TrimWhitespace,
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
			name: "ReplaceVariable",
			fn:   ReplaceVariable,
		},
		{
			name: "DeleteVariable",
			fn:   DeleteVariable,
		},
	}
	for _, tt := range tests {
		if tt.name == "trimwhitespace" && len(opts) > 0 && !opts[0].HTTPValidation || len(opts) == 0 {
			continue
		}
		t.Run(tt.name, func(t *testing.T) {
			tt := tt
			t.Parallel()
			tt.fn(init, t)
		})
	}
}

func TrimWhitespace(init func(VariableFields, *testing.T) (influxdb.VariableService, string, func()), t *testing.T) {
	type args struct {
		variable *influxdb.Variable
	}
	type wants struct {
		err       *errors.Error
		variables []*influxdb.Variable
	}

	tests := []struct {
		name   string
		fields VariableFields
		args   args
		wants  wants
	}{
		{
			// trims white space, but fails when variable name already exists
			name: "trimwhitespace",
			fields: VariableFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return MustIDBase16(idA)
					},
				},
				TimeGenerator: fakeGenerator,
				Variables: []*influxdb.Variable{
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(3),
						Name:           "existing-variable",
						Selected:       []string{"b"},
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{"b"},
						},
					},
				},
			},
			args: args{
				variable: &influxdb.Variable{
					ID:             MustIDBase16(idA),
					OrganizationID: platform.ID(3),
					Name:           "   existing-variable   ",
					Selected:       []string{"a"},
					Arguments: &influxdb.VariableArguments{
						Type:   "constant",
						Values: influxdb.VariableConstantValues{"a"},
					},
					CRUDLog: influxdb.CRUDLog{
						CreatedAt: fakeDate,
						UpdatedAt: fakeDate,
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.EConflict,
					Msg:  "variable is not unique",
				},
				variables: []*influxdb.Variable{
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(3),
						Name:           "existing-variable",
						Selected:       []string{"b"},
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{"b"},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			err := s.CreateVariable(ctx, tt.args.variable)
			influxErrsEqual(t, tt.wants.err, err)

			variables, err := s.FindVariables(ctx, influxdb.VariableFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve variables: %v", err)
			}
			if diff := cmp.Diff(variables, tt.wants.variables, variableCmpOptions...); diff != "" {
				t.Fatalf("found unexpected variables -got/+want\ndiff %s", diff)
			}
		})
	}
}

// CreateVariable tests influxdb.VariableService CreateVariable interface method
func CreateVariable(init func(VariableFields, *testing.T) (influxdb.VariableService, string, func()), t *testing.T) {
	type args struct {
		variable *influxdb.Variable
	}
	type wants struct {
		err       *errors.Error
		variables []*influxdb.Variable
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
				TimeGenerator: fakeGenerator,
				Variables: []*influxdb.Variable{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(1),
						Name:           "already there",
					},
				},
			},
			args: args{
				variable: &influxdb.Variable{
					OrganizationID: platform.ID(3),
					Name:           "basic variable",
					Selected:       []string{"a"},
					Arguments: &influxdb.VariableArguments{
						Type:   "constant",
						Values: influxdb.VariableConstantValues{"a"},
					},
				},
			},
			wants: wants{
				variables: []*influxdb.Variable{
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
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{"a"},
						},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: fakeDate,
							UpdatedAt: fakeDate,
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
				TimeGenerator: fakeGenerator,
				Variables: []*influxdb.Variable{
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(3),
						Name:           "existing-variable",
						Selected:       []string{"b"},
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{"b"},
						},
					},
				},
			},
			args: args{
				variable: &influxdb.Variable{
					ID:             MustIDBase16(idA),
					OrganizationID: platform.ID(3),
					Name:           "MY-variable",
					Selected:       []string{"a"},
					Arguments: &influxdb.VariableArguments{
						Type:   "constant",
						Values: influxdb.VariableConstantValues{"a"},
					},
					CRUDLog: influxdb.CRUDLog{
						CreatedAt: fakeDate,
						UpdatedAt: fakeDate,
					},
				},
			},
			wants: wants{
				err: nil,
				variables: []*influxdb.Variable{
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(3),
						Name:           "existing-variable",
						Selected:       []string{"b"},
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{"b"},
						},
					},
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(3),
						Name:           "MY-variable",
						Selected:       []string{"a"},
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{"a"},
						},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: fakeDate,
							UpdatedAt: fakeDate,
						},
					},
				},
			},
		},
		{
			name: "cant create a new variable with a name that exists",
			fields: VariableFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return MustIDBase16(idA)
					},
				},
				TimeGenerator: fakeGenerator,
				Variables: []*influxdb.Variable{
					{
						ID:             MustIDBase16(idB),
						OrganizationID: MustIDBase16(idD),
						Name:           "existing-variable",
						Selected:       []string{"b"},
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{"b"},
						},
					},
				},
			},
			args: args{
				variable: &influxdb.Variable{
					ID:             MustIDBase16(idA),
					OrganizationID: MustIDBase16(idD),
					Name:           "existing-variable",
					Selected:       []string{"a"},
					Arguments: &influxdb.VariableArguments{
						Type:   "constant",
						Values: influxdb.VariableConstantValues{"a"},
					},
					CRUDLog: influxdb.CRUDLog{
						CreatedAt: fakeDate,
						UpdatedAt: fakeDate,
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.EConflict,
					Msg:  "variable is not unique",
				},
				variables: []*influxdb.Variable{
					{
						ID:             MustIDBase16(idB),
						OrganizationID: MustIDBase16(idD),
						Name:           "existing-variable",
						Selected:       []string{"b"},
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{"b"},
						},
					},
				},
			},
		},
		{
			name: "variable names should be unique and case-insensitive",
			fields: VariableFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return MustIDBase16(idA)
					},
				},
				TimeGenerator: fakeGenerator,
				Variables: []*influxdb.Variable{
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(3),
						Name:           "existing-variable",
						Selected:       []string{"b"},
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{"b"},
						},
					},
				},
			},
			args: args{
				variable: &influxdb.Variable{
					ID:             MustIDBase16(idA),
					OrganizationID: platform.ID(3),
					Name:           "EXISTING-variable",
					Selected:       []string{"a"},
					Arguments: &influxdb.VariableArguments{
						Type:   "constant",
						Values: influxdb.VariableConstantValues{"a"},
					},
					CRUDLog: influxdb.CRUDLog{
						CreatedAt: fakeDate,
						UpdatedAt: fakeDate,
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.EConflict,
					Msg:  "variable is not unique for key ",
				},
				variables: []*influxdb.Variable{
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(3),
						Name:           "existing-variable",
						Selected:       []string{"b"},
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{"b"},
						},
					},
				},
			},
		},
		{
			name: "cant create a new variable when variable name exists with a different type",
			fields: VariableFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return MustIDBase16(idA)
					},
				},
				TimeGenerator: fakeGenerator,
				Variables: []*influxdb.Variable{
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(3),
						Name:           "existing-variable",
						Selected:       []string{"b"},
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{"b"},
						},
					},
				},
			},
			args: args{
				variable: &influxdb.Variable{
					ID:             MustIDBase16(idA),
					OrganizationID: platform.ID(3),
					Name:           "existing-variable",
					Selected:       []string{"a"},
					Arguments: &influxdb.VariableArguments{
						Type:   "constant",
						Values: influxdb.VariableConstantValues{"a"},
					},
					CRUDLog: influxdb.CRUDLog{
						CreatedAt: fakeDate,
						UpdatedAt: fakeDate,
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.EConflict,
					Msg:  "variable is not unique",
				},
				variables: []*influxdb.Variable{
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(3),
						Name:           "existing-variable",
						Selected:       []string{"b"},
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{"b"},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			err := s.CreateVariable(ctx, tt.args.variable)
			influxErrsEqual(t, tt.wants.err, err)

			variables, err := s.FindVariables(ctx, influxdb.VariableFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve variables: %v", err)
			}
			if diff := cmp.Diff(variables, tt.wants.variables, variableCmpOptions...); diff != "" {
				t.Fatalf("found unexpected variables -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindVariableByID tests influxdb.VariableService FindVariableByID interface method
func FindVariableByID(init func(VariableFields, *testing.T) (influxdb.VariableService, string, func()), t *testing.T) {
	type args struct {
		id platform.ID
	}
	type wants struct {
		err      *errors.Error
		variable *influxdb.Variable
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
				Variables: []*influxdb.Variable{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(5),
						Name:           "existing-variable-a",
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{},
						},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: fakeDate,
							UpdatedAt: fakeDate,
						},
					},
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(5),
						Name:           "existing-variable-b",
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{},
						},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: fakeDate,
							UpdatedAt: fakeDate,
						},
					},
				},
			},
			args: args{
				id: MustIDBase16(idB),
			},
			wants: wants{
				err: nil,
				variable: &influxdb.Variable{
					ID:             MustIDBase16(idB),
					OrganizationID: platform.ID(5),
					Name:           "existing-variable-b",
					Arguments: &influxdb.VariableArguments{
						Type:   "constant",
						Values: influxdb.VariableConstantValues{},
					},
					CRUDLog: influxdb.CRUDLog{
						CreatedAt: fakeDate,
						UpdatedAt: fakeDate,
					},
				},
			},
		},
		{
			name: "finding a non-existent variable",
			fields: VariableFields{
				Variables: []*influxdb.Variable{},
			},
			args: args{
				id: MustIDBase16(idA),
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.ENotFound,
					Op:   influxdb.OpFindVariableByID,
					Msg:  influxdb.ErrVariableNotFound,
				},
				variable: nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			variable, err := s.FindVariableByID(ctx, tt.args.id)
			if err != nil {
				if tt.wants.err == nil {
					require.NoError(t, err)
				}
				iErr, ok := err.(*errors.Error)
				require.True(t, ok)
				assert.Equal(t, iErr.Code, tt.wants.err.Code)
				assert.Equal(t, strings.HasPrefix(iErr.Error(), tt.wants.err.Error()), true)
				return
			}

			if diff := cmp.Diff(variable, tt.wants.variable, variableCmpOptions...); diff != "" {
				t.Fatalf("found unexpected variable -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindVariables tests influxdb.variableService FindVariables interface method
func FindVariables(init func(VariableFields, *testing.T) (influxdb.VariableService, string, func()), t *testing.T) {
	// todo(leodido)
	type args struct {
		// todo(leodido) > use VariableFilter as arg
		orgID    *platform.ID
		findOpts influxdb.FindOptions
	}
	type wants struct {
		variables []*influxdb.Variable
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
				Variables: []*influxdb.Variable{},
			},
			args: args{
				findOpts: influxdb.DefaultVariableFindOptions,
			},
			wants: wants{
				variables: []*influxdb.Variable{},
			},
		},
		{
			name: "find all variables",
			fields: VariableFields{
				Variables: []*influxdb.Variable{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(22),
						Name:           "a",
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: fakeDate,
							UpdatedAt: fakeDate,
						},
					},
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(22),
						Name:           "b",
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: fakeDate,
							UpdatedAt: fakeDate,
						},
					},
				},
			},
			args: args{
				findOpts: influxdb.DefaultVariableFindOptions,
			},
			wants: wants{
				variables: []*influxdb.Variable{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(22),
						Name:           "a",
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: fakeDate,
							UpdatedAt: fakeDate,
						},
					},
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(22),
						Name:           "b",
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: fakeDate,
							UpdatedAt: fakeDate,
						},
					},
				},
			},
		},
		{
			name: "find variables by wrong org id",
			fields: VariableFields{
				Variables: []*influxdb.Variable{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(22),
						Name:           "a",
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: fakeDate,
							UpdatedAt: fakeDate,
						},
					},
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(22),
						Name:           "b",
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: fakeDate,
							UpdatedAt: fakeDate,
						},
					},
				},
			},
			args: args{
				findOpts: influxdb.DefaultVariableFindOptions,
				orgID:    idPtr(platform.ID(1)),
			},
			wants: wants{
				variables: []*influxdb.Variable{},
			},
		},
		{
			name: "find all variables by org 22",
			fields: VariableFields{
				Variables: []*influxdb.Variable{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(1),
						Name:           "a",
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: fakeDate,
							UpdatedAt: fakeDate,
						},
					},
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(22),
						Name:           "b",
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: fakeDate,
							UpdatedAt: fakeDate,
						},
					},
					{
						ID:             MustIDBase16(idC),
						OrganizationID: platform.ID(2),
						Name:           "c",
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: fakeDate,
							UpdatedAt: fakeDate,
						},
					},
					{
						ID:             MustIDBase16(idD),
						OrganizationID: platform.ID(22),
						Name:           "d",
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: fakeDate,
							UpdatedAt: fakeDate,
						},
					},
				},
			},
			args: args{
				findOpts: influxdb.DefaultVariableFindOptions,
				orgID:    idPtr(platform.ID(22)),
			},
			wants: wants{
				variables: []*influxdb.Variable{
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(22),
						Name:           "b",
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: fakeDate,
							UpdatedAt: fakeDate,
						},
					},
					{
						ID:             MustIDBase16(idD),
						OrganizationID: platform.ID(22),
						Name:           "d",
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: fakeDate,
							UpdatedAt: fakeDate,
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

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			filter := influxdb.VariableFilter{}
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

// UpdateVariable tests influxdb.VariableService UpdateVariable interface method
func UpdateVariable(init func(VariableFields, *testing.T) (influxdb.VariableService, string, func()), t *testing.T) {
	type args struct {
		id     platform.ID
		update *influxdb.VariableUpdate
	}
	type wants struct {
		err       *errors.Error
		variables []*influxdb.Variable
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
				TimeGenerator: fakeGenerator,
				Variables: []*influxdb.Variable{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(7),
						Name:           "existing-variable-a",
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{},
						},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: oldFakeDate,
							UpdatedAt: fakeDate,
						},
					},
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(7),
						Name:           "existing-variable-b",
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{},
						},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: oldFakeDate,
							UpdatedAt: fakeDate,
						},
					},
				},
			},
			args: args{
				id: MustIDBase16(idB),
				update: &influxdb.VariableUpdate{
					Name: "new-variable-b-name",
				},
			},
			wants: wants{
				err: nil,
				variables: []*influxdb.Variable{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(7),
						Name:           "existing-variable-a",
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{},
						},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: oldFakeDate,
							UpdatedAt: fakeDate,
						},
					},
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(7),
						Name:           "new-variable-b-name",
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{},
						},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: oldFakeDate,
							UpdatedAt: fakeDate,
						},
					},
				},
			},
		},
		{
			name: "updating a non-existent variable fails",
			fields: VariableFields{
				Variables: []*influxdb.Variable{},
			},
			args: args{
				id: MustIDBase16(idA),
				update: &influxdb.VariableUpdate{
					Name: "howdy",
				},
			},
			wants: wants{
				err: &errors.Error{
					Op:   influxdb.OpUpdateVariable,
					Msg:  influxdb.ErrVariableNotFound,
					Code: errors.ENotFound,
				},
				variables: []*influxdb.Variable{},
			},
		},
		{
			name: "updating fails when variable name already exists",
			fields: VariableFields{
				TimeGenerator: fakeGenerator,
				Variables: []*influxdb.Variable{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(7),
						Name:           "variable-a",
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{},
						},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: oldFakeDate,
							UpdatedAt: fakeDate,
						},
					},
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(7),
						Name:           "variable-b",
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{},
						},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: oldFakeDate,
							UpdatedAt: fakeDate,
						},
					},
				},
			},
			args: args{
				id: MustIDBase16(idB),
				update: &influxdb.VariableUpdate{
					Name: "variable-a",
				},
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.EConflict,
					Msg:  "variable entity update conflicts with an existing entity",
				},
				variables: []*influxdb.Variable{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(7),
						Name:           "variable-a",
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{},
						},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: oldFakeDate,
							UpdatedAt: fakeDate,
						},
					},
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(7),
						Name:           "variable-b",
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{},
						},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: oldFakeDate,
							UpdatedAt: fakeDate,
						},
					},
				},
			},
		},
		{
			name: "trims the variable name but updating fails when variable name already exists",
			fields: VariableFields{
				TimeGenerator: fakeGenerator,
				Variables: []*influxdb.Variable{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(7),
						Name:           "variable-a",
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{},
						},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: oldFakeDate,
							UpdatedAt: fakeDate,
						},
					},
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(7),
						Name:           "variable-b",
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{},
						},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: oldFakeDate,
							UpdatedAt: fakeDate,
						},
					},
				},
			},
			args: args{
				id: MustIDBase16(idB),
				update: &influxdb.VariableUpdate{
					Name: "    variable-a    ",
				},
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.EConflict,
					Msg:  "variable entity update conflicts with an existing entity",
				},
				variables: []*influxdb.Variable{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(7),
						Name:           "variable-a",
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{},
						},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: oldFakeDate,
							UpdatedAt: fakeDate,
						},
					},
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(7),
						Name:           "variable-b",
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{},
						},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: oldFakeDate,
							UpdatedAt: fakeDate,
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			variable, err := s.UpdateVariable(ctx, tt.args.id, tt.args.update)
			influxErrsEqual(t, tt.wants.err, err)
			if err != nil {
				return
			}

			if variable != nil {
				if tt.args.update.Name != "" && variable.Name != tt.args.update.Name {
					t.Fatalf("variable name not updated")
				}
			}

			variables, err := s.FindVariables(ctx, influxdb.VariableFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve variables: %v", err)
			}
			if diff := cmp.Diff(variables, tt.wants.variables, variableCmpOptions...); diff != "" {
				t.Fatalf("found unexpected variables -got/+want\ndiff %s", diff)
			}
		})
	}
}

// ReplaceVariable tests influxdb.VariableService ReplaceVariable interface method
func ReplaceVariable(init func(VariableFields, *testing.T) (influxdb.VariableService, string, func()), t *testing.T) {
	type args struct {
		id          platform.ID
		newVariable *influxdb.Variable
	}
	type wants struct {
		err       *errors.Error
		variables []*influxdb.Variable
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
				TimeGenerator: fakeGenerator,
				Variables: []*influxdb.Variable{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(7),
						Name:           "existing-variable",
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{},
						},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: oldFakeDate,
							UpdatedAt: fakeDate,
						},
					},
				},
			},
			args: args{
				id: MustIDBase16(idB),
				newVariable: &influxdb.Variable{
					ID:             MustIDBase16(idA),
					OrganizationID: platform.ID(7),
					Name:           "renamed-variable",
					Arguments: &influxdb.VariableArguments{
						Type:   "constant",
						Values: influxdb.VariableConstantValues{},
					},
					CRUDLog: influxdb.CRUDLog{
						CreatedAt: oldFakeDate,
						UpdatedAt: fakeDate,
					},
				},
			},
			wants: wants{
				err: nil,
				variables: []*influxdb.Variable{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(7),
						Name:           "renamed-variable",
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{},
						},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: oldFakeDate,
							UpdatedAt: fakeDate,
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			err := s.ReplaceVariable(ctx, tt.args.newVariable)
			influxErrsEqual(t, tt.wants.err, err)
			if err != nil {
				return
			}

			variable, err := s.FindVariableByID(ctx, tt.args.id)
			if err != nil {
				return
			}

			if variable != nil {
				if tt.args.newVariable.Name != "" && variable.Name != tt.args.newVariable.Name {
					t.Fatalf("variable name not updated")
				}
			}

			variables, err := s.FindVariables(ctx, influxdb.VariableFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve variables: %v", err)
			}
			if diff := cmp.Diff(variables, tt.wants.variables, variableCmpOptions...); diff != "" {
				t.Fatalf("found unexpected variables -got/+want\ndiff %s", diff)
			}
		})
	}
}

// DeleteVariable tests influxdb.VariableService DeleteVariable interface method
func DeleteVariable(init func(VariableFields, *testing.T) (influxdb.VariableService, string, func()), t *testing.T) {
	type args struct {
		id platform.ID
	}
	type wants struct {
		err       *errors.Error
		variables []*influxdb.Variable
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
				Variables: []*influxdb.Variable{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(9),
						Name:           "m",
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{},
						},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: oldFakeDate,
							UpdatedAt: oldFakeDate,
						},
					},
				},
			},
			args: args{
				id: MustIDBase16(idA),
			},
			wants: wants{
				err:       nil,
				variables: []*influxdb.Variable{},
			},
		},
		{
			name: "deleting a variable that doesn't exist",
			fields: VariableFields{
				Variables: []*influxdb.Variable{
					{
						ID:             MustIDBase16(idD),
						OrganizationID: platform.ID(1),
						Name:           "existing-variable",
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{},
						},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: oldFakeDate,
							UpdatedAt: oldFakeDate,
						},
					},
				},
			},
			args: args{
				id: MustIDBase16(idB),
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.ENotFound,
					Op:   influxdb.OpDeleteVariable,
					Msg:  influxdb.ErrVariableNotFound,
				},
				variables: []*influxdb.Variable{
					{
						ID:             MustIDBase16(idD),
						OrganizationID: platform.ID(1),
						Name:           "existing-variable",
						Arguments: &influxdb.VariableArguments{
							Type:   "constant",
							Values: influxdb.VariableConstantValues{},
						},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: oldFakeDate,
							UpdatedAt: oldFakeDate,
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			defer s.ReplaceVariable(ctx, &influxdb.Variable{
				ID:             tt.args.id,
				OrganizationID: platform.ID(1),
			})

			err := s.DeleteVariable(ctx, tt.args.id)
			if err != nil {
				if tt.wants.err == nil {
					require.NoError(t, err)
				}
				iErr, ok := err.(*errors.Error)
				require.True(t, ok)
				assert.Equal(t, iErr.Code, tt.wants.err.Code)
				assert.Equal(t, strings.HasPrefix(iErr.Error(), tt.wants.err.Error()), true)
				return
			}

			variables, err := s.FindVariables(ctx, influxdb.VariableFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve variables: %v", err)
			}
			if diff := cmp.Diff(variables, tt.wants.variables, variableCmpOptions...); diff != "" {
				t.Fatalf("found unexpected variables -got/+want\ndiff %s", diff)
			}
		})
	}
}
