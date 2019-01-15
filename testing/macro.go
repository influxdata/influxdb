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

var macroCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*platform.Macro) []*platform.Macro {
		out := append([]*platform.Macro(nil), in...)
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID.String() > out[j].ID.String()
		})
		return out
	}),
}

// MacroFields defines fields for a macro test
type MacroFields struct {
	Macros      []*platform.Macro
	IDGenerator platform.IDGenerator
}

// MacroService tests all the service functions.
func MacroService(
	init func(MacroFields, *testing.T) (platform.MacroService, string, func()), t *testing.T,
) {
	tests := []struct {
		name string
		fn   func(init func(MacroFields, *testing.T) (platform.MacroService, string, func()),
			t *testing.T)
	}{
		{
			name: "CreateMacro",
			fn:   CreateMacro,
		},
		{
			name: "FindMacroByID",
			fn:   FindMacroByID,
		},
		{
			name: "FindMacros",
			fn:   FindMacros,
		},
		{
			name: "UpdateMacro",
			fn:   UpdateMacro,
		},
		{
			name: "DeleteMacro",
			fn:   DeleteMacro,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(init, t)
		})
	}
}

// CreateMacro tests platform.MacroService CreateMacro interface method
func CreateMacro(init func(MacroFields, *testing.T) (platform.MacroService, string, func()), t *testing.T) {
	type args struct {
		macro *platform.Macro
	}
	type wants struct {
		err    error
		macros []*platform.Macro
	}

	tests := []struct {
		name   string
		fields MacroFields
		args   args
		wants  wants
	}{
		{
			name: "basic create with missing id",
			fields: MacroFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return MustIDBase16(idD)
					},
				},
				Macros: []*platform.Macro{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(1),
						Name:           "already there",
					},
				},
			},
			args: args{
				macro: &platform.Macro{
					OrganizationID: platform.ID(3),
					Name:           "basic macro",
					Selected:       []string{"a"},
					Arguments: &platform.MacroArguments{
						Type:   "constant",
						Values: platform.MacroConstantValues{"a"},
					},
				},
			},
			wants: wants{
				macros: []*platform.Macro{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(1),
						Name:           "already there",
					},
					{
						ID:             MustIDBase16(idD),
						OrganizationID: platform.ID(3),
						Name:           "basic macro",
						Selected:       []string{"a"},
						Arguments: &platform.MacroArguments{
							Type:   "constant",
							Values: platform.MacroConstantValues{"a"},
						},
					},
				},
			},
		},
		{
			name: "creating a macro assigns the macro an id and adds it to the store",
			fields: MacroFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return MustIDBase16(idA)
					},
				},
				Macros: []*platform.Macro{
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(3),
						Name:           "existing-macro",
						Selected:       []string{"b"},
						Arguments: &platform.MacroArguments{
							Type:   "constant",
							Values: platform.MacroConstantValues{"b"},
						},
					},
				},
			},
			args: args{
				macro: &platform.Macro{
					ID:             MustIDBase16(idA),
					OrganizationID: platform.ID(3),
					Name:           "my-macro",
					Selected:       []string{"a"},
					Arguments: &platform.MacroArguments{
						Type:   "constant",
						Values: platform.MacroConstantValues{"a"},
					},
				},
			},
			wants: wants{
				err: nil,
				macros: []*platform.Macro{
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(3),
						Name:           "existing-macro",
						Selected:       []string{"b"},
						Arguments: &platform.MacroArguments{
							Type:   "constant",
							Values: platform.MacroConstantValues{"b"},
						},
					},
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(3),
						Name:           "my-macro",
						Selected:       []string{"a"},
						Arguments: &platform.MacroArguments{
							Type:   "constant",
							Values: platform.MacroConstantValues{"a"},
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

			err := s.CreateMacro(ctx, tt.args.macro)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			macros, err := s.FindMacros(ctx, platform.MacroFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve macros: %v", err)
			}
			if diff := cmp.Diff(macros, tt.wants.macros, macroCmpOptions...); diff != "" {
				t.Fatalf("found unexpected macros -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindMacroByID tests platform.MacroService FindMacroByID interface method
func FindMacroByID(init func(MacroFields, *testing.T) (platform.MacroService, string, func()), t *testing.T) {
	type args struct {
		id platform.ID
	}
	type wants struct {
		err   error
		macro *platform.Macro
	}

	tests := []struct {
		name   string
		fields MacroFields
		args   args
		wants  wants
	}{
		{
			name: "finding a macro that exists by id",
			fields: MacroFields{
				Macros: []*platform.Macro{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(5),
						Name:           "existing-macro-a",
						Arguments: &platform.MacroArguments{
							Type:   "constant",
							Values: platform.MacroConstantValues{},
						},
					},
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(5),
						Name:           "existing-macro-b",
						Arguments: &platform.MacroArguments{
							Type:   "constant",
							Values: platform.MacroConstantValues{},
						},
					},
				},
			},
			args: args{
				id: MustIDBase16(idB),
			},
			wants: wants{
				err: nil,
				macro: &platform.Macro{
					ID:             MustIDBase16(idB),
					OrganizationID: platform.ID(5),
					Name:           "existing-macro-b",
					Arguments: &platform.MacroArguments{
						Type:   "constant",
						Values: platform.MacroConstantValues{},
					},
				},
			},
		},
		{
			name: "finding a non-existant macro",
			fields: MacroFields{
				Macros: []*platform.Macro{},
			},
			args: args{
				id: MustIDBase16(idA),
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.ENotFound,
					Op:   platform.OpFindMacroByID,
					Msg:  platform.ErrMacroNotFound,
				},
				macro: nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			macro, err := s.FindMacroByID(ctx, tt.args.id)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(macro, tt.wants.macro); diff != "" {
				t.Fatalf("found unexpected macro -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindMacros tests platform.macroService FindMacros interface method
func FindMacros(init func(MacroFields, *testing.T) (platform.MacroService, string, func()), t *testing.T) {
	// todo(leodido)
	type args struct {
		// todo(leodido) > use MacroFilter as arg
		orgID    *platform.ID
		findOpts platform.FindOptions
	}
	type wants struct {
		macros []*platform.Macro
		err    error
	}

	tests := []struct {
		name   string
		fields MacroFields
		args   args
		wants  wants
	}{
		{
			name: "find nothing (empty set)",
			fields: MacroFields{
				Macros: []*platform.Macro{},
			},
			args: args{
				findOpts: platform.DefaultMacroFindOptions,
			},
			wants: wants{
				macros: []*platform.Macro{},
			},
		},
		{
			name: "find all macros",
			fields: MacroFields{
				Macros: []*platform.Macro{
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
				findOpts: platform.DefaultMacroFindOptions,
			},
			wants: wants{
				macros: []*platform.Macro{
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
			name: "find macros by wrong org id",
			fields: MacroFields{
				Macros: []*platform.Macro{
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
				findOpts: platform.DefaultMacroFindOptions,
				orgID:    idPtr(platform.ID(1)),
			},
			wants: wants{
				macros: []*platform.Macro{},
			},
		},
		{
			name: "find all macros by org 22",
			fields: MacroFields{
				Macros: []*platform.Macro{
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
				findOpts: platform.DefaultMacroFindOptions,
				orgID:    idPtr(platform.ID(22)),
			},
			wants: wants{
				macros: []*platform.Macro{
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

			filter := platform.MacroFilter{}
			if tt.args.orgID != nil {
				filter.OrganizationID = tt.args.orgID
			}

			macros, err := s.FindMacros(ctx, filter, tt.args.findOpts)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(macros, tt.wants.macros, macroCmpOptions...); diff != "" {
				t.Errorf("macros are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// UpdateMacro tests platform.MacroService UpdateMacro interface method
func UpdateMacro(init func(MacroFields, *testing.T) (platform.MacroService, string, func()), t *testing.T) {
	type args struct {
		id     platform.ID
		update *platform.MacroUpdate
	}
	type wants struct {
		err    error
		macros []*platform.Macro
	}

	tests := []struct {
		name   string
		fields MacroFields
		args   args
		wants  wants
	}{
		{
			name: "updating a macro's name",
			fields: MacroFields{
				Macros: []*platform.Macro{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(7),
						Name:           "existing-macro-a",
						Arguments: &platform.MacroArguments{
							Type:   "constant",
							Values: platform.MacroConstantValues{},
						},
					},
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(7),
						Name:           "existing-macro-b",
						Arguments: &platform.MacroArguments{
							Type:   "constant",
							Values: platform.MacroConstantValues{},
						},
					},
				},
			},
			args: args{
				id: MustIDBase16(idB),
				update: &platform.MacroUpdate{
					Name: "new-macro-b-name",
				},
			},
			wants: wants{
				err: nil,
				macros: []*platform.Macro{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(7),
						Name:           "existing-macro-a",
						Arguments: &platform.MacroArguments{
							Type:   "constant",
							Values: platform.MacroConstantValues{},
						},
					},
					{
						ID:             MustIDBase16(idB),
						OrganizationID: platform.ID(7),
						Name:           "new-macro-b-name",
						Arguments: &platform.MacroArguments{
							Type:   "constant",
							Values: platform.MacroConstantValues{},
						},
					},
				},
			},
		},
		{
			name: "updating a non-existant macro fails",
			fields: MacroFields{
				Macros: []*platform.Macro{},
			},
			args: args{
				id: MustIDBase16(idA),
				update: &platform.MacroUpdate{
					Name: "howdy",
				},
			},
			wants: wants{
				err: &platform.Error{
					Op:   platform.OpUpdateMacro,
					Msg:  platform.ErrMacroNotFound,
					Code: platform.ENotFound,
				},
				macros: []*platform.Macro{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			macro, err := s.UpdateMacro(ctx, tt.args.id, tt.args.update)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if macro != nil {
				if tt.args.update.Name != "" && macro.Name != tt.args.update.Name {
					t.Fatalf("macro name not updated")
				}
			}

			macros, err := s.FindMacros(ctx, platform.MacroFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve macros: %v", err)
			}
			if diff := cmp.Diff(macros, tt.wants.macros, macroCmpOptions...); diff != "" {
				t.Fatalf("found unexpected macros -got/+want\ndiff %s", diff)
			}
		})
	}
}

// DeleteMacro tests platform.MacroService DeleteMacro interface method
func DeleteMacro(init func(MacroFields, *testing.T) (platform.MacroService, string, func()), t *testing.T) {
	type args struct {
		id platform.ID
	}
	type wants struct {
		err    error
		macros []*platform.Macro
	}

	tests := []struct {
		name   string
		fields MacroFields
		args   args
		wants  wants
	}{
		{
			name: "deleting a macro",
			fields: MacroFields{
				Macros: []*platform.Macro{
					{
						ID:             MustIDBase16(idA),
						OrganizationID: platform.ID(9),
						Name:           "m",
						Arguments: &platform.MacroArguments{
							Type:   "constant",
							Values: platform.MacroConstantValues{},
						},
					},
				},
			},
			args: args{
				id: MustIDBase16(idA),
			},
			wants: wants{
				err:    nil,
				macros: []*platform.Macro{},
			},
		},
		{
			name: "deleting a macro that doesn't exist",
			fields: MacroFields{
				Macros: []*platform.Macro{
					{
						ID:             MustIDBase16(idD),
						OrganizationID: platform.ID(1),
						Name:           "existing-macro",
						Arguments: &platform.MacroArguments{
							Type:   "constant",
							Values: platform.MacroConstantValues{},
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
					Op:   platform.OpDeleteMacro,
					Msg:  platform.ErrMacroNotFound,
				},
				macros: []*platform.Macro{
					{
						ID:             MustIDBase16(idD),
						OrganizationID: platform.ID(1),
						Name:           "existing-macro",
						Arguments: &platform.MacroArguments{
							Type:   "constant",
							Values: platform.MacroConstantValues{},
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

			err := s.DeleteMacro(ctx, tt.args.id)
			defer s.ReplaceMacro(ctx, &platform.Macro{
				ID:             tt.args.id,
				OrganizationID: platform.ID(1),
			})
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			macros, err := s.FindMacros(ctx, platform.MacroFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve macros: %v", err)
			}
			if diff := cmp.Diff(macros, tt.wants.macros, macroCmpOptions...); diff != "" {
				t.Fatalf("found unexpected macros -got/+want\ndiff %s", diff)
			}
		})
	}
}
