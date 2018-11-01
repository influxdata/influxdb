package testing

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform"
	kerrors "github.com/influxdata/platform/kit/errors"
	"github.com/influxdata/platform/mock"
)

const (
	idA = "020f755c3c082000"
	idB = "020f755c3c082001"
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
	init func(MacroFields, *testing.T) (platform.MacroService, func()), t *testing.T,
) {
	tests := []struct {
		name string
		fn   func(init func(MacroFields, *testing.T) (platform.MacroService, func()),
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
func CreateMacro(init func(MacroFields, *testing.T) (platform.MacroService, func()), t *testing.T) {
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
			name: "creating a macro assigns the macro an id and adds it to the store",
			fields: MacroFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return MustIDBase16(idA)
					},
				},
				Macros: []*platform.Macro{
					{
						ID:       MustIDBase16(idB),
						Name:     "existing-macro",
						Selected: []string{"b"},
						Arguments: &platform.MacroArguments{
							Type:   "constant",
							Values: platform.MacroConstantValues{"b"},
						},
					},
				},
			},
			args: args{
				macro: &platform.Macro{
					ID:       MustIDBase16(idA),
					Name:     "my-macro",
					Selected: []string{"a"},
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
						ID:       MustIDBase16(idB),
						Name:     "existing-macro",
						Selected: []string{"b"},
						Arguments: &platform.MacroArguments{
							Type:   "constant",
							Values: platform.MacroConstantValues{"b"},
						},
					},
					{
						ID:       MustIDBase16(idA),
						Name:     "my-macro",
						Selected: []string{"a"},
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
		s, done := init(tt.fields, t)
		defer done()
		ctx := context.TODO()

		err := s.CreateMacro(ctx, tt.args.macro)
		diffErrors(err, tt.wants.err, t)

		macros, err := s.FindMacros(ctx)
		if err != nil {
			t.Fatalf("failed to retrieve macros: %v", err)
		}
		if diff := cmp.Diff(macros, tt.wants.macros, macroCmpOptions...); diff != "" {
			t.Fatalf("found unexpected macros -got/+want\ndiff %s", diff)
		}
	}
}

// FindMacroByID tests platform.MacroService FindMacroByID interface method
func FindMacroByID(init func(MacroFields, *testing.T) (platform.MacroService, func()), t *testing.T) {
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
						ID:   MustIDBase16(idA),
						Name: "existing-macro-a",
						Arguments: &platform.MacroArguments{
							Type:   "constant",
							Values: platform.MacroConstantValues{},
						},
					},
					{
						ID:   MustIDBase16(idB),
						Name: "existing-macro-b",
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
					ID:   MustIDBase16(idB),
					Name: "existing-macro-b",
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
				err:   kerrors.Errorf(kerrors.NotFound, "macro with ID %s not found", idA),
				macro: nil,
			},
		},
	}

	for _, tt := range tests {
		s, done := init(tt.fields, t)
		defer done()
		ctx := context.TODO()

		macro, err := s.FindMacroByID(ctx, tt.args.id)
		diffErrors(err, tt.wants.err, t)

		if diff := cmp.Diff(macro, tt.wants.macro); diff != "" {
			t.Fatalf("found unexpected macro -got/+want\ndiff %s", diff)
		}
	}
}

// UpdateMacro tests platform.MacroService UpdateMacro interface method
func UpdateMacro(init func(MacroFields, *testing.T) (platform.MacroService, func()), t *testing.T) {
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
						ID:   MustIDBase16(idA),
						Name: "existing-macro-a",
						Arguments: &platform.MacroArguments{
							Type:   "constant",
							Values: platform.MacroConstantValues{},
						},
					},
					{
						ID:   MustIDBase16(idB),
						Name: "existing-macro-b",
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
						ID:   MustIDBase16(idA),
						Name: "existing-macro-a",
						Arguments: &platform.MacroArguments{
							Type:   "constant",
							Values: platform.MacroConstantValues{},
						},
					},
					{
						ID:   MustIDBase16(idB),
						Name: "new-macro-b-name",
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
				err:    fmt.Errorf("macro with ID %s not found", idA),
				macros: []*platform.Macro{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()

			macro, err := s.UpdateMacro(ctx, tt.args.id, tt.args.update)
			diffErrors(err, tt.wants.err, t)

			if macro != nil {
				if tt.args.update.Name != "" && macro.Name != tt.args.update.Name {
					t.Fatalf("macro name not updated")
				}
			}

			macros, err := s.FindMacros(ctx)
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
func DeleteMacro(init func(MacroFields, *testing.T) (platform.MacroService, func()), t *testing.T) {
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
						ID:   MustIDBase16(idA),
						Name: "existing-macro",
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
						ID:   MustIDBase16(idA),
						Name: "existing-macro",
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
				err: kerrors.Errorf(kerrors.NotFound, "macro with ID %s not found", idB),
				macros: []*platform.Macro{
					{
						ID:   MustIDBase16(idA),
						Name: "existing-macro",
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
		s, done := init(tt.fields, t)
		defer done()
		ctx := context.TODO()

		err := s.DeleteMacro(ctx, tt.args.id)
		defer s.ReplaceMacro(ctx, &platform.Macro{
			ID: tt.args.id,
		})
		diffErrors(err, tt.wants.err, t)

		macros, err := s.FindMacros(ctx)
		if err != nil {
			t.Fatalf("failed to retrieve macros: %v", err)
		}
		if diff := cmp.Diff(macros, tt.wants.macros, macroCmpOptions...); diff != "" {
			t.Fatalf("found unexpected macros -got/+want\ndiff %s", diff)
		}
	}
}

func diffErrors(actual, expected error, t *testing.T) {
	if expected == nil && actual != nil {
		t.Fatalf("unexpected error %q", actual.Error())
	}

	if expected != nil && actual == nil {
		t.Fatalf("expected error %q but received nil", expected.Error())
	}

	if expected != nil && actual != nil && expected.Error() != actual.Error() {
		t.Fatalf("expected error %q but received error %q", expected.Error(), actual.Error())
	}
}
