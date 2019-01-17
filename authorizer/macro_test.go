package authorizer_test

import (
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/authorizer"
	influxdbcontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/mock"
	influxdbtesting "github.com/influxdata/influxdb/testing"
)

var macroCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*influxdb.Macro) []*influxdb.Macro {
		out := append([]*influxdb.Macro(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID.String() > out[j].ID.String()
		})
		return out
	}),
}

func TestMacroService_FindMacroByID(t *testing.T) {
	type fields struct {
		MacroService influxdb.MacroService
	}
	type args struct {
		permission influxdb.Permission
		id         influxdb.ID
	}
	type wants struct {
		err error
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to access id",
			fields: fields{
				MacroService: &mock.MacroService{
					FindMacroByIDF: func(ctx context.Context, id influxdb.ID) (*influxdb.Macro, error) {
						return &influxdb.Macro{
							ID:             id,
							OrganizationID: 10,
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.MacrosResourceType,
						ID:   influxdbtesting.IDPtr(1),
					},
				},
				id: 1,
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to access id",
			fields: fields{
				MacroService: &mock.MacroService{
					FindMacroByIDF: func(ctx context.Context, id influxdb.ID) (*influxdb.Macro, error) {
						return &influxdb.Macro{
							ID:             id,
							OrganizationID: 10,
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.MacrosResourceType,
						ID:   influxdbtesting.IDPtr(2),
					},
				},
				id: 1,
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "read:orgs/000000000000000a/macros/0000000000000001 is unauthorized",
					Code: influxdb.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewMacroService(tt.fields.MacroService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{[]influxdb.Permission{tt.args.permission}})

			_, err := s.FindMacroByID(ctx, tt.args.id)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestMacroService_FindMacros(t *testing.T) {
	type fields struct {
		MacroService influxdb.MacroService
	}
	type args struct {
		permission influxdb.Permission
	}
	type wants struct {
		err    error
		macros []*influxdb.Macro
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to see all macros",
			fields: fields{
				MacroService: &mock.MacroService{
					FindMacrosF: func(ctx context.Context, filter influxdb.MacroFilter, opt ...influxdb.FindOptions) ([]*influxdb.Macro, error) {
						return []*influxdb.Macro{
							{
								ID:             1,
								OrganizationID: 10,
							},
							{
								ID:             2,
								OrganizationID: 10,
							},
							{
								ID:             3,
								OrganizationID: 11,
							},
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.MacrosResourceType,
					},
				},
			},
			wants: wants{
				macros: []*influxdb.Macro{
					{
						ID:             1,
						OrganizationID: 10,
					},
					{
						ID:             2,
						OrganizationID: 10,
					},
					{
						ID:             3,
						OrganizationID: 11,
					},
				},
			},
		},
		{
			name: "authorized to access a single orgs macros",
			fields: fields{
				MacroService: &mock.MacroService{
					FindMacrosF: func(ctx context.Context, filter influxdb.MacroFilter, opt ...influxdb.FindOptions) ([]*influxdb.Macro, error) {
						return []*influxdb.Macro{
							{
								ID:             1,
								OrganizationID: 10,
							},
							{
								ID:             2,
								OrganizationID: 10,
							},
							{
								ID:             3,
								OrganizationID: 11,
							},
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type:  influxdb.MacrosResourceType,
						OrgID: influxdbtesting.IDPtr(10),
					},
				},
			},
			wants: wants{
				macros: []*influxdb.Macro{
					{
						ID:             1,
						OrganizationID: 10,
					},
					{
						ID:             2,
						OrganizationID: 10,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewMacroService(tt.fields.MacroService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{[]influxdb.Permission{tt.args.permission}})

			macros, err := s.FindMacros(ctx, influxdb.MacroFilter{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)

			if diff := cmp.Diff(macros, tt.wants.macros, macroCmpOptions...); diff != "" {
				t.Errorf("macros are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func TestMacroService_UpdateMacro(t *testing.T) {
	type fields struct {
		MacroService influxdb.MacroService
	}
	type args struct {
		id          influxdb.ID
		permissions []influxdb.Permission
	}
	type wants struct {
		err error
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to update macro",
			fields: fields{
				MacroService: &mock.MacroService{
					FindMacroByIDF: func(ctx context.Context, id influxdb.ID) (*influxdb.Macro, error) {
						return &influxdb.Macro{
							ID:             1,
							OrganizationID: 10,
						}, nil
					},
					UpdateMacroF: func(ctx context.Context, id influxdb.ID, upd *influxdb.MacroUpdate) (*influxdb.Macro, error) {
						return &influxdb.Macro{
							ID:             1,
							OrganizationID: 10,
						}, nil
					},
				},
			},
			args: args{
				id: 1,
				permissions: []influxdb.Permission{
					{
						Action: "write",
						Resource: influxdb.Resource{
							Type: influxdb.MacrosResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.MacrosResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to update macro",
			fields: fields{
				MacroService: &mock.MacroService{
					FindMacroByIDF: func(ctx context.Context, id influxdb.ID) (*influxdb.Macro, error) {
						return &influxdb.Macro{
							ID:             1,
							OrganizationID: 10,
						}, nil
					},
					UpdateMacroF: func(ctx context.Context, id influxdb.ID, upd *influxdb.MacroUpdate) (*influxdb.Macro, error) {
						return &influxdb.Macro{
							ID:             1,
							OrganizationID: 10,
						}, nil
					},
				},
			},
			args: args{
				id: 1,
				permissions: []influxdb.Permission{
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.MacrosResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "write:orgs/000000000000000a/macros/0000000000000001 is unauthorized",
					Code: influxdb.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewMacroService(tt.fields.MacroService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{tt.args.permissions})

			_, err := s.UpdateMacro(ctx, tt.args.id, &influxdb.MacroUpdate{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestMacroService_ReplaceMacro(t *testing.T) {
	type fields struct {
		MacroService influxdb.MacroService
	}
	type args struct {
		macro       influxdb.Macro
		permissions []influxdb.Permission
	}
	type wants struct {
		err error
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to replace macro",
			fields: fields{
				MacroService: &mock.MacroService{
					FindMacroByIDF: func(ctx context.Context, id influxdb.ID) (*influxdb.Macro, error) {
						return &influxdb.Macro{
							ID:             1,
							OrganizationID: 10,
						}, nil
					},
					ReplaceMacroF: func(ctx context.Context, m *influxdb.Macro) error {
						return nil
					},
				},
			},
			args: args{
				macro: influxdb.Macro{
					ID:             1,
					OrganizationID: 10,
					Name:           "replace",
				},
				permissions: []influxdb.Permission{
					{
						Action: "write",
						Resource: influxdb.Resource{
							Type: influxdb.MacrosResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.MacrosResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to replace macro",
			fields: fields{
				MacroService: &mock.MacroService{
					FindMacroByIDF: func(ctx context.Context, id influxdb.ID) (*influxdb.Macro, error) {
						return &influxdb.Macro{
							ID:             1,
							OrganizationID: 10,
						}, nil
					},
					ReplaceMacroF: func(ctx context.Context, m *influxdb.Macro) error {
						return nil
					},
				},
			},
			args: args{
				macro: influxdb.Macro{
					ID:             1,
					OrganizationID: 10,
				},
				permissions: []influxdb.Permission{
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.MacrosResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "write:orgs/000000000000000a/macros/0000000000000001 is unauthorized",
					Code: influxdb.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewMacroService(tt.fields.MacroService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{tt.args.permissions})

			err := s.ReplaceMacro(ctx, &tt.args.macro)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestMacroService_DeleteMacro(t *testing.T) {
	type fields struct {
		MacroService influxdb.MacroService
	}
	type args struct {
		id          influxdb.ID
		permissions []influxdb.Permission
	}
	type wants struct {
		err error
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to delete macro",
			fields: fields{
				MacroService: &mock.MacroService{
					FindMacroByIDF: func(ctx context.Context, id influxdb.ID) (*influxdb.Macro, error) {
						return &influxdb.Macro{
							ID:             1,
							OrganizationID: 10,
						}, nil
					},
					DeleteMacroF: func(ctx context.Context, id influxdb.ID) error {
						return nil
					},
				},
			},
			args: args{
				id: 1,
				permissions: []influxdb.Permission{
					{
						Action: "write",
						Resource: influxdb.Resource{
							Type: influxdb.MacrosResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.MacrosResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to delete macro",
			fields: fields{
				MacroService: &mock.MacroService{
					FindMacroByIDF: func(ctx context.Context, id influxdb.ID) (*influxdb.Macro, error) {
						return &influxdb.Macro{
							ID:             1,
							OrganizationID: 10,
						}, nil
					},
					DeleteMacroF: func(ctx context.Context, id influxdb.ID) error {
						return nil
					},
				},
			},
			args: args{
				id: 1,
				permissions: []influxdb.Permission{
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.MacrosResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "write:orgs/000000000000000a/macros/0000000000000001 is unauthorized",
					Code: influxdb.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewMacroService(tt.fields.MacroService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{tt.args.permissions})

			err := s.DeleteMacro(ctx, tt.args.id)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestMacroService_CreateMacro(t *testing.T) {
	type fields struct {
		MacroService influxdb.MacroService
	}
	type args struct {
		permission influxdb.Permission
		orgID      influxdb.ID
	}
	type wants struct {
		err error
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to create macro",
			fields: fields{
				MacroService: &mock.MacroService{
					CreateMacroF: func(ctx context.Context, o *influxdb.Macro) error {
						return nil
					},
				},
			},
			args: args{
				orgID: 10,
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type:  influxdb.MacrosResourceType,
						OrgID: influxdbtesting.IDPtr(10),
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to create macro",
			fields: fields{
				MacroService: &mock.MacroService{
					CreateMacroF: func(ctx context.Context, o *influxdb.Macro) error {
						return nil
					},
				},
			},
			args: args{
				orgID: 10,
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type: influxdb.MacrosResourceType,
						ID:   influxdbtesting.IDPtr(1),
					},
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "write:orgs/000000000000000a/macros is unauthorized",
					Code: influxdb.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewMacroService(tt.fields.MacroService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{[]influxdb.Permission{tt.args.permission}})

			err := s.CreateMacro(ctx, &influxdb.Macro{OrganizationID: tt.args.orgID})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}
