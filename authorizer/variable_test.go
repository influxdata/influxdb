package authorizer_test

import (
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
	influxdbcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/mock"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
)

var variableCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*influxdb.Variable) []*influxdb.Variable {
		out := append([]*influxdb.Variable(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID.String() > out[j].ID.String()
		})
		return out
	}),
}

func TestVariableService_FindVariableByID(t *testing.T) {
	type fields struct {
		VariableService influxdb.VariableService
	}
	type args struct {
		permission influxdb.Permission
		id         platform.ID
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
				VariableService: &mock.VariableService{
					FindVariableByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Variable, error) {
						return &influxdb.Variable{
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
						Type: influxdb.VariablesResourceType,
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
				VariableService: &mock.VariableService{
					FindVariableByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Variable, error) {
						return &influxdb.Variable{
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
						Type: influxdb.VariablesResourceType,
						ID:   influxdbtesting.IDPtr(2),
					},
				},
				id: 1,
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "read:orgs/000000000000000a/variables/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewVariableService(tt.fields.VariableService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			_, err := s.FindVariableByID(ctx, tt.args.id)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestVariableService_FindVariables(t *testing.T) {
	type fields struct {
		VariableService influxdb.VariableService
	}
	type args struct {
		permission influxdb.Permission
	}
	type wants struct {
		err       error
		variables []*influxdb.Variable
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to see all variables",
			fields: fields{
				VariableService: &mock.VariableService{
					FindVariablesF: func(ctx context.Context, filter influxdb.VariableFilter, opt ...influxdb.FindOptions) ([]*influxdb.Variable, error) {
						return []*influxdb.Variable{
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
						Type: influxdb.VariablesResourceType,
					},
				},
			},
			wants: wants{
				variables: []*influxdb.Variable{
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
			name: "authorized to access a single orgs variables",
			fields: fields{
				VariableService: &mock.VariableService{
					FindVariablesF: func(ctx context.Context, filter influxdb.VariableFilter, opt ...influxdb.FindOptions) ([]*influxdb.Variable, error) {
						return []*influxdb.Variable{
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
						Type:  influxdb.VariablesResourceType,
						OrgID: influxdbtesting.IDPtr(10),
					},
				},
			},
			wants: wants{
				variables: []*influxdb.Variable{
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
			s := authorizer.NewVariableService(tt.fields.VariableService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			variables, err := s.FindVariables(ctx, influxdb.VariableFilter{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)

			if diff := cmp.Diff(variables, tt.wants.variables, variableCmpOptions...); diff != "" {
				t.Errorf("variables are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func TestVariableService_UpdateVariable(t *testing.T) {
	type fields struct {
		VariableService influxdb.VariableService
	}
	type args struct {
		id          platform.ID
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
			name: "authorized to update variable",
			fields: fields{
				VariableService: &mock.VariableService{
					FindVariableByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Variable, error) {
						return &influxdb.Variable{
							ID:             1,
							OrganizationID: 10,
						}, nil
					},
					UpdateVariableF: func(ctx context.Context, id platform.ID, upd *influxdb.VariableUpdate) (*influxdb.Variable, error) {
						return &influxdb.Variable{
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
							Type: influxdb.VariablesResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.VariablesResourceType,
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
			name: "unauthorized to update variable",
			fields: fields{
				VariableService: &mock.VariableService{
					FindVariableByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Variable, error) {
						return &influxdb.Variable{
							ID:             1,
							OrganizationID: 10,
						}, nil
					},
					UpdateVariableF: func(ctx context.Context, id platform.ID, upd *influxdb.VariableUpdate) (*influxdb.Variable, error) {
						return &influxdb.Variable{
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
							Type: influxdb.VariablesResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/variables/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewVariableService(tt.fields.VariableService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			_, err := s.UpdateVariable(ctx, tt.args.id, &influxdb.VariableUpdate{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestVariableService_ReplaceVariable(t *testing.T) {
	type fields struct {
		VariableService influxdb.VariableService
	}
	type args struct {
		variable    influxdb.Variable
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
			name: "authorized to replace variable",
			fields: fields{
				VariableService: &mock.VariableService{
					FindVariableByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Variable, error) {
						return &influxdb.Variable{
							ID:             1,
							OrganizationID: 10,
						}, nil
					},
					ReplaceVariableF: func(ctx context.Context, m *influxdb.Variable) error {
						return nil
					},
				},
			},
			args: args{
				variable: influxdb.Variable{
					ID:             1,
					OrganizationID: 10,
					Name:           "replace",
				},
				permissions: []influxdb.Permission{
					{
						Action: "write",
						Resource: influxdb.Resource{
							Type: influxdb.VariablesResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.VariablesResourceType,
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
			name: "unauthorized to replace variable",
			fields: fields{
				VariableService: &mock.VariableService{
					FindVariableByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Variable, error) {
						return &influxdb.Variable{
							ID:             1,
							OrganizationID: 10,
						}, nil
					},
					ReplaceVariableF: func(ctx context.Context, m *influxdb.Variable) error {
						return nil
					},
				},
			},
			args: args{
				variable: influxdb.Variable{
					ID:             1,
					OrganizationID: 10,
				},
				permissions: []influxdb.Permission{
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.VariablesResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/variables/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewVariableService(tt.fields.VariableService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			err := s.ReplaceVariable(ctx, &tt.args.variable)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestVariableService_DeleteVariable(t *testing.T) {
	type fields struct {
		VariableService influxdb.VariableService
	}
	type args struct {
		id          platform.ID
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
			name: "authorized to delete variable",
			fields: fields{
				VariableService: &mock.VariableService{
					FindVariableByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Variable, error) {
						return &influxdb.Variable{
							ID:             1,
							OrganizationID: 10,
						}, nil
					},
					DeleteVariableF: func(ctx context.Context, id platform.ID) error {
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
							Type: influxdb.VariablesResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.VariablesResourceType,
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
			name: "unauthorized to delete variable",
			fields: fields{
				VariableService: &mock.VariableService{
					FindVariableByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Variable, error) {
						return &influxdb.Variable{
							ID:             1,
							OrganizationID: 10,
						}, nil
					},
					DeleteVariableF: func(ctx context.Context, id platform.ID) error {
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
							Type: influxdb.VariablesResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/variables/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewVariableService(tt.fields.VariableService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			err := s.DeleteVariable(ctx, tt.args.id)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestVariableService_CreateVariable(t *testing.T) {
	type fields struct {
		VariableService influxdb.VariableService
	}
	type args struct {
		permission influxdb.Permission
		orgID      platform.ID
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
			name: "authorized to create variable",
			fields: fields{
				VariableService: &mock.VariableService{
					CreateVariableF: func(ctx context.Context, o *influxdb.Variable) error {
						return nil
					},
				},
			},
			args: args{
				orgID: 10,
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type:  influxdb.VariablesResourceType,
						OrgID: influxdbtesting.IDPtr(10),
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to create variable",
			fields: fields{
				VariableService: &mock.VariableService{
					CreateVariableF: func(ctx context.Context, o *influxdb.Variable) error {
						return nil
					},
				},
			},
			args: args{
				orgID: 10,
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type: influxdb.VariablesResourceType,
						ID:   influxdbtesting.IDPtr(1),
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/variables is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewVariableService(tt.fields.VariableService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			err := s.CreateVariable(ctx, &influxdb.Variable{OrganizationID: tt.args.orgID})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}
