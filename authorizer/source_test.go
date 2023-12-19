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

var sourceCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*influxdb.Source) []*influxdb.Source {
		out := append([]*influxdb.Source(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID.String() > out[j].ID.String()
		})
		return out
	}),
}

func TestSourceService_DefaultSource(t *testing.T) {
	type fields struct {
		SourceService influxdb.SourceService
	}
	type args struct {
		permission influxdb.Permission
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
				SourceService: &mock.SourceService{
					DefaultSourceFn: func(ctx context.Context) (*influxdb.Source, error) {
						return &influxdb.Source{
							ID:             1,
							OrganizationID: 10,
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.SourcesResourceType,
						ID:   influxdbtesting.IDPtr(1),
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to access id",
			fields: fields{
				SourceService: &mock.SourceService{
					DefaultSourceFn: func(ctx context.Context) (*influxdb.Source, error) {
						return &influxdb.Source{
							ID:             1,
							OrganizationID: 10,
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.SourcesResourceType,
						ID:   influxdbtesting.IDPtr(2),
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "read:orgs/000000000000000a/sources/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewSourceService(tt.fields.SourceService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			_, err := s.DefaultSource(ctx)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestSourceService_FindSourceByID(t *testing.T) {
	type fields struct {
		SourceService influxdb.SourceService
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
				SourceService: &mock.SourceService{
					FindSourceByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Source, error) {
						return &influxdb.Source{
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
						Type: influxdb.SourcesResourceType,
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
				SourceService: &mock.SourceService{
					FindSourceByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Source, error) {
						return &influxdb.Source{
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
						Type: influxdb.SourcesResourceType,
						ID:   influxdbtesting.IDPtr(2),
					},
				},
				id: 1,
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "read:orgs/000000000000000a/sources/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewSourceService(tt.fields.SourceService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			_, err := s.FindSourceByID(ctx, tt.args.id)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestSourceService_FindSources(t *testing.T) {
	type fields struct {
		SourceService influxdb.SourceService
	}
	type args struct {
		permission influxdb.Permission
	}
	type wants struct {
		err     error
		sources []*influxdb.Source
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to see all sources",
			fields: fields{
				SourceService: &mock.SourceService{
					FindSourcesFn: func(ctx context.Context, opts influxdb.FindOptions) ([]*influxdb.Source, int, error) {
						return []*influxdb.Source{
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
						}, 3, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.SourcesResourceType,
					},
				},
			},
			wants: wants{
				sources: []*influxdb.Source{
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
			name: "authorized to access a single org sources",
			fields: fields{
				SourceService: &mock.SourceService{
					FindSourcesFn: func(ctx context.Context, opts influxdb.FindOptions) ([]*influxdb.Source, int, error) {
						return []*influxdb.Source{
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
						}, 3, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type:  influxdb.SourcesResourceType,
						OrgID: influxdbtesting.IDPtr(10),
					},
				},
			},
			wants: wants{
				sources: []*influxdb.Source{
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
			s := authorizer.NewSourceService(tt.fields.SourceService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			sources, _, err := s.FindSources(ctx, influxdb.DefaultSourceFindOptions)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)

			if diff := cmp.Diff(sources, tt.wants.sources, sourceCmpOptions...); diff != "" {
				t.Errorf("sources are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func TestSourceService_UpdateSource(t *testing.T) {
	type fields struct {
		SourceService influxdb.SourceService
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
			name: "authorized to update source",
			fields: fields{
				SourceService: &mock.SourceService{
					FindSourceByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Source, error) {
						return &influxdb.Source{
							ID:             1,
							OrganizationID: 10,
						}, nil
					},
					UpdateSourceFn: func(ctx context.Context, id platform.ID, upd influxdb.SourceUpdate) (*influxdb.Source, error) {
						return &influxdb.Source{
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
							Type: influxdb.SourcesResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.SourcesResourceType,
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
			name: "unauthorized to update source",
			fields: fields{
				SourceService: &mock.SourceService{
					FindSourceByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Source, error) {
						return &influxdb.Source{
							ID:             1,
							OrganizationID: 10,
						}, nil
					},
					UpdateSourceFn: func(ctx context.Context, id platform.ID, upd influxdb.SourceUpdate) (*influxdb.Source, error) {
						return &influxdb.Source{
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
							Type: influxdb.SourcesResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/sources/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewSourceService(tt.fields.SourceService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			_, err := s.UpdateSource(ctx, tt.args.id, influxdb.SourceUpdate{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestSourceService_DeleteSource(t *testing.T) {
	type fields struct {
		SourceService influxdb.SourceService
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
			name: "authorized to delete source",
			fields: fields{
				SourceService: &mock.SourceService{
					FindSourceByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Source, error) {
						return &influxdb.Source{
							ID:             1,
							OrganizationID: 10,
						}, nil
					},
					DeleteSourceFn: func(ctx context.Context, id platform.ID) error {
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
							Type: influxdb.SourcesResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.SourcesResourceType,
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
			name: "unauthorized to delete source",
			fields: fields{
				SourceService: &mock.SourceService{
					FindSourceByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Source, error) {
						return &influxdb.Source{
							ID:             1,
							OrganizationID: 10,
						}, nil
					},
					DeleteSourceFn: func(ctx context.Context, id platform.ID) error {
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
							Type: influxdb.SourcesResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/sources/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewSourceService(tt.fields.SourceService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			err := s.DeleteSource(ctx, tt.args.id)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestSourceService_CreateSource(t *testing.T) {
	type fields struct {
		SourceService influxdb.SourceService
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
			name: "authorized to create Source",
			fields: fields{
				SourceService: &mock.SourceService{
					CreateSourceFn: func(ctx context.Context, o *influxdb.Source) error {
						return nil
					},
				},
			},
			args: args{
				orgID: 10,
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type:  influxdb.SourcesResourceType,
						OrgID: influxdbtesting.IDPtr(10),
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to create Source",
			fields: fields{
				SourceService: &mock.SourceService{
					CreateSourceFn: func(ctx context.Context, o *influxdb.Source) error {
						return nil
					},
				},
			},
			args: args{
				orgID: 10,
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type: influxdb.SourcesResourceType,
						ID:   influxdbtesting.IDPtr(1),
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/sources is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewSourceService(tt.fields.SourceService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			err := s.CreateSource(ctx, &influxdb.Source{OrganizationID: tt.args.orgID})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}
