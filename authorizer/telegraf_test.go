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

var telegrafCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*influxdb.TelegrafConfig) []*influxdb.TelegrafConfig {
		out := append([]*influxdb.TelegrafConfig(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID.String() > out[j].ID.String()
		})
		return out
	}),
}

func TestTelegrafConfigStore_FindTelegrafConfigByID(t *testing.T) {
	type fields struct {
		TelegrafConfigStore influxdb.TelegrafConfigStore
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
				TelegrafConfigStore: &mock.TelegrafConfigStore{
					FindTelegrafConfigByIDF: func(ctx context.Context, id influxdb.ID) (*influxdb.TelegrafConfig, error) {
						return &influxdb.TelegrafConfig{
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
						Type: influxdb.TelegrafsResourceType,
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
				TelegrafConfigStore: &mock.TelegrafConfigStore{
					FindTelegrafConfigByIDF: func(ctx context.Context, id influxdb.ID) (*influxdb.TelegrafConfig, error) {
						return &influxdb.TelegrafConfig{
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
						Type: influxdb.TelegrafsResourceType,
						ID:   influxdbtesting.IDPtr(2),
					},
				},
				id: 1,
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "read:orgs/000000000000000a/telegrafs/0000000000000001 is unauthorized",
					Code: influxdb.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewTelegrafConfigService(tt.fields.TelegrafConfigStore, mock.NewUserResourceMappingService())

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{[]influxdb.Permission{tt.args.permission}})

			_, err := s.FindTelegrafConfigByID(ctx, tt.args.id)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestTelegrafConfigStore_FindTelegrafConfig(t *testing.T) {
	type fields struct {
		TelegrafConfigStore influxdb.TelegrafConfigStore
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
			name: "authorized to access telegraf",
			fields: fields{
				TelegrafConfigStore: &mock.TelegrafConfigStore{
					FindTelegrafConfigF: func(ctx context.Context, filter influxdb.TelegrafConfigFilter) (*influxdb.TelegrafConfig, error) {
						return &influxdb.TelegrafConfig{
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
						Type: influxdb.TelegrafsResourceType,
						ID:   influxdbtesting.IDPtr(1),
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to access telegraf",
			fields: fields{
				TelegrafConfigStore: &mock.TelegrafConfigStore{
					FindTelegrafConfigF: func(ctx context.Context, filter influxdb.TelegrafConfigFilter) (*influxdb.TelegrafConfig, error) {
						return &influxdb.TelegrafConfig{
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
						Type: influxdb.TelegrafsResourceType,
						ID:   influxdbtesting.IDPtr(2),
					},
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "read:orgs/000000000000000a/telegrafs/0000000000000001 is unauthorized",
					Code: influxdb.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewTelegrafConfigService(tt.fields.TelegrafConfigStore, mock.NewUserResourceMappingService())

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{[]influxdb.Permission{tt.args.permission}})

			_, err := s.FindTelegrafConfig(ctx, influxdb.TelegrafConfigFilter{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestTelegrafConfigStore_FindTelegrafConfigs(t *testing.T) {
	type fields struct {
		TelegrafConfigStore influxdb.TelegrafConfigStore
	}
	type args struct {
		permission influxdb.Permission
	}
	type wants struct {
		err       error
		telegrafs []*influxdb.TelegrafConfig
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to see all telegrafs",
			fields: fields{
				TelegrafConfigStore: &mock.TelegrafConfigStore{
					FindTelegrafConfigsF: func(ctx context.Context, filter influxdb.TelegrafConfigFilter, opt ...influxdb.FindOptions) ([]*influxdb.TelegrafConfig, int, error) {
						return []*influxdb.TelegrafConfig{
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
						Type: influxdb.TelegrafsResourceType,
					},
				},
			},
			wants: wants{
				telegrafs: []*influxdb.TelegrafConfig{
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
			name: "authorized to access a single orgs telegrafs",
			fields: fields{
				TelegrafConfigStore: &mock.TelegrafConfigStore{
					FindTelegrafConfigsF: func(ctx context.Context, filter influxdb.TelegrafConfigFilter, opt ...influxdb.FindOptions) ([]*influxdb.TelegrafConfig, int, error) {
						return []*influxdb.TelegrafConfig{
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
						Type:  influxdb.TelegrafsResourceType,
						OrgID: influxdbtesting.IDPtr(10),
					},
				},
			},
			wants: wants{
				telegrafs: []*influxdb.TelegrafConfig{
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
			s := authorizer.NewTelegrafConfigService(tt.fields.TelegrafConfigStore, mock.NewUserResourceMappingService())

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{[]influxdb.Permission{tt.args.permission}})

			ts, _, err := s.FindTelegrafConfigs(ctx, influxdb.TelegrafConfigFilter{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)

			if diff := cmp.Diff(ts, tt.wants.telegrafs, telegrafCmpOptions...); diff != "" {
				t.Errorf("telegrafs are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func TestTelegrafConfigStore_UpdateTelegrafConfig(t *testing.T) {
	type fields struct {
		TelegrafConfigStore influxdb.TelegrafConfigStore
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
			name: "authorized to update telegraf",
			fields: fields{
				TelegrafConfigStore: &mock.TelegrafConfigStore{
					FindTelegrafConfigByIDF: func(ctc context.Context, id influxdb.ID) (*influxdb.TelegrafConfig, error) {
						return &influxdb.TelegrafConfig{
							ID:             1,
							OrganizationID: 10,
						}, nil
					},
					UpdateTelegrafConfigF: func(ctx context.Context, id influxdb.ID, upd *influxdb.TelegrafConfig, userID influxdb.ID) (*influxdb.TelegrafConfig, error) {
						return &influxdb.TelegrafConfig{
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
							Type: influxdb.TelegrafsResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.TelegrafsResourceType,
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
			name: "unauthorized to update telegraf",
			fields: fields{
				TelegrafConfigStore: &mock.TelegrafConfigStore{
					FindTelegrafConfigByIDF: func(ctc context.Context, id influxdb.ID) (*influxdb.TelegrafConfig, error) {
						return &influxdb.TelegrafConfig{
							ID:             1,
							OrganizationID: 10,
						}, nil
					},
					UpdateTelegrafConfigF: func(ctx context.Context, id influxdb.ID, upd *influxdb.TelegrafConfig, userID influxdb.ID) (*influxdb.TelegrafConfig, error) {
						return &influxdb.TelegrafConfig{
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
							Type: influxdb.TelegrafsResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "write:orgs/000000000000000a/telegrafs/0000000000000001 is unauthorized",
					Code: influxdb.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewTelegrafConfigService(tt.fields.TelegrafConfigStore, mock.NewUserResourceMappingService())

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{tt.args.permissions})

			_, err := s.UpdateTelegrafConfig(ctx, tt.args.id, &influxdb.TelegrafConfig{}, influxdb.ID(1))
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestTelegrafConfigStore_DeleteTelegrafConfig(t *testing.T) {
	type fields struct {
		TelegrafConfigStore influxdb.TelegrafConfigStore
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
			name: "authorized to delete telegraf",
			fields: fields{
				TelegrafConfigStore: &mock.TelegrafConfigStore{
					FindTelegrafConfigByIDF: func(ctc context.Context, id influxdb.ID) (*influxdb.TelegrafConfig, error) {
						return &influxdb.TelegrafConfig{
							ID:             1,
							OrganizationID: 10,
						}, nil
					},
					DeleteTelegrafConfigF: func(ctx context.Context, id influxdb.ID) error {
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
							Type: influxdb.TelegrafsResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.TelegrafsResourceType,
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
			name: "unauthorized to delete telegraf",
			fields: fields{
				TelegrafConfigStore: &mock.TelegrafConfigStore{
					FindTelegrafConfigByIDF: func(ctc context.Context, id influxdb.ID) (*influxdb.TelegrafConfig, error) {
						return &influxdb.TelegrafConfig{
							ID:             1,
							OrganizationID: 10,
						}, nil
					},
					DeleteTelegrafConfigF: func(ctx context.Context, id influxdb.ID) error {
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
							Type: influxdb.TelegrafsResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "write:orgs/000000000000000a/telegrafs/0000000000000001 is unauthorized",
					Code: influxdb.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewTelegrafConfigService(tt.fields.TelegrafConfigStore, mock.NewUserResourceMappingService())

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{tt.args.permissions})

			err := s.DeleteTelegrafConfig(ctx, tt.args.id)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestTelegrafConfigStore_CreateTelegrafConfig(t *testing.T) {
	type fields struct {
		TelegrafConfigStore influxdb.TelegrafConfigStore
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
			name: "authorized to create telegraf",
			fields: fields{
				TelegrafConfigStore: &mock.TelegrafConfigStore{
					CreateTelegrafConfigF: func(ctx context.Context, tc *influxdb.TelegrafConfig, userID influxdb.ID) error {
						return nil
					},
				},
			},
			args: args{
				orgID: 10,
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type:  influxdb.TelegrafsResourceType,
						OrgID: influxdbtesting.IDPtr(10),
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to create telegraf",
			fields: fields{
				TelegrafConfigStore: &mock.TelegrafConfigStore{
					CreateTelegrafConfigF: func(ctx context.Context, tc *influxdb.TelegrafConfig, userID influxdb.ID) error {
						return nil
					},
				},
			},
			args: args{
				orgID: 10,
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type: influxdb.TelegrafsResourceType,
						ID:   influxdbtesting.IDPtr(1),
					},
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "write:orgs/000000000000000a/telegrafs is unauthorized",
					Code: influxdb.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewTelegrafConfigService(tt.fields.TelegrafConfigStore, mock.NewUserResourceMappingService())

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{[]influxdb.Permission{tt.args.permission}})

			err := s.CreateTelegrafConfig(ctx, &influxdb.TelegrafConfig{OrganizationID: tt.args.orgID}, influxdb.ID(1))
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}
