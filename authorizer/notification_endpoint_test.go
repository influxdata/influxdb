package authorizer_test

import (
	"bytes"
	"context"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
	influxdbcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/notification/endpoint"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
)

var notificationEndpointCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []influxdb.NotificationEndpoint) []influxdb.NotificationEndpoint {
		out := append([]influxdb.NotificationEndpoint(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].GetID().String() > out[j].GetID().String()
		})
		return out
	}),
}

func TestNotificationEndpointService_FindNotificationEndpointByID(t *testing.T) {
	type fields struct {
		NotificationEndpointService influxdb.NotificationEndpointService
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
			name: "authorized to access id with org",
			fields: fields{
				NotificationEndpointService: &mock.NotificationEndpointService{
					FindNotificationEndpointByIDF: func(ctx context.Context, id platform.ID) (influxdb.NotificationEndpoint, error) {
						orgID := platform.ID(10)
						return &endpoint.Slack{
							Base: endpoint.Base{
								ID:    &id,
								OrgID: &orgID,
							},
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						Type:  influxdb.NotificationEndpointResourceType,
						OrgID: influxdbtesting.IDPtr(10),
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
				NotificationEndpointService: &mock.NotificationEndpointService{
					FindNotificationEndpointByIDF: func(ctx context.Context, id platform.ID) (influxdb.NotificationEndpoint, error) {
						orgID := platform.ID(10)
						return &endpoint.Slack{
							Base: endpoint.Base{
								ID:    &id,
								OrgID: &orgID,
							},
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						Type: influxdb.NotificationEndpointResourceType,
						ID:   influxdbtesting.IDPtr(2),
					},
				},
				id: 1,
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "read:orgs/000000000000000a/notificationEndpoints/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewNotificationEndpointService(tt.fields.NotificationEndpointService, mock.NewUserResourceMappingService(), mock.NewOrganizationService())

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			_, err := s.FindNotificationEndpointByID(ctx, tt.args.id)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestNotificationEndpointService_FindNotificationEndpoints(t *testing.T) {
	type fields struct {
		NotificationEndpointService influxdb.NotificationEndpointService
	}
	type args struct {
		permission influxdb.Permission
	}
	type wants struct {
		err                   error
		notificationEndpoints []influxdb.NotificationEndpoint
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to access a single orgs notificationEndpoints",
			fields: fields{
				NotificationEndpointService: &mock.NotificationEndpointService{
					FindNotificationEndpointsF: func(ctx context.Context, filter influxdb.NotificationEndpointFilter, opt ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, int, error) {
						return []influxdb.NotificationEndpoint{
							&endpoint.Slack{
								Base: endpoint.Base{
									ID:    idPtr(1),
									OrgID: idPtr(10),
								},
							},
							&endpoint.HTTP{
								Base: endpoint.Base{
									ID:    idPtr(1),
									OrgID: idPtr(10),
								},
							},
						}, 3, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						Type:  influxdb.NotificationEndpointResourceType,
						OrgID: influxdbtesting.IDPtr(10),
					},
				},
			},
			wants: wants{
				notificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						Base: endpoint.Base{
							ID:    idPtr(1),
							OrgID: idPtr(10),
						},
					},
					&endpoint.HTTP{
						Base: endpoint.Base{
							ID:    idPtr(1),
							OrgID: idPtr(10),
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewNotificationEndpointService(tt.fields.NotificationEndpointService,
				mock.NewUserResourceMappingService(),
				mock.NewOrganizationService())

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			oid := platform.ID(10)
			edps, _, err := s.FindNotificationEndpoints(ctx, influxdb.NotificationEndpointFilter{OrgID: &oid})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)

			if diff := cmp.Diff(edps, tt.wants.notificationEndpoints, notificationEndpointCmpOptions...); diff != "" {
				t.Errorf("notificationEndpoints are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func TestNotificationEndpointService_UpdateNotificationEndpoint(t *testing.T) {
	type fields struct {
		NotificationEndpointService influxdb.NotificationEndpointService
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
			name: "authorized to update notificationEndpoint with org owner",
			fields: fields{
				NotificationEndpointService: &mock.NotificationEndpointService{
					FindNotificationEndpointByIDF: func(ctc context.Context, id platform.ID) (influxdb.NotificationEndpoint, error) {
						return &endpoint.Slack{
							Base: endpoint.Base{
								ID:    idPtr(1),
								OrgID: idPtr(10),
							},
						}, nil
					},
					UpdateNotificationEndpointF: func(ctx context.Context, id platform.ID, upd influxdb.NotificationEndpoint, userID platform.ID) (influxdb.NotificationEndpoint, error) {
						return &endpoint.Slack{
							Base: endpoint.Base{
								ID:    idPtr(1),
								OrgID: idPtr(10),
							},
						}, nil
					},
				},
			},
			args: args{
				id: 1,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type:  influxdb.NotificationEndpointResourceType,
							OrgID: influxdbtesting.IDPtr(10),
						},
					},
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type:  influxdb.NotificationEndpointResourceType,
							OrgID: influxdbtesting.IDPtr(10),
						},
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to update notificationEndpoint",
			fields: fields{
				NotificationEndpointService: &mock.NotificationEndpointService{
					FindNotificationEndpointByIDF: func(ctc context.Context, id platform.ID) (influxdb.NotificationEndpoint, error) {
						return &endpoint.Slack{
							Base: endpoint.Base{
								ID:    idPtr(1),
								OrgID: idPtr(10),
							},
						}, nil
					},
					UpdateNotificationEndpointF: func(ctx context.Context, id platform.ID, upd influxdb.NotificationEndpoint, userID platform.ID) (influxdb.NotificationEndpoint, error) {
						return &endpoint.Slack{
							Base: endpoint.Base{
								ID:    idPtr(1),
								OrgID: idPtr(10),
							},
						}, nil
					},
				},
			},
			args: args{
				id: 1,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type:  influxdb.NotificationEndpointResourceType,
							OrgID: influxdbtesting.IDPtr(10),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/notificationEndpoints/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewNotificationEndpointService(tt.fields.NotificationEndpointService,
				mock.NewUserResourceMappingService(),
				mock.NewOrganizationService())

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			_, err := s.UpdateNotificationEndpoint(ctx, tt.args.id, &endpoint.Slack{}, platform.ID(1))
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestNotificationEndpointService_PatchNotificationEndpoint(t *testing.T) {
	type fields struct {
		NotificationEndpointService influxdb.NotificationEndpointService
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
			name: "authorized to patch notificationEndpoint",
			fields: fields{
				NotificationEndpointService: &mock.NotificationEndpointService{
					FindNotificationEndpointByIDF: func(ctc context.Context, id platform.ID) (influxdb.NotificationEndpoint, error) {
						return &endpoint.Slack{
							Base: endpoint.Base{
								ID:    idPtr(1),
								OrgID: idPtr(10),
							},
						}, nil
					},
					PatchNotificationEndpointF: func(ctx context.Context, id platform.ID, upd influxdb.NotificationEndpointUpdate) (influxdb.NotificationEndpoint, error) {
						return &endpoint.Slack{
							Base: endpoint.Base{
								ID:    idPtr(1),
								OrgID: idPtr(10),
							},
						}, nil
					},
				},
			},
			args: args{
				id: 1,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type:  influxdb.NotificationEndpointResourceType,
							OrgID: influxdbtesting.IDPtr(10),
						},
					},
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type:  influxdb.NotificationEndpointResourceType,
							OrgID: influxdbtesting.IDPtr(10),
						},
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to patch notificationEndpoint",
			fields: fields{
				NotificationEndpointService: &mock.NotificationEndpointService{
					FindNotificationEndpointByIDF: func(ctc context.Context, id platform.ID) (influxdb.NotificationEndpoint, error) {
						return &endpoint.Slack{
							Base: endpoint.Base{
								ID:    idPtr(1),
								OrgID: idPtr(10),
							},
						}, nil
					},
					PatchNotificationEndpointF: func(ctx context.Context, id platform.ID, upd influxdb.NotificationEndpointUpdate) (influxdb.NotificationEndpoint, error) {
						return &endpoint.Slack{
							Base: endpoint.Base{
								ID:    idPtr(1),
								OrgID: idPtr(10),
							},
						}, nil
					},
				},
			},
			args: args{
				id: 1,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type:  influxdb.NotificationEndpointResourceType,
							OrgID: influxdbtesting.IDPtr(10),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/notificationEndpoints/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewNotificationEndpointService(tt.fields.NotificationEndpointService, mock.NewUserResourceMappingService(),
				mock.NewOrganizationService())

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			_, err := s.PatchNotificationEndpoint(ctx, tt.args.id, influxdb.NotificationEndpointUpdate{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestNotificationEndpointService_DeleteNotificationEndpoint(t *testing.T) {
	type fields struct {
		NotificationEndpointService influxdb.NotificationEndpointService
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
			name: "authorized to delete notificationEndpoint",
			fields: fields{
				NotificationEndpointService: &mock.NotificationEndpointService{
					FindNotificationEndpointByIDF: func(ctc context.Context, id platform.ID) (influxdb.NotificationEndpoint, error) {
						return &endpoint.Slack{
							Base: endpoint.Base{
								ID:    idPtr(1),
								OrgID: idPtr(10),
							},
						}, nil
					},
					DeleteNotificationEndpointF: func(ctx context.Context, id platform.ID) ([]influxdb.SecretField, platform.ID, error) {
						return nil, 0, nil
					},
				},
			},
			args: args{
				id: 1,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type:  influxdb.NotificationEndpointResourceType,
							OrgID: influxdbtesting.IDPtr(10),
						},
					},
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type:  influxdb.NotificationEndpointResourceType,
							OrgID: influxdbtesting.IDPtr(10),
						},
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to delete notificationEndpoint",
			fields: fields{
				NotificationEndpointService: &mock.NotificationEndpointService{
					FindNotificationEndpointByIDF: func(ctc context.Context, id platform.ID) (influxdb.NotificationEndpoint, error) {
						return &endpoint.Slack{
							Base: endpoint.Base{
								ID:    idPtr(1),
								OrgID: idPtr(10),
							},
						}, nil
					},
					DeleteNotificationEndpointF: func(ctx context.Context, id platform.ID) ([]influxdb.SecretField, platform.ID, error) {
						return nil, 0, nil
					},
				},
			},
			args: args{
				id: 1,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type:  influxdb.NotificationEndpointResourceType,
							OrgID: influxdbtesting.IDPtr(10),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/notificationEndpoints/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewNotificationEndpointService(tt.fields.NotificationEndpointService, mock.NewUserResourceMappingService(),
				mock.NewOrganizationService(),
			)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			_, _, err := s.DeleteNotificationEndpoint(ctx, tt.args.id)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestNotificationEndpointService_CreateNotificationEndpoint(t *testing.T) {
	type fields struct {
		NotificationEndpointService influxdb.NotificationEndpointService
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
			name: "authorized to create notificationEndpoint",
			fields: fields{
				NotificationEndpointService: &mock.NotificationEndpointService{
					CreateNotificationEndpointF: func(ctx context.Context, tc influxdb.NotificationEndpoint, userID platform.ID) error {
						return nil
					},
				},
			},
			args: args{
				orgID: 10,
				permission: influxdb.Permission{
					Action: influxdb.WriteAction,
					Resource: influxdb.Resource{
						Type:  influxdb.NotificationEndpointResourceType,
						OrgID: influxdbtesting.IDPtr(10),
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "authorized to create notificationEndpoint with org owner",
			fields: fields{
				NotificationEndpointService: &mock.NotificationEndpointService{
					CreateNotificationEndpointF: func(ctx context.Context, tc influxdb.NotificationEndpoint, userID platform.ID) error {
						return nil
					},
				},
			},
			args: args{
				orgID: 10,
				permission: influxdb.Permission{
					Action: influxdb.WriteAction,
					Resource: influxdb.Resource{
						Type:  influxdb.NotificationEndpointResourceType,
						OrgID: influxdbtesting.IDPtr(10),
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to create notificationEndpoint",
			fields: fields{
				NotificationEndpointService: &mock.NotificationEndpointService{
					CreateNotificationEndpointF: func(ctx context.Context, tc influxdb.NotificationEndpoint, userID platform.ID) error {
						return nil
					},
				},
			},
			args: args{
				orgID: 10,
				permission: influxdb.Permission{
					Action: influxdb.WriteAction,
					Resource: influxdb.Resource{
						Type: influxdb.NotificationEndpointResourceType,
						ID:   influxdbtesting.IDPtr(1),
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/notificationEndpoints is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewNotificationEndpointService(tt.fields.NotificationEndpointService,
				mock.NewUserResourceMappingService(),
				mock.NewOrganizationService(),
			)
			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			err := s.CreateNotificationEndpoint(ctx, &endpoint.Slack{
				Base: endpoint.Base{
					OrgID: idPtr(tt.args.orgID)},
			}, platform.ID(1))
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func idPtr(id platform.ID) *platform.ID {
	return &id
}
