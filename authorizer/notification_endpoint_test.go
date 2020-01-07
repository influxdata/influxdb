package authorizer_test

import (
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/influxdata/influxdb/endpoints"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/authorizer"
	influxdbcontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/notification/endpoint"
	influxdbtesting "github.com/influxdata/influxdb/testing"
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
			name: "authorized to access id with org",
			fields: fields{
				NotificationEndpointService: &mock.NotificationEndpointService{
					FindByIDF: func(ctx context.Context, id influxdb.ID) (influxdb.NotificationEndpoint, error) {
						orgID := influxdb.ID(10)
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
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.OrgsResourceType,
						ID:   influxdbtesting.IDPtr(10),
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
					FindByIDF: func(ctx context.Context, id influxdb.ID) (influxdb.NotificationEndpoint, error) {
						orgID := influxdb.ID(10)
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
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.NotificationEndpointResourceType,
						ID:   influxdbtesting.IDPtr(2),
					},
				},
				id: 1,
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "read:orgs/000000000000000a is unauthorized",
					Code: influxdb.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewNotificationEndpointService(tt.fields.NotificationEndpointService, mock.NewUserResourceMappingService(), mock.NewOrganizationService())

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{[]influxdb.Permission{tt.args.permission}})

			_, err := s.FindByID(ctx, tt.args.id)
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
					FindF: func(ctx context.Context, filter influxdb.NotificationEndpointFilter, opt ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, error) {
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
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.OrgsResourceType,
						ID:   influxdbtesting.IDPtr(10),
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
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{[]influxdb.Permission{tt.args.permission}})

			oid := influxdb.ID(10)
			edps, err := s.Find(ctx, influxdb.NotificationEndpointFilter{OrgID: &oid})
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
			name: "authorized to update notificationEndpoint with org owner",
			fields: fields{
				NotificationEndpointService: &mock.NotificationEndpointService{
					FindByIDF: func(ctc context.Context, id influxdb.ID) (influxdb.NotificationEndpoint, error) {
						return &endpoint.Slack{
							Base: endpoint.Base{
								ID:    idPtr(1),
								OrgID: idPtr(10),
							},
						}, nil
					},
					UpdateF: func(ctx context.Context, upd influxdb.EndpointUpdate) (influxdb.NotificationEndpoint, error) {
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
						Action: "write",
						Resource: influxdb.Resource{
							Type: influxdb.OrgsResourceType,
							ID:   influxdbtesting.IDPtr(10),
						},
					},
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.OrgsResourceType,
							ID:   influxdbtesting.IDPtr(10),
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
					FindByIDF: func(ctc context.Context, id influxdb.ID) (influxdb.NotificationEndpoint, error) {
						return &endpoint.Slack{
							Base: endpoint.Base{
								ID:    idPtr(1),
								OrgID: idPtr(10),
							},
						}, nil
					},
					UpdateF: func(ctx context.Context, update influxdb.EndpointUpdate) (influxdb.NotificationEndpoint, error) {
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
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.OrgsResourceType,
							ID:   influxdbtesting.IDPtr(10),
						},
					},
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "write:orgs/000000000000000a is unauthorized",
					Code: influxdb.EUnauthorized,
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
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{tt.args.permissions})

			e := &endpoint.Slack{}
			e.SetID(1)
			_, err := s.Update(ctx, endpoints.UpdateEndpoint(e))
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestNotificationEndpointService_DeleteNotificationEndpoint(t *testing.T) {
	type fields struct {
		NotificationEndpointService influxdb.NotificationEndpointService
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
			name: "authorized to delete notificationEndpoint",
			fields: fields{
				NotificationEndpointService: &mock.NotificationEndpointService{
					FindByIDF: func(ctc context.Context, id influxdb.ID) (influxdb.NotificationEndpoint, error) {
						return &endpoint.Slack{
							Base: endpoint.Base{
								ID:    idPtr(1),
								OrgID: idPtr(10),
							},
						}, nil
					},
					DeleteF: func(ctx context.Context, id influxdb.ID) error {
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
							Type: influxdb.OrgsResourceType,
							ID:   influxdbtesting.IDPtr(10),
						},
					},
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.OrgsResourceType,
							ID:   influxdbtesting.IDPtr(10),
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
					FindByIDF: func(ctc context.Context, id influxdb.ID) (influxdb.NotificationEndpoint, error) {
						return &endpoint.Slack{
							Base: endpoint.Base{
								ID:    idPtr(1),
								OrgID: idPtr(10),
							},
						}, nil
					},
					DeleteF: func(ctx context.Context, id influxdb.ID) error {
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
							Type: influxdb.OrgsResourceType,
							ID:   influxdbtesting.IDPtr(10),
						},
					},
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "write:orgs/000000000000000a is unauthorized",
					Code: influxdb.EUnauthorized,
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
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{tt.args.permissions})

			err := s.Delete(ctx, tt.args.id)
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
			name: "authorized to create notificationEndpoint",
			fields: fields{
				NotificationEndpointService: mock.NewNotificationEndpointService(),
			},
			args: args{
				orgID: 10,
				permission: influxdb.Permission{
					Action: "write",
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
				NotificationEndpointService: mock.NewNotificationEndpointService(),
			},
			args: args{
				orgID: 10,
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type: influxdb.OrgsResourceType,
						ID:   influxdbtesting.IDPtr(10),
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
				NotificationEndpointService: mock.NewNotificationEndpointService(),
			},
			args: args{
				orgID: 10,
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type: influxdb.NotificationEndpointResourceType,
						ID:   influxdbtesting.IDPtr(1),
					},
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "write:orgs/000000000000000a/notificationEndpoints is unauthorized",
					Code: influxdb.EUnauthorized,
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
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{[]influxdb.Permission{tt.args.permission}})

			err := s.Create(ctx, influxdb.ID(1), &endpoint.Slack{
				Base: endpoint.Base{
					OrgID: idPtr(tt.args.orgID)},
			})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func idPtr(id influxdb.ID) *influxdb.ID {
	return &id
}
