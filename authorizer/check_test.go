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
	"github.com/influxdata/influxdb/v2/notification/check"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
)

var checkCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []influxdb.Check) []influxdb.Check {
		out := append([]influxdb.Check(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].GetID() > out[j].GetID()
		})
		return out
	}),
}

func TestCheckService_FindCheckByID(t *testing.T) {
	type fields struct {
		CheckService influxdb.CheckService
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
				CheckService: &mock.CheckService{
					FindCheckByIDFn: func(ctx context.Context, id platform.ID) (influxdb.Check, error) {
						return &check.Deadman{
							Base: check.Base{
								ID:    id,
								OrgID: 10,
							},
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						Type:  influxdb.ChecksResourceType,
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
				CheckService: &mock.CheckService{
					FindCheckByIDFn: func(ctx context.Context, id platform.ID) (influxdb.Check, error) {
						return &check.Deadman{
							Base: check.Base{
								ID:    id,
								OrgID: 10,
							},
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						Type:  influxdb.ChecksResourceType,
						OrgID: influxdbtesting.IDPtr(2),
					},
				},
				id: 1,
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "read:orgs/000000000000000a/checks/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewCheckService(tt.fields.CheckService, mock.NewUserResourceMappingService(), mock.NewOrganizationService())

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			_, err := s.FindCheckByID(ctx, tt.args.id)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestCheckService_FindChecks(t *testing.T) {
	type fields struct {
		CheckService influxdb.CheckService
	}
	type args struct {
		permission influxdb.Permission
	}
	type wants struct {
		err    error
		checks []influxdb.Check
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to see all checks",
			fields: fields{
				CheckService: &mock.CheckService{
					FindChecksFn: func(ctx context.Context, filter influxdb.CheckFilter, opt ...influxdb.FindOptions) ([]influxdb.Check, int, error) {
						return []influxdb.Check{
							&check.Deadman{
								Base: check.Base{
									ID:    1,
									OrgID: 10,
								},
							},
							&check.Deadman{
								Base: check.Base{
									ID:    2,
									OrgID: 10,
								},
							},
							&check.Threshold{
								Base: check.Base{
									ID:    3,
									OrgID: 11,
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
						Type: influxdb.ChecksResourceType,
					},
				},
			},
			wants: wants{
				checks: []influxdb.Check{
					&check.Deadman{
						Base: check.Base{
							ID:    1,
							OrgID: 10,
						},
					},
					&check.Deadman{
						Base: check.Base{
							ID:    2,
							OrgID: 10,
						},
					},
					&check.Threshold{
						Base: check.Base{
							ID:    3,
							OrgID: 11,
						},
					},
				},
			},
		},
		{
			name: "authorized to access a single orgs checks",
			fields: fields{
				CheckService: &mock.CheckService{
					FindChecksFn: func(ctx context.Context, filter influxdb.CheckFilter, opt ...influxdb.FindOptions) ([]influxdb.Check, int, error) {
						return []influxdb.Check{
							&check.Deadman{
								Base: check.Base{
									ID:    1,
									OrgID: 10,
								},
							},
							&check.Deadman{
								Base: check.Base{
									ID:    2,
									OrgID: 10,
								},
							},
							&check.Threshold{
								Base: check.Base{
									ID:    3,
									OrgID: 11,
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
						Type:  influxdb.ChecksResourceType,
						OrgID: influxdbtesting.IDPtr(10),
					},
				},
			},
			wants: wants{
				checks: []influxdb.Check{
					&check.Deadman{
						Base: check.Base{
							ID:    1,
							OrgID: 10,
						},
					},
					&check.Deadman{
						Base: check.Base{
							ID:    2,
							OrgID: 10,
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewCheckService(tt.fields.CheckService, mock.NewUserResourceMappingService(), mock.NewOrganizationService())

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			ts, _, err := s.FindChecks(ctx, influxdb.CheckFilter{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)

			if diff := cmp.Diff(ts, tt.wants.checks, checkCmpOptions...); diff != "" {
				t.Errorf("checks are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func TestCheckService_UpdateCheck(t *testing.T) {
	type fields struct {
		CheckService influxdb.CheckService
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
			name: "authorized to update check",
			fields: fields{
				CheckService: &mock.CheckService{
					FindCheckByIDFn: func(ctx context.Context, id platform.ID) (influxdb.Check, error) {
						return &check.Deadman{
							Base: check.Base{
								ID:    1,
								OrgID: 10,
							},
						}, nil
					},
					UpdateCheckFn: func(ctx context.Context, id platform.ID, upd influxdb.CheckCreate) (influxdb.Check, error) {
						return &check.Deadman{
							Base: check.Base{
								ID:    1,
								OrgID: 10,
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
							Type:  influxdb.ChecksResourceType,
							OrgID: influxdbtesting.IDPtr(10),
						},
					},
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type:  influxdb.ChecksResourceType,
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
			name: "unauthorized to update check",
			fields: fields{
				CheckService: &mock.CheckService{
					FindCheckByIDFn: func(ctx context.Context, id platform.ID) (influxdb.Check, error) {
						return &check.Deadman{
							Base: check.Base{
								ID:    1,
								OrgID: 10,
							},
						}, nil
					},
					UpdateCheckFn: func(ctx context.Context, id platform.ID, upd influxdb.CheckCreate) (influxdb.Check, error) {
						return &check.Deadman{
							Base: check.Base{
								ID:    1,
								OrgID: 10,
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
							Type:  influxdb.ChecksResourceType,
							OrgID: influxdbtesting.IDPtr(10),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/checks/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewCheckService(tt.fields.CheckService, mock.NewUserResourceMappingService(), mock.NewOrganizationService())

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			cc := influxdb.CheckCreate{
				Check:  &check.Deadman{},
				Status: influxdb.Active,
			}

			_, err := s.UpdateCheck(ctx, tt.args.id, cc)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestCheckService_PatchCheck(t *testing.T) {
	type fields struct {
		CheckService influxdb.CheckService
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
			name: "authorized to patch check",
			fields: fields{
				CheckService: &mock.CheckService{
					FindCheckByIDFn: func(ctx context.Context, id platform.ID) (influxdb.Check, error) {
						return &check.Deadman{
							Base: check.Base{
								ID:    1,
								OrgID: 10,
							},
						}, nil
					},
					PatchCheckFn: func(ctx context.Context, id platform.ID, upd influxdb.CheckUpdate) (influxdb.Check, error) {
						return &check.Deadman{
							Base: check.Base{
								ID:    1,
								OrgID: 10,
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
							Type:  influxdb.ChecksResourceType,
							OrgID: influxdbtesting.IDPtr(10),
						},
					},
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type:  influxdb.ChecksResourceType,
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
			name: "unauthorized to patch check",
			fields: fields{
				CheckService: &mock.CheckService{
					FindCheckByIDFn: func(ctx context.Context, id platform.ID) (influxdb.Check, error) {
						return &check.Deadman{
							Base: check.Base{
								ID:    1,
								OrgID: 10,
							},
						}, nil
					},
					PatchCheckFn: func(ctx context.Context, id platform.ID, upd influxdb.CheckUpdate) (influxdb.Check, error) {
						return &check.Deadman{
							Base: check.Base{
								ID:    1,
								OrgID: 10,
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
							Type:  influxdb.ChecksResourceType,
							OrgID: influxdbtesting.IDPtr(10),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/checks/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewCheckService(tt.fields.CheckService, mock.NewUserResourceMappingService(), mock.NewOrganizationService())

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			_, err := s.PatchCheck(ctx, tt.args.id, influxdb.CheckUpdate{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestCheckService_DeleteCheck(t *testing.T) {
	type fields struct {
		CheckService influxdb.CheckService
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
			name: "authorized to delete check",
			fields: fields{
				CheckService: &mock.CheckService{
					FindCheckByIDFn: func(ctx context.Context, id platform.ID) (influxdb.Check, error) {
						return &check.Deadman{
							Base: check.Base{
								ID:    1,
								OrgID: 10,
							},
						}, nil
					},
					DeleteCheckFn: func(ctx context.Context, id platform.ID) error {
						return nil
					},
				},
			},
			args: args{
				id: 1,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type:  influxdb.ChecksResourceType,
							OrgID: influxdbtesting.IDPtr(10),
						},
					},
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type:  influxdb.ChecksResourceType,
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
			name: "unauthorized to delete check",
			fields: fields{
				CheckService: &mock.CheckService{
					FindCheckByIDFn: func(ctx context.Context, id platform.ID) (influxdb.Check, error) {
						return &check.Deadman{
							Base: check.Base{
								ID:    1,
								OrgID: 10,
							},
						}, nil
					},
					DeleteCheckFn: func(ctx context.Context, id platform.ID) error {
						return nil
					},
				},
			},
			args: args{
				id: 1,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type:  influxdb.ChecksResourceType,
							OrgID: influxdbtesting.IDPtr(10),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/checks/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewCheckService(tt.fields.CheckService, mock.NewUserResourceMappingService(), mock.NewOrganizationService())

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			err := s.DeleteCheck(ctx, tt.args.id)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestCheckService_CreateCheck(t *testing.T) {
	type fields struct {
		CheckService influxdb.CheckService
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
			name: "authorized to create check with org owner",
			fields: fields{
				CheckService: &mock.CheckService{
					CreateCheckFn: func(ctx context.Context, chk influxdb.CheckCreate, userID platform.ID) error {
						return nil
					},
				},
			},
			args: args{
				orgID: 10,
				permission: influxdb.Permission{
					Action: influxdb.WriteAction,
					Resource: influxdb.Resource{
						Type:  influxdb.ChecksResourceType,
						OrgID: influxdbtesting.IDPtr(10),
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to create check",
			fields: fields{
				CheckService: &mock.CheckService{
					CreateCheckFn: func(ctx context.Context, chk influxdb.CheckCreate, userID platform.ID) error {
						return nil
					},
				},
			},
			args: args{
				orgID: 10,
				permission: influxdb.Permission{
					Action: influxdb.WriteAction,
					Resource: influxdb.Resource{
						Type:  influxdb.ChecksResourceType,
						OrgID: influxdbtesting.IDPtr(1),
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/checks is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewCheckService(tt.fields.CheckService, mock.NewUserResourceMappingService(), mock.NewOrganizationService())

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			c := &check.Deadman{
				Base: check.Base{
					OrgID: tt.args.orgID},
			}

			cc := influxdb.CheckCreate{
				Check:  c,
				Status: influxdb.Active,
			}

			err := s.CreateCheck(ctx, cc, 3)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}
