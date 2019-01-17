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

var scraperCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []influxdb.ScraperTarget) []influxdb.ScraperTarget {
		out := append([]influxdb.ScraperTarget(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID.String() > out[j].ID.String()
		})
		return out
	}),
}

func TestScraperTargetStoreService_GetTargetByID(t *testing.T) {
	type fields struct {
		ScraperTargetStoreService influxdb.ScraperTargetStoreService
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
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					GetTargetByIDF: func(ctx context.Context, id influxdb.ID) (*influxdb.ScraperTarget, error) {
						return &influxdb.ScraperTarget{
							ID:    id,
							OrgID: 10,
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.ScraperResourceType,
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
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					GetTargetByIDF: func(ctx context.Context, id influxdb.ID) (*influxdb.ScraperTarget, error) {
						return &influxdb.ScraperTarget{
							ID:    id,
							OrgID: 10,
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.ScraperResourceType,
						ID:   influxdbtesting.IDPtr(2),
					},
				},
				id: 1,
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "read:orgs/000000000000000a/scrapers/0000000000000001 is unauthorized",
					Code: influxdb.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewScraperTargetStoreService(tt.fields.ScraperTargetStoreService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{[]influxdb.Permission{tt.args.permission}})

			_, err := s.GetTargetByID(ctx, tt.args.id)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestScraperTargetStoreService_ListTargets(t *testing.T) {
	type fields struct {
		ScraperTargetStoreService influxdb.ScraperTargetStoreService
	}
	type args struct {
		permission influxdb.Permission
	}
	type wants struct {
		err      error
		scrapers []influxdb.ScraperTarget
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to see all scrapers",
			fields: fields{
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					ListTargetsF: func(ctx context.Context) ([]influxdb.ScraperTarget, error) {
						return []influxdb.ScraperTarget{
							{
								ID:    1,
								OrgID: 10,
							},
							{
								ID:    2,
								OrgID: 10,
							},
							{
								ID:    3,
								OrgID: 11,
							},
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.ScraperResourceType,
					},
				},
			},
			wants: wants{
				scrapers: []influxdb.ScraperTarget{
					{
						ID:    1,
						OrgID: 10,
					},
					{
						ID:    2,
						OrgID: 10,
					},
					{
						ID:    3,
						OrgID: 11,
					},
				},
			},
		},
		{
			name: "authorized to access a single orgs scrapers",
			fields: fields{
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					ListTargetsF: func(ctx context.Context) ([]influxdb.ScraperTarget, error) {
						return []influxdb.ScraperTarget{
							{
								ID:    1,
								OrgID: 10,
							},
							{
								ID:    2,
								OrgID: 10,
							},
							{
								ID:    3,
								OrgID: 11,
							},
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type:  influxdb.ScraperResourceType,
						OrgID: influxdbtesting.IDPtr(10),
					},
				},
			},
			wants: wants{
				scrapers: []influxdb.ScraperTarget{
					{
						ID:    1,
						OrgID: 10,
					},
					{
						ID:    2,
						OrgID: 10,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewScraperTargetStoreService(tt.fields.ScraperTargetStoreService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{[]influxdb.Permission{tt.args.permission}})

			ts, err := s.ListTargets(ctx)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)

			if diff := cmp.Diff(ts, tt.wants.scrapers, scraperCmpOptions...); diff != "" {
				t.Errorf("scrapers are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func TestScraperTargetStoreService_UpdateTarget(t *testing.T) {
	type fields struct {
		ScraperTargetStoreService influxdb.ScraperTargetStoreService
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
			name: "authorized to update scraper",
			fields: fields{
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					GetTargetByIDF: func(ctc context.Context, id influxdb.ID) (*influxdb.ScraperTarget, error) {
						return &influxdb.ScraperTarget{
							ID:    1,
							OrgID: 10,
						}, nil
					},
					UpdateTargetF: func(ctx context.Context, upd *influxdb.ScraperTarget) (*influxdb.ScraperTarget, error) {
						return &influxdb.ScraperTarget{
							ID:    1,
							OrgID: 10,
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
							Type: influxdb.ScraperResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.ScraperResourceType,
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
			name: "unauthorized to update scraper",
			fields: fields{
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					GetTargetByIDF: func(ctc context.Context, id influxdb.ID) (*influxdb.ScraperTarget, error) {
						return &influxdb.ScraperTarget{
							ID:    1,
							OrgID: 10,
						}, nil
					},
					UpdateTargetF: func(ctx context.Context, upd *influxdb.ScraperTarget) (*influxdb.ScraperTarget, error) {
						return &influxdb.ScraperTarget{
							ID:    1,
							OrgID: 10,
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
							Type: influxdb.ScraperResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "write:orgs/000000000000000a/scrapers/0000000000000001 is unauthorized",
					Code: influxdb.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewScraperTargetStoreService(tt.fields.ScraperTargetStoreService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{tt.args.permissions})

			_, err := s.UpdateTarget(ctx, &influxdb.ScraperTarget{ID: tt.args.id})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestScraperTargetStoreService_RemoveTarget(t *testing.T) {
	type fields struct {
		ScraperTargetStoreService influxdb.ScraperTargetStoreService
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
			name: "authorized to delete scraper",
			fields: fields{
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					GetTargetByIDF: func(ctc context.Context, id influxdb.ID) (*influxdb.ScraperTarget, error) {
						return &influxdb.ScraperTarget{
							ID:    1,
							OrgID: 10,
						}, nil
					},
					RemoveTargetF: func(ctx context.Context, id influxdb.ID) error {
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
							Type: influxdb.ScraperResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
					{
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.ScraperResourceType,
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
			name: "unauthorized to delete scraper",
			fields: fields{
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					GetTargetByIDF: func(ctc context.Context, id influxdb.ID) (*influxdb.ScraperTarget, error) {
						return &influxdb.ScraperTarget{
							ID:    1,
							OrgID: 10,
						}, nil
					},
					RemoveTargetF: func(ctx context.Context, id influxdb.ID) error {
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
							Type: influxdb.ScraperResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "write:orgs/000000000000000a/scrapers/0000000000000001 is unauthorized",
					Code: influxdb.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewScraperTargetStoreService(tt.fields.ScraperTargetStoreService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{tt.args.permissions})

			err := s.RemoveTarget(ctx, tt.args.id)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestScraperTargetStoreService_AddTarget(t *testing.T) {
	type fields struct {
		ScraperTargetStoreService influxdb.ScraperTargetStoreService
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
			name: "authorized to create scraper",
			fields: fields{
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					AddTargetF: func(ctx context.Context, st *influxdb.ScraperTarget) error {
						return nil
					},
				},
			},
			args: args{
				orgID: 10,
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type:  influxdb.ScraperResourceType,
						OrgID: influxdbtesting.IDPtr(10),
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to create scraper",
			fields: fields{
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					AddTargetF: func(ctx context.Context, st *influxdb.ScraperTarget) error {
						return nil
					},
				},
			},
			args: args{
				orgID: 10,
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type: influxdb.ScraperResourceType,
						ID:   influxdbtesting.IDPtr(1),
					},
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "write:orgs/000000000000000a/scrapers is unauthorized",
					Code: influxdb.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewScraperTargetStoreService(tt.fields.ScraperTargetStoreService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{[]influxdb.Permission{tt.args.permission}})

			err := s.AddTarget(ctx, &influxdb.ScraperTarget{OrgID: tt.args.orgID})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}
