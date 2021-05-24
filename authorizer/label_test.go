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
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
)

const (
	orgOneID = "020f755c3c083000"
)

var (
	orgOneInfluxID = influxdbtesting.MustIDBase16(orgOneID)
	orgSvc         = &mock.OrganizationService{
		FindResourceOrganizationIDF: func(_ context.Context, _ influxdb.ResourceType, _ platform.ID) (platform.ID, error) {
			return orgOneInfluxID, nil
		},
	}
)

var labelCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*influxdb.Label) []*influxdb.Label {
		out := append([]*influxdb.Label(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID.String() > out[j].ID.String()
		})
		return out
	}),
}

func TestLabelService_FindLabelByID(t *testing.T) {
	type fields struct {
		LabelService influxdb.LabelService
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
				LabelService: &mock.LabelService{
					FindLabelByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Label, error) {
						return &influxdb.Label{
							ID:    id,
							OrgID: orgOneInfluxID,
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						Type: influxdb.LabelsResourceType,
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
				LabelService: &mock.LabelService{
					FindLabelByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Label, error) {
						return &influxdb.Label{
							ID:    id,
							OrgID: orgOneInfluxID,
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						Type: influxdb.LabelsResourceType,
						ID:   influxdbtesting.IDPtr(2),
					},
				},
				id: 1,
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "read:orgs/020f755c3c083000/labels/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewLabelServiceWithOrg(tt.fields.LabelService, orgSvc)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			_, err := s.FindLabelByID(ctx, tt.args.id)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestLabelService_FindLabels(t *testing.T) {
	type fields struct {
		LabelService influxdb.LabelService
	}
	type args struct {
		permission influxdb.Permission
	}
	type wants struct {
		err    error
		labels []*influxdb.Label
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to see all labels",
			fields: fields{
				LabelService: &mock.LabelService{
					FindLabelsFn: func(ctx context.Context, filter influxdb.LabelFilter) ([]*influxdb.Label, error) {
						return []*influxdb.Label{
							{
								ID:    1,
								OrgID: orgOneInfluxID,
							},
							{
								ID:    2,
								OrgID: orgOneInfluxID,
							},
							{
								ID:    3,
								OrgID: orgOneInfluxID,
							},
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						Type: influxdb.LabelsResourceType,
					},
				},
			},
			wants: wants{
				labels: []*influxdb.Label{
					{
						ID:    1,
						OrgID: orgOneInfluxID,
					},
					{
						ID:    2,
						OrgID: orgOneInfluxID,
					},
					{
						ID:    3,
						OrgID: orgOneInfluxID,
					},
				},
			},
		},
		{
			name: "authorized to access a single label",
			fields: fields{
				LabelService: &mock.LabelService{
					FindLabelsFn: func(ctx context.Context, filter influxdb.LabelFilter) ([]*influxdb.Label, error) {
						return []*influxdb.Label{
							{
								ID:    1,
								OrgID: orgOneInfluxID,
							},
							{
								ID:    2,
								OrgID: orgOneInfluxID,
							},
							{
								ID:    3,
								OrgID: orgOneInfluxID,
							},
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						Type: influxdb.LabelsResourceType,
						ID:   influxdbtesting.IDPtr(1),
					},
				},
			},
			wants: wants{
				labels: []*influxdb.Label{
					{
						ID:    1,
						OrgID: orgOneInfluxID,
					},
				},
			},
		},
		{
			name: "unable to access labels",
			fields: fields{
				LabelService: &mock.LabelService{
					FindLabelsFn: func(ctx context.Context, filter influxdb.LabelFilter) ([]*influxdb.Label, error) {
						return []*influxdb.Label{
							{
								ID:    1,
								OrgID: orgOneInfluxID,
							},
							{
								ID:    2,
								OrgID: orgOneInfluxID,
							},
							{
								ID:    3,
								OrgID: orgOneInfluxID,
							},
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						Type: influxdb.LabelsResourceType,
						ID:   influxdbtesting.IDPtr(10),
					},
				},
			},
			wants: wants{
				// fixme(leodido) > should we return error in this case?
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewLabelServiceWithOrg(tt.fields.LabelService, orgSvc)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			labels, err := s.FindLabels(ctx, influxdb.LabelFilter{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)

			if diff := cmp.Diff(labels, tt.wants.labels, labelCmpOptions...); diff != "" {
				t.Errorf("labels are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func TestLabelService_UpdateLabel(t *testing.T) {
	type fields struct {
		LabelService influxdb.LabelService
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
			name: "authorized to update label",
			fields: fields{
				LabelService: &mock.LabelService{
					FindLabelByIDFn: func(ctc context.Context, id platform.ID) (*influxdb.Label, error) {
						return &influxdb.Label{
							ID:    1,
							OrgID: orgOneInfluxID,
						}, nil
					},
					UpdateLabelFn: func(ctx context.Context, id platform.ID, upd influxdb.LabelUpdate) (*influxdb.Label, error) {
						return &influxdb.Label{
							ID:    1,
							OrgID: orgOneInfluxID,
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
							Type: influxdb.LabelsResourceType,
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
			name: "unauthorized to update label",
			fields: fields{
				LabelService: &mock.LabelService{
					FindLabelByIDFn: func(ctc context.Context, id platform.ID) (*influxdb.Label, error) {
						return &influxdb.Label{
							ID:    1,
							OrgID: orgOneInfluxID,
						}, nil
					},
					UpdateLabelFn: func(ctx context.Context, id platform.ID, upd influxdb.LabelUpdate) (*influxdb.Label, error) {
						return &influxdb.Label{
							ID:    1,
							OrgID: orgOneInfluxID,
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
							Type: influxdb.LabelsResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/020f755c3c083000/labels/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewLabelServiceWithOrg(tt.fields.LabelService, orgSvc)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			_, err := s.UpdateLabel(ctx, tt.args.id, influxdb.LabelUpdate{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestLabelService_DeleteLabel(t *testing.T) {
	type fields struct {
		LabelService influxdb.LabelService
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
			name: "authorized to delete label",
			fields: fields{
				LabelService: &mock.LabelService{
					FindLabelByIDFn: func(ctc context.Context, id platform.ID) (*influxdb.Label, error) {
						return &influxdb.Label{
							ID:    1,
							OrgID: orgOneInfluxID,
						}, nil
					},
					DeleteLabelFn: func(ctx context.Context, id platform.ID) error {
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
							Type:  influxdb.LabelsResourceType,
							ID:    influxdbtesting.IDPtr(1),
							OrgID: influxdbtesting.IDPtr(orgOneInfluxID),
						},
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to delete label",
			fields: fields{
				LabelService: &mock.LabelService{
					FindLabelByIDFn: func(ctc context.Context, id platform.ID) (*influxdb.Label, error) {
						return &influxdb.Label{
							ID:    1,
							OrgID: orgOneInfluxID,
						}, nil
					},
					DeleteLabelFn: func(ctx context.Context, id platform.ID) error {
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
							Type:  influxdb.LabelsResourceType,
							ID:    influxdbtesting.IDPtr(1),
							OrgID: influxdbtesting.IDPtr(orgOneInfluxID),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/020f755c3c083000/labels/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewLabelServiceWithOrg(tt.fields.LabelService, orgSvc)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			err := s.DeleteLabel(ctx, tt.args.id)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestLabelService_CreateLabel(t *testing.T) {
	type fields struct {
		LabelService influxdb.LabelService
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
			name: "unauthorized to create label with read only permission",
			fields: fields{
				LabelService: &mock.LabelService{
					CreateLabelFn: func(ctx context.Context, l *influxdb.Label) error {
						return nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						ID:   influxdbtesting.IDPtr(orgOneInfluxID),
						Type: influxdb.OrgsResourceType,
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/020f755c3c083000/labels is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
		{
			name: "unauthorized to create label with wrong write permission",
			fields: fields{
				LabelService: &mock.LabelService{
					CreateLabelFn: func(ctx context.Context, b *influxdb.Label) error {
						return nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: influxdb.WriteAction,
					Resource: influxdb.Resource{
						Type: influxdb.OrgsResourceType,
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/020f755c3c083000/labels is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},

		{
			name: "authorized to create label",
			fields: fields{
				LabelService: &mock.LabelService{
					CreateLabelFn: func(ctx context.Context, l *influxdb.Label) error {
						return nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: influxdb.WriteAction,
					Resource: influxdb.Resource{
						OrgID: influxdbtesting.IDPtr(orgOneInfluxID),
						Type:  influxdb.LabelsResourceType,
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewLabelServiceWithOrg(tt.fields.LabelService, orgSvc)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			err := s.CreateLabel(ctx, &influxdb.Label{Name: "name", OrgID: orgOneInfluxID})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestLabelService_FindResourceLabels(t *testing.T) {
	type fields struct {
		LabelService influxdb.LabelService
	}
	type args struct {
		filter      influxdb.LabelMappingFilter
		permissions []influxdb.Permission
	}
	type wants struct {
		err    error
		labels []*influxdb.Label
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to see all labels belonging to a resource",
			fields: fields{
				LabelService: &mock.LabelService{
					FindResourceLabelsFn: func(ctx context.Context, f influxdb.LabelMappingFilter) ([]*influxdb.Label, error) {
						return []*influxdb.Label{
							{
								ID:    1,
								OrgID: orgOneInfluxID,
							},
							{
								ID:    2,
								OrgID: orgOneInfluxID,
							},
							{
								ID:    3,
								OrgID: orgOneInfluxID,
							},
						}, nil
					},
				},
			},
			args: args{
				filter: influxdb.LabelMappingFilter{
					ResourceID:   10,
					ResourceType: influxdb.BucketsResourceType,
				},
				permissions: []influxdb.Permission{
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type: influxdb.LabelsResourceType,
						},
					},
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type: influxdb.BucketsResourceType,
							ID:   influxdbtesting.IDPtr(10),
						},
					},
				},
			},
			wants: wants{
				err: nil,
				labels: []*influxdb.Label{
					{
						ID:    1,
						OrgID: orgOneInfluxID,
					},
					{
						ID:    2,
						OrgID: orgOneInfluxID,
					},
					{
						ID:    3,
						OrgID: orgOneInfluxID,
					},
				},
			},
		},
		{
			name: "authorized to access a single label",
			fields: fields{
				LabelService: &mock.LabelService{
					FindResourceLabelsFn: func(ctx context.Context, f influxdb.LabelMappingFilter) ([]*influxdb.Label, error) {
						return []*influxdb.Label{
							{
								ID:    1,
								OrgID: orgOneInfluxID,
							},
							{
								ID:    2,
								OrgID: orgOneInfluxID,
							},
							{
								ID:    3,
								OrgID: orgOneInfluxID,
							},
						}, nil
					},
				},
			},
			args: args{
				filter: influxdb.LabelMappingFilter{
					ResourceID:   10,
					ResourceType: influxdb.BucketsResourceType,
				},
				permissions: []influxdb.Permission{
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type: influxdb.LabelsResourceType,
							ID:   influxdbtesting.IDPtr(3),
						},
					},
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type: influxdb.BucketsResourceType,
							ID:   influxdbtesting.IDPtr(10),
						},
					},
				},
			},
			wants: wants{
				err: nil,
				labels: []*influxdb.Label{
					{
						ID:    3,
						OrgID: orgOneInfluxID,
					},
				},
			},
		},
		{
			name: "unable to access labels when missing read permission on labels",
			fields: fields{
				LabelService: &mock.LabelService{
					FindResourceLabelsFn: func(ctx context.Context, f influxdb.LabelMappingFilter) ([]*influxdb.Label, error) {
						return []*influxdb.Label{
							{
								ID:    1,
								OrgID: orgOneInfluxID,
							},
							{
								ID:    2,
								OrgID: orgOneInfluxID,
							},
							{
								ID:    3,
								OrgID: orgOneInfluxID,
							},
						}, nil
					},
				},
			},
			args: args{
				filter: influxdb.LabelMappingFilter{
					ResourceID:   10,
					ResourceType: influxdb.BucketsResourceType,
				},
				permissions: []influxdb.Permission{
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type: influxdb.BucketsResourceType,
							ID:   influxdbtesting.IDPtr(10),
						},
					},
				},
			},
			wants: wants{
				// fixme(leodido) > should we return error in this case?
			},
		},
		{
			name: "unable to access labels when missing read permission on filtering resource",
			fields: fields{
				LabelService: &mock.LabelService{
					FindResourceLabelsFn: func(ctx context.Context, f influxdb.LabelMappingFilter) ([]*influxdb.Label, error) {
						return []*influxdb.Label{
							{
								ID:    1,
								OrgID: orgOneInfluxID,
							},
							{
								ID:    2,
								OrgID: orgOneInfluxID,
							},
							{
								ID:    3,
								OrgID: orgOneInfluxID,
							},
						}, nil
					},
				},
			},
			args: args{
				filter: influxdb.LabelMappingFilter{
					ResourceID:   10,
					ResourceType: influxdb.BucketsResourceType,
				},
				permissions: []influxdb.Permission{
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type: influxdb.LabelsResourceType,
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "read:orgs/020f755c3c083000/buckets/000000000000000a is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewLabelServiceWithOrg(tt.fields.LabelService, orgSvc)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			labels, err := s.FindResourceLabels(ctx, tt.args.filter)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)

			if diff := cmp.Diff(labels, tt.wants.labels, labelCmpOptions...); diff != "" {
				t.Errorf("labels are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func TestLabelService_CreateLabelMapping(t *testing.T) {
	type fields struct {
		LabelService influxdb.LabelService
	}
	type args struct {
		mapping     influxdb.LabelMapping
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
			name: "authorized to create label mapping",
			fields: fields{
				LabelService: &mock.LabelService{
					FindLabelByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Label, error) {
						return &influxdb.Label{
							ID:    1,
							OrgID: orgOneInfluxID,
						}, nil
					},
					CreateLabelMappingFn: func(ctx context.Context, lm *influxdb.LabelMapping) error {
						return nil
					},
				},
			},
			args: args{
				mapping: influxdb.LabelMapping{
					LabelID:      1,
					ResourceID:   2,
					ResourceType: influxdb.BucketsResourceType,
				},
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.LabelsResourceType,
						},
					},
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.BucketsResourceType,
							ID:   influxdbtesting.IDPtr(2),
						},
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to create label mapping for resources on which the user does not have write access",
			fields: fields{
				LabelService: &mock.LabelService{
					FindLabelByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Label, error) {
						return &influxdb.Label{
							ID:    1,
							OrgID: orgOneInfluxID,
						}, nil
					},
					CreateLabelMappingFn: func(ctx context.Context, lm *influxdb.LabelMapping) error {
						return nil
					},
				},
			},
			args: args{
				mapping: influxdb.LabelMapping{
					LabelID:      1,
					ResourceID:   2,
					ResourceType: influxdb.BucketsResourceType,
				},
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.LabelsResourceType,
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.EUnauthorized,
					Msg:  "write:orgs/020f755c3c083000/buckets/0000000000000002 is unauthorized",
				},
			},
		},
		{
			name: "unauthorized to create label mapping",
			fields: fields{
				LabelService: &mock.LabelService{
					FindLabelByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Label, error) {
						return &influxdb.Label{
							ID:    1,
							OrgID: orgOneInfluxID,
						}, nil
					},
					CreateLabelMappingFn: func(ctx context.Context, lm *influxdb.LabelMapping) error {
						return nil
					},
				},
			},
			args: args{
				mapping: influxdb.LabelMapping{
					LabelID:      1,
					ResourceID:   2,
					ResourceType: influxdb.BucketsResourceType,
				},
				permissions: []influxdb.Permission{
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type: influxdb.LabelsResourceType,
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/020f755c3c083000/labels/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewLabelServiceWithOrg(tt.fields.LabelService, orgSvc)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			err := s.CreateLabelMapping(ctx, &tt.args.mapping)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestLabelService_DeleteLabelMapping(t *testing.T) {
	type fields struct {
		LabelService influxdb.LabelService
	}
	type args struct {
		mapping     influxdb.LabelMapping
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
			name: "authorized to delete label mapping",
			fields: fields{
				LabelService: &mock.LabelService{
					FindLabelByIDFn: func(ctc context.Context, id platform.ID) (*influxdb.Label, error) {
						return &influxdb.Label{
							ID:    1,
							OrgID: orgOneInfluxID,
						}, nil
					},
					DeleteLabelMappingFn: func(ctx context.Context, m *influxdb.LabelMapping) error {
						return nil
					},
				},
			},
			args: args{
				mapping: influxdb.LabelMapping{
					LabelID:      1,
					ResourceID:   2,
					ResourceType: influxdb.BucketsResourceType,
				},
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.LabelsResourceType,
						},
					},
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.BucketsResourceType,
							ID:   influxdbtesting.IDPtr(2),
						},
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to delete label mapping containing a resources on which the user does not have write access",
			fields: fields{
				LabelService: &mock.LabelService{
					FindLabelByIDFn: func(ctc context.Context, id platform.ID) (*influxdb.Label, error) {
						return &influxdb.Label{
							ID:    1,
							OrgID: orgOneInfluxID,
						}, nil
					},
					DeleteLabelMappingFn: func(ctx context.Context, m *influxdb.LabelMapping) error {
						return nil
					},
				},
			},
			args: args{
				mapping: influxdb.LabelMapping{
					LabelID:      1,
					ResourceID:   2,
					ResourceType: influxdb.BucketsResourceType,
				},
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.LabelsResourceType,
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.EUnauthorized,
					Msg:  "write:orgs/020f755c3c083000/buckets/0000000000000002 is unauthorized",
				},
			},
		},
		{
			name: "unauthorized to delete label mapping",
			fields: fields{
				LabelService: &mock.LabelService{
					FindLabelByIDFn: func(ctc context.Context, id platform.ID) (*influxdb.Label, error) {
						return &influxdb.Label{
							ID:    1,
							OrgID: orgOneInfluxID,
						}, nil
					},
					DeleteLabelMappingFn: func(ctx context.Context, m *influxdb.LabelMapping) error {
						return nil
					},
				},
			},
			args: args{
				mapping: influxdb.LabelMapping{
					LabelID:      1,
					ResourceID:   2,
					ResourceType: influxdb.BucketsResourceType,
				},
				permissions: []influxdb.Permission{
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type: influxdb.LabelsResourceType,
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/020f755c3c083000/labels/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewLabelServiceWithOrg(tt.fields.LabelService, orgSvc)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			err := s.DeleteLabelMapping(ctx, &tt.args.mapping)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}
