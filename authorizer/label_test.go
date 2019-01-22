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
				LabelService: &mock.LabelService{
					FindLabelByIDFn: func(ctx context.Context, id influxdb.ID) (*influxdb.Label, error) {
						return &influxdb.Label{
							ID: id,
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
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
					FindLabelByIDFn: func(ctx context.Context, id influxdb.ID) (*influxdb.Label, error) {
						return &influxdb.Label{
							ID: id,
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.LabelsResourceType,
						ID:   influxdbtesting.IDPtr(2),
					},
				},
				id: 1,
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "read:labels/0000000000000001 is unauthorized",
					Code: influxdb.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewLabelService(tt.fields.LabelService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{[]influxdb.Permission{tt.args.permission}})

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
								ID: 1,
							},
							{
								ID: 2,
							},
							{
								ID: 3,
							},
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.LabelsResourceType,
					},
				},
			},
			wants: wants{
				labels: []*influxdb.Label{
					{
						ID: 1,
					},
					{
						ID: 2,
					},
					{
						ID: 3,
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
								ID: 1,
							},
							{
								ID: 2,
							},
							{
								ID: 3,
							},
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.LabelsResourceType,
						ID:   influxdbtesting.IDPtr(1),
					},
				},
			},
			wants: wants{
				labels: []*influxdb.Label{
					{
						ID: 1,
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
								ID: 1,
							},
							{
								ID: 2,
							},
							{
								ID: 3,
							},
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
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
			s := authorizer.NewLabelService(tt.fields.LabelService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{[]influxdb.Permission{tt.args.permission}})

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
			name: "authorized to update label",
			fields: fields{
				LabelService: &mock.LabelService{
					FindLabelByIDFn: func(ctc context.Context, id influxdb.ID) (*influxdb.Label, error) {
						return &influxdb.Label{
							ID: 1,
						}, nil
					},
					UpdateLabelFn: func(ctx context.Context, id influxdb.ID, upd influxdb.LabelUpdate) (*influxdb.Label, error) {
						return &influxdb.Label{
							ID: 1,
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
					FindLabelByIDFn: func(ctc context.Context, id influxdb.ID) (*influxdb.Label, error) {
						return &influxdb.Label{
							ID: 1,
						}, nil
					},
					UpdateLabelFn: func(ctx context.Context, id influxdb.ID, upd influxdb.LabelUpdate) (*influxdb.Label, error) {
						return &influxdb.Label{
							ID: 1,
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
							Type: influxdb.LabelsResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "write:labels/0000000000000001 is unauthorized",
					Code: influxdb.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewLabelService(tt.fields.LabelService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{tt.args.permissions})

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
			name: "authorized to delete label",
			fields: fields{
				LabelService: &mock.LabelService{
					FindLabelByIDFn: func(ctc context.Context, id influxdb.ID) (*influxdb.Label, error) {
						return &influxdb.Label{
							ID: 1,
						}, nil
					},
					DeleteLabelFn: func(ctx context.Context, id influxdb.ID) error {
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
			name: "unauthorized to delete label",
			fields: fields{
				LabelService: &mock.LabelService{
					FindLabelByIDFn: func(ctc context.Context, id influxdb.ID) (*influxdb.Label, error) {
						return &influxdb.Label{
							ID: 1,
						}, nil
					},
					DeleteLabelFn: func(ctx context.Context, id influxdb.ID) error {
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
							Type: influxdb.LabelsResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "write:labels/0000000000000001 is unauthorized",
					Code: influxdb.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewLabelService(tt.fields.LabelService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{tt.args.permissions})

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
					Action: "write",
					Resource: influxdb.Resource{
						Type: influxdb.LabelsResourceType,
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to create label",
			fields: fields{
				LabelService: &mock.LabelService{
					CreateLabelFn: func(ctx context.Context, b *influxdb.Label) error {
						return nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.LabelsResourceType,
					},
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "write:labels is unauthorized",
					Code: influxdb.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewLabelService(tt.fields.LabelService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{[]influxdb.Permission{tt.args.permission}})

			err := s.CreateLabel(ctx, &influxdb.Label{Name: "name"})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
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
						Action: "write",
						Resource: influxdb.Resource{
							Type: influxdb.LabelsResourceType,
						},
					},
					{
						Action: "write",
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
						Action: "write",
						Resource: influxdb.Resource{
							Type: influxdb.LabelsResourceType,
						},
					},
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.EUnauthorized,
					Msg:  "write:buckets/0000000000000002 is unauthorized",
				},
			},
		},
		{
			name: "unauthorized to create label mapping",
			fields: fields{
				LabelService: &mock.LabelService{
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
						Action: "read",
						Resource: influxdb.Resource{
							Type: influxdb.LabelsResourceType,
						},
					},
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "write:labels/0000000000000001 is unauthorized",
					Code: influxdb.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewLabelService(tt.fields.LabelService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{tt.args.permissions})

			err := s.CreateLabelMapping(ctx, &tt.args.mapping)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}
