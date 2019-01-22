package testing

import (
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
)

const (
	labelOneID = "41a9f7288d4e2d64"
	labelTwoID = "b7c5355e1134b11c"
)

var labelCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*influxdb.Label) []*influxdb.Label {
		out := append([]*influxdb.Label(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].Name < out[j].Name
		})
		return out
	}),
}

// LabelFields include the IDGenerator, labels and their mappings
type LabelFields struct {
	Labels      []*influxdb.Label
	Mappings    []*influxdb.LabelMapping
	IDGenerator influxdb.IDGenerator
}

type labelServiceF func(
	init func(LabelFields, *testing.T) (influxdb.LabelService, string, func()),
	t *testing.T,
)

// LabelService tests all the service functions.
func LabelService(
	init func(LabelFields, *testing.T) (influxdb.LabelService, string, func()),
	t *testing.T,
) {
	tests := []struct {
		name string
		fn   labelServiceF
	}{
		{
			name: "CreateLabel",
			fn:   CreateLabel,
		},
		{
			name: "CreateLabelMapping",
			fn:   CreateLabelMapping,
		},
		{
			name: "FindLabels",
			fn:   FindLabels,
		},
		{
			name: "FindLabelByID",
			fn:   FindLabelByID,
		},
		{
			name: "UpdateLabel",
			fn:   UpdateLabel,
		},
		{
			name: "DeleteLabel",
			fn:   DeleteLabel,
		},
		{
			name: "DeleteLabelMapping",
			fn:   DeleteLabelMapping,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(init, t)
		})
	}
}

func CreateLabel(
	init func(LabelFields, *testing.T) (influxdb.LabelService, string, func()),
	t *testing.T,
) {
	type args struct {
		label *influxdb.Label
	}
	type wants struct {
		err    error
		labels []*influxdb.Label
	}

	tests := []struct {
		name   string
		fields LabelFields
		args   args
		wants  wants
	}{
		{
			name: "basic create label",
			fields: LabelFields{
				IDGenerator: mock.NewIDGenerator(labelOneID, t),
				Labels:      []*influxdb.Label{},
			},
			args: args{
				label: &influxdb.Label{
					Name: "Tag2",
					Properties: map[string]string{
						"color": "fff000",
					},
				},
			},
			wants: wants{
				labels: []*influxdb.Label{
					{
						ID:   MustIDBase16(labelOneID),
						Name: "Tag2",
						Properties: map[string]string{
							"color": "fff000",
						},
					},
				},
			},
		},
		// {
		// 	name: "duplicate labels fail",
		// 	fields: LabelFields{
		// 		IDGenerator: mock.NewIDGenerator(labelTwoID, t),
		// 		Labels: []*influxdb.Label{
		// 			{
		// 				Name: "Tag1",
		// 			},
		// 		},
		// 	},
		// 	args: args{
		// 		label: &influxdb.Label{
		// 			Name: "Tag1",
		// 		},
		// 	},
		// 	wants: wants{
		// 		labels: []*influxdb.Label{
		// 			{
		// 				Name: "Tag1",
		// 			},
		// 		},
		// 		err: &influxdb.Error{
		// 			Code: influxdb.EConflict,
		// 			Op:   influxdb.OpCreateLabel,
		// 			Msg:  "label Tag1 already exists",
		// 		},
		// 	},
		// },
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.CreateLabel(ctx, tt.args.label)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			defer s.DeleteLabel(ctx, tt.args.label.ID)

			labels, err := s.FindLabels(ctx, influxdb.LabelFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve labels: %v", err)
			}
			if diff := cmp.Diff(labels, tt.wants.labels, labelCmpOptions...); diff != "" {
				t.Errorf("labels are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func FindLabels(
	init func(LabelFields, *testing.T) (influxdb.LabelService, string, func()),
	t *testing.T,
) {
	type args struct {
		filter influxdb.LabelFilter
	}
	type wants struct {
		err    error
		labels []*influxdb.Label
	}

	tests := []struct {
		name   string
		fields LabelFields
		args   args
		wants  wants
	}{
		{
			name: "basic find labels",
			fields: LabelFields{
				Labels: []*influxdb.Label{
					{
						ID:   MustIDBase16(labelOneID),
						Name: "Tag1",
					},
					{
						ID:   MustIDBase16(labelTwoID),
						Name: "Tag2",
					},
				},
			},
			args: args{
				filter: influxdb.LabelFilter{},
			},
			wants: wants{
				labels: []*influxdb.Label{
					{
						ID:   MustIDBase16(labelOneID),
						Name: "Tag1",
					},
					{
						ID:   MustIDBase16(labelTwoID),
						Name: "Tag2",
					},
				},
			},
		},
		{
			name: "find labels filtering",
			fields: LabelFields{
				Labels: []*influxdb.Label{
					{
						ID:   MustIDBase16(labelOneID),
						Name: "Tag1",
					},
					{
						ID:   MustIDBase16(labelTwoID),
						Name: "Tag2",
					},
				},
			},
			args: args{
				filter: influxdb.LabelFilter{
					Name: "Tag1",
				},
			},
			wants: wants{
				labels: []*influxdb.Label{
					{
						ID:   MustIDBase16(labelOneID),
						Name: "Tag1",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			labels, err := s.FindLabels(ctx, tt.args.filter)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(labels, tt.wants.labels, labelCmpOptions...); diff != "" {
				t.Errorf("labels are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func FindLabelByID(
	init func(LabelFields, *testing.T) (influxdb.LabelService, string, func()),
	t *testing.T,
) {
	type args struct {
		id influxdb.ID
	}
	type wants struct {
		err   error
		label *influxdb.Label
	}

	tests := []struct {
		name   string
		fields LabelFields
		args   args
		wants  wants
	}{
		{
			name: "find label by ID",
			fields: LabelFields{
				Labels: []*influxdb.Label{
					{
						ID:   MustIDBase16(labelOneID),
						Name: "Tag1",
					},
					{
						ID:   MustIDBase16(labelTwoID),
						Name: "Tag2",
					},
				},
			},
			args: args{
				id: MustIDBase16(labelOneID),
			},
			wants: wants{
				label: &influxdb.Label{
					ID:   MustIDBase16(labelOneID),
					Name: "Tag1",
				},
			},
		},
		{
			name: "label does not exist",
			fields: LabelFields{
				Labels: []*influxdb.Label{},
			},
			args: args{
				id: MustIDBase16(labelOneID),
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Op:   influxdb.OpFindLabelByID,
					Err:  influxdb.ErrLabelNotFound,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			label, err := s.FindLabelByID(ctx, tt.args.id)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(label, tt.wants.label, labelCmpOptions...); diff != "" {
				t.Errorf("labels are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func UpdateLabel(
	init func(LabelFields, *testing.T) (influxdb.LabelService, string, func()),
	t *testing.T,
) {
	type args struct {
		labelID influxdb.ID
		update  influxdb.LabelUpdate
	}
	type wants struct {
		err    error
		labels []*influxdb.Label
	}

	tests := []struct {
		name   string
		fields LabelFields
		args   args
		wants  wants
	}{
		{
			name: "update label properties",
			fields: LabelFields{
				Labels: []*influxdb.Label{
					{
						ID:   MustIDBase16(labelOneID),
						Name: "Tag1",
					},
				},
			},
			args: args{
				labelID: MustIDBase16(labelOneID),
				update: influxdb.LabelUpdate{
					Properties: map[string]string{
						"color": "fff000",
					},
				},
			},
			wants: wants{
				labels: []*influxdb.Label{
					{
						ID:   MustIDBase16(labelOneID),
						Name: "Tag1",
						Properties: map[string]string{
							"color": "fff000",
						},
					},
				},
			},
		},
		{
			name: "replacing a label property",
			fields: LabelFields{
				Labels: []*influxdb.Label{
					{
						ID:   MustIDBase16(labelOneID),
						Name: "Tag1",
						Properties: map[string]string{
							"color":       "fff000",
							"description": "description",
						},
					},
				},
			},
			args: args{
				labelID: MustIDBase16(labelOneID),
				update: influxdb.LabelUpdate{
					Properties: map[string]string{
						"color": "abc123",
					},
				},
			},
			wants: wants{
				labels: []*influxdb.Label{
					{
						ID:   MustIDBase16(labelOneID),
						Name: "Tag1",
						Properties: map[string]string{
							"color":       "abc123",
							"description": "description",
						},
					},
				},
			},
		},
		{
			name: "deleting a label property",
			fields: LabelFields{
				Labels: []*influxdb.Label{
					{
						ID:   MustIDBase16(labelOneID),
						Name: "Tag1",
						Properties: map[string]string{
							"color":       "fff000",
							"description": "description",
						},
					},
				},
			},
			args: args{
				labelID: MustIDBase16(labelOneID),
				update: influxdb.LabelUpdate{
					Properties: map[string]string{
						"description": "",
					},
				},
			},
			wants: wants{
				labels: []*influxdb.Label{
					{
						ID:   MustIDBase16(labelOneID),
						Name: "Tag1",
						Properties: map[string]string{
							"color": "fff000",
						},
					},
				},
			},
		},
		// {
		// 	name: "label update proliferation",
		// 	fields: LabelFields{
		// 		Labels: []*influxdb.Label{
		// 			{
		// 				ResourceID: MustIDBase16(bucketOneID),
		// 				Name:       "Tag1",
		// 			},
		// 			{
		// 				ResourceID: MustIDBase16(bucketTwoID),
		// 				Name:       "Tag1",
		// 			},
		// 		},
		// 	},
		// 	args: args{
		// 		label: influxdb.Label{
		// 			ResourceID: MustIDBase16(bucketOneID),
		// 			Name:       "Tag1",
		// 		},
		// 		update: influxdb.LabelUpdate{
		// 			Color: &validColor,
		// 		},
		// 	},
		// 	wants: wants{
		// 		labels: []*influxdb.Label{
		// 			{
		// 				ResourceID: MustIDBase16(bucketOneID),
		// 				Name:       "Tag1",
		// 				Color:      "fff000",
		// 			},
		// 			{
		// 				ResourceID: MustIDBase16(bucketTwoID),
		// 				Name:       "Tag1",
		// 				Color:      "fff000",
		// 			},
		// 		},
		// 	},
		// },
		{
			name: "updating a non-existent label",
			fields: LabelFields{
				Labels: []*influxdb.Label{},
			},
			args: args{
				labelID: MustIDBase16(labelOneID),
				update: influxdb.LabelUpdate{
					Properties: map[string]string{
						"color": "fff000",
					},
				},
			},
			wants: wants{
				labels: []*influxdb.Label{},
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Op:   influxdb.OpUpdateLabel,
					Err:  influxdb.ErrLabelNotFound,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			_, err := s.UpdateLabel(ctx, tt.args.labelID, tt.args.update)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			labels, err := s.FindLabels(ctx, influxdb.LabelFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve labels: %v", err)
			}
			if diff := cmp.Diff(labels, tt.wants.labels, labelCmpOptions...); diff != "" {
				t.Errorf("labels are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func DeleteLabel(
	init func(LabelFields, *testing.T) (influxdb.LabelService, string, func()),
	t *testing.T,
) {
	type args struct {
		labelID influxdb.ID
	}
	type wants struct {
		err    error
		labels []*influxdb.Label
	}

	tests := []struct {
		name   string
		fields LabelFields
		args   args
		wants  wants
	}{
		{
			name: "basic delete label",
			fields: LabelFields{
				Labels: []*influxdb.Label{
					{
						ID:   MustIDBase16(labelOneID),
						Name: "Tag1",
					},
					{
						ID:   MustIDBase16(labelTwoID),
						Name: "Tag2",
					},
				},
			},
			args: args{
				labelID: MustIDBase16(labelOneID),
			},
			wants: wants{
				labels: []*influxdb.Label{
					{
						ID:   MustIDBase16(labelTwoID),
						Name: "Tag2",
					},
				},
			},
		},
		{
			name: "deleting a non-existant label",
			fields: LabelFields{
				Labels: []*influxdb.Label{
					{
						ID:   MustIDBase16(labelOneID),
						Name: "Tag1",
					},
				},
			},
			args: args{
				labelID: MustIDBase16(labelTwoID),
			},
			wants: wants{
				labels: []*influxdb.Label{
					{
						ID:   MustIDBase16(labelOneID),
						Name: "Tag1",
					},
				},
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Op:   influxdb.OpDeleteLabel,
					Err:  influxdb.ErrLabelNotFound,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.DeleteLabel(ctx, tt.args.labelID)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			labels, err := s.FindLabels(ctx, influxdb.LabelFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve labels: %v", err)
			}
			if diff := cmp.Diff(labels, tt.wants.labels, labelCmpOptions...); diff != "" {
				t.Errorf("labels are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func CreateLabelMapping(
	init func(LabelFields, *testing.T) (influxdb.LabelService, string, func()),
	t *testing.T,
) {
	type args struct {
		mapping *influxdb.LabelMapping
		filter  *influxdb.LabelMappingFilter
	}
	type wants struct {
		err    error
		labels []*influxdb.Label
	}

	tests := []struct {
		name   string
		fields LabelFields
		args   args
		wants  wants
	}{
		{
			name: "create label mapping",
			fields: LabelFields{
				Labels: []*influxdb.Label{
					{
						ID:   MustIDBase16(labelOneID),
						Name: "Tag1",
					},
				},
			},
			args: args{
				mapping: &influxdb.LabelMapping{
					LabelID:    IDPtr(MustIDBase16(labelOneID)),
					ResourceID: IDPtr(MustIDBase16(bucketOneID)),
				},
				filter: &influxdb.LabelMappingFilter{
					ResourceID: MustIDBase16(bucketOneID),
				},
			},
			wants: wants{
				labels: []*influxdb.Label{
					{
						ID:   MustIDBase16(labelOneID),
						Name: "Tag1",
					},
				},
			},
		},
		{
			name: "mapping to a nonexistent label",
			fields: LabelFields{
				IDGenerator: mock.NewIDGenerator(labelOneID, t),
				Labels:      []*influxdb.Label{},
			},
			args: args{
				mapping: &influxdb.LabelMapping{
					LabelID:    IDPtr(MustIDBase16(labelOneID)),
					ResourceID: IDPtr(MustIDBase16(bucketOneID)),
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Op:   influxdb.OpDeleteLabel,
					Err:  influxdb.ErrLabelNotFound,
				},
			},
		},
		// {
		// 	name: "duplicate label mappings",
		// 	fields: LabelFields{
		// 		IDGenerator: mock.NewIDGenerator(labelOneID, t),
		// 		Labels:      []*influxdb.Label{},
		// 	},
		// 	args: args{
		// 		label: &influxdb.Label{
		// 			Name: "Tag2",
		// 			Properties: map[string]string{
		// 				"color": "fff000",
		// 			},
		// 		},
		// 	},
		// 	wants: wants{
		// 		labels: []*influxdb.Label{
		// 			{
		// 				ID:   MustIDBase16(labelOneID),
		// 				Name: "Tag2",
		// 				Properties: map[string]string{
		// 					"color": "fff000",
		// 				},
		// 			},
		// 		},
		// 	},
		// },
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.CreateLabelMapping(ctx, tt.args.mapping)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			defer s.DeleteLabelMapping(ctx, tt.args.mapping)

			if tt.args.filter == nil {
				return
			}

			labels, err := s.FindResourceLabels(ctx, *tt.args.filter)
			if err != nil {
				t.Fatalf("failed to retrieve labels: %v", err)
			}
			if diff := cmp.Diff(labels, tt.wants.labels, labelCmpOptions...); diff != "" {
				t.Errorf("labels are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func DeleteLabelMapping(
	init func(LabelFields, *testing.T) (influxdb.LabelService, string, func()),
	t *testing.T,
) {
	type args struct {
		mapping *influxdb.LabelMapping
		filter  influxdb.LabelMappingFilter
	}
	type wants struct {
		err    error
		labels []*influxdb.Label
	}

	tests := []struct {
		name   string
		fields LabelFields
		args   args
		wants  wants
	}{
		{
			name: "delete label mapping",
			fields: LabelFields{
				Labels: []*influxdb.Label{
					{
						ID:   MustIDBase16(labelOneID),
						Name: "Tag1",
					},
				},
				Mappings: []*influxdb.LabelMapping{
					{
						LabelID:    IDPtr(MustIDBase16(labelOneID)),
						ResourceID: IDPtr(MustIDBase16(bucketOneID)),
					},
				},
			},
			args: args{
				mapping: &influxdb.LabelMapping{
					LabelID:    IDPtr(MustIDBase16(labelOneID)),
					ResourceID: IDPtr(MustIDBase16(bucketOneID)),
				},
				filter: influxdb.LabelMappingFilter{
					ResourceID: MustIDBase16(bucketOneID),
				},
			},
			wants: wants{
				labels: []*influxdb.Label{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.DeleteLabelMapping(ctx, tt.args.mapping)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			labels, err := s.FindResourceLabels(ctx, tt.args.filter)
			if err != nil {
				t.Fatalf("failed to retrieve labels: %v", err)
			}
			if diff := cmp.Diff(labels, tt.wants.labels, labelCmpOptions...); diff != "" {
				t.Errorf("labels are different -got/+want\ndiff %s", diff)
			}
		})
	}
}
