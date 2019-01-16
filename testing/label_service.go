package testing

import (
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
)

const (
	labelOneID   = "41a9f7288d4e2d64"
	labelTwoID   = "b7c5355e1134b11c"
	labelThreeID = "52ec3216eaf647e4"
)

var labelCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*platform.Label) []*platform.Label {
		out := append([]*platform.Label(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].Name < out[j].Name
		})
		return out
	}),
}

// LabelFields include the IDGenerator and labels
type LabelFields struct {
	Labels      []*platform.Label
	IDGenerator platform.IDGenerator
}

type labelServiceF func(
	init func(LabelFields, *testing.T) (platform.LabelService, string, func()),
	t *testing.T,
)

// LabelService tests all the service functions.
func LabelService(
	init func(LabelFields, *testing.T) (platform.LabelService, string, func()),
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
		// {
		// 	name: "FindLabels",
		// 	fn:   FindLabels,
		// },
		// {
		// 	name: "UpdateLabel",
		// 	fn:   UpdateLabel,
		// },
		{
			name: "DeleteLabel",
			fn:   DeleteLabel,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(init, t)
		})
	}
}

func CreateLabel(
	init func(LabelFields, *testing.T) (platform.LabelService, string, func()),
	t *testing.T,
) {
	type args struct {
		label *platform.Label
	}
	type wants struct {
		err    error
		labels []*platform.Label
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
				Labels:      []*platform.Label{},
			},
			args: args{
				label: &platform.Label{
					Name: "Tag2",
					Properties: map[string]string{
						"color": "fff000",
					},
				},
			},
			wants: wants{
				labels: []*platform.Label{
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
		// 		Labels: []*platform.Label{
		// 			{
		// 				Name: "Tag1",
		// 			},
		// 		},
		// 	},
		// 	args: args{
		// 		label: &platform.Label{
		// 			Name: "Tag1",
		// 		},
		// 	},
		// 	wants: wants{
		// 		labels: []*platform.Label{
		// 			{
		// 				Name: "Tag1",
		// 			},
		// 		},
		// 		err: &platform.Error{
		// 			Code: platform.EConflict,
		// 			Op:   platform.OpCreateLabel,
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

			labels, err := s.FindLabels(ctx, platform.LabelFilter{})
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
	init func(LabelFields, *testing.T) (platform.LabelService, string, func()),
	t *testing.T,
) {
	type args struct {
		filter platform.LabelFilter
	}
	type wants struct {
		err    error
		labels []*platform.Label
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
				Labels: []*platform.Label{
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
				filter: platform.LabelFilter{},
			},
			wants: wants{
				labels: []*platform.Label{
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
				Labels: []*platform.Label{
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
				filter: platform.LabelFilter{
					Name: "Tag1",
				},
			},
			wants: wants{
				labels: []*platform.Label{
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

func UpdateLabel(
	init func(LabelFields, *testing.T) (platform.LabelService, string, func()),
	t *testing.T,
) {
	type args struct {
		labelID platform.ID
		update  platform.LabelUpdate
	}
	type wants struct {
		err    error
		labels []*platform.Label
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
				Labels: []*platform.Label{
					{
						ID:   MustIDBase16(labelOneID),
						Name: "Tag1",
					},
				},
			},
			args: args{
				labelID: MustIDBase16(labelOneID),
				update: platform.LabelUpdate{
					Properties: map[string]string{
						"color": "fff000",
					},
				},
			},
			wants: wants{
				labels: []*platform.Label{
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
				Labels: []*platform.Label{
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
				update: platform.LabelUpdate{
					Properties: map[string]string{
						"color": "abc123",
					},
				},
			},
			wants: wants{
				labels: []*platform.Label{
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
				Labels: []*platform.Label{
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
				update: platform.LabelUpdate{
					Properties: map[string]string{
						"description": "",
					},
				},
			},
			wants: wants{
				labels: []*platform.Label{
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
		// 		Labels: []*platform.Label{
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
		// 		label: platform.Label{
		// 			ResourceID: MustIDBase16(bucketOneID),
		// 			Name:       "Tag1",
		// 		},
		// 		update: platform.LabelUpdate{
		// 			Color: &validColor,
		// 		},
		// 	},
		// 	wants: wants{
		// 		labels: []*platform.Label{
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
				Labels: []*platform.Label{},
			},
			args: args{
				labelID: MustIDBase16(labelOneID),
				update: platform.LabelUpdate{
					Properties: map[string]string{
						"color": "fff000",
					},
				},
			},
			wants: wants{
				labels: []*platform.Label{},
				err: &platform.Error{
					Code: platform.ENotFound,
					Op:   platform.OpUpdateLabel,
					Msg:  "label not found",
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

			labels, err := s.FindLabels(ctx, platform.LabelFilter{})
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
	init func(LabelFields, *testing.T) (platform.LabelService, string, func()),
	t *testing.T,
) {
	type args struct {
		labelID platform.ID
	}
	type wants struct {
		err    error
		labels []*platform.Label
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
				Labels: []*platform.Label{
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
				labels: []*platform.Label{
					{
						ID:   MustIDBase16(labelTwoID),
						Name: "Tag2",
					},
				},
			},
		},
		// {
		// 	name: "deleting a non-existant label",
		// 	fields: LabelFields{
		// 		Labels: []*platform.Label{
		// 			{
		// 				ID:   MustIDBase16(labelOneID),
		// 				Name: "Tag1",
		// 			},
		// 		},
		// 	},
		// 	args: args{
		// 		labelID: MustIDBase16(labelTwoID),
		// 	},
		// 	wants: wants{
		// 		labels: []*platform.Label{
		// 			{
		// 				ID:   MustIDBase16(labelOneID),
		// 				Name: "Tag1",
		// 			},
		// 		},
		// 		err: &platform.Error{
		// 			Code: platform.ENotFound,
		// 			Op:   platform.OpDeleteLabel,
		// 			Msg:  "label not found",
		// 		},
		// 	},
		// },
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.DeleteLabel(ctx, tt.args.labelID)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			labels, err := s.FindLabels(ctx, platform.LabelFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve labels: %v", err)
			}
			if diff := cmp.Diff(labels, tt.wants.labels, labelCmpOptions...); diff != "" {
				t.Errorf("labels are different -got/+want\ndiff %s", diff)
			}
		})
	}
}
