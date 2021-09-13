package testing

import (
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/mock"
)

const (
	labelOneID   = "41a9f7288d4e2d64"
	labelTwoID   = "b7c5355e1134b11c"
	labelThreeID = "c8d6466f2245c22d"
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

// LabelFields include the IDGenerator, labels and their mappings
type LabelFields struct {
	Labels      []*influxdb.Label
	Mappings    []*influxdb.LabelMapping
	IDGenerator platform.IDGenerator
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
			tt := tt
			t.Parallel()
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
			name: "names should be unique",
			fields: LabelFields{
				IDGenerator: mock.NewMockIDGenerator(),
				Labels: []*influxdb.Label{
					{
						ID:    MustIDBase16(labelOneID),
						Name:  "label_1",
						OrgID: idOne,
						Properties: map[string]string{
							"color": "fff000",
						},
					},
				},
			},
			args: args{
				label: &influxdb.Label{
					ID:    MustIDBase16(labelTwoID),
					Name:  "label_1",
					OrgID: idOne,
					Properties: map[string]string{
						"color": "fff000",
					},
				},
			},
			wants: wants{
				labels: []*influxdb.Label{
					{
						ID:    MustIDBase16(labelOneID),
						Name:  "label_1",
						OrgID: idOne,
						Properties: map[string]string{
							"color": "fff000",
						},
					},
				},
				err: &errors.Error{
					Code: errors.EConflict,
					Op:   influxdb.OpCreateLabel,
					Msg:  "label with name label_1 already exists",
				},
			},
		},
		{
			name: "names should be trimmed of spacing",
			fields: LabelFields{
				IDGenerator: mock.NewMockIDGenerator(),
				Labels: []*influxdb.Label{
					{
						ID:    MustIDBase16(labelOneID),
						Name:  "tag_1",
						OrgID: idOne,
						Properties: map[string]string{
							"color": "fff000",
						},
					},
				},
			},
			args: args{
				label: &influxdb.Label{
					ID:    MustIDBase16(labelOneID),
					Name:  "     tag_1     ",
					OrgID: idOne,
					Properties: map[string]string{
						"color": "fff000",
					},
				},
			},
			wants: wants{
				labels: []*influxdb.Label{
					{
						ID:    MustIDBase16(labelOneID),
						Name:  "tag_1",
						OrgID: idOne,
						Properties: map[string]string{
							"color": "fff000",
						},
					},
				},
				err: &errors.Error{
					Code: errors.EConflict,
					Op:   influxdb.OpCreateLabel,
					Msg:  "label with name tag_1 already exists",
				},
			},
		},
		{
			name: "labels should be unique and case-agnostic",
			fields: LabelFields{
				IDGenerator: mock.NewMockIDGenerator(),
				Labels: []*influxdb.Label{
					{
						ID:    MustIDBase16(labelOneID),
						Name:  "tag_1",
						OrgID: idOne,
						Properties: map[string]string{
							"color": "fff000",
						},
					},
				},
			},
			args: args{
				label: &influxdb.Label{
					ID:    MustIDBase16(labelOneID),
					Name:  "TAG_1",
					OrgID: idOne,
					Properties: map[string]string{
						"color": "fff000",
					},
				},
			},
			wants: wants{
				labels: []*influxdb.Label{
					{
						ID:    MustIDBase16(labelOneID),
						Name:  "tag_1",
						OrgID: idOne,
						Properties: map[string]string{
							"color": "fff000",
						},
					},
				},
				err: &errors.Error{
					Code: errors.EConflict,
					Op:   influxdb.OpCreateLabel,
					Msg:  "label with name TAG_1 already exists",
				},
			},
		},
		{
			name: "basic create label",
			fields: LabelFields{
				IDGenerator: mock.NewIDGenerator(labelOneID, t),
				Labels:      []*influxdb.Label{},
			},
			args: args{
				label: &influxdb.Label{
					Name:  "Tag2",
					ID:    MustIDBase16(labelOneID),
					OrgID: idOne,
					Properties: map[string]string{
						"color": "fff000",
					},
				},
			},
			wants: wants{
				labels: []*influxdb.Label{
					{
						ID:    MustIDBase16(labelOneID),
						Name:  "Tag2",
						OrgID: idOne,
						Properties: map[string]string{
							"color": "fff000",
						},
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
						ID:    MustIDBase16(labelOneID),
						Name:  "Tag1",
						OrgID: idOne,
					},
					{
						ID:    MustIDBase16(labelTwoID),
						Name:  "Tag2",
						OrgID: idOne,
					},
				},
			},
			args: args{
				filter: influxdb.LabelFilter{},
			},
			wants: wants{
				labels: []*influxdb.Label{
					{
						ID:    MustIDBase16(labelOneID),
						Name:  "Tag1",
						OrgID: idOne,
					},
					{
						ID:    MustIDBase16(labelTwoID),
						Name:  "Tag2",
						OrgID: idOne,
					},
				},
			},
		},
		{
			name: "find labels filtering",
			fields: LabelFields{
				Labels: []*influxdb.Label{
					{
						ID:    MustIDBase16(labelOneID),
						Name:  "Tag1",
						OrgID: idOne,
					},
					{
						ID:    MustIDBase16(labelTwoID),
						Name:  "Tag2",
						OrgID: idOne,
					},
					{
						ID:    MustIDBase16(labelThreeID),
						Name:  "Tag1",
						OrgID: idTwo,
					},
				},
			},
			args: args{
				filter: influxdb.LabelFilter{
					Name:  "Tag1",
					OrgID: idPtr(idOne),
				},
			},
			wants: wants{
				labels: []*influxdb.Label{
					{
						ID:    MustIDBase16(labelOneID),
						Name:  "Tag1",
						OrgID: idOne,
					},
				},
			},
		},
		{
			name: "find a label by name is case-agnostic",
			fields: LabelFields{
				Labels: []*influxdb.Label{
					{
						ID:    MustIDBase16(labelOneID),
						Name:  "tag1",
						OrgID: idOne,
					},
				},
			},
			args: args{
				filter: influxdb.LabelFilter{
					Name:  "TAG1",
					OrgID: idPtr(idOne),
				},
			},
			wants: wants{
				labels: []*influxdb.Label{
					{
						ID:    MustIDBase16(labelOneID),
						Name:  "tag1",
						OrgID: idOne,
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
		id platform.ID
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
						ID:    MustIDBase16(labelOneID),
						Name:  "Tag1",
						OrgID: idOne,
					},
					{
						ID:    MustIDBase16(labelTwoID),
						Name:  "Tag2",
						OrgID: idOne,
					},
				},
			},
			args: args{
				id: MustIDBase16(labelOneID),
			},
			wants: wants{
				label: &influxdb.Label{
					ID:    MustIDBase16(labelOneID),
					Name:  "Tag1",
					OrgID: idOne,
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
				err: &errors.Error{
					Code: errors.ENotFound,
					Op:   influxdb.OpFindLabelByID,
					Msg:  influxdb.ErrLabelNotFound,
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
		labelID platform.ID
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
			name: "update label name",
			fields: LabelFields{
				Labels: []*influxdb.Label{
					{
						ID:    MustIDBase16(labelOneID),
						OrgID: idOne,
						Name:  "Tag1",
					},
				},
			},
			args: args{
				labelID: MustIDBase16(labelOneID),
				update: influxdb.LabelUpdate{
					Name: "NotTag1",
				},
			},
			wants: wants{
				labels: []*influxdb.Label{
					{
						ID:    MustIDBase16(labelOneID),
						OrgID: idOne,
						Name:  "NotTag1",
					},
				},
			},
		},
		{
			name: "cant update a label with a name that already exists",
			fields: LabelFields{
				Labels: []*influxdb.Label{
					{
						ID:    MustIDBase16(labelOneID),
						OrgID: idOne,
						Name:  "tag_1",
					},
					{
						ID:    MustIDBase16(labelTwoID),
						OrgID: idOne,
						Name:  "tag_2",
					},
				},
			},
			args: args{
				labelID: MustIDBase16(labelTwoID),
				update: influxdb.LabelUpdate{
					Name: "tag_1",
				},
			},
			wants: wants{
				labels: []*influxdb.Label{
					{
						ID:    MustIDBase16(labelOneID),
						OrgID: idOne,
						Name:  "tag_1",
					},
					{
						ID:    MustIDBase16(labelTwoID),
						OrgID: idOne,
						Name:  "tag_2",
					},
				},
				err: &errors.Error{
					Code: errors.EConflict,
					Op:   influxdb.OpCreateLabel,
					Msg:  "label with name tag_1 already exists",
				},
			},
		},
		{
			name: "should trim space but fails to update existing label",
			fields: LabelFields{
				Labels: []*influxdb.Label{
					{
						ID:    MustIDBase16(labelOneID),
						OrgID: idOne,
						Name:  "tag_1",
					},
					{
						ID:    MustIDBase16(labelTwoID),
						OrgID: idOne,
						Name:  "tag_2",
					},
				},
			},
			args: args{
				labelID: MustIDBase16(labelTwoID),
				update: influxdb.LabelUpdate{
					Name: " tag_1 ",
				},
			},
			wants: wants{
				labels: []*influxdb.Label{
					{
						ID:    MustIDBase16(labelOneID),
						OrgID: idOne,
						Name:  "tag_1",
					},
					{
						ID:    MustIDBase16(labelTwoID),
						OrgID: idOne,
						Name:  "tag_2",
					},
				},
				err: &errors.Error{
					Code: errors.EConflict,
					Op:   influxdb.OpCreateLabel,
					Msg:  "label with name tag_1 already exists",
				},
			},
		},
		{
			name: "update label properties",
			fields: LabelFields{
				Labels: []*influxdb.Label{
					{
						ID:    MustIDBase16(labelOneID),
						OrgID: idOne,
						Name:  "Tag1",
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
						ID:    MustIDBase16(labelOneID),
						OrgID: idOne,
						Name:  "Tag1",
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
						ID:    MustIDBase16(labelOneID),
						OrgID: idOne,
						Name:  "Tag1",
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
						ID:    MustIDBase16(labelOneID),
						OrgID: idOne,
						Name:  "Tag1",
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
						ID:    MustIDBase16(labelOneID),
						OrgID: idOne,
						Name:  "Tag1",
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
						ID:    MustIDBase16(labelOneID),
						OrgID: idOne,
						Name:  "Tag1",
						Properties: map[string]string{
							"color": "fff000",
						},
					},
				},
			},
		},
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
				err: &errors.Error{
					Code: errors.ENotFound,
					Op:   influxdb.OpUpdateLabel,
					Msg:  influxdb.ErrLabelNotFound,
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
		labelID platform.ID
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
						ID:    MustIDBase16(labelOneID),
						OrgID: idOne,
						Name:  "Tag1",
					},
					{
						ID:    MustIDBase16(labelTwoID),
						OrgID: idOne,
						Name:  "Tag2",
					},
				},
			},
			args: args{
				labelID: MustIDBase16(labelOneID),
			},
			wants: wants{
				labels: []*influxdb.Label{
					{
						ID:    MustIDBase16(labelTwoID),
						OrgID: idOne,
						Name:  "Tag2",
					},
				},
			},
		},
		{
			name: "deleting a non-existent label",
			fields: LabelFields{
				Labels: []*influxdb.Label{
					{
						ID:    MustIDBase16(labelOneID),
						OrgID: idOne,
						Name:  "Tag1",
					},
				},
			},
			args: args{
				labelID: MustIDBase16(labelTwoID),
			},
			wants: wants{
				labels: []*influxdb.Label{
					{
						ID:    MustIDBase16(labelOneID),
						OrgID: idOne,
						Name:  "Tag1",
					},
				},
				err: &errors.Error{
					Code: errors.ENotFound,
					Op:   influxdb.OpDeleteLabel,
					Msg:  influxdb.ErrLabelNotFound,
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
						ID:    MustIDBase16(labelOneID),
						OrgID: idOne,
						Name:  "Tag1",
					},
				},
			},
			args: args{
				mapping: &influxdb.LabelMapping{
					LabelID:    MustIDBase16(labelOneID),
					ResourceID: idOne,
				},
				filter: &influxdb.LabelMappingFilter{
					ResourceID: idOne,
				},
			},
			wants: wants{
				labels: []*influxdb.Label{
					{
						ID:    MustIDBase16(labelOneID),
						Name:  "Tag1",
						OrgID: idOne,
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
					LabelID:    MustIDBase16(labelOneID),
					ResourceID: idOne,
				},
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.ENotFound,
					Op:   influxdb.OpDeleteLabel,
					Msg:  influxdb.ErrLabelNotFound,
				},
			},
		},
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
						ID:    MustIDBase16(labelOneID),
						Name:  "Tag1",
						OrgID: idOne,
					},
				},
				Mappings: []*influxdb.LabelMapping{
					{
						LabelID:    MustIDBase16(labelOneID),
						ResourceID: idOne,
					},
				},
			},
			args: args{
				mapping: &influxdb.LabelMapping{
					LabelID:    MustIDBase16(labelOneID),
					ResourceID: idOne,
				},
				filter: influxdb.LabelMappingFilter{
					ResourceID: idOne,
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
