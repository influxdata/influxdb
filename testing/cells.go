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
	viewOneID   = "020f755c3c082000"
	viewTwoID   = "020f755c3c082001"
	viewThreeID = "020f755c3c082002"
)

var viewCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*platform.View) []*platform.View {
		out := append([]*platform.View(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID.String() > out[j].ID.String()
		})
		return out
	}),
}

// ViewFields will include the IDGenerator, and views
type ViewFields struct {
	IDGenerator platform.IDGenerator
	Views       []*platform.View
}

// CreateView testing
func CreateView(
	init func(ViewFields, *testing.T) (platform.ViewService, string, func()),
	t *testing.T,
) {
	type args struct {
		view *platform.View
	}
	type wants struct {
		err   error
		views []*platform.View
	}

	tests := []struct {
		name   string
		fields ViewFields
		args   args
		wants  wants
	}{
		{
			name: "basic create view",
			fields: ViewFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return MustIDBase16(viewTwoID)
					},
				},
				Views: []*platform.View{
					{
						ViewContents: platform.ViewContents{
							ID:   MustIDBase16(viewOneID),
							Name: "view1",
						},
					},
				},
			},
			args: args{
				view: &platform.View{
					ViewContents: platform.ViewContents{
						Name: "view2",
					},
					Properties: platform.TableViewProperties{
						Type:       "table",
						TimeFormat: "rfc3339",
					},
				},
			},
			wants: wants{
				views: []*platform.View{
					{
						ViewContents: platform.ViewContents{
							ID:   MustIDBase16(viewOneID),
							Name: "view1",
						},
						Properties: platform.EmptyViewProperties{},
					},
					{
						ViewContents: platform.ViewContents{
							ID:   MustIDBase16(viewTwoID),
							Name: "view2",
						},
						Properties: platform.TableViewProperties{
							Type:       "table",
							TimeFormat: "rfc3339",
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
			err := s.CreateView(ctx, tt.args.view)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)
			defer s.DeleteView(ctx, tt.args.view.ID)

			views, _, err := s.FindViews(ctx, platform.ViewFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve views: %v", err)
			}
			if diff := cmp.Diff(views, tt.wants.views, viewCmpOptions...); diff != "" {
				t.Errorf("views are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindViewByID testing
func FindViewByID(
	init func(ViewFields, *testing.T) (platform.ViewService, string, func()),
	t *testing.T,
) {
	type args struct {
		id platform.ID
	}
	type wants struct {
		err  error
		view *platform.View
	}

	tests := []struct {
		name   string
		fields ViewFields
		args   args
		wants  wants
	}{
		{
			name: "basic find view by id",
			fields: ViewFields{
				Views: []*platform.View{
					{
						ViewContents: platform.ViewContents{
							ID:   MustIDBase16(viewOneID),
							Name: "view1",
						},
						Properties: platform.EmptyViewProperties{},
					},
					{
						ViewContents: platform.ViewContents{
							ID:   MustIDBase16(viewTwoID),
							Name: "view2",
						},
						Properties: platform.TableViewProperties{
							Type:       "table",
							TimeFormat: "rfc3339",
						},
					},
				},
			},
			args: args{
				id: MustIDBase16(viewTwoID),
			},
			wants: wants{
				view: &platform.View{
					ViewContents: platform.ViewContents{
						ID:   MustIDBase16(viewTwoID),
						Name: "view2",
					},
					Properties: platform.TableViewProperties{
						Type:       "table",
						TimeFormat: "rfc3339",
					},
				},
			},
		},
		{
			name: "find view by id not found",
			fields: ViewFields{
				Views: []*platform.View{
					{
						ViewContents: platform.ViewContents{
							ID:   MustIDBase16(viewOneID),
							Name: "view1",
						},
						Properties: platform.EmptyViewProperties{},
					},
					{
						ViewContents: platform.ViewContents{
							ID:   MustIDBase16(viewTwoID),
							Name: "view2",
						},
						Properties: platform.TableViewProperties{
							Type:       "table",
							TimeFormat: "rfc3339",
						},
					},
				},
			},
			args: args{
				id: MustIDBase16(threeID),
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.ENotFound,
					Op:   platform.OpFindViewByID,
					Msg:  "view not found",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			view, err := s.FindViewByID(ctx, tt.args.id)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(view, tt.wants.view, viewCmpOptions...); diff != "" {
				t.Errorf("view is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindViews testing
func FindViews(
	init func(ViewFields, *testing.T) (platform.ViewService, string, func()),
	t *testing.T,
) {
	type args struct {
		ID platform.ID
	}

	type wants struct {
		views []*platform.View
		err   error
	}
	tests := []struct {
		name   string
		fields ViewFields
		args   args
		wants  wants
	}{
		{
			name: "find all views",
			fields: ViewFields{
				Views: []*platform.View{
					{
						ViewContents: platform.ViewContents{
							ID:   MustIDBase16(viewOneID),
							Name: "view1",
						},
						Properties: platform.EmptyViewProperties{},
					},
					{
						ViewContents: platform.ViewContents{
							ID:   MustIDBase16(viewTwoID),
							Name: "view2",
						},
						Properties: platform.TableViewProperties{
							Type:       "table",
							TimeFormat: "rfc3339",
						},
					},
				},
			},
			args: args{},
			wants: wants{
				views: []*platform.View{
					{
						ViewContents: platform.ViewContents{
							ID:   MustIDBase16(viewOneID),
							Name: "view1",
						},
						Properties: platform.EmptyViewProperties{},
					},
					{
						ViewContents: platform.ViewContents{
							ID:   MustIDBase16(viewTwoID),
							Name: "view2",
						},
						Properties: platform.TableViewProperties{
							Type:       "table",
							TimeFormat: "rfc3339",
						},
					},
				},
			},
		},
		{
			name: "find view by id",
			fields: ViewFields{
				Views: []*platform.View{
					{
						ViewContents: platform.ViewContents{
							ID:   MustIDBase16(viewOneID),
							Name: "view1",
						},
						Properties: platform.EmptyViewProperties{},
					},
					{
						ViewContents: platform.ViewContents{
							ID:   MustIDBase16(viewTwoID),
							Name: "view2",
						},
						Properties: platform.TableViewProperties{
							Type:       "table",
							TimeFormat: "rfc3339",
						},
					},
				},
			},
			args: args{
				ID: MustIDBase16(viewTwoID),
			},
			wants: wants{
				views: []*platform.View{
					{
						ViewContents: platform.ViewContents{
							ID:   MustIDBase16(viewTwoID),
							Name: "view2",
						},
						Properties: platform.TableViewProperties{
							Type:       "table",
							TimeFormat: "rfc3339",
						},
					},
				},
			},
		},
		{
			name: "find view by id not found",
			fields: ViewFields{
				Views: []*platform.View{
					{
						ViewContents: platform.ViewContents{
							ID:   MustIDBase16(viewOneID),
							Name: "view1",
						},
						Properties: platform.EmptyViewProperties{},
					},
					{
						ViewContents: platform.ViewContents{
							ID:   MustIDBase16(viewTwoID),
							Name: "view2",
						},
						Properties: platform.TableViewProperties{
							Type:       "table",
							TimeFormat: "rfc3339",
						},
					},
				},
			},
			args: args{
				ID: MustIDBase16(threeID),
			},
			wants: wants{
				views: []*platform.View{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			filter := platform.ViewFilter{}
			if tt.args.ID.Valid() {
				filter.ID = &tt.args.ID
			}

			views, _, err := s.FindViews(ctx, filter)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(views, tt.wants.views, viewCmpOptions...); diff != "" {
				t.Errorf("views are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// DeleteView testing
func DeleteView(
	init func(ViewFields, *testing.T) (platform.ViewService, string, func()),
	t *testing.T,
) {
	type args struct {
		ID platform.ID
	}
	type wants struct {
		err   error
		views []*platform.View
	}

	tests := []struct {
		name   string
		fields ViewFields
		args   args
		wants  wants
	}{
		{
			name: "delete views using exist id",
			fields: ViewFields{
				Views: []*platform.View{
					{
						ViewContents: platform.ViewContents{
							ID:   MustIDBase16(viewOneID),
							Name: "view1",
						},
						Properties: platform.EmptyViewProperties{},
					},
					{
						ViewContents: platform.ViewContents{
							ID:   MustIDBase16(viewTwoID),
							Name: "view2",
						},
						Properties: platform.TableViewProperties{
							Type:       "table",
							TimeFormat: "rfc3339",
						},
					},
				},
			},
			args: args{
				ID: MustIDBase16(viewOneID),
			},
			wants: wants{
				views: []*platform.View{
					{
						ViewContents: platform.ViewContents{
							ID:   MustIDBase16(viewTwoID),
							Name: "view2",
						},
						Properties: platform.TableViewProperties{
							Type:       "table",
							TimeFormat: "rfc3339",
						},
					},
				},
			},
		},
		{
			name: "delete views using id that does not exist",
			fields: ViewFields{
				Views: []*platform.View{
					{
						ViewContents: platform.ViewContents{
							ID:   MustIDBase16(viewOneID),
							Name: "view1",
						},
						Properties: platform.EmptyViewProperties{},
					},
					{
						ViewContents: platform.ViewContents{
							ID:   MustIDBase16(viewTwoID),
							Name: "view2",
						},
						Properties: platform.TableViewProperties{
							Type:       "table",
							TimeFormat: "rfc3339",
						},
					},
				},
			},
			args: args{
				ID: MustIDBase16(viewThreeID),
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.ENotFound,
					Op:   platform.OpDeleteView,
					Msg:  "view not found",
				},
				views: []*platform.View{
					{
						ViewContents: platform.ViewContents{
							ID:   MustIDBase16(viewOneID),
							Name: "view1",
						},
						Properties: platform.EmptyViewProperties{},
					},
					{
						ViewContents: platform.ViewContents{
							ID:   MustIDBase16(viewTwoID),
							Name: "view2",
						},
						Properties: platform.TableViewProperties{
							Type:       "table",
							TimeFormat: "rfc3339",
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
			err := s.DeleteView(ctx, tt.args.ID)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			filter := platform.ViewFilter{}
			views, _, err := s.FindViews(ctx, filter)
			if err != nil {
				t.Fatalf("failed to retrieve views: %v", err)
			}
			if diff := cmp.Diff(views, tt.wants.views, viewCmpOptions...); diff != "" {
				t.Errorf("views are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// UpdateView testing
func UpdateView(
	init func(ViewFields, *testing.T) (platform.ViewService, string, func()),
	t *testing.T,
) {
	type args struct {
		name       string
		properties platform.ViewProperties
		id         platform.ID
	}
	type wants struct {
		err  error
		view *platform.View
	}

	tests := []struct {
		name   string
		fields ViewFields
		args   args
		wants  wants
	}{
		{
			name: "update name",
			fields: ViewFields{
				Views: []*platform.View{
					{
						ViewContents: platform.ViewContents{
							ID:   MustIDBase16(viewOneID),
							Name: "view1",
						},
						Properties: platform.EmptyViewProperties{},
					},
					{
						ViewContents: platform.ViewContents{
							ID:   MustIDBase16(viewTwoID),
							Name: "view2",
						},
						Properties: platform.TableViewProperties{
							Type:       "table",
							TimeFormat: "rfc3339",
						},
					},
				},
			},
			args: args{
				id:   MustIDBase16(viewOneID),
				name: "changed",
			},
			wants: wants{
				view: &platform.View{
					ViewContents: platform.ViewContents{
						ID:   MustIDBase16(viewOneID),
						Name: "changed",
					},
					Properties: platform.EmptyViewProperties{},
				},
			},
		},
		{
			name: "update properties",
			fields: ViewFields{
				Views: []*platform.View{
					{
						ViewContents: platform.ViewContents{
							ID:   MustIDBase16(viewOneID),
							Name: "view1",
						},
						Properties: platform.EmptyViewProperties{},
					},
					{
						ViewContents: platform.ViewContents{
							ID:   MustIDBase16(viewTwoID),
							Name: "view2",
						},
						Properties: platform.TableViewProperties{
							Type:       "table",
							TimeFormat: "rfc3339",
						},
					},
				},
			},
			args: args{
				id: MustIDBase16(viewOneID),
				properties: platform.TableViewProperties{
					Type:       "table",
					TimeFormat: "rfc3339",
				},
			},
			wants: wants{
				view: &platform.View{
					ViewContents: platform.ViewContents{
						ID:   MustIDBase16(viewOneID),
						Name: "view1",
					},
					Properties: platform.TableViewProperties{
						Type:       "table",
						TimeFormat: "rfc3339",
					},
				},
			},
		},
		{
			name: "update id not exists",
			fields: ViewFields{
				Views: []*platform.View{
					{
						ViewContents: platform.ViewContents{
							ID:   MustIDBase16(viewOneID),
							Name: "view1",
						},
						Properties: platform.EmptyViewProperties{},
					},
					{
						ViewContents: platform.ViewContents{
							ID:   MustIDBase16(viewTwoID),
							Name: "view2",
						},
						Properties: platform.TableViewProperties{
							Type:       "table",
							TimeFormat: "rfc3339",
						},
					},
				},
			},
			args: args{
				id:   MustIDBase16(threeID),
				name: "changed",
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.ENotFound,
					Op:   platform.OpUpdateView,
					Msg:  "view not found",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			upd := platform.ViewUpdate{}
			if tt.args.name != "" {
				upd.Name = &tt.args.name
			}
			if tt.args.properties != nil {
				upd.Properties = tt.args.properties
			}

			view, err := s.UpdateView(ctx, tt.args.id, upd)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(view, tt.wants.view, viewCmpOptions...); diff != "" {
				t.Errorf("view is different -got/+want\ndiff %s", diff)
			}
		})
	}
}
