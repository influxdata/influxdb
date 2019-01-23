package fs_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/fs"
	platformtesting "github.com/influxdata/influxdb/testing"
	"go.uber.org/zap"
)

var protoCmpOptions = cmp.Options{
	cmpopts.EquateEmpty(),
}

func TestProtoService_FindProtos(t *testing.T) {
	type fields struct {
		protos []*platform.Proto
	}
	type wants struct {
		protos []*platform.Proto
	}

	tests := []struct {
		name   string
		fields fields
		wants  wants
	}{
		{
			name: "get protos",
			fields: fields{
				protos: []*platform.Proto{
					{
						ID:   1,
						Name: "system",
						Dashboards: []platform.ProtoDashboard{
							{
								Dashboard: platform.Dashboard{
									Name:        "hello",
									Description: "oh hello there!",
									Meta: platform.DashboardMeta{
										CreatedAt: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
										UpdatedAt: time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC),
									},
									Cells: []*platform.Cell{
										{
											ID: platformtesting.MustIDBase16("da7aba5e5d81e550"),
											X:  1,
											Y:  2,
											W:  3,
											H:  4,
										},
									},
								},
								Views: map[string]platform.View{
									"da7aba5e5d81e550": platform.View{
										ViewContents: platform.ViewContents{
											Name: "hello",
										},
										Properties: platform.XYViewProperties{
											Type: "xy",
										},
									},
								},
							},
						},
					},
				},
			},
			wants: wants{
				protos: []*platform.Proto{
					{
						ID:   1,
						Name: "system",
						Dashboards: []platform.ProtoDashboard{
							{
								Dashboard: platform.Dashboard{
									Name:        "hello",
									Description: "oh hello there!",
									Meta: platform.DashboardMeta{
										CreatedAt: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
										UpdatedAt: time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC),
									},
									Cells: []*platform.Cell{
										{
											ID: platformtesting.MustIDBase16("da7aba5e5d81e550"),
											X:  1,
											Y:  2,
											W:  3,
											H:  4,
										},
									},
								},
								Views: map[string]platform.View{
									"da7aba5e5d81e550": platform.View{
										ViewContents: platform.ViewContents{
											Name: "hello",
										},
										Properties: platform.XYViewProperties{
											Type: "xy",
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// dir is not actually required for this test
			dir := ""
			logger := zap.NewNop()
			s := fs.NewProtoService(dir, logger, nil)
			s.WithProtos(tt.fields.protos)

			ctx := context.Background()
			protos, err := s.FindProtos(ctx)
			if err != nil {
				// TODO(desa): fill is sad path eventually
				t.Fatalf("unexpected error: %v", err)
			}

			if diff := cmp.Diff(protos, tt.wants.protos, protoCmpOptions...); diff != "" {
				t.Errorf("protos are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// TODO(desa): Add tests for CreateDashboardsFromProto.
