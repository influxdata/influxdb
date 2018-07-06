package bolt_test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/chronograf"
)

func TestOrganizationConfig_FindOrCreate(t *testing.T) {
	type args struct {
		organizationID string
	}
	type wants struct {
		organizationConfig *chronograf.OrganizationConfig
		err                error
	}
	tests := []struct {
		name     string
		args     args
		addFirst bool
		wants    wants
	}{
		{
			name: "Get non-existent default config from default org",
			args: args{
				organizationID: "default",
			},
			addFirst: false,
			wants: wants{
				organizationConfig: &chronograf.OrganizationConfig{
					OrganizationID: "default",
					LogViewer: chronograf.LogViewerConfig{
						Columns: []chronograf.LogViewerColumn{
							{
								Name:     "time",
								Position: 0,
								Encodings: []chronograf.ColumnEncoding{
									{
										Type:  "visibility",
										Value: "hidden",
									},
								},
							},
							{
								Name:     "severity",
								Position: 1,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
									{
										Type:  "label",
										Value: "icon",
									},
									{
										Type:  "label",
										Value: "text",
									},
								},
							},
							{
								Name:     "timestamp",
								Position: 2,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
							{
								Name:     "message",
								Position: 3,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
							{
								Name:     "facility",
								Position: 4,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
							{
								Name:     "procid",
								Position: 5,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
									{
										Type:  "displayName",
										Value: "Proc ID",
									},
								},
							},
							{
								Name:     "appname",
								Position: 6,
								Encodings: []chronograf.ColumnEncoding{
									{
										Type:  "visibility",
										Value: "visible",
									},
									{
										Type:  "displayName",
										Value: "Application",
									},
								},
							},
							{
								Name:     "host",
								Position: 7,
								Encodings: []chronograf.ColumnEncoding{
									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Get non-existent default config from non-default org",
			args: args{
				organizationID: "1",
			},
			addFirst: false,
			wants: wants{
				organizationConfig: &chronograf.OrganizationConfig{
					OrganizationID: "1",
					LogViewer: chronograf.LogViewerConfig{
						Columns: []chronograf.LogViewerColumn{
							{
								Name:     "time",
								Position: 0,
								Encodings: []chronograf.ColumnEncoding{
									{
										Type:  "visibility",
										Value: "hidden",
									},
								},
							},
							{
								Name:     "severity",
								Position: 1,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
									{
										Type:  "label",
										Value: "icon",
									},
									{
										Type:  "label",
										Value: "text",
									},
								},
							},
							{
								Name:     "timestamp",
								Position: 2,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
							{
								Name:     "message",
								Position: 3,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
							{
								Name:     "facility",
								Position: 4,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
							{
								Name:     "procid",
								Position: 5,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
									{
										Type:  "displayName",
										Value: "Proc ID",
									},
								},
							},
							{
								Name:     "appname",
								Position: 6,
								Encodings: []chronograf.ColumnEncoding{
									{
										Type:  "visibility",
										Value: "visible",
									},
									{
										Type:  "displayName",
										Value: "Application",
									},
								},
							},
							{
								Name:     "host",
								Position: 7,
								Encodings: []chronograf.ColumnEncoding{
									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Get existing/modified config from default org",
			args: args{
				organizationID: "default",
			},
			addFirst: true,
			wants: wants{
				organizationConfig: &chronograf.OrganizationConfig{
					OrganizationID: "default",
					LogViewer: chronograf.LogViewerConfig{
						Columns: []chronograf.LogViewerColumn{
							{
								Name:     "time",
								Position: 1,
								Encodings: []chronograf.ColumnEncoding{
									{
										Type:  "visibility",
										Value: "hidden",
									},
								},
							},
							{
								Name:     "severity",
								Position: 0,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "hidden",
									},
									{
										Type:  "label",
										Value: "icon",
									},
									{
										Type:  "label",
										Value: "text",
									},
								},
							},
							{
								Name:     "timestamp",
								Position: 2,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
							{
								Name:     "message",
								Position: 3,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
							{
								Name:     "facility",
								Position: 4,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
							{
								Name:     "procid",
								Position: 5,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
									{
										Type:  "displayName",
										Value: "Proc ID",
									},
								},
							},
							{
								Name:     "appname",
								Position: 6,
								Encodings: []chronograf.ColumnEncoding{
									{
										Type:  "visibility",
										Value: "visible",
									},
									{
										Type:  "displayName",
										Value: "Application",
									},
								},
							},
							{
								Name:     "host",
								Position: 7,
								Encodings: []chronograf.ColumnEncoding{
									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Get existing/modified config from non-default org",
			args: args{
				organizationID: "1",
			},
			addFirst: true,
			wants: wants{
				organizationConfig: &chronograf.OrganizationConfig{
					OrganizationID: "1",
					LogViewer: chronograf.LogViewerConfig{
						Columns: []chronograf.LogViewerColumn{
							{
								Name:     "time",
								Position: 1,
								Encodings: []chronograf.ColumnEncoding{
									{
										Type:  "visibility",
										Value: "hidden",
									},
								},
							},
							{
								Name:     "severity",
								Position: 0,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "hidden",
									},
									{
										Type:  "label",
										Value: "icon",
									},
									{
										Type:  "label",
										Value: "text",
									},
								},
							},
							{
								Name:     "timestamp",
								Position: 2,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
							{
								Name:     "message",
								Position: 3,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
							{
								Name:     "facility",
								Position: 4,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
							{
								Name:     "procid",
								Position: 5,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
									{
										Type:  "displayName",
										Value: "Proc ID",
									},
								},
							},
							{
								Name:     "appname",
								Position: 6,
								Encodings: []chronograf.ColumnEncoding{
									{
										Type:  "visibility",
										Value: "visible",
									},
									{
										Type:  "displayName",
										Value: "Application",
									},
								},
							},
							{
								Name:     "host",
								Position: 7,
								Encodings: []chronograf.ColumnEncoding{
									{
										Type:  "visibility",
										Value: "visible",
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
		client, err := NewTestClient()
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		s := client.OrganizationConfigStore

		if tt.addFirst {
			if err := s.Update(context.Background(), tt.wants.organizationConfig); err != nil {
				t.Fatal(err)
			}
		}

		got, err := s.FindOrCreate(context.Background(), tt.args.organizationID)

		if (tt.wants.err != nil) != (err != nil) {
			t.Errorf("%q. OrganizationConfigStore.FindOrCreate() error = %v, wantErr %v", tt.name, err, tt.wants.err)
			continue
		}
		if diff := cmp.Diff(got, tt.wants.organizationConfig); diff != "" {
			t.Errorf("%q. OrganizationConfigStore.FindOrCreate():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}

func TestOrganizationConfig_Update(t *testing.T) {
	type args struct {
		organizationConfig *chronograf.OrganizationConfig
		organizationID     string
	}
	type wants struct {
		organizationConfig *chronograf.OrganizationConfig
		err                error
	}
	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name: "Set default org config",
			args: args{
				organizationConfig: &chronograf.OrganizationConfig{
					OrganizationID: "default",
					LogViewer: chronograf.LogViewerConfig{
						Columns: []chronograf.LogViewerColumn{
							{
								Name:     "time",
								Position: 1,
								Encodings: []chronograf.ColumnEncoding{
									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
							{
								Name:     "severity",
								Position: 0,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
									{
										Type:  "label",
										Value: "text",
									},
								},
							},
							{
								Name:     "timestamp",
								Position: 2,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
							{
								Name:     "message",
								Position: 3,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
							{
								Name:     "facility",
								Position: 4,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
							{
								Name:     "procid",
								Position: 5,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
									{
										Type:  "displayName",
										Value: "Milkshake",
									},
								},
							},
							{
								Name:     "appname",
								Position: 6,
								Encodings: []chronograf.ColumnEncoding{
									{
										Type:  "visibility",
										Value: "visible",
									},
									{
										Type:  "displayName",
										Value: "Application",
									},
								},
							},
							{
								Name:     "host",
								Position: 7,
								Encodings: []chronograf.ColumnEncoding{
									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
						},
					},
				},
				organizationID: "default",
			},
			wants: wants{
				organizationConfig: &chronograf.OrganizationConfig{
					LogViewer: chronograf.LogViewerConfig{
						Columns: []chronograf.LogViewerColumn{
							{
								Name:     "time",
								Position: 1,
								Encodings: []chronograf.ColumnEncoding{
									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
							{
								Name:     "severity",
								Position: 0,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
									{
										Type:  "label",
										Value: "text",
									},
								},
							},
							{
								Name:     "timestamp",
								Position: 2,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
							{
								Name:     "message",
								Position: 3,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
							{
								Name:     "facility",
								Position: 4,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
							{
								Name:     "procid",
								Position: 5,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
									{
										Type:  "displayName",
										Value: "Milkshake",
									},
								},
							},
							{
								Name:     "appname",
								Position: 6,
								Encodings: []chronograf.ColumnEncoding{
									{
										Type:  "visibility",
										Value: "visible",
									},
									{
										Type:  "displayName",
										Value: "Application",
									},
								},
							},
							{
								Name:     "host",
								Position: 7,
								Encodings: []chronograf.ColumnEncoding{
									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
						},
					},
					OrganizationID: "default",
				},
			},
		},
		{
			name: "Set non-default org config",
			args: args{
				organizationConfig: &chronograf.OrganizationConfig{
					OrganizationID: "1337",
					LogViewer: chronograf.LogViewerConfig{
						Columns: []chronograf.LogViewerColumn{
							{
								Name:     "time",
								Position: 1,
								Encodings: []chronograf.ColumnEncoding{
									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
							{
								Name:     "severity",
								Position: 0,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
									{
										Type:  "label",
										Value: "text",
									},
								},
							},
							{
								Name:     "timestamp",
								Position: 2,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
							{
								Name:     "message",
								Position: 3,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
							{
								Name:     "facility",
								Position: 4,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
							{
								Name:     "procid",
								Position: 5,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
									{
										Type:  "displayName",
										Value: "Milkshake",
									},
								},
							},
							{
								Name:     "appname",
								Position: 6,
								Encodings: []chronograf.ColumnEncoding{
									{
										Type:  "visibility",
										Value: "visible",
									},
									{
										Type:  "displayName",
										Value: "Application",
									},
								},
							},
							{
								Name:     "host",
								Position: 7,
								Encodings: []chronograf.ColumnEncoding{
									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
						},
					},
				},
				organizationID: "1337",
			},
			wants: wants{
				organizationConfig: &chronograf.OrganizationConfig{
					LogViewer: chronograf.LogViewerConfig{
						Columns: []chronograf.LogViewerColumn{
							{
								Name:     "time",
								Position: 1,
								Encodings: []chronograf.ColumnEncoding{
									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
							{
								Name:     "severity",
								Position: 0,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
									{
										Type:  "label",
										Value: "text",
									},
								},
							},
							{
								Name:     "timestamp",
								Position: 2,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
							{
								Name:     "message",
								Position: 3,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
							{
								Name:     "facility",
								Position: 4,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
							{
								Name:     "procid",
								Position: 5,
								Encodings: []chronograf.ColumnEncoding{

									{
										Type:  "visibility",
										Value: "visible",
									},
									{
										Type:  "displayName",
										Value: "Milkshake",
									},
								},
							},
							{
								Name:     "appname",
								Position: 6,
								Encodings: []chronograf.ColumnEncoding{
									{
										Type:  "visibility",
										Value: "visible",
									},
									{
										Type:  "displayName",
										Value: "Application",
									},
								},
							},
							{
								Name:     "host",
								Position: 7,
								Encodings: []chronograf.ColumnEncoding{
									{
										Type:  "visibility",
										Value: "visible",
									},
								},
							},
						},
					},
					OrganizationID: "1337",
				},
			},
		},
	}
	for _, tt := range tests {
		client, err := NewTestClient()
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		s := client.OrganizationConfigStore
		err = s.Update(context.Background(), tt.args.organizationConfig)
		if (tt.wants.err != nil) != (err != nil) {
			t.Errorf("%q. OrganizationConfigStore.Update() error = %v, wantErr %v", tt.name, err, tt.wants.err)
			continue
		}

		got, _ := s.FindOrCreate(context.Background(), tt.args.organizationID)
		if (tt.wants.err != nil) != (err != nil) {
			t.Errorf("%q. OrganizationConfigStore.Update() error = %v, wantErr %v", tt.name, err, tt.wants.err)
			continue
		}

		if diff := cmp.Diff(got, tt.wants.organizationConfig); diff != "" {
			t.Errorf("%q. OrganizationConfigStore.Update():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}
