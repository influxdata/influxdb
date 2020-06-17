package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/httprouter"
	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/inmem"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/mock"
	platformtesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/yudai/gojsondiff"
	"github.com/yudai/gojsondiff/formatter"
	"go.uber.org/zap/zaptest"
)

// NewMockDashboardBackend returns a DashboardBackend with mock services.
func NewMockDashboardBackend(t *testing.T) *DashboardBackend {
	return &DashboardBackend{
		log: zaptest.NewLogger(t),

		DashboardService:             mock.NewDashboardService(),
		DashboardOperationLogService: mock.NewDashboardOperationLogService(),
		UserResourceMappingService:   mock.NewUserResourceMappingService(),
		LabelService:                 mock.NewLabelService(),
		UserService:                  mock.NewUserService(),
	}
}

func TestService_handleGetDashboards(t *testing.T) {
	type fields struct {
		DashboardService platform.DashboardService
		LabelService     platform.LabelService
	}
	type args struct {
		queryParams map[string][]string
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "get all dashboards",
			fields: fields{
				&mock.DashboardService{
					FindDashboardsF: func(ctx context.Context, filter platform.DashboardFilter, opts platform.FindOptions) ([]*platform.Dashboard, int, error) {
						return []*platform.Dashboard{
							{
								ID:             platformtesting.MustIDBase16("da7aba5e5d81e550"),
								OrganizationID: 1,
								Name:           "hello",
								Description:    "oh hello there!",
								Meta: platform.DashboardMeta{
									CreatedAt: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
									UpdatedAt: time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC),
								},
								Cells: []*platform.Cell{
									{
										ID: platformtesting.MustIDBase16("da7aba5e5d81e550"),
										CellProperty: platform.CellProperty{
											X: 1,
											Y: 2,
											W: 3,
											H: 4,
										},
									},
								},
							},
							{
								ID:             platformtesting.MustIDBase16("0ca2204eca2204e0"),
								OrganizationID: 1,
								Meta: platform.DashboardMeta{
									CreatedAt: time.Date(2012, time.November, 10, 23, 0, 0, 0, time.UTC),
									UpdatedAt: time.Date(2012, time.November, 10, 24, 0, 0, 0, time.UTC),
								},
								Name: "example",
							},
						}, 2, nil
					},
				},
				&mock.LabelService{
					FindResourceLabelsFn: func(ctx context.Context, f platform.LabelMappingFilter) ([]*platform.Label, error) {
						labels := []*platform.Label{
							{
								ID:   platformtesting.MustIDBase16("fc3dc670a4be9b9a"),
								Name: "label",
								Properties: map[string]string{
									"color": "fff000",
								},
							},
						}
						return labels, nil
					},
				},
			},
			args: args{},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/dashboards?descending=false&limit=20&offset=0"
  },
  "dashboards": [
    {
      "id": "da7aba5e5d81e550",
      "orgID": "0000000000000001",
      "name": "hello",
      "description": "oh hello there!",
      "labels": [
        {
          "id": "fc3dc670a4be9b9a",
          "name": "label",
          "properties": {
            "color": "fff000"
          }
        }
      ],
      "meta": {
        "createdAt": "2009-11-10T23:00:00Z",
        "updatedAt": "2009-11-11T00:00:00Z"
      },
      "cells": [
        {
          "id": "da7aba5e5d81e550",
          "x": 1,
          "y": 2,
          "w": 3,
          "h": 4,
          "links": {
            "self": "/api/v2/dashboards/da7aba5e5d81e550/cells/da7aba5e5d81e550",
            "view": "/api/v2/dashboards/da7aba5e5d81e550/cells/da7aba5e5d81e550/view"
          }
        }
      ],
      "links": {
        "self": "/api/v2/dashboards/da7aba5e5d81e550",
        "org": "/api/v2/orgs/0000000000000001",
        "members": "/api/v2/dashboards/da7aba5e5d81e550/members",
        "owners": "/api/v2/dashboards/da7aba5e5d81e550/owners",
        "cells": "/api/v2/dashboards/da7aba5e5d81e550/cells",
        "labels": "/api/v2/dashboards/da7aba5e5d81e550/labels"
      }
    },
    {
      "id": "0ca2204eca2204e0",
      "orgID": "0000000000000001",
      "name": "example",
      "description": "",
			"labels": [
        {
          "id": "fc3dc670a4be9b9a",
          "name": "label",
          "properties": {
            "color": "fff000"
          }
        }
      ],
      "meta": {
        "createdAt": "2012-11-10T23:00:00Z",
        "updatedAt": "2012-11-11T00:00:00Z"
      },
      "cells": [],
      "links": {
        "self": "/api/v2/dashboards/0ca2204eca2204e0",
        "org": "/api/v2/orgs/0000000000000001",
        "members": "/api/v2/dashboards/0ca2204eca2204e0/members",
        "owners": "/api/v2/dashboards/0ca2204eca2204e0/owners",
        "cells": "/api/v2/dashboards/0ca2204eca2204e0/cells",
        "labels": "/api/v2/dashboards/0ca2204eca2204e0/labels"
      }
    }
  ]
}
`,
			},
		},
		{
			name: "get all dashboards when there are none",
			fields: fields{
				&mock.DashboardService{
					FindDashboardsF: func(ctx context.Context, filter platform.DashboardFilter, opts platform.FindOptions) ([]*platform.Dashboard, int, error) {
						return []*platform.Dashboard{}, 0, nil
					},
				},
				&mock.LabelService{
					FindResourceLabelsFn: func(ctx context.Context, f platform.LabelMappingFilter) ([]*platform.Label, error) {
						return []*platform.Label{}, nil
					},
				},
			},
			args: args{},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/dashboards?descending=false&limit=20&offset=0"
  },
  "dashboards": []
}`,
			},
		},
		{
			name: "get all dashboards belonging to org 1",
			fields: fields{
				&mock.DashboardService{
					FindDashboardsF: func(ctx context.Context, filter platform.DashboardFilter, opts platform.FindOptions) ([]*platform.Dashboard, int, error) {
						return []*platform.Dashboard{
							{
								ID:             platformtesting.MustIDBase16("da7aba5e5d81e550"),
								OrganizationID: 1,
								Name:           "hello",
								Description:    "oh hello there!",
								Meta: platform.DashboardMeta{
									CreatedAt: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
									UpdatedAt: time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC),
								},
								Cells: []*platform.Cell{
									{
										ID: platformtesting.MustIDBase16("da7aba5e5d81e550"),
										CellProperty: platform.CellProperty{
											X: 1,
											Y: 2,
											W: 3,
											H: 4,
										},
									},
								},
							},
						}, 1, nil
					},
				},
				&mock.LabelService{
					FindResourceLabelsFn: func(ctx context.Context, f platform.LabelMappingFilter) ([]*platform.Label, error) {
						labels := []*platform.Label{
							{
								ID:   platformtesting.MustIDBase16("fc3dc670a4be9b9a"),
								Name: "label",
								Properties: map[string]string{
									"color": "fff000",
								},
							},
						}
						return labels, nil
					},
				},
			},
			args: args{
				map[string][]string{
					"orgID": {"0000000000000001"},
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/dashboards?descending=false&limit=20&offset=0&orgID=0000000000000001"
  },
  "dashboards": [
    {
      "id": "da7aba5e5d81e550",
      "orgID": "0000000000000001",
      "name": "hello",
      "description": "oh hello there!",
      "meta": {
        "createdAt": "2009-11-10T23:00:00Z",
        "updatedAt": "2009-11-11T00:00:00Z"
    },
    "labels": [
      {
        "id": "fc3dc670a4be9b9a",
        "name": "label",
        "properties": {
          "color": "fff000"
        }
      }
    ],
      "cells": [
        {
          "id": "da7aba5e5d81e550",
          "x": 1,
          "y": 2,
          "w": 3,
          "h": 4,
          "links": {
            "self": "/api/v2/dashboards/da7aba5e5d81e550/cells/da7aba5e5d81e550",
            "view": "/api/v2/dashboards/da7aba5e5d81e550/cells/da7aba5e5d81e550/view"
          }
        }
      ],
      "links": {
        "self": "/api/v2/dashboards/da7aba5e5d81e550",
        "org": "/api/v2/orgs/0000000000000001",
        "members": "/api/v2/dashboards/da7aba5e5d81e550/members",
        "owners": "/api/v2/dashboards/da7aba5e5d81e550/owners",
        "cells": "/api/v2/dashboards/da7aba5e5d81e550/cells",
        "labels": "/api/v2/dashboards/da7aba5e5d81e550/labels"
      }
    }
  ]
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dashboardBackend := NewMockDashboardBackend(t)
			dashboardBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			dashboardBackend.LabelService = tt.fields.LabelService
			dashboardBackend.DashboardService = tt.fields.DashboardService
			h := NewDashboardHandler(zaptest.NewLogger(t), dashboardBackend)

			r := httptest.NewRequest("GET", "http://any.url", nil)

			qp := r.URL.Query()
			for k, vs := range tt.args.queryParams {
				for _, v := range vs {
					qp.Add(k, v)
				}
			}
			r.URL.RawQuery = qp.Encode()

			w := httptest.NewRecorder()

			h.handleGetDashboards(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetDashboards() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetDashboards() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
				t.Errorf("%q, handleGetDashboards(). error unmarshaling json %v", tt.name, err)
			} else if tt.wants.body != "" && !eq {
				t.Errorf("%q. handleGetDashboards() = ***%s***", tt.name, diff)
			}
		})
	}
}

func TestService_handleGetDashboard(t *testing.T) {
	type fields struct {
		DashboardService platform.DashboardService
	}
	type args struct {
		id          string
		queryString map[string]string
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "get a dashboard by id with view properties",
			fields: fields{
				&mock.DashboardService{
					GetDashboardCellViewF: func(ctx context.Context, dashboardID platform.ID, cellID platform.ID) (*platform.View, error) {
						return &platform.View{ViewContents: platform.ViewContents{Name: "the cell name"}, Properties: platform.XYViewProperties{Type: platform.ViewPropertyTypeXY}}, nil
					},
					FindDashboardByIDF: func(ctx context.Context, id platform.ID) (*platform.Dashboard, error) {
						if id == platformtesting.MustIDBase16("020f755c3c082000") {
							return &platform.Dashboard{
								ID:             platformtesting.MustIDBase16("020f755c3c082000"),
								OrganizationID: 1,
								Meta: platform.DashboardMeta{
									CreatedAt: time.Date(2012, time.November, 10, 23, 0, 0, 0, time.UTC),
									UpdatedAt: time.Date(2012, time.November, 10, 24, 0, 0, 0, time.UTC),
								},
								Name: "hello",
								Cells: []*platform.Cell{
									{
										ID: platformtesting.MustIDBase16("da7aba5e5d81e550"),
										CellProperty: platform.CellProperty{
											X: 1,
											Y: 2,
											W: 3,
											H: 4,
										},
										View: &platform.View{ViewContents: platform.ViewContents{Name: "the cell name"}, Properties: platform.XYViewProperties{Type: platform.ViewPropertyTypeXY}},
									},
								},
							}, nil
						}

						return nil, fmt.Errorf("not found")
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
				queryString: map[string]string{
					"include": "properties",
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "id": "020f755c3c082000",
  "orgID": "0000000000000001",
  "name": "hello",
  "description": "",
  "labels": [],
  "meta": {
    "createdAt": "2012-11-10T23:00:00Z",
    "updatedAt": "2012-11-11T00:00:00Z"
  },
  "cells": [
    {
      "id": "da7aba5e5d81e550",
      "x": 1,
      "y": 2,
      "w": 3,
	  "h": 4,
	  "name": "the cell name",
	  "properties": {
		"shape": "chronograf-v2",
		"axes": null,
		"colors": null,
		"geom": "",
		"legend": {},
		"position": "",
		"note": "",
		"queries": null,
		"shadeBelow": false,
		"hoverDimension": "y",
		"showNoteWhenEmpty": false,
		"timeFormat": "",
		"type": "xy",
		"xColumn": "",
		"yColumn": ""
	  },
	  "links": {
		"self": "/api/v2/dashboards/020f755c3c082000/cells/da7aba5e5d81e550",
		"view": "/api/v2/dashboards/020f755c3c082000/cells/da7aba5e5d81e550/view"
	  }
    }
  ],
  "links": {
	     "self": "/api/v2/dashboards/020f755c3c082000",
	     "org": "/api/v2/orgs/0000000000000001",
	     "members": "/api/v2/dashboards/020f755c3c082000/members",
	     "owners": "/api/v2/dashboards/020f755c3c082000/owners",
	     "cells": "/api/v2/dashboards/020f755c3c082000/cells",
	     "labels": "/api/v2/dashboards/020f755c3c082000/labels"
	}
}
`,
			},
		},
		{
			name: "get a dashboard by id with view properties, but a cell doesnt exist",
			fields: fields{
				&mock.DashboardService{
					GetDashboardCellViewF: func(ctx context.Context, dashboardID platform.ID, cellID platform.ID) (*platform.View, error) {
						return nil, nil
					},
					FindDashboardByIDF: func(ctx context.Context, id platform.ID) (*platform.Dashboard, error) {
						if id == platformtesting.MustIDBase16("020f755c3c082000") {
							return &platform.Dashboard{
								ID:             platformtesting.MustIDBase16("020f755c3c082000"),
								OrganizationID: 1,
								Meta: platform.DashboardMeta{
									CreatedAt: time.Date(2012, time.November, 10, 23, 0, 0, 0, time.UTC),
									UpdatedAt: time.Date(2012, time.November, 10, 24, 0, 0, 0, time.UTC),
								},
								Name: "hello",
								Cells: []*platform.Cell{
									{
										ID: platformtesting.MustIDBase16("da7aba5e5d81e550"),
										CellProperty: platform.CellProperty{
											X: 1,
											Y: 2,
											W: 3,
											H: 4,
										},
									},
								},
							}, nil
						}

						return nil, fmt.Errorf("not found")
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
				queryString: map[string]string{
					"include": "properties",
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "id": "020f755c3c082000",
  "orgID": "0000000000000001",
  "name": "hello",
  "description": "",
  "labels": [],
  "meta": {
    "createdAt": "2012-11-10T23:00:00Z",
    "updatedAt": "2012-11-11T00:00:00Z"
  },
  "cells": [
    {
      "id": "da7aba5e5d81e550",
      "x": 1,
      "y": 2,
      "w": 3,
	  "h": 4,
	  "links": {
		"self": "/api/v2/dashboards/020f755c3c082000/cells/da7aba5e5d81e550",
		"view": "/api/v2/dashboards/020f755c3c082000/cells/da7aba5e5d81e550/view"
	  }
    }
  ],
  "links": {
	     "self": "/api/v2/dashboards/020f755c3c082000",
	     "org": "/api/v2/orgs/0000000000000001",
	     "members": "/api/v2/dashboards/020f755c3c082000/members",
	     "owners": "/api/v2/dashboards/020f755c3c082000/owners",
	     "cells": "/api/v2/dashboards/020f755c3c082000/cells",
	     "labels": "/api/v2/dashboards/020f755c3c082000/labels"
	}
}
`,
			},
		},
		{
			name: "get a dashboard by id doesnt return cell properties if they exist by default",
			fields: fields{
				&mock.DashboardService{
					GetDashboardCellViewF: func(ctx context.Context, dashboardID platform.ID, cellID platform.ID) (*platform.View, error) {
						return &platform.View{ViewContents: platform.ViewContents{Name: "the cell name"}, Properties: platform.XYViewProperties{Type: platform.ViewPropertyTypeXY}}, nil
					},
					FindDashboardByIDF: func(ctx context.Context, id platform.ID) (*platform.Dashboard, error) {
						if id == platformtesting.MustIDBase16("020f755c3c082000") {
							return &platform.Dashboard{
								ID:             platformtesting.MustIDBase16("020f755c3c082000"),
								OrganizationID: 1,
								Meta: platform.DashboardMeta{
									CreatedAt: time.Date(2012, time.November, 10, 23, 0, 0, 0, time.UTC),
									UpdatedAt: time.Date(2012, time.November, 10, 24, 0, 0, 0, time.UTC),
								},
								Name: "hello",
								Cells: []*platform.Cell{
									{
										ID: platformtesting.MustIDBase16("da7aba5e5d81e550"),
										CellProperty: platform.CellProperty{
											X: 1,
											Y: 2,
											W: 3,
											H: 4,
										},
									},
								},
							}, nil
						}

						return nil, fmt.Errorf("not found")
					},
				},
			},
			args: args{
				id:          "020f755c3c082000",
				queryString: map[string]string{},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "id": "020f755c3c082000",
  "orgID": "0000000000000001",
  "name": "hello",
  "description": "",
  "labels": [],
  "meta": {
    "createdAt": "2012-11-10T23:00:00Z",
    "updatedAt": "2012-11-11T00:00:00Z"
  },
  "cells": [
    {
      "id": "da7aba5e5d81e550",
      "x": 1,
      "y": 2,
      "w": 3,
	  "h": 4,
	  "links": {
		"self": "/api/v2/dashboards/020f755c3c082000/cells/da7aba5e5d81e550",
		"view": "/api/v2/dashboards/020f755c3c082000/cells/da7aba5e5d81e550/view"
	  }
    }
  ],
  "links": {
	     "self": "/api/v2/dashboards/020f755c3c082000",
	     "org": "/api/v2/orgs/0000000000000001",
	     "members": "/api/v2/dashboards/020f755c3c082000/members",
	     "owners": "/api/v2/dashboards/020f755c3c082000/owners",
	     "cells": "/api/v2/dashboards/020f755c3c082000/cells",
	     "labels": "/api/v2/dashboards/020f755c3c082000/labels"
	}
}
`,
			},
		},
		{
			name: "get a dashboard by id",
			fields: fields{
				&mock.DashboardService{
					FindDashboardByIDF: func(ctx context.Context, id platform.ID) (*platform.Dashboard, error) {
						if id == platformtesting.MustIDBase16("020f755c3c082000") {
							return &platform.Dashboard{
								ID:             platformtesting.MustIDBase16("020f755c3c082000"),
								OrganizationID: 1,
								Meta: platform.DashboardMeta{
									CreatedAt: time.Date(2012, time.November, 10, 23, 0, 0, 0, time.UTC),
									UpdatedAt: time.Date(2012, time.November, 10, 24, 0, 0, 0, time.UTC),
								},
								Name: "hello",
								Cells: []*platform.Cell{
									{
										ID: platformtesting.MustIDBase16("da7aba5e5d81e550"),
										CellProperty: platform.CellProperty{
											X: 1,
											Y: 2,
											W: 3,
											H: 4,
										},
									},
								},
							}, nil
						}

						return nil, fmt.Errorf("not found")
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
		{
		  "id": "020f755c3c082000",
		  "orgID": "0000000000000001",
		  "name": "hello",
		  "description": "",
		  "labels": [],
		  "meta": {
		    "createdAt": "2012-11-10T23:00:00Z",
		    "updatedAt": "2012-11-11T00:00:00Z"
		  },
		  "cells": [
		    {
			  "id": "da7aba5e5d81e550",
		      "x": 1,
		      "y": 2,
		      "w": 3,
		      "h": 4,
		      "links": {
		        "self": "/api/v2/dashboards/020f755c3c082000/cells/da7aba5e5d81e550",
		        "view": "/api/v2/dashboards/020f755c3c082000/cells/da7aba5e5d81e550/view"
		      }
		    }
		  ],
		  "links": {
		    "self": "/api/v2/dashboards/020f755c3c082000",
		    "org": "/api/v2/orgs/0000000000000001",
		    "members": "/api/v2/dashboards/020f755c3c082000/members",
		    "owners": "/api/v2/dashboards/020f755c3c082000/owners",
		    "cells": "/api/v2/dashboards/020f755c3c082000/cells",
		    "labels": "/api/v2/dashboards/020f755c3c082000/labels"
		  }
		}
		`,
			},
		},
		{
			name: "not found",
			fields: fields{
				&mock.DashboardService{
					FindDashboardByIDF: func(ctx context.Context, id platform.ID) (*platform.Dashboard, error) {
						return nil, &platform.Error{
							Code: platform.ENotFound,
							Msg:  platform.ErrDashboardNotFound,
						}
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
			},
			wants: wants{
				statusCode: http.StatusNotFound,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dashboardBackend := NewMockDashboardBackend(t)
			dashboardBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			dashboardBackend.DashboardService = tt.fields.DashboardService
			h := NewDashboardHandler(zaptest.NewLogger(t), dashboardBackend)

			r := httptest.NewRequest("GET", "http://any.url", nil)

			urlQuery := r.URL.Query()

			for k, v := range tt.args.queryString {
				urlQuery.Add(k, v)
			}

			r.URL.RawQuery = urlQuery.Encode()

			r = r.WithContext(context.WithValue(
				context.Background(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.id,
					},
				}))

			w := httptest.NewRecorder()

			h.handleGetDashboard(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)
			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetDashboard() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetDashboard() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, diff, err := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && err != nil {
				t.Errorf("%q, handleGetDashboard(). error unmarshaling json %v", tt.name, err)
			} else if tt.wants.body != "" && !eq {
				t.Errorf("%q. handleGetDashboard() = ***%s***", tt.name, diff)
			}
		})
	}
}

func TestService_handlePostDashboard(t *testing.T) {
	type fields struct {
		DashboardService platform.DashboardService
	}
	type args struct {
		dashboard *platform.Dashboard
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "create a new dashboard",
			fields: fields{
				&mock.DashboardService{
					CreateDashboardF: func(ctx context.Context, c *platform.Dashboard) error {
						c.ID = platformtesting.MustIDBase16("020f755c3c082000")
						c.Meta = platform.DashboardMeta{
							CreatedAt: time.Date(2012, time.November, 10, 23, 0, 0, 0, time.UTC),
							UpdatedAt: time.Date(2012, time.November, 10, 24, 0, 0, 0, time.UTC),
						}
						return nil
					},
				},
			},
			args: args{
				dashboard: &platform.Dashboard{
					ID:             platformtesting.MustIDBase16("020f755c3c082000"),
					OrganizationID: 1,
					Name:           "hello",
					Description:    "howdy there",
					Cells: []*platform.Cell{
						{
							ID: platformtesting.MustIDBase16("da7aba5e5d81e550"),
							CellProperty: platform.CellProperty{
								X: 1,
								Y: 2,
								W: 3,
								H: 4,
							},
						},
					},
				},
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `
						{
						"id": "020f755c3c082000",
						"orgID": "0000000000000001",
						"name": "hello",
						"description": "howdy there",
						"labels": [],
						"meta": {
							"createdAt": "2012-11-10T23:00:00Z",
							"updatedAt": "2012-11-11T00:00:00Z"
						},
						"cells": [
							{
								"id": "da7aba5e5d81e550",
								"x": 1,
								"y": 2,
								"w": 3,
								"h": 4,
								"links": {
									"self": "/api/v2/dashboards/020f755c3c082000/cells/da7aba5e5d81e550",
									"view": "/api/v2/dashboards/020f755c3c082000/cells/da7aba5e5d81e550/view"
								}
							}
						],
						"links": {
							"self": "/api/v2/dashboards/020f755c3c082000",
							"org": "/api/v2/orgs/0000000000000001",
							"members": "/api/v2/dashboards/020f755c3c082000/members",
							"owners": "/api/v2/dashboards/020f755c3c082000/owners",
							"cells": "/api/v2/dashboards/020f755c3c082000/cells",
							"labels": "/api/v2/dashboards/020f755c3c082000/labels"
						}
						}`,
			},
		},
		{
			name: "create a new dashboard with cell view properties",
			fields: fields{
				&mock.DashboardService{
					CreateDashboardF: func(ctx context.Context, c *platform.Dashboard) error {
						c.ID = platformtesting.MustIDBase16("020f755c3c082000")
						c.Meta = platform.DashboardMeta{
							CreatedAt: time.Date(2012, time.November, 10, 23, 0, 0, 0, time.UTC),
							UpdatedAt: time.Date(2012, time.November, 10, 24, 0, 0, 0, time.UTC),
						}
						c.Cells = []*platform.Cell{
							{
								ID: platformtesting.MustIDBase16("da7aba5e5d81e550"),
								CellProperty: platform.CellProperty{
									X: 1,
									Y: 2,
									W: 3,
									H: 4,
								},
								View: &platform.View{
									ViewContents: platform.ViewContents{
										Name: "hello a view",
									},
									Properties: platform.XYViewProperties{
										Type: platform.ViewPropertyTypeXY,
										Note: "note",
									},
								},
							},
						}
						return nil
					},
				},
			},
			args: args{
				dashboard: &platform.Dashboard{
					ID:             platformtesting.MustIDBase16("020f755c3c082000"),
					OrganizationID: 1,
					Name:           "hello",
					Description:    "howdy there",
					Cells: []*platform.Cell{
						{
							ID: platformtesting.MustIDBase16("da7aba5e5d81e550"),
							CellProperty: platform.CellProperty{
								X: 1,
								Y: 2,
								W: 3,
								H: 4,
							},
							View: &platform.View{
								ViewContents: platform.ViewContents{
									Name: "hello a view",
								},
								Properties: struct {
									platform.XYViewProperties
									Shape string
								}{
									XYViewProperties: platform.XYViewProperties{
										Note: "note",
										Type: platform.ViewPropertyTypeXY,
									},
									Shape: "chronograf-v2",
								},
							},
						},
					},
				},
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `
				{
					"id": "020f755c3c082000",
					"orgID": "0000000000000001",
					"name": "hello",
					"description": "howdy there",
					"labels": [],
					"meta": {
						"createdAt": "2012-11-10T23:00:00Z",
						"updatedAt": "2012-11-11T00:00:00Z"
					},
					"cells": [
						{
							"id": "da7aba5e5d81e550",
							"x": 1,
							"y": 2,
							"w": 3,
							"h": 4,
							"name": "hello a view",
							"properties": {
								"shape": "chronograf-v2",
								"axes": null,
								"colors": null,
								"geom": "",
								"legend": {},
								"note": "note",
								"position": "",
								"queries": null,
								"shadeBelow": false,
								"hoverDimension": "y",
								"showNoteWhenEmpty": false,
								"timeFormat": "",
								"type": "",
								"xColumn": "",
								"yColumn": "",
								"type": "xy"
							},
							"links": {
								"self": "/api/v2/dashboards/020f755c3c082000/cells/da7aba5e5d81e550",
								"view": "/api/v2/dashboards/020f755c3c082000/cells/da7aba5e5d81e550/view"
							}
						}
					],
					"links": {
						"self": "/api/v2/dashboards/020f755c3c082000",
						"org": "/api/v2/orgs/0000000000000001",
						"members": "/api/v2/dashboards/020f755c3c082000/members",
						"owners": "/api/v2/dashboards/020f755c3c082000/owners",
						"cells": "/api/v2/dashboards/020f755c3c082000/cells",
						"labels": "/api/v2/dashboards/020f755c3c082000/labels"
					}
				}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dashboardBackend := NewMockDashboardBackend(t)
			dashboardBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			dashboardBackend.DashboardService = tt.fields.DashboardService
			h := NewDashboardHandler(zaptest.NewLogger(t), dashboardBackend)

			b, err := json.Marshal(tt.args.dashboard)
			if err != nil {
				t.Fatalf("failed to unmarshal dashboard: %v", err)
			}

			r := httptest.NewRequest("GET", "http://any.url", bytes.NewReader(b))
			w := httptest.NewRecorder()

			h.handlePostDashboard(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePostDashboard() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePostDashboard() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
				t.Errorf("%q, handlePostDashboard(). error unmarshaling json %v", tt.name, err)
			} else if tt.wants.body != "" && !eq {
				t.Errorf("%q. handlePostDashboard() = ***%s***", tt.name, diff)
			}
		})
	}
}

func TestService_handleDeleteDashboard(t *testing.T) {
	type fields struct {
		DashboardService platform.DashboardService
	}
	type args struct {
		id string
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "remove a dashboard by id",
			fields: fields{
				&mock.DashboardService{
					DeleteDashboardF: func(ctx context.Context, id platform.ID) error {
						if id == platformtesting.MustIDBase16("020f755c3c082000") {
							return nil
						}

						return fmt.Errorf("wrong id")
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
			},
			wants: wants{
				statusCode: http.StatusNoContent,
			},
		},
		{
			name: "dashboard not found",
			fields: fields{
				&mock.DashboardService{
					DeleteDashboardF: func(ctx context.Context, id platform.ID) error {
						return &platform.Error{
							Code: platform.ENotFound,
							Msg:  platform.ErrDashboardNotFound,
						}
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
			},
			wants: wants{
				statusCode: http.StatusNotFound,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dashboardBackend := NewMockDashboardBackend(t)
			dashboardBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			dashboardBackend.DashboardService = tt.fields.DashboardService
			h := NewDashboardHandler(zaptest.NewLogger(t), dashboardBackend)

			r := httptest.NewRequest("GET", "http://any.url", nil)

			r = r.WithContext(context.WithValue(
				context.Background(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.id,
					},
				}))

			w := httptest.NewRecorder()

			h.handleDeleteDashboard(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleDeleteDashboard() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleDeleteDashboard() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handleDeleteDashboard(). error unmarshaling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handleDeleteDashboard() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestService_handlePatchDashboard(t *testing.T) {
	type fields struct {
		DashboardService platform.DashboardService
	}
	type args struct {
		id   string
		name string
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "update a dashboard name",
			fields: fields{
				&mock.DashboardService{
					UpdateDashboardF: func(ctx context.Context, id platform.ID, upd platform.DashboardUpdate) (*platform.Dashboard, error) {
						if id == platformtesting.MustIDBase16("020f755c3c082000") {
							d := &platform.Dashboard{
								ID:             platformtesting.MustIDBase16("020f755c3c082000"),
								OrganizationID: 1,
								Name:           "hello",
								Meta: platform.DashboardMeta{
									CreatedAt: time.Date(2012, time.November, 10, 23, 0, 0, 0, time.UTC),
									UpdatedAt: time.Date(2012, time.November, 10, 25, 0, 0, 0, time.UTC),
								},
								Cells: []*platform.Cell{
									{
										ID: platformtesting.MustIDBase16("da7aba5e5d81e550"),
										CellProperty: platform.CellProperty{
											X: 1,
											Y: 2,
											W: 3,
											H: 4,
										},
									},
								},
							}

							if upd.Name != nil {
								d.Name = *upd.Name
							}

							return d, nil
						}

						return nil, fmt.Errorf("not found")
					},
				},
			},
			args: args{
				id:   "020f755c3c082000",
				name: "example",
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
		{
		  "id": "020f755c3c082000",
		  "orgID": "0000000000000001",
		  "name": "example",
		  "description": "",
		  "labels": [],
		  "meta": {
		    "createdAt": "2012-11-10T23:00:00Z",
		    "updatedAt": "2012-11-11T01:00:00Z"
		  },
		  "cells": [
		    {
		      "id": "da7aba5e5d81e550",
		      "x": 1,
		      "y": 2,
		      "w": 3,
		      "h": 4,
		      "links": {
		        "self": "/api/v2/dashboards/020f755c3c082000/cells/da7aba5e5d81e550",
		        "view": "/api/v2/dashboards/020f755c3c082000/cells/da7aba5e5d81e550/view"
		      }
		    }
		  ],
		  "links": {
		    "self": "/api/v2/dashboards/020f755c3c082000",
		    "org": "/api/v2/orgs/0000000000000001",
		    "members": "/api/v2/dashboards/020f755c3c082000/members",
		    "owners": "/api/v2/dashboards/020f755c3c082000/owners",
		    "cells": "/api/v2/dashboards/020f755c3c082000/cells",
		    "labels": "/api/v2/dashboards/020f755c3c082000/labels"
		  }
		}
		`,
			},
		},
		{
			name: "update a dashboard with empty request body",
			fields: fields{
				&mock.DashboardService{
					UpdateDashboardF: func(ctx context.Context, id platform.ID, upd platform.DashboardUpdate) (*platform.Dashboard, error) {
						return nil, fmt.Errorf("not found")
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
			},
			wants: wants{
				statusCode: http.StatusBadRequest,
			},
		},
		{
			name: "dashboard not found",
			fields: fields{
				&mock.DashboardService{
					UpdateDashboardF: func(ctx context.Context, id platform.ID, upd platform.DashboardUpdate) (*platform.Dashboard, error) {
						return nil, &platform.Error{
							Code: platform.ENotFound,
							Msg:  platform.ErrDashboardNotFound,
						}
					},
				},
			},
			args: args{
				id:   "020f755c3c082000",
				name: "hello",
			},
			wants: wants{
				statusCode: http.StatusNotFound,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dashboardBackend := NewMockDashboardBackend(t)
			dashboardBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			dashboardBackend.DashboardService = tt.fields.DashboardService
			h := NewDashboardHandler(zaptest.NewLogger(t), dashboardBackend)

			upd := platform.DashboardUpdate{}
			if tt.args.name != "" {
				upd.Name = &tt.args.name
			}

			b, err := json.Marshal(upd)
			if err != nil {
				t.Fatalf("failed to unmarshal dashboard update: %v", err)
			}

			r := httptest.NewRequest("GET", "http://any.url", bytes.NewReader(b))

			r = r.WithContext(context.WithValue(
				context.Background(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.id,
					},
				}))

			w := httptest.NewRecorder()

			h.handlePatchDashboard(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePatchDashboard() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePatchDashboard() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handlePatchDashboard(). error unmarshaling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handlePatchDashboard() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestService_handlePostDashboardCell(t *testing.T) {
	type fields struct {
		DashboardService platform.DashboardService
	}
	type args struct {
		id   string
		body string
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "empty body",
			fields: fields{
				&mock.DashboardService{
					AddDashboardCellF: func(ctx context.Context, id platform.ID, c *platform.Cell, opt platform.AddDashboardCellOptions) error {
						c.ID = platformtesting.MustIDBase16("020f755c3c082000")
						return nil
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
			},
			wants: wants{
				statusCode:  http.StatusBadRequest,
				contentType: "application/json; charset=utf-8",
				body:        `{"code":"invalid","message":"bad request json body: EOF"}`,
			},
		},
		{
			name: "no properties",
			fields: fields{
				&mock.DashboardService{
					AddDashboardCellF: func(ctx context.Context, id platform.ID, c *platform.Cell, opt platform.AddDashboardCellOptions) error {
						c.ID = platformtesting.MustIDBase16("020f755c3c082000")
						return nil
					},
				},
			},
			args: args{
				id:   "020f755c3c082000",
				body: `{"bad":1}`,
			},
			wants: wants{
				statusCode:  http.StatusBadRequest,
				contentType: "application/json; charset=utf-8",
				body: `
				{
					"code": "invalid",
					"message": "req body is empty"
				}`,
			},
		},
		{
			name: "bad dash id",
			fields: fields{
				&mock.DashboardService{
					AddDashboardCellF: func(ctx context.Context, id platform.ID, c *platform.Cell, opt platform.AddDashboardCellOptions) error {
						c.ID = platformtesting.MustIDBase16("020f755c3c082000")
						return nil
					},
				},
			},
			args: args{
				id:   "fff",
				body: `{}`,
			},
			wants: wants{
				statusCode:  http.StatusBadRequest,
				contentType: "application/json; charset=utf-8",
				body: `
				{
					"code": "invalid",
					"message": "id must have a length of 16 bytes"
				}`,
			},
		},
		{
			name: "general create a dashboard cell",
			fields: fields{
				&mock.DashboardService{
					AddDashboardCellF: func(ctx context.Context, id platform.ID, c *platform.Cell, opt platform.AddDashboardCellOptions) error {
						c.ID = platformtesting.MustIDBase16("020f755c3c082000")
						return nil
					},
					GetDashboardCellViewF: func(ctx context.Context, id1, id2 platform.ID) (*platform.View, error) {
						return &platform.View{
							ViewContents: platform.ViewContents{
								ID: platformtesting.MustIDBase16("020f755c3c082001"),
							}}, nil
					},
				},
			},
			args: args{
				id:   "020f755c3c082000",
				body: `{"x":10,"y":11,"name":"name1","usingView":"020f755c3c082001"}`,
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "id": "020f755c3c082000",
  "x": 10,
  "y": 11,
  "w": 0,
  "h": 0,
  "links": {
    "self": "/api/v2/dashboards/020f755c3c082000/cells/020f755c3c082000",
    "view": "/api/v2/dashboards/020f755c3c082000/cells/020f755c3c082000/view"
  }
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dashboardBackend := NewMockDashboardBackend(t)
			dashboardBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			dashboardBackend.DashboardService = tt.fields.DashboardService
			h := NewDashboardHandler(zaptest.NewLogger(t), dashboardBackend)
			buf := new(bytes.Buffer)
			_, _ = buf.WriteString(tt.args.body)
			r := httptest.NewRequest("POST", "http://any.url", buf)

			r = r.WithContext(context.WithValue(
				context.Background(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.id,
					},
				}))

			w := httptest.NewRecorder()

			h.handlePostDashboardCell(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePostDashboardCell() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePostDashboardCell() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(tt.wants.body, string(body)); err != nil {
					t.Errorf("%q, handlePostDashboardCell(). error unmarshaling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handlePostDashboardCell() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestService_handleDeleteDashboardCell(t *testing.T) {
	type fields struct {
		DashboardService platform.DashboardService
	}
	type args struct {
		id     string
		cellID string
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "remove a dashboard cell",
			fields: fields{
				&mock.DashboardService{
					RemoveDashboardCellF: func(ctx context.Context, id platform.ID, cellID platform.ID) error {
						return nil
					},
				},
			},
			args: args{
				id:     "020f755c3c082000",
				cellID: "020f755c3c082000",
			},
			wants: wants{
				statusCode: http.StatusNoContent,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dashboardBackend := NewMockDashboardBackend(t)
			dashboardBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			dashboardBackend.DashboardService = tt.fields.DashboardService
			h := NewDashboardHandler(zaptest.NewLogger(t), dashboardBackend)

			r := httptest.NewRequest("GET", "http://any.url", nil)

			r = r.WithContext(context.WithValue(
				context.Background(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.id,
					},
					{
						Key:   "cellID",
						Value: tt.args.cellID,
					},
				}))

			w := httptest.NewRecorder()

			h.handleDeleteDashboardCell(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleDeleteDashboardCell() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleDeleteDashboardCell() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handleDeleteDashboardCell(). error unmarshaling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handleDeleteDashboardCell() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestService_handlePatchDashboardCell(t *testing.T) {
	type fields struct {
		DashboardService platform.DashboardService
	}
	type args struct {
		id     string
		cellID string
		x      int32
		y      int32
		w      int32
		h      int32
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "update a dashboard cell",
			fields: fields{
				&mock.DashboardService{
					UpdateDashboardCellF: func(ctx context.Context, id, cellID platform.ID, upd platform.CellUpdate) (*platform.Cell, error) {
						cell := &platform.Cell{
							ID: platformtesting.MustIDBase16("020f755c3c082000"),
						}

						if err := upd.Apply(cell); err != nil {
							return nil, err
						}

						return cell, nil
					},
				},
			},
			args: args{
				id:     "020f755c3c082000",
				cellID: "020f755c3c082000",
				x:      10,
				y:      11,
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "id": "020f755c3c082000",
  "x": 10,
  "y": 11,
  "w": 0,
  "h": 0,
  "links": {
    "self": "/api/v2/dashboards/020f755c3c082000/cells/020f755c3c082000",
    "view": "/api/v2/dashboards/020f755c3c082000/cells/020f755c3c082000/view"
  }
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dashboardBackend := NewMockDashboardBackend(t)
			dashboardBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			dashboardBackend.DashboardService = tt.fields.DashboardService
			h := NewDashboardHandler(zaptest.NewLogger(t), dashboardBackend)

			upd := platform.CellUpdate{}
			if tt.args.x != 0 {
				upd.X = &tt.args.x
			}
			if tt.args.y != 0 {
				upd.Y = &tt.args.y
			}
			if tt.args.w != 0 {
				upd.W = &tt.args.w
			}
			if tt.args.h != 0 {
				upd.H = &tt.args.h
			}

			b, err := json.Marshal(upd)
			if err != nil {
				t.Fatalf("failed to unmarshal cell: %v", err)
			}

			r := httptest.NewRequest("GET", "http://any.url", bytes.NewReader(b))

			r = r.WithContext(context.WithValue(
				context.Background(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.id,
					},
					{
						Key:   "cellID",
						Value: tt.args.cellID,
					},
				}))

			w := httptest.NewRecorder()

			h.handlePatchDashboardCell(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePatchDashboardCell() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePatchDashboardCell() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handlePatchDashboardCell(). error unmarshaling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handlePatchDashboardCell() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func Test_dashboardCellIDPath(t *testing.T) {
	t.Parallel()
	dashboard, err := platform.IDFromString("deadbeefdeadbeef")
	if err != nil {
		t.Fatal(err)
	}

	cell, err := platform.IDFromString("cade9a7ecade9a7e")
	if err != nil {
		t.Fatal(err)
	}

	want := "/api/v2/dashboards/deadbeefdeadbeef/cells/cade9a7ecade9a7e"
	if got := dashboardCellIDPath(*dashboard, *cell); got != want {
		t.Errorf("dashboardCellIDPath() = got: %s want: %s", got, want)
	}
}

func initDashboardService(f platformtesting.DashboardFields, t *testing.T) (platform.DashboardService, string, func()) {
	t.Helper()
	svc := newInMemKVSVC(t)
	svc.IDGenerator = f.IDGenerator
	ctx := context.Background()
	for _, d := range f.Dashboards {
		if err := svc.PutDashboard(ctx, d); err != nil {
			t.Fatalf("failed to populate dashboard")
		}
	}

	dashboardBackend := NewMockDashboardBackend(t)
	dashboardBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
	dashboardBackend.DashboardService = svc
	h := NewDashboardHandler(zaptest.NewLogger(t), dashboardBackend)
	server := httptest.NewServer(h)

	client := DashboardService{Client: mustNewHTTPClient(t, server.URL, "")}

	return &client, "", server.Close
}

func TestDashboardService(t *testing.T) {
	t.Parallel()
	platformtesting.DeleteDashboard(initDashboardService, t)
}

func TestService_handlePostDashboardLabel(t *testing.T) {
	type fields struct {
		LabelService platform.LabelService
	}
	type args struct {
		labelMapping *platform.LabelMapping
		dashboardID  platform.ID
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "add label to dashboard",
			fields: fields{
				LabelService: &mock.LabelService{
					FindLabelByIDFn: func(ctx context.Context, id platform.ID) (*platform.Label, error) {
						return &platform.Label{
							ID:   1,
							Name: "label",
							Properties: map[string]string{
								"color": "fff000",
							},
						}, nil
					},
					CreateLabelMappingFn: func(ctx context.Context, m *platform.LabelMapping) error { return nil },
				},
			},
			args: args{
				labelMapping: &platform.LabelMapping{
					ResourceID: 100,
					LabelID:    1,
				},
				dashboardID: 100,
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "label": {
    "id": "0000000000000001",
    "name": "label",
    "properties": {
      "color": "fff000"
    }
  },
  "links": {
    "self": "/api/v2/labels/0000000000000001"
  }
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dashboardBackend := NewMockDashboardBackend(t)
			dashboardBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			dashboardBackend.LabelService = tt.fields.LabelService
			h := NewDashboardHandler(zaptest.NewLogger(t), dashboardBackend)

			b, err := json.Marshal(tt.args.labelMapping)
			if err != nil {
				t.Fatalf("failed to unmarshal label mapping: %v", err)
			}

			url := fmt.Sprintf("http://localhost:9999/api/v2/dashboards/%s/labels", tt.args.dashboardID)
			r := httptest.NewRequest("POST", url, bytes.NewReader(b))
			w := httptest.NewRecorder()

			h.ServeHTTP(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("got %v, want %v", res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("got %v, want %v", content, tt.wants.contentType)
			}
			if eq, diff, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("Diff\n%s", diff)
			}
		})
	}
}

func jsonEqual(s1, s2 string) (eq bool, diff string, err error) {
	if s1 == s2 {
		return true, "", nil
	}

	if s1 == "" {
		return false, s2, fmt.Errorf("s1 is empty")
	}

	if s2 == "" {
		return false, s1, fmt.Errorf("s2 is empty")
	}

	var o1 interface{}
	if err = json.Unmarshal([]byte(s1), &o1); err != nil {
		return
	}

	var o2 interface{}
	if err = json.Unmarshal([]byte(s2), &o2); err != nil {
		return
	}

	differ := gojsondiff.New()
	d, err := differ.Compare([]byte(s1), []byte(s2))
	if err != nil {
		return
	}

	config := formatter.AsciiFormatterConfig{}

	formatter := formatter.NewAsciiFormatter(o1, config)
	diff, err = formatter.Format(d)

	return cmp.Equal(o1, o2), diff, err
}

func newInMemKVSVC(t *testing.T) *kv.Service {
	t.Helper()

	svc := kv.NewService(zaptest.NewLogger(t), inmem.NewKVStore())
	if err := svc.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	return svc
}
