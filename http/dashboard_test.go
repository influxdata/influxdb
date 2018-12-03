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

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/inmem"
	"github.com/influxdata/platform/mock"
	platformtesting "github.com/influxdata/platform/testing"
	"github.com/julienschmidt/httprouter"
)

func TestService_handleGetDashboards(t *testing.T) {
	type fields struct {
		DashboardService platform.DashboardService
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
								ID:          platformtesting.MustIDBase16("da7aba5e5d81e550"),
								Name:        "hello",
								Description: "oh hello there!",
								Meta: platform.DashboardMeta{
									CreatedAt: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
									UpdatedAt: time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC),
								},
								Cells: []*platform.Cell{
									{
										ID:     platformtesting.MustIDBase16("da7aba5e5d81e550"),
										X:      1,
										Y:      2,
										W:      3,
										H:      4,
										ViewID: platformtesting.MustIDBase16("ba0bab707a11ed12"),
									},
								},
							},
							{
								ID: platformtesting.MustIDBase16("0ca2204eca2204e0"),
								Meta: platform.DashboardMeta{
									CreatedAt: time.Date(2012, time.November, 10, 23, 0, 0, 0, time.UTC),
									UpdatedAt: time.Date(2012, time.November, 10, 24, 0, 0, 0, time.UTC),
								},
								Name: "example",
							},
						}, 2, nil
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
    "self": "/api/v2/dashboards"
  },
  "dashboards": [
    {
      "id": "da7aba5e5d81e550",
      "name": "hello",
      "description": "oh hello there!",
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
          "viewID": "ba0bab707a11ed12",
          "links": {
            "self": "/api/v2/dashboards/da7aba5e5d81e550/cells/da7aba5e5d81e550",
            "view": "/api/v2/views/ba0bab707a11ed12"
          }
        }
      ],
      "links": {
        "self": "/api/v2/dashboards/da7aba5e5d81e550",
        "cells": "/api/v2/dashboards/da7aba5e5d81e550/cells",
        "log": "/api/v2/dashboards/da7aba5e5d81e550/log"
      }
    },
    {
      "id": "0ca2204eca2204e0",
      "name": "example",
      "description": "",
      "meta": {
        "createdAt": "2012-11-10T23:00:00Z",
        "updatedAt": "2012-11-11T00:00:00Z"
      },
      "cells": [],
      "links": {
        "self": "/api/v2/dashboards/0ca2204eca2204e0",
        "log": "/api/v2/dashboards/0ca2204eca2204e0/log",
        "cells": "/api/v2/dashboards/0ca2204eca2204e0/cells"
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
			},
			args: args{},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/dashboards"
  },
  "dashboards": []
}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mappingService := mock.NewUserResourceMappingService()
			h := NewDashboardHandler(mappingService)
			h.DashboardService = tt.fields.DashboardService

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
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handleGetDashboards() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}

		})
	}
}

func TestService_handleGetDashboard(t *testing.T) {
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
			name: "get a dashboard by id",
			fields: fields{
				&mock.DashboardService{
					FindDashboardByIDF: func(ctx context.Context, id platform.ID) (*platform.Dashboard, error) {
						if id == platformtesting.MustIDBase16("020f755c3c082000") {
							return &platform.Dashboard{
								ID: platformtesting.MustIDBase16("020f755c3c082000"),
								Meta: platform.DashboardMeta{
									CreatedAt: time.Date(2012, time.November, 10, 23, 0, 0, 0, time.UTC),
									UpdatedAt: time.Date(2012, time.November, 10, 24, 0, 0, 0, time.UTC),
								},
								Name: "hello",
								Cells: []*platform.Cell{
									{
										ID:     platformtesting.MustIDBase16("da7aba5e5d81e550"),
										X:      1,
										Y:      2,
										W:      3,
										H:      4,
										ViewID: platformtesting.MustIDBase16("ba0bab707a11ed12"),
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
  "name": "hello",
  "description": "",
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
      "viewID": "ba0bab707a11ed12",
      "links": {
        "self": "/api/v2/dashboards/020f755c3c082000/cells/da7aba5e5d81e550",
        "view": "/api/v2/views/ba0bab707a11ed12"
      }
    }
  ],
  "links": {
    "self": "/api/v2/dashboards/020f755c3c082000",
    "log": "/api/v2/dashboards/020f755c3c082000/log",
    "cells": "/api/v2/dashboards/020f755c3c082000/cells"
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
						return nil, platform.ErrDashboardNotFound
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
			mappingService := mock.NewUserResourceMappingService()
			h := NewDashboardHandler(mappingService)
			h.DashboardService = tt.fields.DashboardService

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
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handleGetDashboard() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
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
					ID:          platformtesting.MustIDBase16("020f755c3c082000"),
					Name:        "hello",
					Description: "howdy there",
					Cells: []*platform.Cell{
						{
							ID:     platformtesting.MustIDBase16("da7aba5e5d81e550"),
							X:      1,
							Y:      2,
							W:      3,
							H:      4,
							ViewID: platformtesting.MustIDBase16("ba0bab707a11ed12"),
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
  "name": "hello",
  "description": "howdy there",
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
      "viewID": "ba0bab707a11ed12",
      "links": {
        "self": "/api/v2/dashboards/020f755c3c082000/cells/da7aba5e5d81e550",
        "view": "/api/v2/views/ba0bab707a11ed12"
      }
    }
  ],
  "links": {
    "self": "/api/v2/dashboards/020f755c3c082000",
    "log": "/api/v2/dashboards/020f755c3c082000/log",
    "cells": "/api/v2/dashboards/020f755c3c082000/cells"
  }
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mappingService := mock.NewUserResourceMappingService()
			h := NewDashboardHandler(mappingService)
			h.DashboardService = tt.fields.DashboardService

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
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handlePostDashboard() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
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
						return platform.ErrDashboardNotFound
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
			mappingService := mock.NewUserResourceMappingService()
			h := NewDashboardHandler(mappingService)
			h.DashboardService = tt.fields.DashboardService

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
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handleDeleteDashboard() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
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
								ID:   platformtesting.MustIDBase16("020f755c3c082000"),
								Name: "hello",
								Meta: platform.DashboardMeta{
									CreatedAt: time.Date(2012, time.November, 10, 23, 0, 0, 0, time.UTC),
									UpdatedAt: time.Date(2012, time.November, 10, 25, 0, 0, 0, time.UTC),
								},
								Cells: []*platform.Cell{
									{
										ID:     platformtesting.MustIDBase16("da7aba5e5d81e550"),
										X:      1,
										Y:      2,
										W:      3,
										H:      4,
										ViewID: platformtesting.MustIDBase16("ba0bab707a11ed12"),
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
  "name": "example",
  "description": "",
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
      "viewID": "ba0bab707a11ed12",
      "links": {
        "self": "/api/v2/dashboards/020f755c3c082000/cells/da7aba5e5d81e550",
        "view": "/api/v2/views/ba0bab707a11ed12"
      }
    }
  ],
  "links": {
    "self": "/api/v2/dashboards/020f755c3c082000",
    "log": "/api/v2/dashboards/020f755c3c082000/log",
    "cells": "/api/v2/dashboards/020f755c3c082000/cells"
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
						return nil, platform.ErrDashboardNotFound
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
			mappingService := mock.NewUserResourceMappingService()
			h := NewDashboardHandler(mappingService)
			h.DashboardService = tt.fields.DashboardService

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
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handlePatchDashboard() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
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
		cell *platform.Cell
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
			name: "create a dashboard cell",
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
				cell: &platform.Cell{
					ID:     platformtesting.MustIDBase16("020f755c3c082000"),
					X:      10,
					Y:      11,
					ViewID: platformtesting.MustIDBase16("da7aba5e5d81e550"),
				},
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
  "viewID": "da7aba5e5d81e550",
  "links": {
    "self": "/api/v2/dashboards/020f755c3c082000/cells/020f755c3c082000",
    "view": "/api/v2/views/da7aba5e5d81e550"
  }
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mappingService := mock.NewUserResourceMappingService()
			h := NewDashboardHandler(mappingService)
			h.DashboardService = tt.fields.DashboardService

			b, err := json.Marshal(tt.args.cell)
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
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handlePostDashboardCell() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
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
			mappingService := mock.NewUserResourceMappingService()
			h := NewDashboardHandler(mappingService)
			h.DashboardService = tt.fields.DashboardService

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
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handleDeleteDashboardCell() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
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
		viewID platform.ID
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
							ID:     platformtesting.MustIDBase16("020f755c3c082000"),
							ViewID: platformtesting.MustIDBase16("da7aba5e5d81e550"),
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
				viewID: platformtesting.MustIDBase16("da7aba5e5d81e550"),
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
  "viewID": "da7aba5e5d81e550",
  "links": {
    "self": "/api/v2/dashboards/020f755c3c082000/cells/020f755c3c082000",
    "view": "/api/v2/views/da7aba5e5d81e550"
  }
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mappingService := mock.NewUserResourceMappingService()
			h := NewDashboardHandler(mappingService)
			h.DashboardService = tt.fields.DashboardService

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
			if tt.args.viewID.Valid() {
				upd.ViewID = tt.args.viewID
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
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handlePatchDashboardCell() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
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

func initDashboardService(f platformtesting.DashboardFields, t *testing.T) (platform.DashboardService, func()) {
	t.Helper()
	svc := inmem.NewService()
	svc.IDGenerator = f.IDGenerator
	svc.WithTime(f.NowFn)
	ctx := context.Background()
	for _, d := range f.Dashboards {
		if err := svc.PutDashboard(ctx, d); err != nil {
			t.Fatalf("failed to populate dashboard")
		}
	}
	for _, b := range f.Views {
		if err := svc.PutView(ctx, b); err != nil {
			t.Fatalf("failed to populate views")
		}
	}

	mappingService := mock.NewUserResourceMappingService()
	handler := NewDashboardHandler(mappingService)
	handler.DashboardService = svc
	server := httptest.NewServer(handler)
	client := DashboardService{
		Addr: server.URL,
	}
	done := server.Close

	return &client, done
}

func TestDashboardService(t *testing.T) {
	t.Parallel()
	platformtesting.DashboardService(initDashboardService, t)
}
