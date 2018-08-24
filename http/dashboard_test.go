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

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/mock"
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
					FindDashboardsF: func(ctx context.Context, filter platform.DashboardFilter) ([]*platform.Dashboard, int, error) {
						return []*platform.Dashboard{
							{
								ID:   platform.ID("0"),
								Name: "hello",
								Cells: []*platform.Cell{
									{
										ID:     platform.ID("0"),
										X:      1,
										Y:      2,
										W:      3,
										H:      4,
										ViewID: platform.ID("1"),
									},
								},
							},
							{
								ID:   platform.ID("2"),
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
    "self": "/v2/dashboards"
  },
  "dashboards": [
    {
      "id": "30",
      "name": "hello",
      "cells": [
        {
          "id": "30",
          "x": 1,
          "y": 2,
          "w": 3,
          "h": 4,
          "viewID": "31",
          "links": {
            "self": "/v2/dashboards/30/cells/30",
            "view": "/v2/views/31"
          }
        }
      ],
      "links": {
        "self": "/v2/dashboards/30",
        "cells": "/v2/dashboards/30/cells"
      }
    },
    {
      "id": "32",
      "name": "example",
      "cells": [],
      "links": {
        "self": "/v2/dashboards/32",
        "cells": "/v2/dashboards/32/cells"
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
					FindDashboardsF: func(ctx context.Context, filter platform.DashboardFilter) ([]*platform.Dashboard, int, error) {
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
    "self": "/v2/dashboards"
  },
  "dashboards": []
}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewDashboardHandler()
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
						if bytes.Equal(id, mustParseID("020f755c3c082000")) {
							return &platform.Dashboard{
								ID:   mustParseID("020f755c3c082000"),
								Name: "hello",
								Cells: []*platform.Cell{
									{
										ID:     platform.ID("0"),
										X:      1,
										Y:      2,
										W:      3,
										H:      4,
										ViewID: platform.ID("1"),
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
  "cells": [
    {
      "id": "30",
      "x": 1,
      "y": 2,
      "w": 3,
      "h": 4,
      "viewID": "31",
      "links": {
        "self": "/v2/dashboards/020f755c3c082000/cells/30",
        "view": "/v2/views/31"
      }
    }
  ],
  "links": {
    "self": "/v2/dashboards/020f755c3c082000",
    "cells": "/v2/dashboards/020f755c3c082000/cells"
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
			h := NewDashboardHandler()
			h.DashboardService = tt.fields.DashboardService

			r := httptest.NewRequest("GET", "http://any.url", nil)

			r = r.WithContext(context.WithValue(
				context.TODO(),
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
						c.ID = mustParseID("020f755c3c082000")
						return nil
					},
				},
			},
			args: args{
				dashboard: &platform.Dashboard{
					Name: "hello",
					Cells: []*platform.Cell{
						{
							ID:     platform.ID("0"),
							X:      1,
							Y:      2,
							W:      3,
							H:      4,
							ViewID: platform.ID("1"),
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
  "cells": [
    {
      "id": "30",
      "x": 1,
      "y": 2,
      "w": 3,
      "h": 4,
      "viewID": "31",
      "links": {
        "self": "/v2/dashboards/020f755c3c082000/cells/30",
        "view": "/v2/views/31"
      }
    }
  ],
  "links": {
    "self": "/v2/dashboards/020f755c3c082000",
    "cells": "/v2/dashboards/020f755c3c082000/cells"
  }
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewDashboardHandler()
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
						if bytes.Equal(id, mustParseID("020f755c3c082000")) {
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
			h := NewDashboardHandler()
			h.DashboardService = tt.fields.DashboardService

			r := httptest.NewRequest("GET", "http://any.url", nil)

			r = r.WithContext(context.WithValue(
				context.TODO(),
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
		id    string
		name  string
		cells []*platform.Cell
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
						if bytes.Equal(id, mustParseID("020f755c3c082000")) {
							d := &platform.Dashboard{
								ID:   mustParseID("020f755c3c082000"),
								Name: "hello",
								Cells: []*platform.Cell{
									{
										ID:     platform.ID("0"),
										X:      1,
										Y:      2,
										W:      3,
										H:      4,
										ViewID: platform.ID("1"),
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
  "cells": [
    {
      "id": "30",
      "x": 1,
      "y": 2,
      "w": 3,
      "h": 4,
      "viewID": "31",
      "links": {
        "self": "/v2/dashboards/020f755c3c082000/cells/30",
        "view": "/v2/views/31"
      }
    }
  ],
  "links": {
    "self": "/v2/dashboards/020f755c3c082000",
    "cells": "/v2/dashboards/020f755c3c082000/cells"
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
			h := NewDashboardHandler()
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
				context.TODO(),
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
						c.ID = mustParseID("020f755c3c082000")
						return nil
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
				cell: &platform.Cell{
					X:      10,
					Y:      11,
					ViewID: platform.ID("0"),
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
  "viewID": "30",
  "links": {
    "self": "/v2/dashboards/020f755c3c082000/cells/020f755c3c082000",
    "view": "/v2/views/30"
  }
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewDashboardHandler()
			h.DashboardService = tt.fields.DashboardService

			b, err := json.Marshal(tt.args.cell)
			if err != nil {
				t.Fatalf("failed to unmarshal cell: %v", err)
			}

			r := httptest.NewRequest("GET", "http://any.url", bytes.NewReader(b))

			r = r.WithContext(context.WithValue(
				context.TODO(),
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
			h := NewDashboardHandler()
			h.DashboardService = tt.fields.DashboardService

			r := httptest.NewRequest("GET", "http://any.url", nil)

			r = r.WithContext(context.WithValue(
				context.TODO(),
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
							ID:     mustParseID("020f755c3c082000"),
							ViewID: platform.ID("0"),
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
  "viewID": "30",
  "links": {
    "self": "/v2/dashboards/020f755c3c082000/cells/020f755c3c082000",
    "view": "/v2/views/30"
  }
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewDashboardHandler()
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
			if len(tt.args.viewID) != 0 {
				upd.ViewID = &tt.args.viewID
			}

			b, err := json.Marshal(upd)
			if err != nil {
				t.Fatalf("failed to unmarshal cell: %v", err)
			}

			r := httptest.NewRequest("GET", "http://any.url", bytes.NewReader(b))

			r = r.WithContext(context.WithValue(
				context.TODO(),
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
