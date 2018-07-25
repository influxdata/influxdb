package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bouk/httprouter"
	"github.com/influxdata/platform/chronograf/log"
	"github.com/influxdata/platform/chronograf/mocks"
	"github.com/influxdata/platform/chronograf/v2"
)

func TestService_DashboardsV2(t *testing.T) {
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
				&mocks.DashboardService{
					FindDashboardsF: func(ctx context.Context, filter platform.DashboardFilter) ([]*platform.Dashboard, int, error) {
						return []*platform.Dashboard{
							{
								ID:   platform.ID("0"),
								Name: "hello",
								Cells: []platform.DashboardCell{
									{
										X:   1,
										Y:   2,
										W:   3,
										H:   4,
										Ref: "/chronograf/v2/cells/12",
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
				contentType: "application/json",
				body: `
{
  "links": {
    "self": "/chronograf/v2/dashboards"
  },
  "dashboards": [
    {
      "id": "0",
      "name": "hello",
      "cells": [
        {
          "x": 1,
          "y": 2,
          "w": 3,
          "h": 4,
          "ref": "/chronograf/v2/cells/12"
        }
      ],
      "links": {
        "self": "/chronograf/v2/dashboards/0"
      }
    },
    {
      "id": "2",
      "name": "example",
      "cells": [],
      "links": {
        "self": "/chronograf/v2/dashboards/2"
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
				&mocks.DashboardService{
					FindDashboardsF: func(ctx context.Context, filter platform.DashboardFilter) ([]*platform.Dashboard, int, error) {
						return []*platform.Dashboard{}, 0, nil
					},
				},
			},
			args: args{},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json",
				body: `
{
  "links": {
    "self": "/chronograf/v2/dashboards"
  },
  "dashboards": []
}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &mocks.Store{
					DashboardService: tt.fields.DashboardService,
				},
				Logger: log.New(log.DebugLevel),
			}

			r := httptest.NewRequest("GET", "http://any.url", nil)

			qp := r.URL.Query()
			for k, vs := range tt.args.queryParams {
				for _, v := range vs {
					qp.Add(k, v)
				}
			}
			r.URL.RawQuery = qp.Encode()

			w := httptest.NewRecorder()

			s.DashboardsV2(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. DashboardsV2() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. DashboardsV2() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. DashboardsV2() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}

		})
	}
}

func TestService_DashboardIDV2(t *testing.T) {
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
				&mocks.DashboardService{
					FindDashboardByIDF: func(ctx context.Context, id platform.ID) (*platform.Dashboard, error) {
						if id == "2" {
							return &platform.Dashboard{
								ID:   platform.ID("2"),
								Name: "hello",
								Cells: []platform.DashboardCell{
									{
										X:   1,
										Y:   2,
										W:   3,
										H:   4,
										Ref: "/chronograf/v2/cells/12",
									},
								},
							}, nil
						}

						return nil, fmt.Errorf("not found")
					},
				},
			},
			args: args{
				id: "2",
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json",
				body: `
{
  "id": "2",
  "name": "hello",
  "cells": [
    {
      "x": 1,
      "y": 2,
      "w": 3,
      "h": 4,
      "ref": "/chronograf/v2/cells/12"
    }
  ],
  "links": {
    "self": "/chronograf/v2/dashboards/2"
  }
}
`,
			},
		},
		{
			name: "not found",
			fields: fields{
				&mocks.DashboardService{
					FindDashboardByIDF: func(ctx context.Context, id platform.ID) (*platform.Dashboard, error) {
						return nil, platform.ErrDashboardNotFound
					},
				},
			},
			args: args{
				id: "2",
			},
			wants: wants{
				statusCode:  http.StatusNotFound,
				contentType: "application/json",
				body:        `{"code":404,"message":"dashboard not found"}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &mocks.Store{
					DashboardService: tt.fields.DashboardService,
				},
				Logger: log.New(log.DebugLevel),
			}

			r := httptest.NewRequest("GET", "http://any.url", nil)

			r = r.WithContext(httprouter.WithParams(
				context.Background(),
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.id,
					},
				}))

			w := httptest.NewRecorder()

			s.DashboardIDV2(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. DashboardIDV2() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. DashboardIDV2() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. DashboardIDV2() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}
		})
	}
}

func TestService_NewDashboardV2(t *testing.T) {
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
				&mocks.DashboardService{
					CreateDashboardF: func(ctx context.Context, c *platform.Dashboard) error {
						c.ID = "2"
						return nil
					},
				},
			},
			args: args{
				dashboard: &platform.Dashboard{
					Name: "hello",
					Cells: []platform.DashboardCell{
						{
							X:   1,
							Y:   2,
							W:   3,
							H:   4,
							Ref: "/chronograf/v2/cells/12",
						},
					},
				},
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json",
				body: `
{
  "id": "2",
  "name": "hello",
  "cells": [
    {
      "x": 1,
      "y": 2,
      "w": 3,
      "h": 4,
      "ref": "/chronograf/v2/cells/12"
    }
  ],
  "links": {
    "self": "/chronograf/v2/dashboards/2"
  }
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &mocks.Store{
					DashboardService: tt.fields.DashboardService,
				},
				Logger: log.New(log.DebugLevel),
			}

			b, err := json.Marshal(tt.args.dashboard)
			if err != nil {
				t.Fatalf("failed to unmarshal dashboard: %v", err)
			}

			r := httptest.NewRequest("GET", "http://any.url", bytes.NewReader(b))
			w := httptest.NewRecorder()

			s.NewDashboardV2(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. DashboardIDV2() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. DashboardIDV2() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. DashboardIDV2() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}
		})
	}
}

func TestService_RemoveDashboardV2(t *testing.T) {
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
				&mocks.DashboardService{
					DeleteDashboardF: func(ctx context.Context, id platform.ID) error {
						if id == "2" {
							return nil
						}

						return fmt.Errorf("wrong id")
					},
				},
			},
			args: args{
				id: "2",
			},
			wants: wants{
				statusCode: http.StatusNoContent,
			},
		},
		{
			name: "dashboard not found",
			fields: fields{
				&mocks.DashboardService{
					DeleteDashboardF: func(ctx context.Context, id platform.ID) error {
						return platform.ErrDashboardNotFound
					},
				},
			},
			args: args{
				id: "2",
			},
			wants: wants{
				statusCode:  http.StatusNotFound,
				contentType: "application/json",
				body:        `{"code":404,"message":"dashboard not found"}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &mocks.Store{
					DashboardService: tt.fields.DashboardService,
				},
				Logger: log.New(log.DebugLevel),
			}

			r := httptest.NewRequest("GET", "http://any.url", nil)

			r = r.WithContext(httprouter.WithParams(
				context.Background(),
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.id,
					},
				}))

			w := httptest.NewRecorder()

			s.RemoveDashboardV2(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. RemoveDashboardV2() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. RemoveDashboardV2() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. RemoveDashboardV2() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}
		})
	}
}

func TestService_UpdateDashboardV2(t *testing.T) {
	type fields struct {
		DashboardService platform.DashboardService
	}
	type args struct {
		id    string
		name  string
		cells []platform.DashboardCell
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
				&mocks.DashboardService{
					UpdateDashboardF: func(ctx context.Context, id platform.ID, upd platform.DashboardUpdate) (*platform.Dashboard, error) {
						if id == "2" {
							d := &platform.Dashboard{
								ID:   platform.ID("2"),
								Name: "hello",
								Cells: []platform.DashboardCell{
									{
										X:   1,
										Y:   2,
										W:   3,
										H:   4,
										Ref: "/chronograf/v2/cells/12",
									},
								},
							}

							if upd.Name != nil {
								d.Name = *upd.Name
							}

							if upd.Cells != nil {
								d.Cells = upd.Cells
							}

							return d, nil
						}

						return nil, fmt.Errorf("not found")
					},
				},
			},
			args: args{
				id:   "2",
				name: "example",
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json",
				body: `
{
  "id": "2",
  "name": "example",
  "cells": [
    {
      "x": 1,
      "y": 2,
      "w": 3,
      "h": 4,
      "ref": "/chronograf/v2/cells/12"
    }
  ],
  "links": {
    "self": "/chronograf/v2/dashboards/2"
  }
}
`,
			},
		},
		{
			name: "update a dashboard cells",
			fields: fields{
				&mocks.DashboardService{
					UpdateDashboardF: func(ctx context.Context, id platform.ID, upd platform.DashboardUpdate) (*platform.Dashboard, error) {
						if id == "2" {
							d := &platform.Dashboard{
								ID:   platform.ID("2"),
								Name: "hello",
								Cells: []platform.DashboardCell{
									{
										X:   1,
										Y:   2,
										W:   3,
										H:   4,
										Ref: "/chronograf/v2/cells/12",
									},
								},
							}

							if upd.Name != nil {
								d.Name = *upd.Name
							}

							if upd.Cells != nil {
								d.Cells = upd.Cells
							}

							return d, nil
						}

						return nil, fmt.Errorf("not found")
					},
				},
			},
			args: args{
				id: "2",
				cells: []platform.DashboardCell{
					{
						X:   1,
						Y:   2,
						W:   3,
						H:   4,
						Ref: "/chronograf/v2/cells/12",
					},
					{
						X:   2,
						Y:   3,
						W:   4,
						H:   5,
						Ref: "/chronograf/v2/cells/1",
					},
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json",
				body: `
{
  "id": "2",
  "name": "hello",
  "cells": [
    {
      "x": 1,
      "y": 2,
      "w": 3,
      "h": 4,
      "ref": "/chronograf/v2/cells/12"
    },
    {
      "x": 2,
      "y": 3,
      "w": 4,
      "h": 5,
      "ref": "/chronograf/v2/cells/1"
    }
  ],
  "links": {
    "self": "/chronograf/v2/dashboards/2"
  }
}
`,
			},
		},
		{
			name: "update a dashboard with empty request body",
			fields: fields{
				&mocks.DashboardService{
					UpdateDashboardF: func(ctx context.Context, id platform.ID, upd platform.DashboardUpdate) (*platform.Dashboard, error) {
						return nil, fmt.Errorf("not found")
					},
				},
			},
			args: args{
				id: "2",
			},
			wants: wants{
				statusCode:  http.StatusBadRequest,
				contentType: "application/json",
				body:        `{"code":400,"message":"must update at least one attribute"}`,
			},
		},
		{
			name: "dashboard not found",
			fields: fields{
				&mocks.DashboardService{
					UpdateDashboardF: func(ctx context.Context, id platform.ID, upd platform.DashboardUpdate) (*platform.Dashboard, error) {
						return nil, platform.ErrDashboardNotFound
					},
				},
			},
			args: args{
				id:   "2",
				name: "hello",
			},
			wants: wants{
				statusCode:  http.StatusNotFound,
				contentType: "application/json",
				body:        `{"code":404,"message":"dashboard not found"}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &mocks.Store{
					DashboardService: tt.fields.DashboardService,
				},
				Logger: log.New(log.DebugLevel),
			}

			upd := platform.DashboardUpdate{}
			if tt.args.name != "" {
				upd.Name = &tt.args.name
			}
			if tt.args.cells != nil {
				upd.Cells = tt.args.cells
			}

			b, err := json.Marshal(upd)
			if err != nil {
				t.Fatalf("failed to unmarshal dashboard update: %v", err)
			}

			r := httptest.NewRequest("GET", "http://any.url", bytes.NewReader(b))

			r = r.WithContext(httprouter.WithParams(
				context.Background(),
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.id,
					},
				}))

			w := httptest.NewRecorder()

			s.UpdateDashboardV2(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. UpdateDashboardV2() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. UpdateDashboardV2() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. UpdateDashboardV2() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}
		})
	}
}
