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

func TestService_CellsV2(t *testing.T) {
	type fields struct {
		CellService platform.CellService
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
			name: "get all cells",
			fields: fields{
				&mocks.CellService{
					FindCellsF: func(ctx context.Context, filter platform.CellFilter) ([]*platform.Cell, int, error) {
						return []*platform.Cell{
							{
								CellContents: platform.CellContents{
									ID:   platform.ID("0"),
									Name: "hello",
								},
								Visualization: platform.V1Visualization{
									Type: "line",
								},
							},
							{
								CellContents: platform.CellContents{
									ID:   platform.ID("2"),
									Name: "example",
								},
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
    "self": "/chronograf/v2/cells"
  },
  "cells": [
    {
      "id": "0",
      "name": "hello",
      "links": {
        "self": "/chronograf/v2/cells/0"
      },
      "visualization": {
        "type": "chronograf-v1",
        "queries": null,
        "axes": null,
        "visualizationType": "line",
        "colors": null,
        "legend": {},
        "tableOptions": {
          "verticalTimeAxis": false,
          "sortBy": {
            "internalName": "",
            "displayName": "",
            "visible": false
          },
          "wrapping": "",
          "fixFirstColumn": false
        },
        "fieldOptions": null,
        "timeFormat": "",
        "decimalPlaces": {
          "isEnforced": false,
          "digits": 0
        }
      }
    },
    {
      "id": "2",
      "name": "example",
      "links": {
        "self": "/chronograf/v2/cells/2"
      },
      "visualization": {
        "type": "empty"
      }
    }
  ]
}`,
			},
		},
		{
			name: "get all cells when there are none",
			fields: fields{
				&mocks.CellService{
					FindCellsF: func(ctx context.Context, filter platform.CellFilter) ([]*platform.Cell, int, error) {
						return []*platform.Cell{}, 0, nil
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
    "self": "/chronograf/v2/cells"
  },
  "cells": []
}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &mocks.Store{
					CellService: tt.fields.CellService,
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

			s.CellsV2(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. CellsV2() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. CellsV2() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. CellsV2() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}

		})
	}
}

func TestService_CellIDV2(t *testing.T) {
	type fields struct {
		CellService platform.CellService
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
			name: "get a cell by id",
			fields: fields{
				&mocks.CellService{
					FindCellByIDF: func(ctx context.Context, id platform.ID) (*platform.Cell, error) {
						if id == "2" {
							return &platform.Cell{
								CellContents: platform.CellContents{
									ID:   platform.ID("2"),
									Name: "example",
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
  "name": "example",
  "links": {
    "self": "/chronograf/v2/cells/2"
  },
  "visualization": {
    "type": "empty"
  }
}
`,
			},
		},
		{
			name: "not found",
			fields: fields{
				&mocks.CellService{
					FindCellByIDF: func(ctx context.Context, id platform.ID) (*platform.Cell, error) {
						return nil, platform.ErrCellNotFound
					},
				},
			},
			args: args{
				id: "2",
			},
			wants: wants{
				statusCode:  http.StatusNotFound,
				contentType: "application/json",
				body:        `{"code":404,"message":"cell not found"}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &mocks.Store{
					CellService: tt.fields.CellService,
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

			s.CellIDV2(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. CellIDV2() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. CellIDV2() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. CellIDV2() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}
		})
	}
}

func TestService_NewCellV2(t *testing.T) {
	type fields struct {
		CellService platform.CellService
	}
	type args struct {
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
			name: "create a new cell",
			fields: fields{
				&mocks.CellService{
					CreateCellF: func(ctx context.Context, c *platform.Cell) error {
						c.ID = "2"
						return nil
					},
				},
			},
			args: args{
				cell: &platform.Cell{
					CellContents: platform.CellContents{
						Name: "hello",
					},
					Visualization: platform.V1Visualization{
						Type: "line",
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
  "links": {
    "self": "/chronograf/v2/cells/2"
  },
  "visualization": {
    "type": "chronograf-v1",
    "queries": null,
    "axes": null,
    "visualizationType": "line",
    "colors": null,
    "legend": {},
    "tableOptions": {
      "verticalTimeAxis": false,
      "sortBy": {
        "internalName": "",
        "displayName": "",
        "visible": false
      },
      "wrapping": "",
      "fixFirstColumn": false
    },
    "fieldOptions": null,
    "timeFormat": "",
    "decimalPlaces": {
      "isEnforced": false,
      "digits": 0
    }
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
					CellService: tt.fields.CellService,
				},
				Logger: log.New(log.DebugLevel),
			}

			b, err := json.Marshal(tt.args.cell)
			if err != nil {
				t.Fatalf("failed to unmarshal cell: %v", err)
			}

			r := httptest.NewRequest("GET", "http://any.url", bytes.NewReader(b))
			w := httptest.NewRecorder()

			s.NewCellV2(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. CellIDV2() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. CellIDV2() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. CellIDV2() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}
		})
	}
}

func TestService_RemoveCellV2(t *testing.T) {
	type fields struct {
		CellService platform.CellService
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
			name: "remove a cell by id",
			fields: fields{
				&mocks.CellService{
					DeleteCellF: func(ctx context.Context, id platform.ID) error {
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
			name: "cell not found",
			fields: fields{
				&mocks.CellService{
					DeleteCellF: func(ctx context.Context, id platform.ID) error {
						return platform.ErrCellNotFound
					},
				},
			},
			args: args{
				id: "2",
			},
			wants: wants{
				statusCode:  http.StatusNotFound,
				contentType: "application/json",
				body:        `{"code":404,"message":"cell not found"}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &mocks.Store{
					CellService: tt.fields.CellService,
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

			s.RemoveCellV2(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. RemoveCellV2() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. RemoveCellV2() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. RemoveCellV2() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}
		})
	}
}

func TestService_UpdateCellV2(t *testing.T) {
	type fields struct {
		CellService platform.CellService
	}
	type args struct {
		id            string
		name          string
		visualization platform.Visualization
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
			name: "update a cell",
			fields: fields{
				&mocks.CellService{
					UpdateCellF: func(ctx context.Context, id platform.ID, upd platform.CellUpdate) (*platform.Cell, error) {
						if id == "2" {
							return &platform.Cell{
								CellContents: platform.CellContents{
									ID:   platform.ID("2"),
									Name: "example",
								},
								Visualization: platform.V1Visualization{
									Type: "line",
								},
							}, nil
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
  "links": {
    "self": "/chronograf/v2/cells/2"
  },
  "visualization": {
    "type": "chronograf-v1",
    "queries": null,
    "axes": null,
    "visualizationType": "line",
    "colors": null,
    "legend": {},
    "tableOptions": {
      "verticalTimeAxis": false,
      "sortBy": {
        "internalName": "",
        "displayName": "",
        "visible": false
      },
      "wrapping": "",
      "fixFirstColumn": false
    },
    "fieldOptions": null,
    "timeFormat": "",
    "decimalPlaces": {
      "isEnforced": false,
      "digits": 0
    }
  }
}
`,
			},
		},
		{
			name: "update a cell with empty request body",
			fields: fields{
				&mocks.CellService{
					UpdateCellF: func(ctx context.Context, id platform.ID, upd platform.CellUpdate) (*platform.Cell, error) {
						if id == "2" {
							return &platform.Cell{
								CellContents: platform.CellContents{
									ID:   platform.ID("2"),
									Name: "example",
								},
								Visualization: platform.V1Visualization{
									Type: "line",
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
				statusCode:  http.StatusBadRequest,
				contentType: "application/json",
				body:        `{"code":400,"message":"expected at least one attribute to be updated"}`,
			},
		},
		{
			name: "cell not found",
			fields: fields{
				&mocks.CellService{
					UpdateCellF: func(ctx context.Context, id platform.ID, upd platform.CellUpdate) (*platform.Cell, error) {
						return nil, platform.ErrCellNotFound
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
				body:        `{"code":404,"message":"cell not found"}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Service{
				Store: &mocks.Store{
					CellService: tt.fields.CellService,
				},
				Logger: log.New(log.DebugLevel),
			}

			upd := platform.CellUpdate{}
			if tt.args.name != "" {
				upd.Name = &tt.args.name
			}
			if tt.args.visualization != nil {
				upd.Visualization = tt.args.visualization
			}

			b, err := json.Marshal(upd)
			if err != nil {
				t.Fatalf("failed to unmarshal cell update: %v", err)
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

			s.UpdateCellV2(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. UpdateCellV2() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. UpdateCellV2() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. UpdateCellV2() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}
		})
	}
}
