package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/influxdata/httprouter"
	platform "github.com/influxdata/influxdb/v2"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/tenant"
	itesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

var faketime = time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)

// NewMockVariableBackend returns a VariableBackend with mock services.
func NewMockVariableBackend(t *testing.T) *VariableBackend {
	return &VariableBackend{
		HTTPErrorHandler: kithttp.NewErrorHandler(zaptest.NewLogger(t)),
		log:              zaptest.NewLogger(t),
		VariableService:  mock.NewVariableService(),
		LabelService:     mock.NewLabelService(),
	}
}

func TestVariableService_handleGetVariables(t *testing.T) {
	type fields struct {
		VariableService platform.VariableService
		LabelService    platform.LabelService
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
			name: "get all variables",
			fields: fields{
				&mock.VariableService{
					FindVariablesF: func(ctx context.Context, filter platform.VariableFilter, opts ...platform.FindOptions) ([]*platform.Variable, error) {
						return []*platform.Variable{
							{
								ID:             itesting.MustIDBase16("6162207574726f71"),
								OrganizationID: platform2.ID(1),
								Name:           "variable-a",
								Selected:       []string{"b"},
								Arguments: &platform.VariableArguments{
									Type:   "constant",
									Values: platform.VariableConstantValues{"a", "b"},
								},
								CRUDLog: platform.CRUDLog{
									CreatedAt: faketime,
									UpdatedAt: faketime,
								},
							},
							{
								ID:             itesting.MustIDBase16("61726920617a696f"),
								OrganizationID: platform2.ID(1),
								Name:           "variable-b",
								Selected:       []string{"c"},
								Arguments: &platform.VariableArguments{
									Type:   "map",
									Values: platform.VariableMapValues{"a": "b", "c": "d"},
								},
								CRUDLog: platform.CRUDLog{
									CreatedAt: faketime,
									UpdatedAt: faketime,
								},
							},
						}, nil
					},
				},
				&mock.LabelService{
					FindResourceLabelsFn: func(ctx context.Context, f platform.LabelMappingFilter) ([]*platform.Label, error) {
						labels := []*platform.Label{
							{
								ID:   itesting.MustIDBase16("fc3dc670a4be9b9a"),
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
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `{
					"links":{
					   "self":"/api/v2/variables?descending=false&limit=` + strconv.Itoa(platform.DefaultPageSize) + `&offset=0"
					},
					"variables":[
					   {
						  "arguments":{
							 "type":"constant",
							 "values":[
								"a",
								"b"
							 ]
						  },
              "createdAt": "2006-05-04T01:02:03Z",
							"updatedAt": "2006-05-04T01:02:03Z",
						  "description":"",
						  "id":"6162207574726f71",
						  "labels":[
							 {
								"id":"fc3dc670a4be9b9a",
								"name":"label",
								"properties":{
								   "color":"fff000"
								}
							 }
						  ],
						  "links":{
							 "labels":"/api/v2/variables/6162207574726f71/labels",
							 "org":"/api/v2/orgs/0000000000000001",
							 "self":"/api/v2/variables/6162207574726f71"
						  },
						  "name":"variable-a",
						  "orgID":"0000000000000001",
						  "selected":[
							 "b"
						  ]
					   },
					   {
						  "arguments":{
							 "type":"map",
							 "values":{
								"a":"b",
								"c":"d"
							 }
						  },
              "createdAt": "2006-05-04T01:02:03Z",
							"updatedAt": "2006-05-04T01:02:03Z",
						  "description":"",
						  "id":"61726920617a696f",
						  "labels":[
							 {
								"id":"fc3dc670a4be9b9a",
								"name":"label",
								"properties":{
								   "color":"fff000"
								}
							 }
						  ],
						  "links":{
							 "labels":"/api/v2/variables/61726920617a696f/labels",
							 "org":"/api/v2/orgs/0000000000000001",
							 "self":"/api/v2/variables/61726920617a696f"
						  },
						  "name":"variable-b",
						  "orgID":"0000000000000001",
						  "selected":[
							 "c"
						  ]
					   }
					]
				 }`,
			},
		},
		{
			name: "get all variables when there are none",
			fields: fields{
				&mock.VariableService{
					FindVariablesF: func(ctx context.Context, filter platform.VariableFilter, opts ...platform.FindOptions) ([]*platform.Variable, error) {
						return []*platform.Variable{}, nil
					},
				},
				&mock.LabelService{
					FindResourceLabelsFn: func(ctx context.Context, f platform.LabelMappingFilter) ([]*platform.Label, error) {
						return []*platform.Label{}, nil
					},
				},
			},
			args: args{
				map[string][]string{
					"limit": {"1"},
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body:        `{"links":{"self":"/api/v2/variables?descending=false&limit=1&offset=0"},"variables":[]}`,
			},
		},
		{
			name: "get all variables belonging to an org",
			fields: fields{
				&mock.VariableService{
					FindVariablesF: func(ctx context.Context, filter platform.VariableFilter, opts ...platform.FindOptions) ([]*platform.Variable, error) {
						return []*platform.Variable{
							{
								ID:             itesting.MustIDBase16("6162207574726f71"),
								OrganizationID: itesting.MustIDBase16("0000000000000001"),
								Name:           "variable-a",
								Selected:       []string{"b"},
								Arguments: &platform.VariableArguments{
									Type:   "constant",
									Values: platform.VariableConstantValues{"a", "b"},
								},
								CRUDLog: platform.CRUDLog{
									CreatedAt: faketime,
									UpdatedAt: faketime,
								},
							},
						}, nil
					},
				},
				&mock.LabelService{
					FindResourceLabelsFn: func(ctx context.Context, f platform.LabelMappingFilter) ([]*platform.Label, error) {
						labels := []*platform.Label{
							{
								ID:   itesting.MustIDBase16("fc3dc670a4be9b9a"),
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
				body: `{
					"links": {
					  "self": "/api/v2/variables?descending=false&limit=` + strconv.Itoa(platform.DefaultPageSize) + `&offset=0&orgID=0000000000000001"
					},
					"variables": [
					  {
						"arguments": {
						  "type": "constant",
						  "values": [
							"a",
							"b"
						  ]
						},
		        "description": "",
						"id": "6162207574726f71",
						"labels": [
						  {
							"id": "fc3dc670a4be9b9a",
							"name": "label",
							"properties": {
							  "color": "fff000"
							}
						  }
						],
						"links": {
						  "labels": "/api/v2/variables/6162207574726f71/labels",
						  "org": "/api/v2/orgs/0000000000000001",
						  "self": "/api/v2/variables/6162207574726f71"
						},
						"name": "variable-a",
						"orgID": "0000000000000001",
						"selected": [
						  "b"
						],
						"createdAt": "2006-05-04T01:02:03Z",
						"updatedAt": "2006-05-04T01:02:03Z"
					  }
					]
				  }`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			variableBackend := NewMockVariableBackend(t)
			variableBackend.HTTPErrorHandler = kithttp.NewErrorHandler(zaptest.NewLogger(t))
			variableBackend.LabelService = tt.fields.LabelService
			variableBackend.VariableService = tt.fields.VariableService
			h := NewVariableHandler(zaptest.NewLogger(t), variableBackend)

			r := httptest.NewRequest("GET", "http://howdy.tld", nil)

			qp := r.URL.Query()
			for k, vs := range tt.args.queryParams {
				for _, v := range vs {
					qp.Add(k, v)
				}
			}
			r.URL.RawQuery = qp.Encode()

			w := httptest.NewRecorder()

			h.handleGetVariables(w, r)

			res := w.Result()
			contentType := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetVariables() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if contentType != tt.wants.contentType {
				t.Errorf("%q. handleGetVariables() = %v, want %v", tt.name, contentType, tt.wants.contentType)
			}
			if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
				t.Errorf("%q, handleGetDashboards(). error unmarshalling json %v", tt.name, err)
			} else if tt.wants.body != "" && !eq {
				t.Errorf("%q. handleGetDashboards() = ***%s***", tt.name, diff)
			}
		})
	}
}

func TestVariableService_handleGetVariable(t *testing.T) {
	type fields struct {
		VariableService platform.VariableService
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
		args   args
		fields fields
		wants  wants
	}{
		{
			name: "get a single variable by id",
			args: args{
				id: "75650d0a636f6d70",
			},
			fields: fields{
				&mock.VariableService{
					FindVariableByIDF: func(ctx context.Context, id platform2.ID) (*platform.Variable, error) {
						return &platform.Variable{
							ID:             itesting.MustIDBase16("75650d0a636f6d70"),
							OrganizationID: platform2.ID(1),
							Name:           "variable-a",
							Selected:       []string{"b"},
							Arguments: &platform.VariableArguments{
								Type:   "constant",
								Values: platform.VariableConstantValues{"a", "b"},
							},
							CRUDLog: platform.CRUDLog{
								CreatedAt: faketime,
								UpdatedAt: faketime,
							},
						}, nil
					},
				},
			},
			wants: wants{
				statusCode:  200,
				contentType: "application/json; charset=utf-8",
				body:        `{"id":"75650d0a636f6d70","orgID":"0000000000000001","name":"variable-a","description":"","selected":["b"],"arguments":{"type":"constant","values":["a","b"]},"createdAt":"2006-05-04T01:02:03Z","updatedAt":"2006-05-04T01:02:03Z","labels":[],"links":{"self":"/api/v2/variables/75650d0a636f6d70","labels":"/api/v2/variables/75650d0a636f6d70/labels","org":"/api/v2/orgs/0000000000000001"}}`,
			},
		},
		{
			name: "get a non-existent variable",
			args: args{
				id: "75650d0a636f6d70",
			},
			fields: fields{
				&mock.VariableService{
					FindVariableByIDF: func(ctx context.Context, id platform2.ID) (*platform.Variable, error) {
						return nil, &errors.Error{
							Code: errors.ENotFound,
							Msg:  fmt.Sprintf("variable with ID %v not found", id),
						}
					},
				},
			},
			wants: wants{
				statusCode:  404,
				contentType: "application/json; charset=utf-8",
				body:        `{"code":"not found","message":"variable with ID 75650d0a636f6d70 not found"}`,
			},
		},
		{
			name: "request an invalid variable ID",
			args: args{
				id: "baz",
			},
			fields: fields{
				&mock.VariableService{
					FindVariableByIDF: func(ctx context.Context, id platform2.ID) (*platform.Variable, error) {
						return nil, nil
					},
				},
			},
			wants: wants{
				statusCode:  400,
				contentType: "application/json; charset=utf-8",
				body:        `{"code":"invalid","message":"id must have a length of 16 bytes"}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			variableBackend := NewMockVariableBackend(t)
			variableBackend.HTTPErrorHandler = kithttp.NewErrorHandler(zaptest.NewLogger(t))
			variableBackend.VariableService = tt.fields.VariableService
			h := NewVariableHandler(zaptest.NewLogger(t), variableBackend)
			r := httptest.NewRequest("GET", "http://howdy.tld", nil)
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

			h.handleGetVariable(w, r)

			res := w.Result()
			contentType := res.Header.Get("Content-Type")
			bodyBytes, _ := ioutil.ReadAll(res.Body)
			body := string(bodyBytes[:])

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("got = %v, want %v", res.StatusCode, tt.wants.statusCode)
			}
			if contentType != tt.wants.contentType {
				t.Errorf("got = %v, want %v", contentType, tt.wants.contentType)
			}
			if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
				t.Errorf("%q, error unmarshalling json %v", tt.name, err)
			} else if tt.wants.body != "" && !eq {
				t.Errorf("%q. ***%s***", tt.name, diff)
			}
		})
	}
}

func TestVariableService_handlePostVariable(t *testing.T) {
	type fields struct {
		VariableService platform.VariableService
	}
	type args struct {
		variable string
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
			name: "create a new variable",
			fields: fields{
				&mock.VariableService{
					CreateVariableF: func(ctx context.Context, m *platform.Variable) error {
						m.ID = itesting.MustIDBase16("75650d0a636f6d70")
						m.OrganizationID = platform2.ID(1)
						m.UpdatedAt = faketime
						m.CreatedAt = faketime
						return nil
					},
				},
			},
			args: args{
				variable: `
{
  "name": "my-great-variable",
  "orgID": "0000000000000001",
  "arguments": {
    "type": "constant",
    "values": [
      "bar",
      "foo"
    ]
  },
  "selected": [
    "'foo'"
  ],
	"createdAt": "2006-05-04T01:02:03Z",
	"updatedAt": "2006-05-04T01:02:03Z"
}
`,
			},
			wants: wants{
				statusCode:  201,
				contentType: "application/json; charset=utf-8",
				body: `{"id":"75650d0a636f6d70","orgID":"0000000000000001","name":"my-great-variable","description":"","selected":["'foo'"],"arguments":{"type":"constant","values":["bar","foo"]},"createdAt":"2006-05-04T01:02:03Z","updatedAt":"2006-05-04T01:02:03Z","labels":[],"links":{"self":"/api/v2/variables/75650d0a636f6d70","labels":"/api/v2/variables/75650d0a636f6d70/labels","org":"/api/v2/orgs/0000000000000001"}}
`,
			},
		},
		{
			name: "create a variable with invalid fields",
			fields: fields{
				&mock.VariableService{
					CreateVariableF: func(ctx context.Context, m *platform.Variable) error {
						m.ID = itesting.MustIDBase16("0")
						return nil
					},
				},
			},
			args: args{
				variable: `{"data": "nonsense"}`,
			},
			wants: wants{
				statusCode:  400,
				contentType: "application/json; charset=utf-8",
				body:        `{"code":"invalid","message":"missing variable name"}`,
			},
		},
		{
			name: "create a variable with invalid json",
			fields: fields{
				&mock.VariableService{
					CreateVariableF: func(ctx context.Context, m *platform.Variable) error {
						m.ID = itesting.MustIDBase16("0")
						return nil
					},
				},
			},
			args: args{
				variable: `howdy`,
			},
			wants: wants{
				statusCode:  400,
				contentType: "application/json; charset=utf-8",
				body:        `{"code":"invalid","message":"invalid character 'h' looking for beginning of value"}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			variableBackend := NewMockVariableBackend(t)
			variableBackend.HTTPErrorHandler = kithttp.NewErrorHandler(zaptest.NewLogger(t))
			variableBackend.VariableService = tt.fields.VariableService
			h := NewVariableHandler(zaptest.NewLogger(t), variableBackend)
			r := httptest.NewRequest("GET", "http://howdy.tld", bytes.NewReader([]byte(tt.args.variable)))
			w := httptest.NewRecorder()

			h.handlePostVariable(w, r)

			res := w.Result()
			contentType := res.Header.Get("Content-Type")
			bodyBytes, _ := ioutil.ReadAll(res.Body)
			body := string(bodyBytes[:])

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("got = %v, want %v", res.StatusCode, tt.wants.statusCode)
			}
			if contentType != tt.wants.contentType {
				t.Errorf("got = %v, want %v", contentType, tt.wants.contentType)
			}
			if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
				t.Errorf("%q, error unmarshalling json %v", tt.name, err)
			} else if tt.wants.body != "" && !eq {
				t.Errorf("%q. ***%s***", tt.name, diff)
			}
		})
	}
}

func TestVariableService_handlePutVariable(t *testing.T) {
	type fields struct {
		VariableService platform.VariableService
	}
	type args struct {
		id       string
		variable string
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
			name: "PUT a variable",
			fields: fields{
				&mock.VariableService{
					ReplaceVariableF: func(ctx context.Context, m *platform.Variable) error {
						m.ID = itesting.MustIDBase16("75650d0a636f6d70")
						m.OrganizationID = platform2.ID(1)
						m.UpdatedAt = faketime
						m.CreatedAt = faketime
						return nil
					},
				},
			},
			args: args{
				id: "75650d0a636f6d70",
				variable: `
{
  "name": "my-great-variable",
  "orgID": "0000000000000001",
  "arguments": {
    "type": "constant",
    "values": [
      "bar",
      "foo"
    ]
  },
  "selected": [
    "'foo'"
  ],
	"createdAt": "2006-05-04T01:02:03Z",
	"updatedAt": "2006-05-04T01:02:03Z"
}
`,
			},
			wants: wants{
				statusCode:  200,
				contentType: "application/json; charset=utf-8",
				body: `{"id":"75650d0a636f6d70","orgID":"0000000000000001","name":"my-great-variable","description":"","selected":["'foo'"],"arguments":{"type":"constant","values":["bar","foo"]},"createdAt":"2006-05-04T01:02:03Z","updatedAt":"2006-05-04T01:02:03Z","labels":[],"links":{"self":"/api/v2/variables/75650d0a636f6d70","labels":"/api/v2/variables/75650d0a636f6d70/labels","org":"/api/v2/orgs/0000000000000001"}}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			variableBackend := NewMockVariableBackend(t)
			variableBackend.HTTPErrorHandler = kithttp.NewErrorHandler(zaptest.NewLogger(t))
			variableBackend.VariableService = tt.fields.VariableService
			h := NewVariableHandler(zaptest.NewLogger(t), variableBackend)
			r := httptest.NewRequest("GET", "http://howdy.tld", bytes.NewReader([]byte(tt.args.variable)))
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

			h.handlePutVariable(w, r)

			res := w.Result()
			contentType := res.Header.Get("Content-Type")
			bodyBytes, _ := ioutil.ReadAll(res.Body)
			body := string(bodyBytes[:])

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("got = %v, want %v", res.StatusCode, tt.wants.statusCode)
			}
			if contentType != tt.wants.contentType {
				t.Errorf("got = %v, want %v", contentType, tt.wants.contentType)
			}
			if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
				t.Errorf("%q, error unmarshalling json %v", tt.name, err)
			} else if tt.wants.body != "" && !eq {
				t.Errorf("%q. ***%s***", tt.name, diff)
			}
		})
	}
}

func TestVariableService_handlePatchVariable(t *testing.T) {
	type fields struct {
		VariableService platform.VariableService
	}
	type args struct {
		id     string
		update string
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
			name: "update a variable name",
			fields: fields{
				&mock.VariableService{
					UpdateVariableF: func(ctx context.Context, id platform2.ID, u *platform.VariableUpdate) (*platform.Variable, error) {
						return &platform.Variable{
							ID:             itesting.MustIDBase16("75650d0a636f6d70"),
							OrganizationID: platform2.ID(2),
							Name:           "new-name",
							Arguments: &platform.VariableArguments{
								Type:   "constant",
								Values: platform.VariableConstantValues{},
							},
							Selected: []string{},
							CRUDLog: platform.CRUDLog{
								CreatedAt: faketime,
								UpdatedAt: faketime,
							},
						}, nil
					},
				},
			},
			args: args{
				id:     "75650d0a636f6d70",
				update: `{"name": "new-name"}`,
			},
			wants: wants{
				statusCode:  200,
				contentType: "application/json; charset=utf-8",
				body:        `{"id":"75650d0a636f6d70","orgID":"0000000000000002","name":"new-name","description":"","selected":[],"arguments":{"type":"constant","values":[]},"createdAt":"2006-05-04T01:02:03Z","updatedAt": "2006-05-04T01:02:03Z","labels":[],"links":{"self":"/api/v2/variables/75650d0a636f6d70","labels":"/api/v2/variables/75650d0a636f6d70/labels","org":"/api/v2/orgs/0000000000000002"}}`,
			},
		},
		{
			name: "with an empty json body",
			fields: fields{
				&mock.VariableService{},
			},
			args: args{
				id:     "75650d0a636f6d70",
				update: `{}`,
			},
			wants: wants{
				statusCode:  400,
				contentType: "application/json; charset=utf-8",
				body:        `{"code":"invalid","message":"no fields supplied in update"}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			variableBackend := NewMockVariableBackend(t)
			variableBackend.HTTPErrorHandler = kithttp.NewErrorHandler(zaptest.NewLogger(t))
			variableBackend.VariableService = tt.fields.VariableService
			h := NewVariableHandler(zaptest.NewLogger(t), variableBackend)
			r := httptest.NewRequest("GET", "http://howdy.tld", bytes.NewReader([]byte(tt.args.update)))
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

			h.handlePatchVariable(w, r)

			res := w.Result()
			contentType := res.Header.Get("Content-Type")
			bodyBytes, _ := ioutil.ReadAll(res.Body)
			body := string(bodyBytes[:])

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("got = %v, want %v", res.StatusCode, tt.wants.statusCode)
			}
			if contentType != tt.wants.contentType {
				t.Errorf("got = %v, want %v", contentType, tt.wants.contentType)
			}
			if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
				t.Errorf("%q, error unmarshalling json %v", tt.name, err)
			} else if tt.wants.body != "" && !eq {
				t.Errorf("%q. ***%s***", tt.name, diff)
			}
		})
	}
}

func TestVariableService_handleDeleteVariable(t *testing.T) {
	type fields struct {
		VariableService platform.VariableService
	}
	type args struct {
		id string
	}
	type wants struct {
		statusCode int
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "delete a variable",
			fields: fields{
				&mock.VariableService{
					DeleteVariableF: func(ctx context.Context, id platform2.ID) error {
						return nil
					},
				},
			},
			args: args{
				id: "75650d0a636f6d70",
			},
			wants: wants{
				statusCode: 204,
			},
		},
		{
			name: "delete a non-existent variable",
			fields: fields{
				&mock.VariableService{
					DeleteVariableF: func(ctx context.Context, id platform2.ID) error {
						return &errors.Error{
							Code: errors.ENotFound,
							Msg:  fmt.Sprintf("variable with ID %v not found", id),
						}
					},
				},
			},
			args: args{
				id: "75650d0a636f6d70",
			},
			wants: wants{
				statusCode: 404,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			variableBackend := NewMockVariableBackend(t)
			variableBackend.HTTPErrorHandler = kithttp.NewErrorHandler(zaptest.NewLogger(t))
			variableBackend.VariableService = tt.fields.VariableService
			h := NewVariableHandler(zaptest.NewLogger(t), variableBackend)
			r := httptest.NewRequest("GET", "http://howdy.tld", nil)
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

			h.handleDeleteVariable(w, r)

			statusCode := w.Result().StatusCode

			if statusCode != tt.wants.statusCode {
				t.Errorf("got = %v, want %v", statusCode, tt.wants.statusCode)
			}
		})
	}
}

func TestService_handlePostVariableLabel(t *testing.T) {
	type fields struct {
		LabelService platform.LabelService
	}
	type args struct {
		labelMapping *platform.LabelMapping
		variableID   platform2.ID
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
			name: "add label to variable",
			fields: fields{
				LabelService: &mock.LabelService{
					FindLabelByIDFn: func(ctx context.Context, id platform2.ID) (*platform.Label, error) {
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
				variableID: 100,
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
			variableBackend := NewMockVariableBackend(t)
			variableBackend.HTTPErrorHandler = kithttp.NewErrorHandler(zaptest.NewLogger(t))
			variableBackend.LabelService = tt.fields.LabelService
			h := NewVariableHandler(zaptest.NewLogger(t), variableBackend)

			b, err := json.Marshal(tt.args.labelMapping)
			if err != nil {
				t.Fatalf("failed to unmarshal label mapping: %v", err)
			}

			url := fmt.Sprintf("http://localhost:8086/api/v2/variables/%s/labels", tt.args.variableID)
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
			if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
				t.Errorf("%q, error unmarshalling json %v", tt.name, err)
			} else if tt.wants.body != "" && !eq {
				t.Errorf("%q. ***%s***", tt.name, diff)
			}
		})
	}
}

func initVariableService(f itesting.VariableFields, t *testing.T) (platform.VariableService, string, func()) {
	store := itesting.NewTestInmemStore(t)
	tenantService := tenant.NewService(tenant.NewStore(store))

	svc := kv.NewService(zaptest.NewLogger(t), store, tenantService)
	svc.IDGenerator = f.IDGenerator
	svc.TimeGenerator = f.TimeGenerator

	ctx := context.Background()

	for _, v := range f.Variables {
		if err := svc.ReplaceVariable(ctx, v); err != nil {
			t.Fatalf("failed to replace variable: %v", err)
		}
	}

	fakeBackend := NewMockVariableBackend(t)
	fakeBackend.VariableService = svc

	handler := NewVariableHandler(zaptest.NewLogger(t), fakeBackend)
	server := httptest.NewServer(handler)
	client := VariableService{
		Client: mustNewHTTPClient(t, server.URL, ""),
	}
	done := server.Close

	return &client, "", done
}

func TestVariableService(t *testing.T) {
	itesting.VariableService(initVariableService, t, itesting.WithHTTPValidation())
}
