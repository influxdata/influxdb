package http

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/mock"
	platformtesting "github.com/influxdata/influxdb/testing"
	"github.com/julienschmidt/httprouter"
)

// NewMockVariableBackend returns a VariableBackend with mock services.
func NewMockVariableBackend() *VariableBackend {
	return &VariableBackend{
		Logger:          zap.NewNop().With(zap.String("handler", "variable")),
		VariableService: mock.NewVariableService(),
	}
}

func TestVariableService_handleGetVariables(t *testing.T) {
	type fields struct {
		VariableService platform.VariableService
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
								ID:             platformtesting.MustIDBase16("6162207574726f71"),
								OrganizationID: platform.ID(1),
								Name:           "variable-a",
								Selected:       []string{"b"},
								Arguments: &platform.VariableArguments{
									Type:   "constant",
									Values: platform.VariableConstantValues{"a", "b"},
								},
							},
							{
								ID:             platformtesting.MustIDBase16("61726920617a696f"),
								OrganizationID: platform.ID(1),
								Name:           "variable-b",
								Selected:       []string{"c"},
								Arguments: &platform.VariableArguments{
									Type:   "map",
									Values: platform.VariableMapValues{"a": "b", "c": "d"},
								},
							},
						}, nil
					},
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body:        `{"variables":[{"id":"6162207574726f71","orgID":"0000000000000001","name":"variable-a","selected":["b"],"arguments":{"type":"constant","values":["a","b"]},"links":{"self":"/api/v2/variables/6162207574726f71","org": "/api/v2/orgs/0000000000000001"}},{"id":"61726920617a696f","orgID":"0000000000000001","name":"variable-b","selected":["c"],"arguments":{"type":"map","values":{"a":"b","c":"d"}},"links":{"self":"/api/v2/variables/61726920617a696f","org": "/api/v2/orgs/0000000000000001"}}],"links":{"self":"/api/v2/variables?descending=false&limit=20&offset=0"}}`,
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
								ID:             platformtesting.MustIDBase16("6162207574726f71"),
								OrganizationID: platformtesting.MustIDBase16("0000000000000001"),
								Name:           "variable-a",
								Selected:       []string{"b"},
								Arguments: &platform.VariableArguments{
									Type:   "constant",
									Values: platform.VariableConstantValues{"a", "b"},
								},
							},
						}, nil
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
				body:        `{"variables":[{"id":"6162207574726f71","orgID":"0000000000000001","name":"variable-a","selected":["b"],"arguments":{"type":"constant","values":["a","b"]},"links":{"self":"/api/v2/variables/6162207574726f71","org":"/api/v2/orgs/0000000000000001"}}],"links":{"self":"/api/v2/variables?descending=false&limit=20&offset=0&orgID=0000000000000001"}}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			variableBackend := NewMockVariableBackend()
			variableBackend.VariableService = tt.fields.VariableService
			h := NewVariableHandler(variableBackend)

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
			if eq, diff, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handleGetVariables() = ***%s***", tt.name, diff)
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
					FindVariableByIDF: func(ctx context.Context, id platform.ID) (*platform.Variable, error) {
						return &platform.Variable{
							ID:             platformtesting.MustIDBase16("75650d0a636f6d70"),
							OrganizationID: platform.ID(1),
							Name:           "variable-a",
							Selected:       []string{"b"},
							Arguments: &platform.VariableArguments{
								Type:   "constant",
								Values: platform.VariableConstantValues{"a", "b"},
							},
						}, nil
					},
				},
			},
			wants: wants{
				statusCode:  200,
				contentType: "application/json; charset=utf-8",
				body: `{"id":"75650d0a636f6d70","orgID":"0000000000000001","name":"variable-a","selected":["b"],"arguments":{"type":"constant","values":["a","b"]},"links":{"self":"/api/v2/variables/75650d0a636f6d70","org":"/api/v2/orgs/0000000000000001"}}
`,
			},
		},
		{
			name: "get a non-existant variable",
			args: args{
				id: "75650d0a636f6d70",
			},
			fields: fields{
				&mock.VariableService{
					FindVariableByIDF: func(ctx context.Context, id platform.ID) (*platform.Variable, error) {
						return nil, &platform.Error{
							Code: platform.ENotFound,
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
					FindVariableByIDF: func(ctx context.Context, id platform.ID) (*platform.Variable, error) {
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
			variableBackend := NewMockVariableBackend()
			variableBackend.VariableService = tt.fields.VariableService
			h := NewVariableHandler(variableBackend)
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
			if body != tt.wants.body {
				t.Errorf("got = %v, want %v", body, tt.wants.body)
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
						m.ID = platformtesting.MustIDBase16("75650d0a636f6d70")
						m.OrganizationID = platform.ID(1)
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
  ]
}
`,
			},
			wants: wants{
				statusCode:  201,
				contentType: "application/json; charset=utf-8",
				body: `{"id":"75650d0a636f6d70","orgID":"0000000000000001","name":"my-great-variable","selected":["'foo'"],"arguments":{"type":"constant","values":["bar","foo"]},"links":{"self":"/api/v2/variables/75650d0a636f6d70","org":"/api/v2/orgs/0000000000000001"}}
`,
			},
		},
		{
			name: "create a variable with invalid fields",
			fields: fields{
				&mock.VariableService{
					CreateVariableF: func(ctx context.Context, m *platform.Variable) error {
						m.ID = platformtesting.MustIDBase16("0")
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
						m.ID = platformtesting.MustIDBase16("0")
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
			variableBackend := NewMockVariableBackend()
			variableBackend.VariableService = tt.fields.VariableService
			h := NewVariableHandler(variableBackend)
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
			if body != tt.wants.body {
				t.Errorf("got = %v, want %v", body, tt.wants.body)
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
					UpdateVariableF: func(ctx context.Context, id platform.ID, u *platform.VariableUpdate) (*platform.Variable, error) {
						return &platform.Variable{
							ID:             platformtesting.MustIDBase16("75650d0a636f6d70"),
							OrganizationID: platform.ID(2),
							Name:           "new-name",
							Arguments: &platform.VariableArguments{
								Type:   "constant",
								Values: platform.VariableConstantValues{},
							},
							Selected: []string{},
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
				body: `{"id":"75650d0a636f6d70","orgID":"0000000000000002","name":"new-name","selected":[],"arguments":{"type":"constant","values":[]},"links":{"self":"/api/v2/variables/75650d0a636f6d70","org":"/api/v2/orgs/0000000000000002"}}
`,
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
			variableBackend := NewMockVariableBackend()
			variableBackend.VariableService = tt.fields.VariableService
			h := NewVariableHandler(variableBackend)
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
			if body != tt.wants.body {
				t.Errorf("got = %v, want %v", body, tt.wants.body)
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
					DeleteVariableF: func(ctx context.Context, id platform.ID) error {
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
			name: "delete a non-existant variable",
			fields: fields{
				&mock.VariableService{
					DeleteVariableF: func(ctx context.Context, id platform.ID) error {
						return &platform.Error{
							Code: platform.ENotFound,
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
			variableBackend := NewMockVariableBackend()
			variableBackend.VariableService = tt.fields.VariableService
			h := NewVariableHandler(variableBackend)
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

func initVariableService(f platformtesting.VariableFields, t *testing.T) (platform.VariableService, string, func()) {
	t.Helper()
	svc := inmem.NewService()
	svc.IDGenerator = f.IDGenerator

	ctx := context.Background()
	for _, variable := range f.Variables {
		if err := svc.ReplaceVariable(ctx, variable); err != nil {
			t.Fatalf("failed to populate variables")
		}
	}

	variableBackend := NewMockVariableBackend()
	variableBackend.VariableService = svc
	handler := NewVariableHandler(variableBackend)
	server := httptest.NewServer(handler)
	client := VariableService{
		Addr: server.URL,
	}
	done := server.Close

	return &client, inmem.OpPrefix, done
}

func TestVariableService(t *testing.T) {
	platformtesting.VariableService(initVariableService, t)
}
