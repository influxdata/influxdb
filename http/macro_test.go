package http

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/inmem"
	kerrors "github.com/influxdata/influxdb/kit/errors"
	"github.com/influxdata/influxdb/mock"
	platformtesting "github.com/influxdata/influxdb/testing"
	"github.com/julienschmidt/httprouter"
)

func TestMacroService_handleGetMacros(t *testing.T) {
	type fields struct {
		MacroService platform.MacroService
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
			name: "get all macros",
			fields: fields{
				&mock.MacroService{
					FindMacrosF: func(ctx context.Context, filter platform.MacroFilter, opts ...platform.FindOptions) ([]*platform.Macro, error) {
						return []*platform.Macro{
							{
								ID:             platformtesting.MustIDBase16("6162207574726f71"),
								OrganizationID: platform.ID(1),
								Name:           "macro-a",
								Selected:       []string{"b"},
								Arguments: &platform.MacroArguments{
									Type:   "constant",
									Values: platform.MacroConstantValues{"a", "b"},
								},
							},
							{
								ID:             platformtesting.MustIDBase16("61726920617a696f"),
								OrganizationID: platform.ID(1),
								Name:           "macro-b",
								Selected:       []string{"c"},
								Arguments: &platform.MacroArguments{
									Type:   "map",
									Values: platform.MacroMapValues{"a": "b", "c": "d"},
								},
							},
						}, nil
					},
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body:        `{"macros":[{"id":"6162207574726f71","org_id":"0000000000000001","name":"macro-a","selected":["b"],"arguments":{"type":"constant","values":["a","b"]},"links":{"self":"/api/v2/macros/6162207574726f71","org": "/api/v2/orgs/0000000000000001"}},{"id":"61726920617a696f","org_id":"0000000000000001","name":"macro-b","selected":["c"],"arguments":{"type":"map","values":{"a":"b","c":"d"}},"links":{"self":"/api/v2/macros/61726920617a696f","org": "/api/v2/orgs/0000000000000001"}}],"links":{"self":"/api/v2/macros?descending=false&limit=20&offset=0"}}`,
			},
		},
		{
			name: "get all macros when there are none",
			fields: fields{
				&mock.MacroService{
					FindMacrosF: func(ctx context.Context, filter platform.MacroFilter, opts ...platform.FindOptions) ([]*platform.Macro, error) {
						return []*platform.Macro{}, nil
					},
				},
			},
			args: args{
				map[string][]string{
					"limit": []string{"1"},
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body:        `{"links":{"self":"/api/v2/macros?descending=false&limit=1&offset=0"},"macros":[]}`,
			},
		},
		{
			name: "get all macros belonging to an org",
			fields: fields{
				&mock.MacroService{
					FindMacrosF: func(ctx context.Context, filter platform.MacroFilter, opts ...platform.FindOptions) ([]*platform.Macro, error) {
						return []*platform.Macro{
							{
								ID:             platformtesting.MustIDBase16("6162207574726f71"),
								OrganizationID: platformtesting.MustIDBase16("0000000000000001"),
								Name:           "macro-a",
								Selected:       []string{"b"},
								Arguments: &platform.MacroArguments{
									Type:   "constant",
									Values: platform.MacroConstantValues{"a", "b"},
								},
							},
						}, nil
					},
				},
			},
			args: args{
				map[string][]string{
					"orgID": []string{"0000000000000001"},
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body:        `{"macros":[{"id":"6162207574726f71","org_id":"0000000000000001","name":"macro-a","selected":["b"],"arguments":{"type":"constant","values":["a","b"]},"links":{"self":"/api/v2/macros/6162207574726f71","org":"/api/v2/orgs/0000000000000001"}}],"links":{"self":"/api/v2/macros?descending=false&limit=20&offset=0&orgID=0000000000000001"}}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewMacroHandler()
			h.MacroService = tt.fields.MacroService

			r := httptest.NewRequest("GET", "http://howdy.tld", nil)
			qp := r.URL.Query()
			for k, vs := range tt.args.queryParams {
				for _, v := range vs {
					qp.Add(k, v)
				}
			}
			r.URL.RawQuery = qp.Encode()

			w := httptest.NewRecorder()

			h.handleGetMacros(w, r)

			res := w.Result()
			contentType := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetMacros() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if contentType != tt.wants.contentType {
				t.Errorf("%q. handleGetMacros() = %v, want %v", tt.name, contentType, tt.wants.contentType)
			}
			if eq, diff, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handleGetMacros() = ***%s***", tt.name, diff)
			}
		})
	}
}

func TestMacroService_handleGetMacro(t *testing.T) {
	type fields struct {
		MacroService platform.MacroService
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
			name: "get a single macro by id",
			args: args{
				id: "75650d0a636f6d70",
			},
			fields: fields{
				&mock.MacroService{
					FindMacroByIDF: func(ctx context.Context, id platform.ID) (*platform.Macro, error) {
						return &platform.Macro{
							ID:             platformtesting.MustIDBase16("75650d0a636f6d70"),
							OrganizationID: platform.ID(1),
							Name:           "macro-a",
							Selected:       []string{"b"},
							Arguments: &platform.MacroArguments{
								Type:   "constant",
								Values: platform.MacroConstantValues{"a", "b"},
							},
						}, nil
					},
				},
			},
			wants: wants{
				statusCode:  200,
				contentType: "application/json; charset=utf-8",
				body: `{"id":"75650d0a636f6d70","org_id":"0000000000000001","name":"macro-a","selected":["b"],"arguments":{"type":"constant","values":["a","b"]},"links":{"self":"/api/v2/macros/75650d0a636f6d70","org":"/api/v2/orgs/0000000000000001"}}
`,
			},
		},
		{
			name: "get a non-existant macro",
			args: args{
				id: "75650d0a636f6d70",
			},
			fields: fields{
				&mock.MacroService{
					FindMacroByIDF: func(ctx context.Context, id platform.ID) (*platform.Macro, error) {
						return nil, kerrors.Errorf(kerrors.NotFound, "macro with ID %v not found", id)
					},
				},
			},
			wants: wants{
				statusCode:  404,
				contentType: "",
				body:        ``,
			},
		},
		{
			name: "request an invalid macro ID",
			args: args{
				id: "baz",
			},
			fields: fields{
				&mock.MacroService{
					FindMacroByIDF: func(ctx context.Context, id platform.ID) (*platform.Macro, error) {
						return nil, nil
					},
				},
			},
			wants: wants{
				statusCode:  400,
				contentType: "application/json; charset=utf-8",
				body:        `{"code":"invalid","message":"An internal error has occurred.","error":"id must have a length of 16 bytes"}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewMacroHandler()
			h.MacroService = tt.fields.MacroService
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

			h.handleGetMacro(w, r)

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

func TestMacroService_handlePostMacro(t *testing.T) {
	type fields struct {
		MacroService platform.MacroService
	}
	type args struct {
		macro string
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
			name: "create a new macro",
			fields: fields{
				&mock.MacroService{
					CreateMacroF: func(ctx context.Context, m *platform.Macro) error {
						m.ID = platformtesting.MustIDBase16("75650d0a636f6d70")
						m.OrganizationID = platform.ID(1)
						return nil
					},
				},
			},
			args: args{
				macro: `
{
  "name": "my-great-macro",
  "org_id": "0000000000000001",
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
				body: `{"id":"75650d0a636f6d70","org_id":"0000000000000001","name":"my-great-macro","selected":["'foo'"],"arguments":{"type":"constant","values":["bar","foo"]},"links":{"self":"/api/v2/macros/75650d0a636f6d70","org":"/api/v2/orgs/0000000000000001"}}
`,
			},
		},
		{
			name: "create a macro with invalid fields",
			fields: fields{
				&mock.MacroService{
					CreateMacroF: func(ctx context.Context, m *platform.Macro) error {
						m.ID = platformtesting.MustIDBase16("0")
						return nil
					},
				},
			},
			args: args{
				macro: `{"data": "nonsense"}`,
			},
			wants: wants{
				statusCode:  422,
				contentType: "",
				body:        "",
			},
		},
		{
			name: "create a macro with invalid json",
			fields: fields{
				&mock.MacroService{
					CreateMacroF: func(ctx context.Context, m *platform.Macro) error {
						m.ID = platformtesting.MustIDBase16("0")
						return nil
					},
				},
			},
			args: args{
				macro: `howdy`,
			},
			wants: wants{
				statusCode:  400,
				contentType: "",
				body:        "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewMacroHandler()
			h.MacroService = tt.fields.MacroService
			r := httptest.NewRequest("GET", "http://howdy.tld", bytes.NewReader([]byte(tt.args.macro)))
			w := httptest.NewRecorder()

			h.handlePostMacro(w, r)

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

func TestMacroService_handlePatchMacro(t *testing.T) {
	type fields struct {
		MacroService platform.MacroService
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
			name: "update a macro name",
			fields: fields{
				&mock.MacroService{
					UpdateMacroF: func(ctx context.Context, id platform.ID, u *platform.MacroUpdate) (*platform.Macro, error) {
						return &platform.Macro{
							ID:             platformtesting.MustIDBase16("75650d0a636f6d70"),
							OrganizationID: platform.ID(2),
							Name:           "new-name",
							Arguments: &platform.MacroArguments{
								Type:   "constant",
								Values: platform.MacroConstantValues{},
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
				body: `{"id":"75650d0a636f6d70","org_id":"0000000000000002","name":"new-name","selected":[],"arguments":{"type":"constant","values":[]},"links":{"self":"/api/v2/macros/75650d0a636f6d70","org":"/api/v2/orgs/0000000000000002"}}
`,
			},
		},
		{
			name: "with an empty json body",
			fields: fields{
				&mock.MacroService{},
			},
			args: args{
				id:     "75650d0a636f6d70",
				update: `{}`,
			},
			wants: wants{
				statusCode:  422,
				contentType: "",
				body:        ``,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewMacroHandler()
			h.MacroService = tt.fields.MacroService
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

			h.handlePatchMacro(w, r)

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

func TestMacroService_handleDeleteMacro(t *testing.T) {
	type fields struct {
		MacroService platform.MacroService
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
			name: "delete a macro",
			fields: fields{
				&mock.MacroService{
					DeleteMacroF: func(ctx context.Context, id platform.ID) error {
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
			name: "delete a non-existant macro",
			fields: fields{
				&mock.MacroService{
					DeleteMacroF: func(ctx context.Context, id platform.ID) error {
						return kerrors.Errorf(kerrors.NotFound, "macro with ID %v not found", id)
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
			h := NewMacroHandler()
			h.MacroService = tt.fields.MacroService
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

			h.handleDeleteMacro(w, r)

			statusCode := w.Result().StatusCode

			if statusCode != tt.wants.statusCode {
				t.Errorf("got = %v, want %v", statusCode, tt.wants.statusCode)
			}
		})
	}
}

func initMacroService(f platformtesting.MacroFields, t *testing.T) (platform.MacroService, string, func()) {
	t.Helper()
	svc := inmem.NewService()
	svc.IDGenerator = f.IDGenerator

	ctx := context.Background()
	for _, macro := range f.Macros {
		if err := svc.ReplaceMacro(ctx, macro); err != nil {
			t.Fatalf("failed to populate macros")
		}
	}

	handler := NewMacroHandler()
	handler.MacroService = svc
	server := httptest.NewServer(handler)
	client := MacroService{
		Addr: server.URL,
	}
	done := server.Close

	return &client, inmem.OpPrefix, done
}

func TestMacroService(t *testing.T) {
	platformtesting.MacroService(initMacroService, t)
}
