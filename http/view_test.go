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

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/mock"
	platformtesting "github.com/influxdata/platform/testing"
	"github.com/julienschmidt/httprouter"
)

func TestService_handleGetViews(t *testing.T) {
	type fields struct {
		ViewService platform.ViewService
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
			name: "get all views",
			fields: fields{
				&mock.ViewService{
					FindViewsF: func(ctx context.Context, filter platform.ViewFilter) ([]*platform.View, int, error) {
						return []*platform.View{
							{
								ViewContents: platform.ViewContents{
									ID:   platformtesting.MustIDBase16("7365637465747572"),
									Name: "hello",
								},
								Properties: platform.LineViewProperties{
									Type: "line",
								},
							},
							{
								ViewContents: platform.ViewContents{
									ID:   platformtesting.MustIDBase16("6167697474697320"),
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
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/views"
  },
  "views": [
    {
      "id": "7365637465747572",
      "name": "hello",
      "links": {
        "self": "/api/v2/views/7365637465747572"
      },
      "properties": {
        "shape": "chronograf-v2",
        "queries": null,
        "axes": null,
        "type": "line",
        "colors": null,
        "legend": {}
      }
    },
    {
      "id": "6167697474697320",
      "name": "example",
      "links": {
        "self": "/api/v2/views/6167697474697320"
      },
      "properties": {
        "shape": "empty"
      }
    }
  ]
}`,
			},
		},
		{
			name: "get all views when there are none",
			fields: fields{
				&mock.ViewService{
					FindViewsF: func(ctx context.Context, filter platform.ViewFilter) ([]*platform.View, int, error) {
						return []*platform.View{}, 0, nil
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
    "self": "/api/v2/views"
  },
  "views": []
}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mappingService := mock.NewUserResourceMappingService()
			h := NewViewHandler(mappingService)
			h.ViewService = tt.fields.ViewService

			r := httptest.NewRequest("GET", "http://any.url", nil)

			qp := r.URL.Query()
			for k, vs := range tt.args.queryParams {
				for _, v := range vs {
					qp.Add(k, v)
				}
			}
			r.URL.RawQuery = qp.Encode()

			w := httptest.NewRecorder()

			h.handleGetViews(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetViews() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetViews() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handleGetViews() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}

		})
	}
}

func TestService_handleGetView(t *testing.T) {
	type fields struct {
		ViewService platform.ViewService
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
			name: "get a view by id",
			fields: fields{
				&mock.ViewService{
					FindViewByIDF: func(ctx context.Context, id platform.ID) (*platform.View, error) {
						return &platform.View{
							ViewContents: platform.ViewContents{
								ID:   platformtesting.MustIDBase16("020f755c3c082000"),
								Name: "example",
							},
						}, nil
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
  "name": "example",
  "links": {
    "self": "/api/v2/views/020f755c3c082000"
  },
  "properties": {
    "shape": "empty"
  }
}
`,
			},
		},
		{
			name: "not found",
			fields: fields{
				&mock.ViewService{
					FindViewByIDF: func(ctx context.Context, id platform.ID) (*platform.View, error) {
						return nil, platform.ErrViewNotFound
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
			h := NewViewHandler(mappingService)
			h.ViewService = tt.fields.ViewService

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

			h.handleGetView(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetView() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetView() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handleGetView() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}
		})
	}
}

func TestService_handlePostViews(t *testing.T) {
	type fields struct {
		ViewService platform.ViewService
	}
	type args struct {
		view *platform.View
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
			name: "create a new view",
			fields: fields{
				&mock.ViewService{
					CreateViewF: func(ctx context.Context, c *platform.View) error {
						c.ID = platformtesting.MustIDBase16("020f755c3c082000")
						return nil
					},
				},
			},
			args: args{
				view: &platform.View{
					ViewContents: platform.ViewContents{
						ID:   platformtesting.MustIDBase16("020f755c3c082000"),
						Name: "hello",
					},
					Properties: platform.LineViewProperties{
						Type: "line",
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
  "links": {
    "self": "/api/v2/views/020f755c3c082000"
  },
  "properties": {
    "shape": "chronograf-v2",
    "queries": null,
    "axes": null,
    "type": "line",
    "colors": null,
    "legend": {}
  }
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mappingService := mock.NewUserResourceMappingService()
			h := NewViewHandler(mappingService)
			h.ViewService = tt.fields.ViewService

			b, err := json.Marshal(tt.args.view)
			if err != nil {
				t.Fatalf("failed to unmarshal view: %v", err)
			}

			r := httptest.NewRequest("GET", "http://any.url", bytes.NewReader(b))
			w := httptest.NewRecorder()

			h.handlePostViews(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePostViews() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePostViews() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handlePostViews() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}
		})
	}
}

func TestService_handleDeleteView(t *testing.T) {
	type fields struct {
		ViewService platform.ViewService
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
			name: "remove a view by id",
			fields: fields{
				&mock.ViewService{
					DeleteViewF: func(ctx context.Context, id platform.ID) error {
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
			name: "view not found",
			fields: fields{
				&mock.ViewService{
					DeleteViewF: func(ctx context.Context, id platform.ID) error {
						return platform.ErrViewNotFound
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
			h := NewViewHandler(mappingService)
			h.ViewService = tt.fields.ViewService

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

			h.handleDeleteView(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleDeleteView() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleDeleteView() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handleDeleteView() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}
		})
	}
}

func TestService_handlePatchView(t *testing.T) {
	type fields struct {
		ViewService platform.ViewService
	}
	type args struct {
		id         string
		name       string
		properties platform.ViewProperties
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
			name: "update a view",
			fields: fields{
				&mock.ViewService{
					UpdateViewF: func(ctx context.Context, id platform.ID, upd platform.ViewUpdate) (*platform.View, error) {
						if id == platformtesting.MustIDBase16("020f755c3c082000") {
							return &platform.View{
								ViewContents: platform.ViewContents{
									ID:   platformtesting.MustIDBase16("020f755c3c082000"),
									Name: "example",
								},
								Properties: platform.LineViewProperties{
									Type: "line",
								},
							}, nil
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
  "links": {
    "self": "/api/v2/views/020f755c3c082000"
  },
  "properties": {
    "shape": "chronograf-v2",
    "queries": null,
    "axes": null,
    "type": "line",
    "colors": null,
    "legend": {}
  }
}
`,
			},
		},
		{
			name: "update a view with empty request body",
			fields: fields{
				&mock.ViewService{
					UpdateViewF: func(ctx context.Context, id platform.ID, upd platform.ViewUpdate) (*platform.View, error) {
						if id == platformtesting.MustIDBase16("020f755c3c082000") {
							return &platform.View{
								ViewContents: platform.ViewContents{
									ID:   platformtesting.MustIDBase16("020f755c3c082000"),
									Name: "example",
								},
								Properties: platform.LineViewProperties{
									Type: "line",
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
				statusCode: http.StatusBadRequest,
			},
		},
		{
			name: "view not found",
			fields: fields{
				&mock.ViewService{
					UpdateViewF: func(ctx context.Context, id platform.ID, upd platform.ViewUpdate) (*platform.View, error) {
						return nil, platform.ErrViewNotFound
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
			h := NewViewHandler(mappingService)
			h.ViewService = tt.fields.ViewService

			upd := platform.ViewUpdate{}
			if tt.args.name != "" {
				upd.Name = &tt.args.name
			}
			if tt.args.properties != nil {
				upd.Properties = tt.args.properties
			}

			b, err := json.Marshal(upd)
			if err != nil {
				t.Fatalf("failed to unmarshal view update: %v", err)
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

			h.handlePatchView(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePatchView() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePatchView() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handlePatchView() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}
		})
	}
}

func jsonEqual(s1, s2 string) (eq bool, err error) {
	var o1, o2 interface{}

	if err = json.Unmarshal([]byte(s1), &o1); err != nil {
		return
	}
	if err = json.Unmarshal([]byte(s2), &o2); err != nil {
		return
	}

	return cmp.Equal(o1, o2), nil
}

/* TODO: Add a go view service client

func initViewService(f platformtesting.ViewFields, t *testing.T) (platform.ViewService, func()) {
	t.Helper()
	svc := inmem.NewService()
	svc.IDGenerator = f.IDGenerator

	ctx := context.Background()
	for _, b := range f.Views {
		if err := s.PutView(ctx, b); err != nil {
			t.Fatalf("failed to populate Views")
		}
	}

	handler := NewViewHandler()
	handler.ViewService = svc
	server := httptest.NewServer(handler)
	client := ViewService{
		Addr: server.URL,
	}
	done := server.Close

	return &client, done
}

func TestViewService_CreateView(t *testing.T) {
	platformtesting.CreateView(initViewService, t)
}

func TestViewService_FindViewByID(t *testing.T) {
	platformtesting.FindViewByID(initViewService, t)
}
func TestViewService_FindViews(t *testing.T) {
	platformtesting.FindViews(initViewService, t)
}

func TestViewService_DeleteView(t *testing.T) {
	platformtesting.DeleteView(initViewService, t)
}

func TestViewService_UpdateView(t *testing.T) {
	platformtesting.UpdateView(initViewService, t)
}
*/
