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

	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/influxdata/httprouter"
	platform "github.com/influxdata/influxdb/v2"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/label"
	"github.com/influxdata/influxdb/v2/mock"
	platformtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestService_handleGetLabels(t *testing.T) {
	type fields struct {
		LabelService platform.LabelService
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		wants  wants
	}{
		{
			name: "get all labels",
			fields: fields{
				&mock.LabelService{
					FindLabelsFn: func(ctx context.Context, filter platform.LabelFilter) ([]*platform.Label, error) {
						return []*platform.Label{
							{
								ID:   platformtesting.MustIDBase16("0b501e7e557ab1ed"),
								Name: "hello",
								Properties: map[string]string{
									"color": "fff000",
								},
							},
							{
								ID:   platformtesting.MustIDBase16("c0175f0077a77005"),
								Name: "example",
								Properties: map[string]string{
									"color": "fff000",
								},
							},
						}, nil
					},
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/labels"
  },
  "labels": [
    {
      "id": "0b501e7e557ab1ed",
      "name": "hello",
      "properties": {
        "color": "fff000"
      }
    },
    {
      "id": "c0175f0077a77005",
      "name": "example",
      "properties": {
        "color": "fff000"
      }
    }
  ]
}
`,
			},
		},
		{
			name: "get all labels when there are none",
			fields: fields{
				&mock.LabelService{
					FindLabelsFn: func(ctx context.Context, filter platform.LabelFilter) ([]*platform.Label, error) {
						return []*platform.Label{}, nil
					},
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/labels"
  },
  "labels": []
}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewLabelHandler(zaptest.NewLogger(t), tt.fields.LabelService, kithttp.ErrorHandler(0))

			r := httptest.NewRequest("GET", "http://any.url", nil)

			w := httptest.NewRecorder()

			h.handleGetLabels(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetLabels() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetLabels() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil || tt.wants.body != "" && !eq {
				t.Errorf("%q. handleGetLabels() = ***%v***", tt.name, diff)
			}
		})
	}
}

func TestService_handleGetLabel(t *testing.T) {
	type fields struct {
		LabelService platform.LabelService
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
			name: "get a label by id",
			fields: fields{
				&mock.LabelService{
					FindLabelByIDFn: func(ctx context.Context, id platform2.ID) (*platform.Label, error) {
						if id == platformtesting.MustIDBase16("020f755c3c082000") {
							return &platform.Label{
								ID:   platformtesting.MustIDBase16("020f755c3c082000"),
								Name: "mylabel",
								Properties: map[string]string{
									"color": "fff000",
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
  "links": {
    "self": "/api/v2/labels/020f755c3c082000"
  },
  "label": {
    "id": "020f755c3c082000",
    "name": "mylabel",
    "properties": {
      "color": "fff000"
    }
  }
}
`,
			},
		},
		{
			name: "not found",
			fields: fields{
				&mock.LabelService{
					FindLabelByIDFn: func(ctx context.Context, id platform2.ID) (*platform.Label, error) {
						return nil, &errors.Error{
							Code: errors.ENotFound,
							Msg:  platform.ErrLabelNotFound,
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
			h := NewLabelHandler(zaptest.NewLogger(t), tt.fields.LabelService, kithttp.ErrorHandler(0))

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

			h.handleGetLabel(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetLabel() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetLabel() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handleGetLabel(). error unmarshalling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handleGetLabel() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestService_handlePostLabel(t *testing.T) {
	type fields struct {
		LabelService platform.LabelService
	}
	type args struct {
		label *platform.Label
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
			name: "create a new label",
			fields: fields{
				&mock.LabelService{
					CreateLabelFn: func(ctx context.Context, l *platform.Label) error {
						l.ID = platformtesting.MustIDBase16("020f755c3c082000")
						return nil
					},
				},
			},
			args: args{
				label: &platform.Label{
					Name:  "mylabel",
					OrgID: platformtesting.MustIDBase16("020f755c3c082008"),
				},
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/labels/020f755c3c082000"
  },
  "label": {
    "id": "020f755c3c082000",
    "name": "mylabel",
		"orgID": "020f755c3c082008"
  }
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewLabelHandler(zaptest.NewLogger(t), tt.fields.LabelService, kithttp.ErrorHandler(0))

			l, err := json.Marshal(tt.args.label)
			if err != nil {
				t.Fatalf("failed to marshal label: %v", err)
			}

			r := httptest.NewRequest("GET", "http://any.url", bytes.NewReader(l))
			w := httptest.NewRecorder()

			h.handlePostLabel(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePostLabel() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePostLabel() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil || tt.wants.body != "" && !eq {
				t.Errorf("%q. handlePostLabel() = ***%v***", tt.name, diff)
			}
		})
	}
}

func TestService_handleDeleteLabel(t *testing.T) {
	type fields struct {
		LabelService platform.LabelService
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
			name: "remove a label by id",
			fields: fields{
				&mock.LabelService{
					DeleteLabelFn: func(ctx context.Context, id platform2.ID) error {
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
			name: "label not found",
			fields: fields{
				&mock.LabelService{
					DeleteLabelFn: func(ctx context.Context, id platform2.ID) error {
						return &errors.Error{
							Code: errors.ENotFound,
							Msg:  platform.ErrLabelNotFound,
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
			h := NewLabelHandler(zaptest.NewLogger(t), tt.fields.LabelService, kithttp.ErrorHandler(0))

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

			h.handleDeleteLabel(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePostLabel() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePostLabel() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handlePostLabel(). error unmarshalling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handlePostLabel() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestService_handlePatchLabel(t *testing.T) {
	type fields struct {
		LabelService platform.LabelService
	}
	type args struct {
		id         string
		properties map[string]string
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
			name: "update label properties",
			fields: fields{
				&mock.LabelService{
					UpdateLabelFn: func(ctx context.Context, id platform2.ID, upd platform.LabelUpdate) (*platform.Label, error) {
						if id == platformtesting.MustIDBase16("020f755c3c082000") {
							l := &platform.Label{
								ID:   platformtesting.MustIDBase16("020f755c3c082000"),
								Name: "mylabel",
								Properties: map[string]string{
									"color": "fff000",
								},
							}

							for k, v := range upd.Properties {
								if v == "" {
									delete(l.Properties, k)
								} else {
									l.Properties[k] = v
								}
							}

							return l, nil
						}

						return nil, fmt.Errorf("not found")
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
				properties: map[string]string{
					"color": "aaabbb",
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/labels/020f755c3c082000"
  },
  "label": {
    "id": "020f755c3c082000",
    "name": "mylabel",
		"properties": {
			"color": "aaabbb"
		}
  }
}
`,
			},
		},
		{
			name: "label not found",
			fields: fields{
				&mock.LabelService{
					UpdateLabelFn: func(ctx context.Context, id platform2.ID, upd platform.LabelUpdate) (*platform.Label, error) {
						return nil, &errors.Error{
							Code: errors.ENotFound,
							Msg:  platform.ErrLabelNotFound,
						}
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
				properties: map[string]string{
					"color": "aaabbb",
				},
			},
			wants: wants{
				statusCode: http.StatusNotFound,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewLabelHandler(zaptest.NewLogger(t), tt.fields.LabelService, kithttp.ErrorHandler(0))

			upd := platform.LabelUpdate{}
			if len(tt.args.properties) > 0 {
				upd.Properties = tt.args.properties
			}

			l, err := json.Marshal(upd)
			if err != nil {
				t.Fatalf("failed to marshal label update: %v", err)
			}

			r := httptest.NewRequest("GET", "http://any.url", bytes.NewReader(l))

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

			h.handlePatchLabel(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePatchLabel() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePatchLabel() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handlePatchLabel(). error unmarshalling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handlePatchLabel() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func initLabelService(f platformtesting.LabelFields, t *testing.T) (platform.LabelService, string, func()) {
	store := platformtesting.NewTestInmemStore(t)
	labelStore, err := label.NewStore(store)
	if err != nil {
		t.Fatal(err)
	}

	if f.IDGenerator != nil {
		labelStore.IDGenerator = f.IDGenerator
	}

	labelService := label.NewService(labelStore)

	ctx := context.Background()
	for _, l := range f.Labels {
		mock.SetIDForFunc(&labelStore.IDGenerator, l.ID, func() {
			if err := labelService.CreateLabel(ctx, l); err != nil {
				t.Fatalf("failed to populate labels: %v", err)
			}
		})
	}

	for _, m := range f.Mappings {
		if err := labelService.CreateLabelMapping(ctx, m); err != nil {
			t.Fatalf("failed to populate label mappings: %v", err)
		}
	}

	handler := NewLabelHandler(zaptest.NewLogger(t), labelService, kithttp.ErrorHandler(0))
	server := httptest.NewServer(handler)
	client := LabelService{
		Client: mustNewHTTPClient(t, server.URL, ""),
	}
	done := server.Close

	return &client, "", done
}

func TestLabelService(t *testing.T) {
	tests := []struct {
		name   string
		testFn func(
			init func(platformtesting.LabelFields, *testing.T) (platform.LabelService, string, func()),
			t *testing.T,
		)
	}{
		{
			name:   "create label",
			testFn: platformtesting.CreateLabel,
		},
		{
			name:   "delete label",
			testFn: platformtesting.DeleteLabel,
		},
		{
			name:   "update label",
			testFn: platformtesting.UpdateLabel,
		},
		{
			name:   "find labels",
			testFn: platformtesting.FindLabels,
		},
		{
			name:   "find label by ID",
			testFn: platformtesting.FindLabelByID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.testFn(initLabelService, t)
		})
	}
}
