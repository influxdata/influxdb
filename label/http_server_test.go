package label

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi"
	"github.com/google/go-cmp/cmp"
	influxdb "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/mock"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/yudai/gojsondiff"
	"github.com/yudai/gojsondiff/formatter"
	"go.uber.org/zap/zaptest"
)

func TestService_handlePostLabel(t *testing.T) {
	type fields struct {
		LabelService influxdb.LabelService
	}
	type args struct {
		label *influxdb.Label
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
					CreateLabelFn: func(ctx context.Context, l *influxdb.Label) error {
						l.ID = influxdbtesting.MustIDBase16("020f755c3c082000")
						return nil
					},
				},
			},
			args: args{
				label: &influxdb.Label{
					Name:  "mylabel",
					OrgID: influxdbtesting.MustIDBase16("020f755c3c082008"),
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
			handler := NewHTTPLabelHandler(zaptest.NewLogger(t), tt.fields.LabelService)
			router := chi.NewRouter()
			router.Mount(handler.Prefix(), handler)

			l, err := json.Marshal(tt.args.label)
			if err != nil {
				t.Fatalf("failed to marshal label: %v", err)
			}

			r := httptest.NewRequest("GET", "http://any.url", bytes.NewReader(l))
			w := httptest.NewRecorder()

			handler.handlePostLabel(w, r)

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

func TestService_handleGetLabel(t *testing.T) {
	type fields struct {
		LabelService influxdb.LabelService
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
					FindLabelByIDFn: func(ctx context.Context, id influxdb.ID) (*influxdb.Label, error) {
						if id == influxdbtesting.MustIDBase16("020f755c3c082000") {
							return &influxdb.Label{
								ID:   influxdbtesting.MustIDBase16("020f755c3c082000"),
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
					FindLabelByIDFn: func(ctx context.Context, id influxdb.ID) (*influxdb.Label, error) {
						return nil, &influxdb.Error{
							Code: influxdb.ENotFound,
							Msg:  influxdb.ErrLabelNotFound,
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
			handler := NewHTTPLabelHandler(zaptest.NewLogger(t), tt.fields.LabelService)
			router := chi.NewRouter()
			router.Mount(handler.Prefix(), handler)

			r := httptest.NewRequest("GET", "http://any.url", nil)
			rctx := chi.NewRouteContext()
			rctx.URLParams.Add("id", tt.args.id)
			r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))

			w := httptest.NewRecorder()

			handler.handleGetLabel(w, r)

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
					t.Errorf("%q, handleGetLabel(). error unmarshaling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handleGetLabel() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestService_handleGetLabels(t *testing.T) {
	type fields struct {
		LabelService influxdb.LabelService
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
					FindLabelsFn: func(ctx context.Context, filter influxdb.LabelFilter) ([]*influxdb.Label, error) {
						return []*influxdb.Label{
							{
								ID:   influxdbtesting.MustIDBase16("0b501e7e557ab1ed"),
								Name: "hello",
								Properties: map[string]string{
									"color": "fff000",
								},
							},
							{
								ID:   influxdbtesting.MustIDBase16("c0175f0077a77005"),
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
					FindLabelsFn: func(ctx context.Context, filter influxdb.LabelFilter) ([]*influxdb.Label, error) {
						return []*influxdb.Label{}, nil
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
			handler := NewHTTPLabelHandler(zaptest.NewLogger(t), tt.fields.LabelService)
			router := chi.NewRouter()
			router.Mount(handler.Prefix(), handler)

			r := httptest.NewRequest("GET", "http://any.url", nil)

			w := httptest.NewRecorder()

			handler.handleGetLabels(w, r)

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

func TestService_handlePatchLabel(t *testing.T) {
	type fields struct {
		LabelService influxdb.LabelService
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
					UpdateLabelFn: func(ctx context.Context, id influxdb.ID, upd influxdb.LabelUpdate) (*influxdb.Label, error) {
						if id == influxdbtesting.MustIDBase16("020f755c3c082000") {
							l := &influxdb.Label{
								ID:   influxdbtesting.MustIDBase16("020f755c3c082000"),
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
					UpdateLabelFn: func(ctx context.Context, id influxdb.ID, upd influxdb.LabelUpdate) (*influxdb.Label, error) {
						return nil, &influxdb.Error{
							Code: influxdb.ENotFound,
							Msg:  influxdb.ErrLabelNotFound,
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
			handler := NewHTTPLabelHandler(zaptest.NewLogger(t), tt.fields.LabelService)
			router := chi.NewRouter()
			router.Mount(handler.Prefix(), handler)

			w := httptest.NewRecorder()

			upd := influxdb.LabelUpdate{}
			if len(tt.args.properties) > 0 {
				upd.Properties = tt.args.properties
			}

			l, err := json.Marshal(upd)
			if err != nil {
				t.Fatalf("failed to marshal label update: %v", err)
			}

			r := httptest.NewRequest("GET", "http://any.url", bytes.NewReader(l))
			rctx := chi.NewRouteContext()
			rctx.URLParams.Add("id", tt.args.id)
			r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))

			handler.handlePatchLabel(w, r)

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
					t.Errorf("%q, handlePatchLabel(). error unmarshaling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handlePatchLabel() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestService_handleDeleteLabel(t *testing.T) {
	type fields struct {
		LabelService influxdb.LabelService
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
					DeleteLabelFn: func(ctx context.Context, id influxdb.ID) error {
						if id == influxdbtesting.MustIDBase16("020f755c3c082000") {
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
					DeleteLabelFn: func(ctx context.Context, id influxdb.ID) error {
						return &influxdb.Error{
							Code: influxdb.ENotFound,
							Msg:  influxdb.ErrLabelNotFound,
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
			handler := NewHTTPLabelHandler(zaptest.NewLogger(t), tt.fields.LabelService)
			router := chi.NewRouter()
			router.Mount(handler.Prefix(), handler)

			w := httptest.NewRecorder()

			r := httptest.NewRequest("GET", "http://any.url", nil)
			rctx := chi.NewRouteContext()
			rctx.URLParams.Add("id", tt.args.id)
			r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))

			handler.handleDeleteLabel(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleDeleteLabel() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleDeleteLabel() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handleDeleteLabel(). error unmarshaling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handleDeleteLabel() = ***%s***", tt.name, diff)
				}
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
