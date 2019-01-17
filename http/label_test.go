package http

import (
	"context"
	"io/ioutil"
	http "net/http"
	"net/http/httptest"
	"testing"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
	platformtesting "github.com/influxdata/influxdb/testing"
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
			h := NewLabelHandler()
			h.LabelService = tt.fields.LabelService

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
