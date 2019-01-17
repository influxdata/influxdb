package http

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
	platformtesting "github.com/influxdata/influxdb/testing"
	"go.uber.org/zap"
)

// TestProtoHandler tests the ProtoHandler.
func TestProtoHandler(t *testing.T) {
	type fields struct {
		ProtoService platform.ProtoService
	}
	type args struct {
		method   string
		endpoint string
		body     interface{}
	}
	type wants struct {
		body        string
		contentType string
		statusCode  int
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "get all protos",
			fields: fields{
				ProtoService: &mock.ProtoService{
					FindProtosFn: func(context.Context) ([]*platform.Proto, error) {
						return []*platform.Proto{
							{
								ID:   1,
								Name: "system",
							},
							{
								ID:   2,
								Name: "k8s",
							},
						}, nil
					},
				},
			},
			args: args{
				method:   "GET",
				endpoint: "/api/v2/protos",
				body:     nil,
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "protos": [
    {
      "id": "0000000000000001",
      "links": {
        "dashboards": "/api/v2/protos/0000000000000001/dashboards"
      },
      "name": "system"
    },
    {
      "id": "0000000000000002",
      "links": {
        "dashboards": "/api/v2/protos/0000000000000002/dashboards"
      },
      "name": "k8s"
    }
  ]
}
`,
			},
		},
		{
			name: "create dashboard from proto",
			fields: fields{
				ProtoService: &mock.ProtoService{
					CreateDashboardsFromProtoFn: func(ctx context.Context, protoID, orgID platform.ID) ([]*platform.Dashboard, error) {
						return []*platform.Dashboard{
							{
								ID:             platformtesting.MustIDBase16("da7aba5e5d81e550"),
								OrganizationID: orgID,
								Name:           "hello",
								Description:    "oh hello there!",
								Meta: platform.DashboardMeta{
									CreatedAt: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
									UpdatedAt: time.Date(2009, time.November, 10, 24, 0, 0, 0, time.UTC),
								},
								Cells: []*platform.Cell{
									{
										ID: platformtesting.MustIDBase16("da7aba5e5d81e550"),
										X:  1,
										Y:  2,
										W:  3,
										H:  4,
									},
								},
							},
							{
								ID:             platformtesting.MustIDBase16("0ca2204eca2204e0"),
								OrganizationID: orgID,
								Meta: platform.DashboardMeta{
									CreatedAt: time.Date(2012, time.November, 10, 23, 0, 0, 0, time.UTC),
									UpdatedAt: time.Date(2012, time.November, 10, 24, 0, 0, 0, time.UTC),
								},
								Name: "example",
							},
						}, nil
					},
				},
			},
			args: args{
				method:   "POST",
				endpoint: "/api/v2/protos/0000000000000001/dashboards",
				body: &createProtoResourcesRequest{
					OrganizationID: 1,
				},
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `
		{
		  "dashboards": [
		    {
		      "cells": [
		        {
		          "h": 4,
		          "id": "da7aba5e5d81e550",
		          "links": {
		            "self": "/api/v2/dashboards/da7aba5e5d81e550/cells/da7aba5e5d81e550",
		            "view": "/api/v2/dashboards/da7aba5e5d81e550/cells/da7aba5e5d81e550/view"
		          },
		          "w": 3,
		          "x": 1,
		          "y": 2
		        }
		      ],
		      "description": "oh hello there!",
		      "id": "da7aba5e5d81e550",
					"orgID": "0000000000000001",
		      "labels": [
		      ],
		      "links": {
		        "members": "/api/v2/dashboards/da7aba5e5d81e550/members",
		        "owners": "/api/v2/dashboards/da7aba5e5d81e550/owners",
		        "cells": "/api/v2/dashboards/da7aba5e5d81e550/cells",
		        "labels": "/api/v2/dashboards/da7aba5e5d81e550/labels",
		        "log": "/api/v2/dashboards/da7aba5e5d81e550/log",
		        "org": "/api/v2/orgs/0000000000000001",
		        "self": "/api/v2/dashboards/da7aba5e5d81e550"
		      },
		      "meta": {
		        "createdAt": "2009-11-10T23:00:00Z",
		        "updatedAt": "2009-11-11T00:00:00Z"
		      },
		      "name": "hello"
		    },
		    {
		      "cells": [
		      ],
		      "description": "",
		      "id": "0ca2204eca2204e0",
					"orgID": "0000000000000001",
		      "labels": [
		      ],
		      "links": {
		        "members": "/api/v2/dashboards/0ca2204eca2204e0/members",
		        "owners": "/api/v2/dashboards/0ca2204eca2204e0/owners",
		        "cells": "/api/v2/dashboards/0ca2204eca2204e0/cells",
		        "labels": "/api/v2/dashboards/0ca2204eca2204e0/labels",
		        "log": "/api/v2/dashboards/0ca2204eca2204e0/log",
		        "org": "/api/v2/orgs/0000000000000001",
		        "self": "/api/v2/dashboards/0ca2204eca2204e0"
		      },
		      "meta": {
		        "createdAt": "2012-11-10T23:00:00Z",
		        "updatedAt": "2012-11-11T00:00:00Z"
		      },
		      "name": "example"
		    }
		  ],
		  "links": {
        "self": "/api/v2/dashboards?descending=false&limit=20&offset=0&orgID=0000000000000001"
		  }
		}
		`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &ProtoBackend{
				Logger:       zap.NewNop(),
				LabelService: mock.NewLabelService(),
				ProtoService: tt.fields.ProtoService,
			}

			h := NewProtoHandler(b)

			var bdy io.Reader
			if tt.args.body != nil {
				bs, err := json.Marshal(tt.args.body)
				if err != nil {
					t.Fatalf("unexpected errror marshalling body: %v", err)
				}

				bdy = bytes.NewReader(bs)
			}

			r := httptest.NewRequest(tt.args.method, "http://localhost:9999"+tt.args.endpoint, bdy)
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
			if eq, diff, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("diff\n-got/+want\n%s", diff)
			}

		})
	}
}
