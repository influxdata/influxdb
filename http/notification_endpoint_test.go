package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"path"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	pcontext "github.com/influxdata/influxdb/v2/context"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/notification/endpoint"
	"github.com/influxdata/influxdb/v2/notification/endpoint/service"
	endpointTesting "github.com/influxdata/influxdb/v2/notification/endpoint/service/testing"
	"github.com/influxdata/influxdb/v2/pkg/testttp"
	"github.com/influxdata/influxdb/v2/secret"
	"github.com/influxdata/influxdb/v2/tenant"
	influxTesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// NewMockNotificationEndpointBackend returns a NotificationEndpointBackend with mock services.
func NewMockNotificationEndpointBackend(t *testing.T) *NotificationEndpointBackend {
	return &NotificationEndpointBackend{
		log:                         zaptest.NewLogger(t),
		HTTPErrorHandler:            kithttp.ErrorHandler(0),
		NotificationEndpointService: &mock.NotificationEndpointService{},
		UserResourceMappingService:  mock.NewUserResourceMappingService(),
		LabelService:                mock.NewLabelService(),
		UserService:                 mock.NewUserService(),
	}
}

func TestService_handleGetNotificationEndpoints(t *testing.T) {
	type fields struct {
		NotificationEndpointService influxdb.NotificationEndpointService
		LabelService                influxdb.LabelService
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
			name: "get all notification endpoints",
			fields: fields{
				&mock.NotificationEndpointService{
					FindNotificationEndpointsF: func(ctx context.Context, filter influxdb.NotificationEndpointFilter, opts ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, int, error) {
						return []influxdb.NotificationEndpoint{
							&endpoint.Slack{
								Base: endpoint.Base{
									ID:     influxTesting.MustIDBase16Ptr("0b501e7e557ab1ed"),
									Name:   "hello",
									OrgID:  influxTesting.MustIDBase16Ptr("50f7ba1150f7ba11"),
									Status: influxdb.Active,
								},
								URL: "http://example.com",
							},
							&endpoint.HTTP{
								Base: endpoint.Base{
									ID:     influxTesting.MustIDBase16Ptr("c0175f0077a77005"),
									Name:   "example",
									OrgID:  influxTesting.MustIDBase16Ptr("7e55e118dbabb1ed"),
									Status: influxdb.Inactive,
								},
								URL:             "example.com",
								Username:        influxdb.SecretField{Key: "http-user-key"},
								Password:        influxdb.SecretField{Key: "http-password-key"},
								AuthMethod:      "basic",
								Method:          "POST",
								ContentTemplate: "template",
								Headers: map[string]string{
									"x-header-1": "header 1",
									"x-header-2": "header 2",
								},
							},
						}, 2, nil
					},
				},
				&mock.LabelService{
					FindResourceLabelsFn: func(ctx context.Context, f influxdb.LabelMappingFilter) ([]*influxdb.Label, error) {
						labels := []*influxdb.Label{
							{
								ID:   influxTesting.MustIDBase16("fc3dc670a4be9b9a"),
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
					"limit": {"1"},
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
		{
		  "links": {
		    "self": "/api/v2/notificationEndpoints?descending=false&limit=1&offset=0",
		    "next": "/api/v2/notificationEndpoints?descending=false&limit=1&offset=1"
		  },
		  "notificationEndpoints": [
		   {
		     "createdAt": "0001-01-01T00:00:00Z",
		     "id": "0b501e7e557ab1ed",
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
		       "labels": "/api/v2/notificationEndpoints/0b501e7e557ab1ed/labels",
		       "members": "/api/v2/notificationEndpoints/0b501e7e557ab1ed/members",
		       "owners": "/api/v2/notificationEndpoints/0b501e7e557ab1ed/owners",
		       "self": "/api/v2/notificationEndpoints/0b501e7e557ab1ed"
		     },
		     "name": "hello",
		     "orgID": "50f7ba1150f7ba11",
		     "status": "active",
			 "type": "slack",
			 "token": "",
		     "updatedAt": "0001-01-01T00:00:00Z",
		     "url": "http://example.com"
		   },
		   {
		     "createdAt": "0001-01-01T00:00:00Z",
		     "url": "example.com",
		     "id": "c0175f0077a77005",
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
		       "labels": "/api/v2/notificationEndpoints/c0175f0077a77005/labels",
		       "members": "/api/v2/notificationEndpoints/c0175f0077a77005/members",
		       "owners": "/api/v2/notificationEndpoints/c0175f0077a77005/owners",
		       "self": "/api/v2/notificationEndpoints/c0175f0077a77005"
		     },
		     "name": "example",
			 "orgID": "7e55e118dbabb1ed",
			 "authMethod": "basic",
             "contentTemplate": "template",
			 "password": "secret: http-password-key",
			 "token":"",
  			 "method": "POST",
		     "status": "inactive",
			 "type": "http",
			 "headers": {
				"x-header-1": "header 1",
				"x-header-2": "header 2"
			 },
		     "updatedAt": "0001-01-01T00:00:00Z",
		     "username": "secret: http-user-key"
		   }
		   ]
		}`,
			},
		},
		{
			name: "get all notification endpoints when there are none",
			fields: fields{
				&mock.NotificationEndpointService{
					FindNotificationEndpointsF: func(ctx context.Context, filter influxdb.NotificationEndpointFilter, opts ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, int, error) {
						return []influxdb.NotificationEndpoint{}, 0, nil
					},
				},
				&mock.LabelService{},
			},
			args: args{
				map[string][]string{
					"limit": {"1"},
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/notificationEndpoints?descending=false&limit=1&offset=0"
  },
  "notificationEndpoints": []
}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			notificationEndpointBackend := NewMockNotificationEndpointBackend(t)
			notificationEndpointBackend.NotificationEndpointService = tt.fields.NotificationEndpointService
			notificationEndpointBackend.LabelService = tt.fields.LabelService
			h := NewNotificationEndpointHandler(zaptest.NewLogger(t), notificationEndpointBackend)

			r := httptest.NewRequest("GET", "http://any.url", nil)

			qp := r.URL.Query()
			for k, vs := range tt.args.queryParams {
				for _, v := range vs {
					qp.Add(k, v)
				}
			}
			r.URL.RawQuery = qp.Encode()
			r = r.WithContext(pcontext.SetAuthorizer(r.Context(), &influxdb.Session{UserID: user1ID}))

			w := httptest.NewRecorder()

			h.handleGetNotificationEndpoints(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetNotificationEndpoints() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetNotificationEndpoints() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil || tt.wants.body != "" && !eq {
				t.Errorf("%q. handleGetNotificationEndpoints() = ***%v***", tt.name, diff)
			}
		})
	}
}

func TestService_handleGetNotificationEndpoint(t *testing.T) {
	type fields struct {
		NotificationEndpointService influxdb.NotificationEndpointService
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
			name: "get a notification endpoint by id",
			fields: fields{
				&mock.NotificationEndpointService{
					FindNotificationEndpointByIDF: func(ctx context.Context, id platform.ID) (influxdb.NotificationEndpoint, error) {
						if id == influxTesting.MustIDBase16("020f755c3c082000") {
							return &endpoint.HTTP{
								Base: endpoint.Base{
									ID:     influxTesting.MustIDBase16Ptr("020f755c3c082000"),
									OrgID:  influxTesting.MustIDBase16Ptr("020f755c3c082000"),
									Name:   "hello",
									Status: influxdb.Active,
								},
								URL:             "example.com",
								Username:        influxdb.SecretField{Key: "http-user-key"},
								Password:        influxdb.SecretField{Key: "http-password-key"},
								AuthMethod:      "basic",
								Method:          "POST",
								ContentTemplate: "template",
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
		    "self": "/api/v2/notificationEndpoints/020f755c3c082000",
		    "labels": "/api/v2/notificationEndpoints/020f755c3c082000/labels",
		    "members": "/api/v2/notificationEndpoints/020f755c3c082000/members",
		    "owners": "/api/v2/notificationEndpoints/020f755c3c082000/owners"
		  },
		  "labels": [],
		  "authMethod": "basic",
		  "method": "POST",
		  "contentTemplate": "template",
		  "createdAt": "0001-01-01T00:00:00Z",
		  "updatedAt": "0001-01-01T00:00:00Z",
		  "id": "020f755c3c082000",
		  "url": "example.com",
		  "username": "secret: http-user-key",
		  "password": "secret: http-password-key",
		  "token":"",
		  "status": "active",
          "type": "http",
		  "orgID": "020f755c3c082000",
		  "name": "hello"
		}
		`,
			},
		},
		{
			name: "not found",
			fields: fields{
				&mock.NotificationEndpointService{
					FindNotificationEndpointByIDF: func(ctx context.Context, id platform.ID) (influxdb.NotificationEndpoint, error) {
						return nil, &errors.Error{
							Code: errors.ENotFound,
							Msg:  "notification endpoint not found",
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
			notificationEndpointBackend := NewMockNotificationEndpointBackend(t)
			notificationEndpointBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			notificationEndpointBackend.NotificationEndpointService = tt.fields.NotificationEndpointService
			h := NewNotificationEndpointHandler(zaptest.NewLogger(t), notificationEndpointBackend)

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

			h.handleGetNotificationEndpoint(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)
			t.Logf(res.Header.Get("X-Influx-Error"))

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetNotificationEndpoint() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetNotificationEndpoint() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handleGetNotificationEndpoint(). error unmarshalling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handleGetNotificationEndpoint() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestService_handlePostNotificationEndpoint(t *testing.T) {
	type fields struct {
		Secrets                     map[string]string
		SecretService               influxdb.SecretService
		NotificationEndpointService influxdb.NotificationEndpointService
		OrganizationService         influxdb.OrganizationService
	}
	type args struct {
		endpoint interface{}
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
			name: "create a new notification endpoint",
			fields: fields{
				Secrets: map[string]string{},
				NotificationEndpointService: &mock.NotificationEndpointService{
					CreateNotificationEndpointF: func(ctx context.Context, edp influxdb.NotificationEndpoint, userID platform.ID) error {
						edp.SetID(influxTesting.MustIDBase16("020f755c3c082000"))
						edp.BackfillSecretKeys()
						return nil
					},
				},
				OrganizationService: &mock.OrganizationService{
					FindOrganizationF: func(ctx context.Context, f influxdb.OrganizationFilter) (*influxdb.Organization, error) {
						return &influxdb.Organization{ID: influxTesting.MustIDBase16("6f626f7274697320")}, nil
					},
				},
			},
			args: args{
				endpoint: map[string]interface{}{
					"name":            "hello",
					"type":            "http",
					"orgID":           "6f626f7274697320",
					"description":     "desc1",
					"status":          "active",
					"url":             "example.com",
					"username":        "user1",
					"password":        "password1",
					"authMethod":      "basic",
					"method":          "POST",
					"contentTemplate": "template",
				},
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/notificationEndpoints/020f755c3c082000",
    "labels": "/api/v2/notificationEndpoints/020f755c3c082000/labels",
    "members": "/api/v2/notificationEndpoints/020f755c3c082000/members",
    "owners": "/api/v2/notificationEndpoints/020f755c3c082000/owners"
  },
  "url": "example.com",
  "status": "active",
  "username": "secret: 020f755c3c082000-username",
  "password": "secret: 020f755c3c082000-password",
  "token":"",
  "authMethod": "basic",
  "contentTemplate": "template",
  "type": "http",
  "method": "POST",
  "createdAt": "0001-01-01T00:00:00Z",
  "updatedAt": "0001-01-01T00:00:00Z",
  "id": "020f755c3c082000",
  "orgID": "6f626f7274697320",
  "name": "hello",
  "description": "desc1",
  "labels": []
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			notificationEndpointBackend := NewMockNotificationEndpointBackend(t)
			notificationEndpointBackend.NotificationEndpointService = tt.fields.NotificationEndpointService

			testttp.
				PostJSON(t, prefixNotificationEndpoints, tt.args.endpoint).
				WrapCtx(authCtxFn(user1ID)).
				Do(NewNotificationEndpointHandler(zaptest.NewLogger(t), notificationEndpointBackend)).
				ExpectHeader("Content-Type", tt.wants.contentType).
				ExpectStatus(tt.wants.statusCode).
				ExpectBody(func(body *bytes.Buffer) {
					if tt.wants.body != "" {
						if eq, diff, err := jsonEqual(body.String(), tt.wants.body); err != nil {
							t.Errorf("%q, handlePostNotificationEndpoint(). error unmarshalling json %v", tt.name, err)
						} else if !eq {
							t.Errorf("%q. handlePostNotificationEndpoint() = ***%s***", tt.name, diff)
						}
					}
				})

		})
	}
}

func TestService_handleDeleteNotificationEndpoint(t *testing.T) {
	type fields struct {
		NotificationEndpointService influxdb.NotificationEndpointService
	}
	type args struct {
		id string
	}
	type wants struct {
		statusCode int
		body       string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "remove a notification endpoint by id",
			fields: fields{
				NotificationEndpointService: &mock.NotificationEndpointService{
					DeleteNotificationEndpointF: func(ctx context.Context, id platform.ID) ([]influxdb.SecretField, platform.ID, error) {
						if id == influxTesting.MustIDBase16("020f755c3c082000") {
							return []influxdb.SecretField{
								{Key: "k1"},
							}, influxTesting.MustIDBase16("020f755c3c082001"), nil
						}

						return nil, 0, fmt.Errorf("wrong id")
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
			name: "notification endpoint not found",
			fields: fields{
				NotificationEndpointService: &mock.NotificationEndpointService{
					DeleteNotificationEndpointF: func(ctx context.Context, id platform.ID) ([]influxdb.SecretField, platform.ID, error) {
						return nil, 0, &errors.Error{
							Code: errors.ENotFound,
							Msg:  "notification endpoint not found",
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
			notificationEndpointBackend := NewMockNotificationEndpointBackend(t)
			notificationEndpointBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			notificationEndpointBackend.NotificationEndpointService = tt.fields.NotificationEndpointService

			testttp.
				Delete(t, path.Join(prefixNotificationEndpoints, tt.args.id)).
				Do(NewNotificationEndpointHandler(zaptest.NewLogger(t), notificationEndpointBackend)).
				ExpectStatus(tt.wants.statusCode).
				ExpectBody(func(buf *bytes.Buffer) {
					if tt.wants.body != "" {
						if eq, diff, err := jsonEqual(buf.String(), tt.wants.body); err != nil {
							t.Errorf("%q, handleDeleteNotificationEndpoint(). error unmarshalling json %v", tt.name, err)
						} else if !eq {
							t.Errorf("%q. handleDeleteNotificationEndpoint() = ***%s***", tt.name, diff)
						}
					}
				})
		})
	}
}

func TestService_handlePatchNotificationEndpoint(t *testing.T) {
	type fields struct {
		NotificationEndpointService influxdb.NotificationEndpointService
	}
	type args struct {
		id   string
		name string
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
			name: "update a notification endpoint name",
			fields: fields{
				&mock.NotificationEndpointService{
					PatchNotificationEndpointF: func(ctx context.Context, id platform.ID, upd influxdb.NotificationEndpointUpdate) (influxdb.NotificationEndpoint, error) {
						if id == influxTesting.MustIDBase16("020f755c3c082000") {
							d := &endpoint.Slack{
								Base: endpoint.Base{
									ID:     influxTesting.MustIDBase16Ptr("020f755c3c082000"),
									Name:   "hello",
									OrgID:  influxTesting.MustIDBase16Ptr("020f755c3c082000"),
									Status: influxdb.Active,
								},
								URL: "http://example.com",
							}

							if upd.Name != nil {
								d.Name = *upd.Name
							}

							return d, nil
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
		  "links": {
		    "self": "/api/v2/notificationEndpoints/020f755c3c082000",
		    "labels": "/api/v2/notificationEndpoints/020f755c3c082000/labels",
		    "members": "/api/v2/notificationEndpoints/020f755c3c082000/members",
		    "owners": "/api/v2/notificationEndpoints/020f755c3c082000/owners"
		  },
		  "createdAt": "0001-01-01T00:00:00Z",
		  "updatedAt": "0001-01-01T00:00:00Z",
		  "id": "020f755c3c082000",
		  "orgID": "020f755c3c082000",
		  "url": "http://example.com",
		  "name": "example",
		  "status": "active",
		  "type": "slack",
		  "token": "",
		  "labels": []
		}
		`,
			},
		},
		{
			name: "notification endpoint not found",
			fields: fields{
				&mock.NotificationEndpointService{
					PatchNotificationEndpointF: func(ctx context.Context, id platform.ID, upd influxdb.NotificationEndpointUpdate) (influxdb.NotificationEndpoint, error) {
						return nil, &errors.Error{
							Code: errors.ENotFound,
							Msg:  "notification endpoint not found",
						}
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
			notificationEndpointBackend := NewMockNotificationEndpointBackend(t)
			notificationEndpointBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			notificationEndpointBackend.NotificationEndpointService = tt.fields.NotificationEndpointService
			h := NewNotificationEndpointHandler(zaptest.NewLogger(t), notificationEndpointBackend)

			upd := influxdb.NotificationEndpointUpdate{}
			if tt.args.name != "" {
				upd.Name = &tt.args.name
			}

			b, err := json.Marshal(upd)
			if err != nil {
				t.Fatalf("failed to unmarshal notification endpoint update: %v", err)
			}

			r := httptest.NewRequest("GET", "http://any.url", bytes.NewReader(b))
			r = r.WithContext(pcontext.SetAuthorizer(r.Context(), &influxdb.Session{UserID: user1ID}))

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

			h.handlePatchNotificationEndpoint(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePatchNotificationEndpoint() = %v, want %v %v", tt.name, res.StatusCode, tt.wants.statusCode, w.Header())
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePatchNotificationEndpoint() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handlePatchNotificationEndpoint(). error unmarshalling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handlePatchNotificationEndpoint() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestService_handleUpdateNotificationEndpoint(t *testing.T) {
	type fields struct {
		NotificationEndpointService influxdb.NotificationEndpointService
	}
	type args struct {
		id  string
		edp map[string]interface{}
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
			name: "update a notification endpoint name",
			fields: fields{
				NotificationEndpointService: &mock.NotificationEndpointService{
					UpdateNotificationEndpointF: func(ctx context.Context, id platform.ID, edp influxdb.NotificationEndpoint, userID platform.ID) (influxdb.NotificationEndpoint, error) {
						if id == influxTesting.MustIDBase16("020f755c3c082000") {
							edp.SetID(id)
							edp.BackfillSecretKeys()
							return edp, nil
						}

						return nil, fmt.Errorf("not found")
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
				edp: map[string]interface{}{
					"name":   "example",
					"status": "active",
					"orgID":  "020f755c3c082001",
					"type":   "slack",
					"token":  "",
					"url":    "example.com",
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
		{
		  "links": {
		    "self": "/api/v2/notificationEndpoints/020f755c3c082000",
		    "labels": "/api/v2/notificationEndpoints/020f755c3c082000/labels",
		    "members": "/api/v2/notificationEndpoints/020f755c3c082000/members",
		    "owners": "/api/v2/notificationEndpoints/020f755c3c082000/owners"
		  },
		  "createdAt": "0001-01-01T00:00:00Z",
		  "updatedAt": "0001-01-01T00:00:00Z",
		  "id": "020f755c3c082000",
		  "orgID": "020f755c3c082001",
		  "name": "example",
		  "url": "example.com",
          "type": "slack",
		  "status": "active",
		  "token": "",
          "labels": []
		}
		`,
			},
		},
		{
			name: "notification endpoint not found",
			fields: fields{
				NotificationEndpointService: &mock.NotificationEndpointService{
					UpdateNotificationEndpointF: func(ctx context.Context, id platform.ID, edp influxdb.NotificationEndpoint, userID platform.ID) (influxdb.NotificationEndpoint, error) {
						return nil, &errors.Error{
							Code: errors.ENotFound,
							Msg:  "notification endpoint not found",
						}
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
				edp: map[string]interface{}{
					"type": "slack",
					"name": "example",
				},
			},
			wants: wants{
				statusCode: http.StatusNotFound,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			notificationEndpointBackend := NewMockNotificationEndpointBackend(t)
			notificationEndpointBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			notificationEndpointBackend.NotificationEndpointService = tt.fields.NotificationEndpointService

			resp := testttp.
				PutJSON(t, path.Join(prefixNotificationEndpoints, tt.args.id), tt.args.edp).
				WrapCtx(authCtxFn(user1ID)).
				Do(NewNotificationEndpointHandler(zaptest.NewLogger(t), notificationEndpointBackend)).
				ExpectStatus(tt.wants.statusCode).
				ExpectBody(func(body *bytes.Buffer) {
					if tt.wants.body != "" {
						if eq, diff, err := jsonEqual(body.String(), tt.wants.body); err != nil {
							t.Errorf("%q, handlePutNotificationEndpoint(). error unmarshalling json %v", tt.name, err)
						} else if !eq {
							t.Errorf("%q. handlePutNotificationEndpoint() = ***%s***", tt.name, diff)
						}
					}
				})
			if tt.wants.contentType != "" {
				resp.ExpectHeader("Content-Type", tt.wants.contentType)
			}
		})
	}
}

func TestService_handlePostNotificationEndpointMember(t *testing.T) {
	type fields struct {
		UserService influxdb.UserService
	}
	type args struct {
		notificationEndpointID string
		user                   *influxdb.User
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
			name: "add a notification endpoint member",
			fields: fields{
				UserService: &mock.UserService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.User, error) {
						return &influxdb.User{
							ID:     id,
							Name:   "name",
							Status: influxdb.Active,
						}, nil
					},
				},
			},
			args: args{
				notificationEndpointID: "020f755c3c082000",
				user: &influxdb.User{
					ID: influxTesting.MustIDBase16("6f626f7274697320"),
				},
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "logs": "/api/v2/users/6f626f7274697320/logs",
    "self": "/api/v2/users/6f626f7274697320"
  },
  "role": "member",
  "id": "6f626f7274697320",
	"name": "name",
	"status": "active"
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			notificationEndpointBackend := NewMockNotificationEndpointBackend(t)
			notificationEndpointBackend.UserService = tt.fields.UserService
			h := NewNotificationEndpointHandler(zaptest.NewLogger(t), notificationEndpointBackend)

			b, err := json.Marshal(tt.args.user)
			if err != nil {
				t.Fatalf("failed to marshal user: %v", err)
			}

			path := fmt.Sprintf("/api/v2/notificationEndpoints/%s/members", tt.args.notificationEndpointID)
			r := httptest.NewRequest("POST", path, bytes.NewReader(b))
			r = r.WithContext(pcontext.SetAuthorizer(r.Context(), &influxdb.Session{UserID: user1ID}))
			w := httptest.NewRecorder()

			h.ServeHTTP(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePostNotificationEndpointMember() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePostNotificationEndpointMember() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
				t.Errorf("%q, handlePostNotificationEndpointMember(). error unmarshalling json %v", tt.name, err)
			} else if tt.wants.body != "" && !eq {
				t.Errorf("%q. handlePostNotificationEndpointMember() = ***%s***", tt.name, diff)
			}
		})
	}
}

func TestService_handlePostNotificationEndpointOwner(t *testing.T) {
	type fields struct {
		UserService influxdb.UserService
	}
	type args struct {
		notificationEndpointID string
		user                   *influxdb.User
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	cases := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "add a notification endpoint owner",
			fields: fields{
				UserService: &mock.UserService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.User, error) {
						return &influxdb.User{
							ID:     id,
							Name:   "name",
							Status: influxdb.Active,
						}, nil
					},
				},
			},
			args: args{
				notificationEndpointID: "020f755c3c082000",
				user: &influxdb.User{
					ID: influxTesting.MustIDBase16("6f626f7274697320"),
				},
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "logs": "/api/v2/users/6f626f7274697320/logs",
    "self": "/api/v2/users/6f626f7274697320"
  },
  "role": "owner",
  "id": "6f626f7274697320",
	"name": "name",
	"status": "active"
}
`,
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			notificationEndpointBackend := NewMockNotificationEndpointBackend(t)
			notificationEndpointBackend.UserService = tt.fields.UserService
			h := NewNotificationEndpointHandler(zaptest.NewLogger(t), notificationEndpointBackend)

			b, err := json.Marshal(tt.args.user)
			if err != nil {
				t.Fatalf("failed to marshal user: %v", err)
			}

			path := fmt.Sprintf("/api/v2/notificationEndpoints/%s/owners", tt.args.notificationEndpointID)
			r := httptest.NewRequest("POST", path, bytes.NewReader(b))
			w := httptest.NewRecorder()

			h.ServeHTTP(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePostNotificationEndpointOwner() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePostNotificationEndpointOwner() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
				t.Errorf("%q, handlePostNotificationEndpointOwner(). error unmarshalling json %v", tt.name, err)
			} else if tt.wants.body != "" && !eq {
				t.Errorf("%q. handlePostNotificationEndpointOwner() = ***%s***", tt.name, diff)
			}
		})
	}
}

func initNotificationEndpointService(f endpointTesting.NotificationEndpointFields, t *testing.T) (influxdb.NotificationEndpointService, influxdb.SecretService, func()) {
	ctx := context.Background()
	store := influxTesting.NewTestInmemStore(t)
	logger := zaptest.NewLogger(t)

	tenantStore := tenant.NewStore(store)
	tenantService := tenant.NewService(tenantStore)

	kvSvc := kv.NewService(logger, store, tenantService)
	kvSvc.IDGenerator = f.IDGenerator
	kvSvc.TimeGenerator = f.TimeGenerator

	secretStore, err := secret.NewStore(store)
	require.NoError(t, err)
	secretSvc := secret.NewService(secretStore)

	endpointStore := service.NewStore(store)
	endpointStore.IDGenerator = f.IDGenerator
	endpointStore.TimeGenerator = f.TimeGenerator
	endpointService := service.New(endpointStore, secretSvc)

	for _, o := range f.Orgs {
		withOrgID(tenantStore, o.ID, func() {
			if err := tenantService.CreateOrganization(ctx, o); err != nil {
				t.Fatalf("failed to populate org: %v", err)
			}
		})
	}

	for _, v := range f.NotificationEndpoints {
		if err := endpointStore.PutNotificationEndpoint(ctx, v); err != nil {
			t.Fatalf("failed to update endpoint: %v", err)
		}
	}

	fakeBackend := NewMockNotificationEndpointBackend(t)
	fakeBackend.NotificationEndpointService = endpointService
	fakeBackend.UserResourceMappingService = tenantService
	fakeBackend.UserService = tenantService

	handler := NewNotificationEndpointHandler(zaptest.NewLogger(t), fakeBackend)
	auth := func(next http.Handler) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			r = r.WithContext(pcontext.SetAuthorizer(r.Context(), &influxdb.Session{UserID: user1ID}))
			next.ServeHTTP(w, r)
		}
	}
	server := httptest.NewServer(auth(handler))
	done := server.Close

	client := mustNewHTTPClient(t, server.URL, "")

	return NewNotificationEndpointService(client), secretSvc, done
}

func TestNotificationEndpointService(t *testing.T) {
	t.Skip("wonky")

	tests := []struct {
		name   string
		testFn func(init func(endpointTesting.NotificationEndpointFields, *testing.T) (influxdb.NotificationEndpointService, influxdb.SecretService, func()), t *testing.T)
	}{
		{
			name:   "CreateNotificationEndpoint",
			testFn: endpointTesting.CreateNotificationEndpoint,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.testFn(initNotificationEndpointService, t)
		})
	}
}

func authCtxFn(userID platform.ID) func(context.Context) context.Context {
	return func(ctx context.Context) context.Context {
		return pcontext.SetAuthorizer(ctx, &influxdb.Session{UserID: userID})
	}
}

func withOrgID(store *tenant.Store, orgID platform.ID, fn func()) {
	backup := store.OrgIDGen
	defer func() { store.OrgIDGen = backup }()

	store.OrgIDGen = mock.NewStaticIDGenerator(orgID)

	fn()
}
