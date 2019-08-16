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

	"github.com/influxdata/influxdb"
	pcontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/notification/endpoint"
	influxTesting "github.com/influxdata/influxdb/testing"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
)

// NewMockNotificationEndpointBackend returns a NotificationEndpointBackend with mock services.
func NewMockNotificationEndpointBackend() *NotificationEndpointBackend {
	return &NotificationEndpointBackend{
		Logger: zap.NewNop().With(zap.String("handler", "notification endpoint")),

		NotificationEndpointService: &mock.NotificationEndpointService{},
		UserResourceMappingService:  mock.NewUserResourceMappingService(),
		LabelService:                mock.NewLabelService(),
		UserService:                 mock.NewUserService(),
		OrganizationService:         mock.NewOrganizationService(),
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
									ID:     influxTesting.MustIDBase16("0b501e7e557ab1ed"),
									Name:   "hello",
									OrgID:  influxTesting.MustIDBase16("50f7ba1150f7ba11"),
									Status: influxdb.Active,
								},
								URL:   "http://example.com",
								Token: influxdb.SecretField{Key: "slack-token-key"},
							},
							&endpoint.WebHook{
								Base: endpoint.Base{
									ID:     influxTesting.MustIDBase16("c0175f0077a77005"),
									Name:   "example",
									OrgID:  influxTesting.MustIDBase16("7e55e118dbabb1ed"),
									Status: influxdb.Inactive,
								},
								URL:             "example.com",
								Username:        influxdb.SecretField{Key: "webhook-user-key"},
								Password:        influxdb.SecretField{Key: "webhook-password-key"},
								AuthMethod:      "basic",
								Method:          "POST",
								ContentTemplate: "template",
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
		     "token": "secret: slack-token-key",
		     "type": "slack",
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
			 "authmethod": "basic",
             "contentTemplate": "template",
			 "password": "secret: webhook-password-key",
			 "token":"",
  			 "method": "POST",
		     "status": "inactive",
		     "type": "webhook",
		     "updatedAt": "0001-01-01T00:00:00Z",
		     "username": "secret: webhook-user-key"
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
			notificationEndpointBackend := NewMockNotificationEndpointBackend()
			notificationEndpointBackend.NotificationEndpointService = tt.fields.NotificationEndpointService
			notificationEndpointBackend.LabelService = tt.fields.LabelService
			h := NewNotificationEndpointHandler(notificationEndpointBackend)

			r := httptest.NewRequest("GET", "http://any.url", nil)

			qp := r.URL.Query()
			for k, vs := range tt.args.queryParams {
				for _, v := range vs {
					qp.Add(k, v)
				}
			}
			r.URL.RawQuery = qp.Encode()

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
					FindNotificationEndpointByIDF: func(ctx context.Context, id influxdb.ID) (influxdb.NotificationEndpoint, error) {
						if id == influxTesting.MustIDBase16("020f755c3c082000") {
							return &endpoint.WebHook{
								Base: endpoint.Base{
									ID:     influxTesting.MustIDBase16("020f755c3c082000"),
									OrgID:  influxTesting.MustIDBase16("020f755c3c082000"),
									Name:   "hello",
									Status: influxdb.Active,
								},
								URL:             "example.com",
								Username:        influxdb.SecretField{Key: "webhook-user-key"},
								Password:        influxdb.SecretField{Key: "webhook-password-key"},
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
		  "authmethod": "basic",
		  "method": "POST",
		  "contentTemplate": "template",
		  "createdAt": "0001-01-01T00:00:00Z",
		  "updatedAt": "0001-01-01T00:00:00Z",
		  "id": "020f755c3c082000",
		  "url": "example.com",
		  "username": "secret: webhook-user-key",
		  "password": "secret: webhook-password-key",
		  "token":"",
		  "status": "active",
          "type": "webhook",
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
					FindNotificationEndpointByIDF: func(ctx context.Context, id influxdb.ID) (influxdb.NotificationEndpoint, error) {
						return nil, &influxdb.Error{
							Code: influxdb.ENotFound,
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
			notificationEndpointBackend := NewMockNotificationEndpointBackend()
			notificationEndpointBackend.HTTPErrorHandler = ErrorHandler(0)
			notificationEndpointBackend.NotificationEndpointService = tt.fields.NotificationEndpointService
			h := NewNotificationEndpointHandler(notificationEndpointBackend)

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
					t.Errorf("%q, handleGetNotificationEndpoint(). error unmarshaling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handleGetNotificationEndpoint() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestService_handlePostNotificationEndpoint(t *testing.T) {
	type fields struct {
		NotificationEndpointService influxdb.NotificationEndpointService
		OrganizationService         influxdb.OrganizationService
	}
	type args struct {
		endpoint influxdb.NotificationEndpoint
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
				NotificationEndpointService: &mock.NotificationEndpointService{
					CreateNotificationEndpointF: func(ctx context.Context, edp influxdb.NotificationEndpoint, userID influxdb.ID) error {
						edp.SetID(influxTesting.MustIDBase16("020f755c3c082000"))
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
				endpoint: &endpoint.WebHook{
					Base: endpoint.Base{
						Name:        "hello",
						OrgID:       influxTesting.MustIDBase16("6f626f7274697320"),
						Description: "desc1",
						Status:      influxdb.Active,
					},
					URL:             "example.com",
					Username:        influxdb.SecretField{Key: "webhook-user-key"},
					Password:        influxdb.SecretField{Key: "webhook-password-key"},
					AuthMethod:      "basic",
					Method:          "POST",
					ContentTemplate: "template",
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
  "username": "secret: webhook-user-key",
  "password": "secret: webhook-password-key",
  "token":"",
  "authmethod": "basic",
  "contentTemplate": "template",
  "type": "webhook",
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
			notificationEndpointBackend := NewMockNotificationEndpointBackend()
			notificationEndpointBackend.NotificationEndpointService = tt.fields.NotificationEndpointService
			notificationEndpointBackend.OrganizationService = tt.fields.OrganizationService
			h := NewNotificationEndpointHandler(notificationEndpointBackend)

			b, err := json.Marshal(tt.args.endpoint)
			if err != nil {
				t.Fatalf("failed to unmarshal endpoint: %v", err)
			}
			r := httptest.NewRequest("GET", "http://any.url?org=30", bytes.NewReader(b))
			r = r.WithContext(pcontext.SetAuthorizer(r.Context(), &influxdb.Session{UserID: user1ID}))
			w := httptest.NewRecorder()

			h.handlePostNotificationEndpoint(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePostNotificationEndpoint() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePostNotificationEndpoint() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handlePostNotificationEndpoint(). error unmarshaling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handlePostNotificationEndpoint() = ***%s***", tt.name, diff)
				}
			}
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
			name: "remove a notification endpoint by id",
			fields: fields{
				&mock.NotificationEndpointService{
					DeleteNotificationEndpointF: func(ctx context.Context, id influxdb.ID) error {
						if id == influxTesting.MustIDBase16("020f755c3c082000") {
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
			name: "notification endpoint not found",
			fields: fields{
				&mock.NotificationEndpointService{
					DeleteNotificationEndpointF: func(ctx context.Context, id influxdb.ID) error {
						return &influxdb.Error{
							Code: influxdb.ENotFound,
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
			notificationEndpointBackend := NewMockNotificationEndpointBackend()
			notificationEndpointBackend.HTTPErrorHandler = ErrorHandler(0)
			notificationEndpointBackend.NotificationEndpointService = tt.fields.NotificationEndpointService
			h := NewNotificationEndpointHandler(notificationEndpointBackend)

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

			h.handleDeleteNotificationEndpoint(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleDeleteNotificationEndpoint() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleDeleteNotificationEndpoint() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handleDeleteNotificationEndpoint(). error unmarshaling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handleDeleteNotificationEndpoint() = ***%s***", tt.name, diff)
				}
			}
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
					PatchNotificationEndpointF: func(ctx context.Context, id influxdb.ID, upd influxdb.NotificationEndpointUpdate) (influxdb.NotificationEndpoint, error) {
						if id == influxTesting.MustIDBase16("020f755c3c082000") {
							d := &endpoint.Slack{
								Base: endpoint.Base{
									ID:     influxTesting.MustIDBase16("020f755c3c082000"),
									Name:   "hello",
									OrgID:  influxTesting.MustIDBase16("020f755c3c082000"),
									Status: influxdb.Active,
								},
								URL:   "http://example.com",
								Token: influxdb.SecretField{Key: "slack-token-key"},
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
          "token": "secret: slack-token-key",
          "type": "slack",
		  "labels": []
		}
		`,
			},
		},
		{
			name: "notification endpoint not found",
			fields: fields{
				&mock.NotificationEndpointService{
					PatchNotificationEndpointF: func(ctx context.Context, id influxdb.ID, upd influxdb.NotificationEndpointUpdate) (influxdb.NotificationEndpoint, error) {
						return nil, &influxdb.Error{
							Code: influxdb.ENotFound,
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
			notificationEndpointBackend := NewMockNotificationEndpointBackend()
			notificationEndpointBackend.HTTPErrorHandler = ErrorHandler(0)
			notificationEndpointBackend.NotificationEndpointService = tt.fields.NotificationEndpointService
			h := NewNotificationEndpointHandler(notificationEndpointBackend)

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
					t.Errorf("%q, handlePatchNotificationEndpoint(). error unmarshaling json %v", tt.name, err)
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
		edp influxdb.NotificationEndpoint
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
					UpdateNotificationEndpointF: func(ctx context.Context, id influxdb.ID, edp influxdb.NotificationEndpoint, userID influxdb.ID) (influxdb.NotificationEndpoint, error) {
						if id == influxTesting.MustIDBase16("020f755c3c082000") {
							d := &endpoint.Slack{
								Base: endpoint.Base{
									ID:     influxTesting.MustIDBase16("020f755c3c082000"),
									Name:   "hello",
									Status: influxdb.Inactive,
									OrgID:  influxTesting.MustIDBase16("020f755c3c082000"),
								},
							}

							d = edp.(*endpoint.Slack)
							d.SetID(influxTesting.MustIDBase16("020f755c3c082000"))
							d.SetOrgID(influxTesting.MustIDBase16("020f755c3c082000"))

							return d, nil
						}

						return nil, fmt.Errorf("not found")
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
				edp: &endpoint.Slack{
					Base: endpoint.Base{
						Name:   "example",
						Status: influxdb.Active,
					},
					URL:   "example.com",
					Token: influxdb.SecretField{Key: "user-key"},
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
		  "orgID": "020f755c3c082000",
		  "name": "example",
		  "url": "example.com",
		  "token": "secret: user-key",
          "type": "slack",
		  "status": "active",
          "labels": []
		}
		`,
			},
		},
		{
			name: "notification endpoint not found",
			fields: fields{
				&mock.NotificationEndpointService{
					UpdateNotificationEndpointF: func(ctx context.Context, id influxdb.ID, edp influxdb.NotificationEndpoint, userID influxdb.ID) (influxdb.NotificationEndpoint, error) {
						return nil, &influxdb.Error{
							Code: influxdb.ENotFound,
							Msg:  "notification endpoint not found",
						}
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
				edp: &endpoint.Slack{
					Base: endpoint.Base{
						Name: "example",
					},
				},
			},
			wants: wants{
				statusCode: http.StatusNotFound,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			notificationEndpointBackend := NewMockNotificationEndpointBackend()
			notificationEndpointBackend.HTTPErrorHandler = ErrorHandler(0)
			notificationEndpointBackend.NotificationEndpointService = tt.fields.NotificationEndpointService
			h := NewNotificationEndpointHandler(notificationEndpointBackend)

			b, err := json.Marshal(tt.args.edp)
			if err != nil {
				t.Fatalf("failed to unmarshal notification endpoint update: %v", err)
			}

			r := httptest.NewRequest("PUT", "http://any.url", bytes.NewReader(b))
			r = r.WithContext(context.WithValue(
				context.Background(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.id,
					},
				}))
			r = r.WithContext(pcontext.SetAuthorizer(r.Context(), &influxdb.Session{UserID: user1ID}))
			w := httptest.NewRecorder()

			h.handlePutNotificationEndpoint(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePutNotificationEndpoint() = %v, want %v %v", tt.name, res.StatusCode, tt.wants.statusCode, w.Header())
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePutNotificationEndpoint() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handlePutNotificationEndpoint(). error unmarshaling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handlePutNotificationEndpoint() = ***%s***", tt.name, diff)
				}
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
					FindUserByIDFn: func(ctx context.Context, id influxdb.ID) (*influxdb.User, error) {
						return &influxdb.User{
							ID:   id,
							Name: "name",
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
  "name": "name"
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			notificationEndpointBackend := NewMockNotificationEndpointBackend()
			notificationEndpointBackend.UserService = tt.fields.UserService
			h := NewNotificationEndpointHandler(notificationEndpointBackend)

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
				t.Errorf("%q, handlePostNotificationEndpointMember(). error unmarshaling json %v", tt.name, err)
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
					FindUserByIDFn: func(ctx context.Context, id influxdb.ID) (*influxdb.User, error) {
						return &influxdb.User{
							ID:   id,
							Name: "name",
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
  "name": "name"
}
`,
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			notificationEndpointBackend := NewMockNotificationEndpointBackend()
			notificationEndpointBackend.UserService = tt.fields.UserService
			h := NewNotificationEndpointHandler(notificationEndpointBackend)

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
				t.Errorf("%q, handlePostNotificationEndpointOwner(). error unmarshaling json %v", tt.name, err)
			} else if tt.wants.body != "" && !eq {
				t.Errorf("%q. handlePostNotificationEndpointOwner() = ***%s***", tt.name, diff)
			}
		})
	}
}
