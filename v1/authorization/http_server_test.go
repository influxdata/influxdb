package authorization

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	icontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/tenant"
	itesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestService_handlePostAuthorization(t *testing.T) {
	type fields struct {
		AuthorizationService influxdb.AuthorizationService
		TenantService        TenantService
	}
	type args struct {
		session       *influxdb.Authorization
		authorization *influxdb.Authorization
		// buckets are created in the store before the request, with the
		// given IDs. The store checks that a bucket permission's bucket exists.
		buckets []*influxdb.Bucket
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
		bodyErr     string
	}

	var (
		defaultFields = fields{
			AuthorizationService: &mock.AuthorizationService{
				CreateAuthorizationFn: func(ctx context.Context, c *influxdb.Authorization) error {
					c.ID = itesting.MustIDBase16("020f755c3c082000")
					return nil
				},
			},
			TenantService: &tenantService{
				FindUserByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.User, error) {
					return &influxdb.User{
						ID:   id,
						Name: "u1",
					}, nil
				},
				FindOrganizationByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Organization, error) {
					return &influxdb.Organization{
						ID:   id,
						Name: "o1",
					}, nil
				},
				FindBucketByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Bucket, error) {
					return &influxdb.Bucket{
						ID:    id,
						OrgID: itesting.MustIDBase16("020f755c3c083000"),
						Name:  "b1",
					}, nil
				},
			},
		}

		defaultSession = &influxdb.Authorization{
			Token:       "session-token",
			ID:          itesting.MustIDBase16("020f755c3c082000"),
			UserID:      itesting.MustIDBase16("aaaaaaaaaaaaaaaa"),
			OrgID:       itesting.MustIDBase16("020f755c3c083000"),
			Description: "can write to authorization resource",
			Permissions: []influxdb.Permission{
				{
					Action: influxdb.WriteAction,
					Resource: influxdb.Resource{
						Type:  influxdb.AuthorizationsResourceType,
						OrgID: itesting.IDPtr(itesting.MustIDBase16("020f755c3c083000")),
					},
				},
			},
		}
	)

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name:   "create an authorization",
			fields: defaultFields,
			args: args{
				session: defaultSession,
				authorization: &influxdb.Authorization{
					ID:          itesting.MustIDBase16("020f755c3c082000"),
					OrgID:       itesting.MustIDBase16("020f755c3c083000"),
					Token:       "first:second",
					Description: "only read dashboards sucka",
					Permissions: []influxdb.Permission{
						{
							Action: influxdb.ReadAction,
							Resource: influxdb.Resource{
								Type:  influxdb.DashboardsResourceType,
								OrgID: itesting.IDPtr(itesting.MustIDBase16("020f755c3c083000")),
							},
						},
					},
				},
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `
{
	"createdAt": "0001-01-01T00:00:00Z",
	"updatedAt": "0001-01-01T00:00:00Z",
  "description": "only read dashboards sucka",
  "id": "020f755c3c082000",
  "links": {
    "user": "/api/v2/users/aaaaaaaaaaaaaaaa"
  },
  "org": "o1",
  "orgID": "020f755c3c083000",
  "permissions": [
    {
      "action": "read",
			"resource": {
				"type": "dashboards",
				"orgID": "020f755c3c083000",
				"org": "o1"
			}
    }
  ],
  "status": "active",
  "authorizationType": "v1_user",
  "user": "u1",
  "userID": "aaaaaaaaaaaaaaaa"
}
`,
			},
		},
		{
			name:   "create an authorization with a bucket permission",
			fields: defaultFields,
			args: args{
				session: defaultSession,
				buckets: []*influxdb.Bucket{
					{
						ID:    itesting.MustIDBase16("020f755c3c084000"),
						OrgID: itesting.MustIDBase16("020f755c3c083000"),
						Name:  "b1",
					},
				},
				authorization: &influxdb.Authorization{
					OrgID:       itesting.MustIDBase16("020f755c3c083000"),
					Token:       "bucket:reader",
					Description: "read one bucket",
					Permissions: []influxdb.Permission{
						{
							Action: influxdb.ReadAction,
							Resource: influxdb.Resource{
								Type:  influxdb.BucketsResourceType,
								OrgID: itesting.IDPtr(itesting.MustIDBase16("020f755c3c083000")),
								ID:    itesting.IDPtr(itesting.MustIDBase16("020f755c3c084000")),
							},
						},
					},
				},
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "description": "read one bucket",
  "links": {
    "user": "/api/v2/users/aaaaaaaaaaaaaaaa"
  },
  "org": "o1",
  "orgID": "020f755c3c083000",
  "permissions": [
    {
      "action": "read",
      "resource": {
        "type": "buckets",
        "orgID": "020f755c3c083000",
        "id": "020f755c3c084000",
        "name": "b1",
        "org": "o1"
      }
    }
  ],
  "status": "active",
  "user": "u1",
  "userID": "aaaaaaaaaaaaaaaa"
}
`,
			},
		},
		{
			name:   "create an authorization with missing token",
			fields: defaultFields,
			args: args{
				session: defaultSession,
				authorization: &influxdb.Authorization{
					ID:          itesting.MustIDBase16("020f755c3c082000"),
					OrgID:       itesting.MustIDBase16("020f755c3c083000"),
					Description: "only read dashboards sucka",
					Permissions: []influxdb.Permission{
						{
							Action: influxdb.ReadAction,
							Resource: influxdb.Resource{
								Type:  influxdb.DashboardsResourceType,
								OrgID: itesting.IDPtr(itesting.MustIDBase16("020f755c3c083000")),
							},
						},
					},
				},
			},
			wants: wants{
				statusCode:  http.StatusBadRequest,
				contentType: "application/json; charset=utf-8",
				bodyErr: `
{
	"code": "invalid",
	"message": "token required for v1 user authorization type"
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()

			s := itesting.NewTestInmemStore(t)
			storage, err := NewStore(s)
			if err != nil {
				t.Fatal(err)
			}

			for _, b := range tt.args.buckets {
				ts := tenant.NewStore(s)
				ts.BucketIDGen = mock.NewStaticIDGenerator(b.ID)
				require.NoError(t, s.Update(context.Background(), func(tx kv.Tx) error {
					return ts.CreateBucket(context.Background(), tx, b)
				}))
			}

			svc := NewService(storage, tt.fields.TenantService)

			handler := NewHTTPAuthHandler(zaptest.NewLogger(t), svc, nil, tt.fields.TenantService)
			router := chi.NewRouter()
			router.Mount(handler.Prefix(), handler)

			newRequest := func(a *influxdb.Authorization) *postAuthorizationRequest {
				return &postAuthorizationRequest{
					OrgID:       a.OrgID,
					Description: a.Description,
					Permissions: a.Permissions,
					Status:      a.Status,
					Token:       a.Token,
				}
			}

			req := newRequest(tt.args.authorization)
			b, err := json.Marshal(req)
			if err != nil {
				t.Fatalf("failed to unmarshal authorization: %v", err)
			}

			r := httptest.NewRequest("GET", "http://any.url", bytes.NewReader(b))
			r = r.WithContext(context.WithValue(
				context.Background(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "userID",
						Value: fmt.Sprintf("%d", tt.args.session.UserID),
					},
				}))

			w := httptest.NewRecorder()

			ctx := icontext.SetAuthorizer(context.Background(), tt.args.session)
			r = r.WithContext(ctx)

			handler.handlePostAuthorization(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := io.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Logf("headers: %v body: %s", res.Header, body)
				t.Errorf("%q. handlePostAuthorization() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePostAuthorization() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				var got, want authResponse
				require.NoErrorf(t, json.Unmarshal(body, &got), "body: %s", body)
				require.NoError(t, json.Unmarshal([]byte(tt.wants.body), &want))

				// The store assigns the ID, so the self link must name the ID
				// that was returned.
				require.Truef(t, got.ID.Valid(), "body: %s", body)
				require.NotNil(t, want.Links, "the expected body must have links")
				want.Links["self"] = "/private/legacy/authorizations/" + got.ID.String()

				if diff := cmp.Diff(got, want, authResponseCmpOptions...); diff != "" {
					t.Errorf("%q. handlePostAuthorization() = ***%s***", tt.name, diff)
				}
			}
			if tt.wants.bodyErr != "" {
				if diff, err := jsonDiffErr(string(body), tt.wants.bodyErr); diff != "" {
					t.Errorf("%q. handlePostAuthorization() = ***%s***", tt.name, diff)
				} else if err != nil {
					t.Errorf("%q, handlePostAuthorization() error: %v", tt.name, err)
				}
			}

		})
	}
}

func TestService_handleGetAuthorization(t *testing.T) {
	type fields struct {
		AuthorizationService influxdb.AuthorizationService
		TenantService        TenantService
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
			name: "get a authorization by id",
			fields: fields{
				AuthorizationService: &mock.AuthorizationService{
					FindAuthorizationByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Authorization, error) {
						if id == itesting.MustIDBase16("020f755c3c082000") {
							return &influxdb.Authorization{
								ID:     itesting.MustIDBase16("020f755c3c082000"),
								UserID: itesting.MustIDBase16("020f755c3c082000"),
								OrgID:  itesting.MustIDBase16("020f755c3c083000"),
								Permissions: []influxdb.Permission{
									{
										Action: influxdb.ReadAction,
										Resource: influxdb.Resource{
											Type:  influxdb.BucketsResourceType,
											OrgID: itesting.IDPtr(itesting.MustIDBase16("020f755c3c083000")),
											ID: func() *platform.ID {
												id := itesting.MustIDBase16("020f755c3c084000")
												return &id
											}(),
										},
									},
								},
								Token: "hello",
							}, nil
						}

						return nil, fmt.Errorf("not found")
					},
				},
				TenantService: &tenantService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.User, error) {
						return &influxdb.User{
							ID:   id,
							Name: "u1",
						}, nil
					},
					FindOrganizationByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Organization, error) {
						return &influxdb.Organization{
							ID:   id,
							Name: "o1",
						}, nil
					},
					FindBucketByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{
							ID:    id,
							OrgID: itesting.MustIDBase16("020f755c3c083000"),
							Name:  "b1",
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
	"createdAt": "0001-01-01T00:00:00Z",
	"updatedAt": "0001-01-01T00:00:00Z",
  "description": "",
  "id": "020f755c3c082000",
  "links": {
    "self": "/private/legacy/authorizations/020f755c3c082000",
    "user": "/api/v2/users/020f755c3c082000"
  },
  "org": "o1",
  "orgID": "020f755c3c083000",
  "permissions": [
    {
      "action": "read",
      "resource": {
				"type": "buckets",
				"orgID": "020f755c3c083000",
				"id": "020f755c3c084000",
				"name": "b1",
				"org": "o1"
			}
    }
  ],
  "status": "",
  "authorizationType": "plain",
  "token": "hello",
  "user": "u1",
  "userID": "020f755c3c082000"
}
`,
			},
		},
		{
			name: "get an authorization with a bucket permission that has no org",
			fields: fields{
				AuthorizationService: &mock.AuthorizationService{
					FindAuthorizationByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Authorization, error) {
						return &influxdb.Authorization{
							ID:     itesting.MustIDBase16("020f755c3c082000"),
							UserID: itesting.MustIDBase16("020f755c3c082000"),
							OrgID:  itesting.MustIDBase16("020f755c3c083000"),
							Permissions: []influxdb.Permission{
								{
									Action: influxdb.ReadAction,
									Resource: influxdb.Resource{
										Type: influxdb.BucketsResourceType,
										ID:   itesting.IDPtr(itesting.MustIDBase16("020f755c3c084000")),
									},
								},
							},
							Token: "hello",
						}, nil
					},
				},
				TenantService: &tenantService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.User, error) {
						return &influxdb.User{
							ID:   id,
							Name: "u1",
						}, nil
					},
					FindOrganizationByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Organization, error) {
						return &influxdb.Organization{
							ID:   id,
							Name: "o1",
						}, nil
					},
					FindBucketByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{
							ID:    id,
							OrgID: itesting.MustIDBase16("020f755c3c083000"),
							Name:  "b1",
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
  "createdAt": "0001-01-01T00:00:00Z",
  "updatedAt": "0001-01-01T00:00:00Z",
  "description": "",
  "id": "020f755c3c082000",
  "links": {
    "self": "/private/legacy/authorizations/020f755c3c082000",
    "user": "/api/v2/users/020f755c3c082000"
  },
  "org": "o1",
  "orgID": "020f755c3c083000",
  "permissions": [
    {
      "action": "read",
      "resource": {
        "type": "buckets",
        "id": "020f755c3c084000",
        "name": "b1"
      }
    }
  ],
  "status": "",
  "authorizationType": "plain",
  "token": "hello",
  "user": "u1",
  "userID": "020f755c3c082000"
}
`,
			},
		},
		{
			// Users and organizations don't belong to an organization, so a
			// permission that names one by ID and has an organization gets no
			// names. Without an organization, it gets the name.
			name: "get an authorization with user and org permissions by ID",
			fields: fields{
				AuthorizationService: &mock.AuthorizationService{
					FindAuthorizationByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Authorization, error) {
						userID := itesting.MustIDBase16("020f755c3c082000")
						orgID := itesting.MustIDBase16("020f755c3c083000")
						return &influxdb.Authorization{
							ID:     itesting.MustIDBase16("020f755c3c082000"),
							UserID: userID,
							OrgID:  orgID,
							Permissions: []influxdb.Permission{
								{
									Action:   influxdb.ReadAction,
									Resource: influxdb.Resource{Type: influxdb.UsersResourceType, ID: &userID, OrgID: &orgID},
								},
								{
									Action:   influxdb.ReadAction,
									Resource: influxdb.Resource{Type: influxdb.OrgsResourceType, ID: &orgID, OrgID: &orgID},
								},
								{
									Action:   influxdb.ReadAction,
									Resource: influxdb.Resource{Type: influxdb.UsersResourceType, ID: &userID},
								},
								{
									Action:   influxdb.ReadAction,
									Resource: influxdb.Resource{Type: influxdb.OrgsResourceType, ID: &orgID},
								},
							},
							Token: "hello",
						}, nil
					},
				},
				TenantService: &tenantService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.User, error) {
						return &influxdb.User{
							ID:   id,
							Name: "u1",
						}, nil
					},
					FindOrganizationByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Organization, error) {
						return &influxdb.Organization{
							ID:   id,
							Name: "o1",
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
  "createdAt": "0001-01-01T00:00:00Z",
  "updatedAt": "0001-01-01T00:00:00Z",
  "description": "",
  "id": "020f755c3c082000",
  "links": {
    "self": "/private/legacy/authorizations/020f755c3c082000",
    "user": "/api/v2/users/020f755c3c082000"
  },
  "org": "o1",
  "orgID": "020f755c3c083000",
  "permissions": [
    {
      "action": "read",
      "resource": {
        "type": "users",
        "id": "020f755c3c082000",
        "orgID": "020f755c3c083000"
      }
    },
    {
      "action": "read",
      "resource": {
        "type": "orgs",
        "id": "020f755c3c083000",
        "orgID": "020f755c3c083000"
      }
    },
    {
      "action": "read",
      "resource": {
        "type": "users",
        "id": "020f755c3c082000",
        "name": "u1"
      }
    },
    {
      "action": "read",
      "resource": {
        "type": "orgs",
        "id": "020f755c3c083000",
        "name": "o1"
      }
    }
  ],
  "status": "",
  "authorizationType": "plain",
  "token": "hello",
  "user": "u1",
  "userID": "020f755c3c082000"
}
`,
			},
		},
		{
			name: "not found",
			fields: fields{
				AuthorizationService: &mock.AuthorizationService{
					FindAuthorizationByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Authorization, error) {
						return nil, &errors.Error{
							Code: errors.ENotFound,
							Msg:  "authorization not found",
						}
					},
				},
				TenantService: &tenantService{},
			},
			args: args{
				id: "020f755c3c082000",
			},
			wants: wants{
				statusCode: http.StatusNotFound,
				body:       `{"code":"not found","message":"authorization not found"}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()

			handler := NewHTTPAuthHandler(zaptest.NewLogger(t), tt.fields.AuthorizationService, nil, tt.fields.TenantService)
			router := chi.NewRouter()
			router.Mount(handler.Prefix(), handler)

			w := httptest.NewRecorder()

			r := httptest.NewRequest("GET", "http://any.url", nil)
			rctx := chi.NewRouteContext()
			rctx.URLParams.Add("id", tt.args.id)
			r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))

			handler.handleGetAuthorization(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := io.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Logf("headers: %v body: %s", res.Header, body)
				t.Errorf("%q. handleGetAuthorization() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetAuthorization() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if diff, err := jsonDiff(string(body), tt.wants.body); err != nil {
				t.Errorf("%q, handleGetAuthorization. error unmarshaling json %v", tt.name, err)
			} else if tt.wants.body != "" && diff != "" {
				t.Errorf("%q. handleGetAuthorization() = -got/+want %s**", tt.name, diff)
			}
		})
	}
}

func TestService_handleGetAuthorizations(t *testing.T) {
	type fields struct {
		AuthorizationService influxdb.AuthorizationService
		TenantService        TenantService
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
			name: "get all authorizations",
			fields: fields{
				&mock.AuthorizationService{
					FindAuthorizationsFn: func(ctx context.Context, filter influxdb.AuthorizationFilter, opts ...influxdb.FindOptions) ([]*influxdb.Authorization, int, error) {
						return []*influxdb.Authorization{
							{
								ID:          itesting.MustIDBase16("0d0a657820696e74"),
								Token:       "hello",
								UserID:      itesting.MustIDBase16("2070616e656d2076"),
								OrgID:       itesting.MustIDBase16("3070616e656d2076"),
								Description: "t1",
								Permissions: influxdb.OperPermissions(),
							},
							{
								ID:          itesting.MustIDBase16("6669646573207375"),
								Token:       "example",
								UserID:      itesting.MustIDBase16("6c7574652c206f6e"),
								OrgID:       itesting.MustIDBase16("9d70616e656d2076"),
								Description: "t2",
								Permissions: influxdb.OperPermissions(),
							},
						}, 2, nil
					},
				},
				&tenantService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.User, error) {
						return &influxdb.User{
							ID:   id,
							Name: id.String(),
						}, nil
					},

					FindOrganizationByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Organization, error) {
						return &influxdb.Organization{
							ID:   id,
							Name: id.String(),
						}, nil
					},
				},
			},
			args: args{},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: fmt.Sprintf(`
{
  "links": {
    "self": "/private/legacy/authorizations"
  },
  "authorizations": [
    {
      "links": {
        "user": "/api/v2/users/2070616e656d2076",
        "self": "/private/legacy/authorizations/0d0a657820696e74"
      },
      "id": "0d0a657820696e74",
	  "userID": "2070616e656d2076",
	  "user": "2070616e656d2076",
	  "org": "3070616e656d2076",
	  "orgID": "3070616e656d2076",
      "status": "",
	  "token": "hello",
	  "description": "t1",
		"permissions": %s,
		"createdAt": "0001-01-01T00:00:00Z",
		"updatedAt": "0001-01-01T00:00:00Z"
    },
    {
      "links": {
        "user": "/api/v2/users/6c7574652c206f6e",
        "self": "/private/legacy/authorizations/6669646573207375"
      },
      "id": "6669646573207375",
      "userID": "6c7574652c206f6e",
      "user": "6c7574652c206f6e",
	  "org": "9d70616e656d2076",
	  "orgID": "9d70616e656d2076",
      "status": "",
      "token": "example",
	  "description": "t2",
		"permissions": %s,
		"createdAt": "0001-01-01T00:00:00Z",
		"updatedAt": "0001-01-01T00:00:00Z"
    }
  ]
}
`,
					MustMarshal(influxdb.OperPermissions()),
					MustMarshal(influxdb.OperPermissions())),
			},
		},
		{
			name: "skip authorizations with no org",
			fields: fields{
				&mock.AuthorizationService{
					FindAuthorizationsFn: func(ctx context.Context, filter influxdb.AuthorizationFilter, opts ...influxdb.FindOptions) ([]*influxdb.Authorization, int, error) {
						return []*influxdb.Authorization{
							{
								ID:          itesting.MustIDBase16("0d0a657820696e74"),
								Token:       "hello",
								UserID:      itesting.MustIDBase16("2070616e656d2076"),
								OrgID:       itesting.MustIDBase16("3070616e656d2076"),
								Description: "t1",
								Permissions: influxdb.OperPermissions(),
							},
							{
								ID:          itesting.MustIDBase16("6669646573207375"),
								Token:       "example",
								UserID:      itesting.MustIDBase16("6c7574652c206f6e"),
								OrgID:       itesting.MustIDBase16("9d70616e656d2076"),
								Description: "t2",
								Permissions: influxdb.OperPermissions(),
							},
						}, 2, nil
					},
				},
				&tenantService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.User, error) {
						if id.String() == "2070616e656d2076" {
							return &influxdb.User{
								ID:   id,
								Name: id.String(),
							}, nil
						}
						return nil, &errors.Error{}
					},
					FindOrganizationByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Organization, error) {
						return &influxdb.Organization{
							ID:   id,
							Name: id.String(),
						}, nil
					},
				},
			},
			args: args{},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: fmt.Sprintf(`
{
  "links": {
    "self": "/private/legacy/authorizations"
  },
  "authorizations": [
    {
      "links": {
        "user": "/api/v2/users/2070616e656d2076",
        "self": "/private/legacy/authorizations/0d0a657820696e74"
      },
      "id": "0d0a657820696e74",
	  "userID": "2070616e656d2076",
	  "user": "2070616e656d2076",
	  "org": "3070616e656d2076",
	  "orgID": "3070616e656d2076",
      "status": "",
	  "token": "hello",
	  "description": "t1",
		"permissions": %s,
		"createdAt": "0001-01-01T00:00:00Z",
		"updatedAt": "0001-01-01T00:00:00Z"
    }
  ]
}
`,
					MustMarshal(influxdb.OperPermissions())),
			},
		},
		{
			name: "skip authorizations with no user",
			fields: fields{
				&mock.AuthorizationService{
					FindAuthorizationsFn: func(ctx context.Context, filter influxdb.AuthorizationFilter, opts ...influxdb.FindOptions) ([]*influxdb.Authorization, int, error) {
						return []*influxdb.Authorization{
							{
								ID:          itesting.MustIDBase16("0d0a657820696e74"),
								Token:       "hello",
								UserID:      itesting.MustIDBase16("2070616e656d2076"),
								OrgID:       itesting.MustIDBase16("3070616e656d2076"),
								Description: "t1",
								Permissions: influxdb.OperPermissions(),
							},
							{
								ID:          itesting.MustIDBase16("6669646573207375"),
								Token:       "example",
								UserID:      itesting.MustIDBase16("6c7574652c206f6e"),
								OrgID:       itesting.MustIDBase16("9d70616e656d2076"),
								Description: "t2",
								Permissions: influxdb.OperPermissions(),
							},
						}, 2, nil
					},
				},
				&tenantService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.User, error) {
						return &influxdb.User{
							ID:   id,
							Name: id.String(),
						}, nil
					},
					FindOrganizationByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Organization, error) {
						if id.String() == "3070616e656d2076" {
							return &influxdb.Organization{
								ID:   id,
								Name: id.String(),
							}, nil
						}
						return nil, &errors.Error{}
					},
				},
			},
			args: args{},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: fmt.Sprintf(`
{
  "links": {
    "self": "/private/legacy/authorizations"
  },
  "authorizations": [
    {
      "links": {
        "user": "/api/v2/users/2070616e656d2076",
        "self": "/private/legacy/authorizations/0d0a657820696e74"
      },
      "id": "0d0a657820696e74",
	  "userID": "2070616e656d2076",
	  "user": "2070616e656d2076",
	  "org": "3070616e656d2076",
	  "orgID": "3070616e656d2076",
      "status": "",
	  "token": "hello",
	  "description": "t1",
		"permissions": %s,
		"createdAt": "0001-01-01T00:00:00Z",
		"updatedAt": "0001-01-01T00:00:00Z"
    }
  ]
}
`,
					MustMarshal(influxdb.OperPermissions())),
			},
		},
		{
			name: "get all authorizations when there are none",
			fields: fields{
				AuthorizationService: &mock.AuthorizationService{
					FindAuthorizationsFn: func(ctx context.Context, filter influxdb.AuthorizationFilter, opts ...influxdb.FindOptions) ([]*influxdb.Authorization, int, error) {
						return []*influxdb.Authorization{}, 0, nil
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
    "self": "/private/legacy/authorizations"
  },
  "authorizations": []
}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()

			s := itesting.NewTestInmemStore(t)
			storage, err := NewStore(s)
			if err != nil {
				t.Fatal(err)
			}

			svc := NewService(storage, tt.fields.TenantService)

			handler := NewHTTPAuthHandler(zaptest.NewLogger(t), svc, nil, tt.fields.TenantService)
			router := chi.NewRouter()
			router.Mount(handler.Prefix(), handler)

			r := httptest.NewRequest("GET", "http://any.url", nil)

			qp := r.URL.Query()
			for k, vs := range tt.args.queryParams {
				for _, v := range vs {
					qp.Add(k, v)
				}
			}
			r.URL.RawQuery = qp.Encode()

			w := httptest.NewRecorder()

			handler.handleGetAuthorizations(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := io.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetAuthorizations() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetAuthorizations() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if diff, err := jsonDiff(string(body), tt.wants.body); diff != "" {
				t.Errorf("%q. handleGetAuthorizations() = ***%s***", tt.name, diff)
			} else if err != nil {
				t.Errorf("%q, handleGetAuthorizations() error: %v", tt.name, err)
			}

		})
	}
}

func TestService_handleDeleteAuthorization(t *testing.T) {
	type fields struct {
		AuthorizationService influxdb.AuthorizationService
		TenantService        TenantService
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
			name: "remove a authorization by id",
			fields: fields{
				&mock.AuthorizationService{
					DeleteAuthorizationFn: func(ctx context.Context, id platform.ID) error {
						if id == itesting.MustIDBase16("020f755c3c082000") {
							return nil
						}

						return fmt.Errorf("wrong id")
					},
				},
				&tenantService{},
			},
			args: args{
				id: "020f755c3c082000",
			},
			wants: wants{
				statusCode: http.StatusNoContent,
			},
		},
		{
			name: "authorization not found",
			fields: fields{
				&mock.AuthorizationService{
					DeleteAuthorizationFn: func(ctx context.Context, id platform.ID) error {
						return &errors.Error{
							Code: errors.ENotFound,
							Msg:  "authorization not found",
						}
					},
				},
				&tenantService{},
			},
			args: args{
				id: "020f755c3c082000",
			},
			wants: wants{
				statusCode: http.StatusNotFound,
				body:       `{"code":"not found","message":"authorization not found"}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()

			handler := NewHTTPAuthHandler(zaptest.NewLogger(t), tt.fields.AuthorizationService, nil, tt.fields.TenantService)
			router := chi.NewRouter()
			router.Mount(handler.Prefix(), handler)

			w := httptest.NewRecorder()

			r := httptest.NewRequest("GET", "http://any.url", nil)
			rctx := chi.NewRouteContext()
			rctx.URLParams.Add("id", tt.args.id)
			r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))

			handler.handleDeleteAuthorization(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := io.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleDeleteAuthorization() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleDeleteAuthorization() = %v, want %v", tt.name, content, tt.wants.contentType)
			}

			if tt.wants.body != "" {
				if diff, err := jsonDiff(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handleDeleteAuthorization(). error unmarshaling json %v", tt.name, err)
				} else if diff != "" {
					t.Errorf("%q. handleDeleteAuthorization() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func jsonDiff(s1, s2 string) (diff string, err error) {
	if s1 == s2 {
		return "", nil
	}

	if s1 == "" {
		return s2, fmt.Errorf("s1 is empty")
	}

	if s2 == "" {
		return s1, fmt.Errorf("s2 is empty")
	}

	// Decode into the response type rather than influxdb.Authorization, so the
	// user, org and resource names in the response are compared too.
	var o1 authResponse
	if err = json.Unmarshal([]byte(s1), &o1); err != nil {
		return
	}

	var o2 authResponse
	if err = json.Unmarshal([]byte(s2), &o2); err != nil {
		return
	}

	return cmp.Diff(o1, o2, authResponseCmpOptions...), err
}

var authResponseCmpOptions = cmp.Options{
	cmpopts.EquateEmpty(),
	cmpopts.IgnoreFields(authResponse{}, "ID", "Token", "CreatedAt", "UpdatedAt"),
}

func jsonDiffErr(s1, s2 string) (diff string, err error) {
	if s1 == s2 {
		return "", nil
	}

	if s1 == "" {
		return s2, fmt.Errorf("s1 is empty")
	}

	if s2 == "" {
		return s1, fmt.Errorf("s2 is empty")
	}

	var o1 errors.Error
	if err = json.Unmarshal([]byte(s1), &o1); err != nil {
		return
	}

	var o2 errors.Error
	if err = json.Unmarshal([]byte(s2), &o2); err != nil {
		return
	}

	return cmp.Diff(o1, o2), err
}

func MustMarshal(o interface{}) []byte {
	b, _ := json.Marshal(o)
	return b
}

// TestAuthHandler_getNameForPermission gets the name of the resource that a
// permission names by ID.
func TestAuthHandler_getNameForPermission(t *testing.T) {
	var (
		orgID     = itesting.MustIDBase16("020f755c3c083000")
		userID    = itesting.MustIDBase16("020f755c3c082000")
		bucketID  = itesting.MustIDBase16("020f755c3c084000")
		missingID = itesting.MustIDBase16("020f755c3c085000")
		failingID = itesting.MustIDBase16("020f755c3c086000")
		dashID    = itesting.MustIDBase16("020f755c3c087000")
		invalidID = platform.ID(0)
	)
	lookupFailed := &errors.Error{Code: errors.EInternal, Msg: "bucket lookup failed"}

	h := &AuthHandler{tenantService: &tenantService{
		FindUserByIDFn: func(_ context.Context, id platform.ID) (*influxdb.User, error) {
			return &influxdb.User{ID: id, Name: "u1"}, nil
		},
		FindOrganizationByIDF: func(_ context.Context, id platform.ID) (*influxdb.Organization, error) {
			return &influxdb.Organization{ID: id, Name: "o1"}, nil
		},
		FindBucketByIDFn: func(_ context.Context, id platform.ID) (*influxdb.Bucket, error) {
			switch id {
			case bucketID:
				return &influxdb.Bucket{ID: id, OrgID: orgID, Name: "b1"}, nil
			case failingID:
				return nil, lookupFailed
			}
			return nil, &errors.Error{Code: errors.ENotFound, Msg: "bucket not found"}
		},
	}}

	perm := func(rt influxdb.ResourceType, id platform.ID, org *platform.ID) influxdb.Permission {
		return influxdb.Permission{
			Action:   influxdb.ReadAction,
			Resource: influxdb.Resource{Type: rt, ID: &id, OrgID: org},
		}
	}

	tests := []struct {
		name string
		p    influxdb.Permission
		// wantName is the name, used when wantErr is empty.
		wantName string
		// wantErr and wantCode are the error's message and code.
		wantErr  string
		wantCode string
	}{
		{
			name:     "bucket in the permission's organization",
			p:        perm(influxdb.BucketsResourceType, bucketID, &orgID),
			wantName: "b1",
		},
		{
			name:     "bucket permission with no organization",
			p:        perm(influxdb.BucketsResourceType, bucketID, nil),
			wantName: "b1",
		},
		{
			name:     "bucket that does not exist",
			p:        perm(influxdb.BucketsResourceType, missingID, &orgID),
			wantErr:  "bucket not found",
			wantCode: errors.ENotFound,
		},
		{
			name:     "bucket lookup fails",
			p:        perm(influxdb.BucketsResourceType, failingID, &orgID),
			wantErr:  "bucket lookup failed",
			wantCode: errors.EInternal,
		},
		{
			name:     "bucket with an invalid ID",
			p:        perm(influxdb.BucketsResourceType, invalidID, &orgID),
			wantErr:  "invalid ID",
			wantCode: errors.EInvalid,
		},
		{
			name:     "user with an organization",
			p:        perm(influxdb.UsersResourceType, userID, &orgID),
			wantErr:  "resource not found",
			wantCode: errors.ENotFound,
		},
		{
			name:     "organization with an organization",
			p:        perm(influxdb.OrgsResourceType, orgID, &orgID),
			wantErr:  "resource not found",
			wantCode: errors.ENotFound,
		},
		{
			name:     "user with no organization",
			p:        perm(influxdb.UsersResourceType, userID, nil),
			wantName: "u1",
		},
		{
			name:     "organization with no organization",
			p:        perm(influxdb.OrgsResourceType, orgID, nil),
			wantName: "o1",
		},
		{
			// Only buckets, organizations and users have names here.
			name: "another resource type with an organization",
			p:    perm(influxdb.DashboardsResourceType, dashID, &orgID),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, err := h.getNameForPermission(context.Background(), tt.p)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				require.Equal(t, tt.wantCode, errors.ErrorCode(err))
				require.Empty(t, name)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantName, name)
			}
		})
	}
}
