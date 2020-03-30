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

	"github.com/influxdata/httprouter"
	platform "github.com/influxdata/influxdb"
	kithttp "github.com/influxdata/influxdb/kit/transport/http"
	"github.com/influxdata/influxdb/mock"
	"go.uber.org/zap/zaptest"
)

func TestUserResourceMappingService_GetMembersHandler(t *testing.T) {
	type fields struct {
		userService                platform.UserService
		userResourceMappingService platform.UserResourceMappingService
	}
	type args struct {
		resourceID string
		userType   platform.UserType
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
			name: "get members",
			fields: fields{
				userService: &mock.UserService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*platform.User, error) {
						return &platform.User{ID: id, Name: fmt.Sprintf("user%s", id), Status: platform.Active}, nil
					},
				},
				userResourceMappingService: &mock.UserResourceMappingService{
					FindMappingsFn: func(ctx context.Context, filter platform.UserResourceMappingFilter) ([]*platform.UserResourceMapping, int, error) {
						ms := []*platform.UserResourceMapping{
							{
								ResourceID:   filter.ResourceID,
								ResourceType: filter.ResourceType,
								UserType:     filter.UserType,
								UserID:       1,
							},
							{
								ResourceID:   filter.ResourceID,
								ResourceType: filter.ResourceType,
								UserType:     filter.UserType,
								UserID:       2,
							},
						}
						return ms, len(ms), nil
					},
				},
			},
			args: args{
				resourceID: "0000000000000099",
				userType:   platform.Member,
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/%s/0000000000000099/members"
  },
  "users": [
    {
      "links": {
        "logs": "/api/v2/users/0000000000000001/logs",
        "self": "/api/v2/users/0000000000000001"
      },
      "id": "0000000000000001",
      "name": "user0000000000000001",
			"role": "member",
			"status": "active"
    },
    {
      "links": {
        "logs": "/api/v2/users/0000000000000002/logs",
        "self": "/api/v2/users/0000000000000002"
      },
      "id": "0000000000000002",
      "name": "user0000000000000002",
      "role": "member",
			"status": "active"
    }
  ]
}`,
			},
		},

		{
			name: "get owners",
			fields: fields{
				userService: &mock.UserService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*platform.User, error) {
						return &platform.User{ID: id, Name: fmt.Sprintf("user%s", id), Status: platform.Active}, nil
					},
				},
				userResourceMappingService: &mock.UserResourceMappingService{
					FindMappingsFn: func(ctx context.Context, filter platform.UserResourceMappingFilter) ([]*platform.UserResourceMapping, int, error) {
						ms := []*platform.UserResourceMapping{
							{
								ResourceID:   filter.ResourceID,
								ResourceType: filter.ResourceType,
								UserType:     filter.UserType,
								UserID:       1,
							},
							{
								ResourceID:   filter.ResourceID,
								ResourceType: filter.ResourceType,
								UserType:     filter.UserType,
								UserID:       2,
							},
						}
						return ms, len(ms), nil
					},
				},
			},
			args: args{
				resourceID: "0000000000000099",
				userType:   platform.Owner,
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/%s/0000000000000099/owners"
  },
  "users": [
    {
      "links": {
        "logs": "/api/v2/users/0000000000000001/logs",
        "self": "/api/v2/users/0000000000000001"
      },
      "id": "0000000000000001",
      "name": "user0000000000000001",
      "role": "owner",
			"status": "active"
    },
    {
      "links": {
        "logs": "/api/v2/users/0000000000000002/logs",
        "self": "/api/v2/users/0000000000000002"
      },
      "id": "0000000000000002",
      "name": "user0000000000000002",
			"role": "owner",
			"status": "active"
    }
  ]
}`,
			},
		},
	}

	for _, tt := range tests {
		resourceTypes := []platform.ResourceType{
			platform.BucketsResourceType,
			platform.DashboardsResourceType,
			platform.OrgsResourceType,
			platform.SourcesResourceType,
			platform.TasksResourceType,
			platform.TelegrafsResourceType,
			platform.UsersResourceType,
		}

		for _, resourceType := range resourceTypes {
			t.Run(tt.name+"_"+string(resourceType), func(t *testing.T) {
				r := httptest.NewRequest("GET", "http://any.url", nil)
				r = r.WithContext(context.WithValue(
					context.TODO(),
					httprouter.ParamsKey,
					httprouter.Params{
						{
							Key:   "id",
							Value: tt.args.resourceID,
						},
					}))

				w := httptest.NewRecorder()
				memberBackend := MemberBackend{
					log:                        zaptest.NewLogger(t),
					ResourceType:               resourceType,
					UserType:                   tt.args.userType,
					UserResourceMappingService: tt.fields.userResourceMappingService,
					UserService:                tt.fields.userService,
				}
				h := newGetMembersHandler(memberBackend)
				h.ServeHTTP(w, r)

				res := w.Result()
				content := res.Header.Get("Content-Type")
				body, _ := ioutil.ReadAll(res.Body)

				if res.StatusCode != tt.wants.statusCode {
					t.Errorf("%q. GetMembersHandler() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
				}
				if tt.wants.contentType != "" && content != tt.wants.contentType {
					t.Errorf("%q. GetMembersHandler() = %v, want %v", tt.name, content, tt.wants.contentType)
				}
				if eq, diff, _ := jsonEqual(string(body), fmt.Sprintf(tt.wants.body, resourceType)); tt.wants.body != "" && !eq {
					t.Errorf("%q. GetMembersHandler() = ***%s***", tt.name, diff)
				}
			})
		}
	}
}

func TestUserResourceMappingService_GetMemberHandler(t *testing.T) {
	type fields struct {
		userService                platform.UserService
		userResourceMappingService platform.UserResourceMappingService
	}
	type args struct {
		resourceID   string
		userID       string
		userType     platform.UserType
		resourceType platform.ResourceType
	}
	type wants struct {
		statusCode  int
		contentType string
		body        func(a args) string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "get member",
			fields: fields{
				userService: &mock.UserService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*platform.User, error) {
						return &platform.User{ID: id, Name: fmt.Sprintf("user%s", id), Status: platform.Active}, nil
					},
				},
				userResourceMappingService: &mock.UserResourceMappingService{
					FindMappingsFn: func(ctx context.Context, filter platform.UserResourceMappingFilter) ([]*platform.UserResourceMapping, int, error) {
						ms := []*platform.UserResourceMapping{
							{
								ResourceID:   filter.ResourceID,
								ResourceType: filter.ResourceType,
								UserType:     filter.UserType,
								UserID:       1,
							},
						}
						return ms, len(ms), nil
					},
				},
			},
			args: args{
				resourceID: "0000000000000099",
				userID:     "0000000000000001",
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: func(a args) string {
					return fmt.Sprintf(`
{
  "links": {
    "logs": "/api/v2/users/0000000000000001/logs",
    "self": "/api/v2/users/0000000000000001"
  },
  "id": "0000000000000001",
  "name": "user0000000000000001",
  "role": "%v",
  "status": "active"
}`, a.userType)
				},
			},
		},
		{
			name: "not found",
			fields: fields{
				userService: &mock.UserService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*platform.User, error) {
						return nil, nil
					},
				},
				userResourceMappingService: &mock.UserResourceMappingService{
					FindMappingsFn: func(ctx context.Context, filter platform.UserResourceMappingFilter) ([]*platform.UserResourceMapping, int, error) {
						return nil, 0, nil
					},
				},
			},
			args: args{
				resourceID: "0000000000000099",
				userID:     "0000000000000001",
			},
			wants: wants{
				statusCode:  http.StatusNotFound,
				contentType: "application/json; charset=utf-8",
				body: func(a args) string {
					return fmt.Sprintf(`
{
  "code": "not found",
  "message": "no user found matching filter [resource: %v/0000000000000099, user: %v/0000000000000001]"
}`, a.resourceType, a.userType)
				},
			},
		},
		{
			name: "multiple found",
			fields: fields{
				userService: &mock.UserService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*platform.User, error) {
						return nil, nil
					},
				},
				userResourceMappingService: &mock.UserResourceMappingService{
					FindMappingsFn: func(ctx context.Context, filter platform.UserResourceMappingFilter) ([]*platform.UserResourceMapping, int, error) {
						ms := []*platform.UserResourceMapping{
							{
								UserID: 1,
							},
							{
								UserID: 2,
							},
						}
						return ms, len(ms), nil
					},
				},
			},
			args: args{
				resourceID: "0000000000000099",
				userID:     "0000000000000001",
			},
			wants: wants{
				statusCode:  http.StatusInternalServerError,
				contentType: "application/json; charset=utf-8",
				body: func(a args) string {
					return fmt.Sprintf(`
{
  "code": "internal error",
  "message": "please report this error: multiple users found matching filter [resource: %v/0000000000000099, user: %v/0000000000000001]"
}`, a.resourceType, a.userType)
				},
			},
		},
		{
			name: "bad url - missing id",
			fields: fields{
				userService: &mock.UserService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*platform.User, error) {
						return nil, nil
					},
				},
				userResourceMappingService: &mock.UserResourceMappingService{
					FindMappingsFn: func(ctx context.Context, filter platform.UserResourceMappingFilter) ([]*platform.UserResourceMapping, int, error) {
						return nil, 0, nil
					},
				},
			},
			args: args{
				// no arg will result in malformed url
			},
			wants: wants{
				statusCode:  http.StatusBadRequest,
				contentType: "application/json; charset=utf-8",
				body: func(a args) string {
					return `
{
  "code": "invalid",
  "message": "url missing id"
}`
				},
			},
		},
		{
			name: "bad url - missing userID",
			fields: fields{
				userService: &mock.UserService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*platform.User, error) {
						return nil, nil
					},
				},
				userResourceMappingService: &mock.UserResourceMappingService{
					FindMappingsFn: func(ctx context.Context, filter platform.UserResourceMappingFilter) ([]*platform.UserResourceMapping, int, error) {
						return nil, 0, nil
					},
				},
			},
			args: args{
				resourceID: "0000000000000001",
			},
			wants: wants{
				statusCode:  http.StatusBadRequest,
				contentType: "application/json; charset=utf-8",
				body: func(a args) string {
					return `
{
  "code": "invalid",
  "message": "url missing userID"
}`
				},
			},
		},
	}

	for _, tt := range tests {
		resourceTypes := []platform.ResourceType{
			platform.BucketsResourceType,
			platform.DashboardsResourceType,
			platform.OrgsResourceType,
			platform.SourcesResourceType,
			platform.TasksResourceType,
			platform.TelegrafsResourceType,
			platform.UsersResourceType,
		}

		for _, resourceType := range resourceTypes {
			tt.args.resourceType = resourceType
			for _, userType := range []platform.UserType{platform.Member, platform.Owner} {
				tt.args.userType = userType
				t.Run(fmt.Sprintf("%v - %v - %v", tt.name, resourceType, userType), func(t *testing.T) {
					r := httptest.NewRequest("GET", "http://any.url", nil)
					r = r.WithContext(context.WithValue(
						context.TODO(),
						httprouter.ParamsKey,
						httprouter.Params{
							{
								Key:   "id",
								Value: tt.args.resourceID,
							},
							{
								Key:   "userID",
								Value: tt.args.userID,
							},
						}))

					w := httptest.NewRecorder()
					memberBackend := MemberBackend{
						HTTPErrorHandler:           kithttp.ErrorHandler(0),
						log:                        zaptest.NewLogger(t),
						ResourceType:               resourceType,
						UserType:                   userType,
						UserResourceMappingService: tt.fields.userResourceMappingService,
						UserService:                tt.fields.userService,
					}
					h := newGetMemberHandler(memberBackend)
					h.ServeHTTP(w, r)

					res := w.Result()
					content := res.Header.Get("Content-Type")
					body, _ := ioutil.ReadAll(res.Body)

					if res.StatusCode != tt.wants.statusCode {
						t.Errorf("%q. GetMemberHandler() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
					}
					if tt.wants.contentType != "" && content != tt.wants.contentType {
						t.Errorf("%q. GetMemberHandler() = %v, want %v", tt.name, content, tt.wants.contentType)
					}
					wBody := tt.wants.body(tt.args)
					if eq, diff, _ := jsonEqual(string(body), wBody); wBody != "" && !eq {
						t.Errorf("%q. GetMemberHandler() = ***%s***", tt.name, diff)
					}
				})
			}
		}
	}
}

func TestUserResourceMappingService_PostMembersHandler(t *testing.T) {
	type fields struct {
		userService                platform.UserService
		userResourceMappingService platform.UserResourceMappingService
	}
	type args struct {
		resourceID string
		userType   platform.UserType
		user       platform.User
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
			name: "post members",
			fields: fields{
				userService: &mock.UserService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*platform.User, error) {
						return &platform.User{ID: id, Name: fmt.Sprintf("user%s", id), Status: platform.Active}, nil
					},
				},
				userResourceMappingService: &mock.UserResourceMappingService{
					CreateMappingFn: func(ctx context.Context, m *platform.UserResourceMapping) error {
						return nil
					},
				},
			},
			args: args{
				resourceID: "0000000000000099",
				user: platform.User{
					ID:     1,
					Name:   "user0000000000000001",
					Status: platform.Active,
				},
				userType: platform.Member,
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `
{
	"links": {
		"logs": "/api/v2/users/0000000000000001/logs",
		"self": "/api/v2/users/0000000000000001"
	},
	"id": "0000000000000001",
	"name": "user0000000000000001",
	"role": "member",
	"status": "active"
}`,
			},
		},

		{
			name: "post owners",
			fields: fields{
				userService: &mock.UserService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*platform.User, error) {
						return &platform.User{ID: id, Name: fmt.Sprintf("user%s", id), Status: platform.Active}, nil
					},
				},
				userResourceMappingService: &mock.UserResourceMappingService{
					CreateMappingFn: func(ctx context.Context, m *platform.UserResourceMapping) error {
						return nil
					},
				},
			},
			args: args{
				resourceID: "0000000000000099",
				user: platform.User{
					ID:     2,
					Name:   "user0000000000000002",
					Status: platform.Active,
				},
				userType: platform.Owner,
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `
{
	"links": {
		"logs": "/api/v2/users/0000000000000002/logs",
		"self": "/api/v2/users/0000000000000002"
	},
	"id": "0000000000000002",
	"name": "user0000000000000002",
	"role": "owner",
	"status": "active"
}`,
			},
		},
	}

	for _, tt := range tests {
		resourceTypes := []platform.ResourceType{
			platform.BucketsResourceType,
			platform.DashboardsResourceType,
			platform.OrgsResourceType,
			platform.SourcesResourceType,
			platform.TasksResourceType,
			platform.TelegrafsResourceType,
			platform.UsersResourceType,
		}

		for _, resourceType := range resourceTypes {
			t.Run(tt.name+"_"+string(resourceType), func(t *testing.T) {
				b, err := json.Marshal(tt.args.user)
				if err != nil {
					t.Fatalf("failed to unmarshal user: %v", err)
				}

				r := httptest.NewRequest("POST", "http://any.url", bytes.NewReader(b))
				r = r.WithContext(context.WithValue(
					context.TODO(),
					httprouter.ParamsKey,
					httprouter.Params{
						{
							Key:   "id",
							Value: tt.args.resourceID,
						},
					}))

				w := httptest.NewRecorder()
				memberBackend := MemberBackend{
					log:                        zaptest.NewLogger(t),
					ResourceType:               resourceType,
					UserType:                   tt.args.userType,
					UserResourceMappingService: tt.fields.userResourceMappingService,
					UserService:                tt.fields.userService,
				}
				h := newPostMemberHandler(memberBackend)
				h.ServeHTTP(w, r)

				res := w.Result()
				content := res.Header.Get("Content-Type")
				body, _ := ioutil.ReadAll(res.Body)

				if res.StatusCode != tt.wants.statusCode {
					t.Errorf("%q. PostMembersHandler() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
				}
				if tt.wants.contentType != "" && content != tt.wants.contentType {
					t.Errorf("%q. PostMembersHandler() = %v, want %v", tt.name, content, tt.wants.contentType)
				}
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, PostMembersHandler(). error unmarshaling json %v", tt.name, err)
				} else if tt.wants.body != "" && !eq {
					t.Errorf("%q. PostMembersHandler() = ***%s***", tt.name, diff)
				}
			})
		}
	}
}
