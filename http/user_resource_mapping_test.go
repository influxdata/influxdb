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

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
	"github.com/julienschmidt/httprouter"
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
						return &platform.User{ID: id, Name: fmt.Sprintf("user%s", id)}, nil
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
        "log": "/api/v2/users/0000000000000001/log",
        "self": "/api/v2/users/0000000000000001"
      },
      "id": "0000000000000001",
      "name": "user0000000000000001",
      "role": "member"
    },
    {
      "links": {
        "log": "/api/v2/users/0000000000000002/log",
        "self": "/api/v2/users/0000000000000002"
      },
      "id": "0000000000000002",
      "name": "user0000000000000002",
      "role": "member"
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
						return &platform.User{ID: id, Name: fmt.Sprintf("user%s", id)}, nil
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
        "log": "/api/v2/users/0000000000000001/log",
        "self": "/api/v2/users/0000000000000001"
      },
      "id": "0000000000000001",
      "name": "user0000000000000001",
      "role": "owner"
    },
    {
      "links": {
        "log": "/api/v2/users/0000000000000002/log",
        "self": "/api/v2/users/0000000000000002"
      },
      "id": "0000000000000002",
      "name": "user0000000000000002",
      "role": "owner"
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
				h := newGetMembersHandler(tt.fields.userResourceMappingService, tt.fields.userService, resourceType, tt.args.userType)
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
						return &platform.User{ID: id, Name: fmt.Sprintf("user%s", id)}, nil
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
					ID:   1,
					Name: "user0000000000000001",
				},
				userType: platform.Member,
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `
{
	"links": {
		"log": "/api/v2/users/0000000000000001/log",
		"self": "/api/v2/users/0000000000000001"
	},
	"id": "0000000000000001",
	"name": "user0000000000000001",
	"role": "member"
}`,
			},
		},

		{
			name: "post owners",
			fields: fields{
				userService: &mock.UserService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*platform.User, error) {
						return &platform.User{ID: id, Name: fmt.Sprintf("user%s", id)}, nil
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
					ID:   2,
					Name: "user0000000000000002",
				},
				userType: platform.Owner,
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `
{
	"links": {
		"log": "/api/v2/users/0000000000000002/log",
		"self": "/api/v2/users/0000000000000002"
	},
	"id": "0000000000000002",
	"name": "user0000000000000002",
	"role": "owner"
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
				h := newPostMemberHandler(tt.fields.userResourceMappingService, tt.fields.userService, resourceType, tt.args.userType)
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
				if eq, diff, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
					t.Errorf("%q. PostMembersHandler() = ***%s***", tt.name, diff)
				}
			})
		}
	}
}
