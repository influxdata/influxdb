package tenant_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	ihttp "github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/tenant"
	itesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestUserResourceMappingService_GetMembersHandler(t *testing.T) {
	type fields struct {
		userService                influxdb.UserService
		userResourceMappingService influxdb.UserResourceMappingService
	}
	type args struct {
		resourceID string
		userType   influxdb.UserType
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
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.User, error) {
						return &influxdb.User{ID: id, Name: fmt.Sprintf("user%s", id), Status: influxdb.Active}, nil
					},
				},
				userResourceMappingService: &mock.UserResourceMappingService{
					FindMappingsFn: func(ctx context.Context, filter influxdb.UserResourceMappingFilter) ([]*influxdb.UserResourceMapping, int, error) {
						ms := []*influxdb.UserResourceMapping{
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
				userType:   influxdb.Member,
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `{
	"links": {
		"self": "/api/v2/%s/0000000000000099/members"
	},
	"users": [
		{
			"role": "member",
			"links": {
				"self": "/api/v2/users/0000000000000001"
			},
			"id": "0000000000000001",
			"name": "user0000000000000001",
			"status": "active"
		},
		{
			"role": "member",
			"links": {
				"self": "/api/v2/users/0000000000000002"
			},
			"id": "0000000000000002",
			"name": "user0000000000000002",
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
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.User, error) {
						return &influxdb.User{ID: id, Name: fmt.Sprintf("user%s", id), Status: influxdb.Active}, nil
					},
				},
				userResourceMappingService: &mock.UserResourceMappingService{
					FindMappingsFn: func(ctx context.Context, filter influxdb.UserResourceMappingFilter) ([]*influxdb.UserResourceMapping, int, error) {
						ms := []*influxdb.UserResourceMapping{
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
				userType:   influxdb.Owner,
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `{
	"links": {
		"self": "/api/v2/%s/0000000000000099/owners"
	},
	"users": [
		{
			"role": "owner",
			"links": {
				"self": "/api/v2/users/0000000000000001"
			},
			"id": "0000000000000001",
			"name": "user0000000000000001",
			"status": "active"
		},
		{
			"role": "owner",
			"links": {
				"self": "/api/v2/users/0000000000000002"
			},
			"id": "0000000000000002",
			"name": "user0000000000000002",
			"status": "active"
		}
	]
}`,
			},
		},
	}

	for _, tt := range tests {
		resourceTypes := []influxdb.ResourceType{
			influxdb.BucketsResourceType,
			influxdb.DashboardsResourceType,
			influxdb.OrgsResourceType,
			influxdb.SourcesResourceType,
			influxdb.TasksResourceType,
			influxdb.TelegrafsResourceType,
			influxdb.UsersResourceType,
		}

		for _, resourceType := range resourceTypes {
			t.Run(tt.name+"_"+string(resourceType), func(t *testing.T) {
				// create server
				h := tenant.NewURMHandler(zaptest.NewLogger(t), resourceType, "id", tt.fields.userService, tt.fields.userResourceMappingService)
				router := chi.NewRouter()
				router.Mount(fmt.Sprintf("/api/v2/%s/{id}/members", resourceType), h)
				router.Mount(fmt.Sprintf("/api/v2/%s/{id}/owners", resourceType), h)
				s := httptest.NewServer(router)
				defer s.Close()

				// craft request
				r, err := http.NewRequest("GET", fmt.Sprintf("%s/api/v2/%s/%s/%ss", s.URL, resourceType, tt.args.resourceID, tt.args.userType), nil)
				if err != nil {
					t.Fatal(err)
				}

				c := s.Client()
				res, err := c.Do(r)
				if err != nil {
					t.Fatal(err)
				}
				// check response
				content := res.Header.Get("Content-Type")
				body, _ := ioutil.ReadAll(res.Body)
				if res.StatusCode != tt.wants.statusCode {
					t.Errorf("%q. GetMembersHandler() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
				}
				if tt.wants.contentType != "" && content != tt.wants.contentType {
					t.Errorf("%q. GetMembersHandler() = %v, want %v", tt.name, content, tt.wants.contentType)
				}
				if diff := cmp.Diff(string(body), fmt.Sprintf(tt.wants.body, resourceType)); tt.wants.body != "" && diff != "" {
					t.Errorf("%q. GetMembersHandler() = ***%s***", tt.name, diff)
				}
			})
		}
	}
}

func TestUserResourceMappingService_PostMembersHandler(t *testing.T) {
	type fields struct {
		userService                influxdb.UserService
		userResourceMappingService influxdb.UserResourceMappingService
	}
	type args struct {
		resourceID string
		userType   influxdb.UserType
		user       influxdb.User
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
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.User, error) {
						return &influxdb.User{ID: id, Name: fmt.Sprintf("user%s", id), Status: influxdb.Active}, nil
					},
				},
				userResourceMappingService: &mock.UserResourceMappingService{
					CreateMappingFn: func(ctx context.Context, m *influxdb.UserResourceMapping) error {
						return nil
					},
				},
			},
			args: args{
				resourceID: "0000000000000099",
				user: influxdb.User{
					ID:     1,
					Name:   "user0000000000000001",
					Status: influxdb.Active,
				},
				userType: influxdb.Member,
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `{
	"role": "member",
	"links": {
		"self": "/api/v2/users/0000000000000001"
	},
	"id": "0000000000000001",
	"name": "user0000000000000001",
	"status": "active"
}`,
			},
		},

		{
			name: "post owners",
			fields: fields{
				userService: &mock.UserService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.User, error) {
						return &influxdb.User{ID: id, Name: fmt.Sprintf("user%s", id), Status: influxdb.Active}, nil
					},
				},
				userResourceMappingService: &mock.UserResourceMappingService{
					CreateMappingFn: func(ctx context.Context, m *influxdb.UserResourceMapping) error {
						return nil
					},
				},
			},
			args: args{
				resourceID: "0000000000000099",
				user: influxdb.User{
					ID:     2,
					Name:   "user0000000000000002",
					Status: influxdb.Active,
				},
				userType: influxdb.Owner,
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `{
	"role": "owner",
	"links": {
		"self": "/api/v2/users/0000000000000002"
	},
	"id": "0000000000000002",
	"name": "user0000000000000002",
	"status": "active"
}`,
			},
		},
	}

	for _, tt := range tests {
		resourceTypes := []influxdb.ResourceType{
			influxdb.BucketsResourceType,
			influxdb.DashboardsResourceType,
			influxdb.OrgsResourceType,
			influxdb.SourcesResourceType,
			influxdb.TasksResourceType,
			influxdb.TelegrafsResourceType,
			influxdb.UsersResourceType,
		}

		for _, resourceType := range resourceTypes {
			t.Run(tt.name+"_"+string(resourceType), func(t *testing.T) {
				// create server
				h := tenant.NewURMHandler(zaptest.NewLogger(t), resourceType, "id", tt.fields.userService, tt.fields.userResourceMappingService)
				router := chi.NewRouter()
				router.Mount(fmt.Sprintf("/api/v2/%s/{id}/members", resourceType), h)
				router.Mount(fmt.Sprintf("/api/v2/%s/{id}/owners", resourceType), h)
				s := httptest.NewServer(router)
				defer s.Close()

				// craft request
				b, err := json.Marshal(tt.args.user)
				if err != nil {
					t.Fatalf("failed to unmarshal user: %v", err)
				}

				r, err := http.NewRequest("POST", fmt.Sprintf("%s/api/v2/%s/%s/%ss", s.URL, resourceType, tt.args.resourceID, tt.args.userType), bytes.NewReader(b))
				if err != nil {
					t.Fatal(err)
				}

				c := s.Client()
				res, err := c.Do(r)
				if err != nil {
					t.Fatal(err)
				}
				content := res.Header.Get("Content-Type")
				body, _ := ioutil.ReadAll(res.Body)

				if res.StatusCode != tt.wants.statusCode {
					t.Errorf("%q. PostMembersHandler() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
				}
				if tt.wants.contentType != "" && content != tt.wants.contentType {
					t.Errorf("%q. PostMembersHandler() = %v, want %v", tt.name, content, tt.wants.contentType)
				}
				if tt.wants.body != "" {
					if diff := cmp.Diff(string(body), tt.wants.body); diff != "" {
						t.Errorf("%q. PostMembersHandler() = ***%s***", tt.name, diff)
					}
				}
			})
		}
	}
}

func TestUserResourceMappingService_Client(t *testing.T) {
	type fields struct {
		userService                influxdb.UserService
		userResourceMappingService influxdb.UserResourceMappingService
	}
	type args struct {
		resourceID string
		userType   influxdb.UserType
		user       influxdb.User
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "post members",
			fields: fields{
				userService: &mock.UserService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.User, error) {
						return &influxdb.User{ID: id, Name: fmt.Sprintf("user%s", id), Status: influxdb.Active}, nil
					},
				},
				userResourceMappingService: &mock.UserResourceMappingService{
					CreateMappingFn: func(ctx context.Context, m *influxdb.UserResourceMapping) error {
						return nil
					},
					FindMappingsFn: func(ctx context.Context, f influxdb.UserResourceMappingFilter) ([]*influxdb.UserResourceMapping, int, error) {
						return []*influxdb.UserResourceMapping{&influxdb.UserResourceMapping{}}, 1, nil
					},
				},
			},
			args: args{
				resourceID: "0000000000000099",
				user: influxdb.User{
					ID:     1,
					Name:   "user0000000000000001",
					Status: influxdb.Active,
				},
				userType: influxdb.Member,
			},
		},

		{
			name: "post owners",
			fields: fields{
				userService: &mock.UserService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.User, error) {
						return &influxdb.User{ID: id, Name: fmt.Sprintf("user%s", id), Status: influxdb.Active}, nil
					},
				},
				userResourceMappingService: &mock.UserResourceMappingService{
					CreateMappingFn: func(ctx context.Context, m *influxdb.UserResourceMapping) error {
						return nil
					},
					FindMappingsFn: func(ctx context.Context, f influxdb.UserResourceMappingFilter) ([]*influxdb.UserResourceMapping, int, error) {
						return []*influxdb.UserResourceMapping{&influxdb.UserResourceMapping{}}, 1, nil
					},
				},
			},
			args: args{
				resourceID: "0000000000000099",
				user: influxdb.User{
					ID:     2,
					Name:   "user0000000000000002",
					Status: influxdb.Active,
				},
				userType: influxdb.Owner,
			},
		},
	}

	for _, tt := range tests {
		resourceTypes := []influxdb.ResourceType{
			influxdb.BucketsResourceType,
			influxdb.DashboardsResourceType,
			influxdb.OrgsResourceType,
			influxdb.SourcesResourceType,
			influxdb.TasksResourceType,
			influxdb.TelegrafsResourceType,
			influxdb.UsersResourceType,
		}

		for _, resourceType := range resourceTypes {
			t.Run(tt.name+"_"+string(resourceType), func(t *testing.T) {
				// create server
				h := tenant.NewURMHandler(zaptest.NewLogger(t), resourceType, "id", tt.fields.userService, tt.fields.userResourceMappingService)
				router := chi.NewRouter()
				router.Mount(fmt.Sprintf("/api/v2/%s/{id}/members", resourceType), h)
				router.Mount(fmt.Sprintf("/api/v2/%s/{id}/owners", resourceType), h)
				s := httptest.NewServer(router)
				defer s.Close()
				ctx := context.Background()

				resourceID := itesting.MustIDBase16(tt.args.resourceID)
				urm := &influxdb.UserResourceMapping{ResourceType: resourceType, ResourceID: resourceID, UserType: tt.args.userType, UserID: tt.args.user.ID}

				httpClient, err := ihttp.NewHTTPClient(s.URL, "", false)
				if err != nil {
					t.Fatal(err)
				}
				c := tenant.UserResourceMappingClient{Client: httpClient}
				err = c.CreateUserResourceMapping(ctx, urm)

				if err != nil {
					t.Fatal(err)
				}

				_, n, err := c.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{ResourceID: resourceID, ResourceType: resourceType, UserType: tt.args.userType})
				if err != nil {
					t.Fatal(err)
				}
				if n != 1 {
					t.Fatalf("expected 1 urm to be created, got: %d", n)
				}
			})
		}
	}
}
