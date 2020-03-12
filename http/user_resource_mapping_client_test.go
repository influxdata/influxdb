package http

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/transport/http"
	"github.com/influxdata/influxdb/mock"
	"go.uber.org/zap/zaptest"
	"net/http/httptest"
)

type OwnerMemberHandler struct {
	*httprouter.Router
}

func urmSetup(t *testing.T, rt influxdb.ResourceType, ut influxdb.UserType) (func() *httptest.Server, func(serverUrl string) influxdb.UserResourceMappingService) {
	idPath := fmt.Sprintf("/api/v2/%v/:id/%vs", rt, ut)
	userIDPath := fmt.Sprintf("/api/v2/%v/:id/%vs/:userID", rt, ut)
	userSvc := mock.NewUserService()
	userSvc.FindUserByIDFn = func(i context.Context, id influxdb.ID) (user *influxdb.User, e error) {
		return &influxdb.User{ID: 1}, nil
	}
	backend := MemberBackend{
		HTTPErrorHandler:           http.ErrorHandler(0),
		log:                        zaptest.NewLogger(t),
		ResourceType:               rt,
		UserType:                   ut,
		UserResourceMappingService: mock.NewUserResourceMappingService(),
		UserService:                userSvc,
	}
	h := OwnerMemberHandler{Router: NewRouter(backend.HTTPErrorHandler)}
	h.HandlerFunc("POST", idPath, newPostMemberHandler(backend))
	h.HandlerFunc("GET", idPath, newGetMembersHandler(backend))
	h.HandlerFunc("GET", userIDPath, newGetMemberHandler(backend))
	// NOTE(affo): deletion is hardcoded to work with orgs, so we must mount that handler.
	h.HandlerFunc("DELETE", "/api/v2/orgs/:id/members/:userID", newDeleteMemberHandler(backend))

	serverFn := func() *httptest.Server {
		return httptest.NewServer(h)
	}
	clientFn := func(serverUrl string) influxdb.UserResourceMappingService {
		return &UserResourceMappingService{Client: mustNewHTTPClient(t, serverUrl, "")}
	}
	return serverFn, clientFn
}

// TestUserResourceMappingHTTPClient tests all the service functions using the urm HTTP client.
func TestUserResourceMappingHTTPClient(t *testing.T) {
	resourceTypes := []influxdb.ResourceType{
		influxdb.BucketsResourceType,
		influxdb.DashboardsResourceType,
		influxdb.OrgsResourceType,
		influxdb.SourcesResourceType,
		influxdb.TasksResourceType,
		influxdb.TelegrafsResourceType,
		influxdb.UsersResourceType,
	}

	tests := []struct {
		name string
		fn   func(t *testing.T, rt influxdb.ResourceType, ut influxdb.UserType)
	}{
		{
			name: "Find",
			fn:   FindUrm,
		},
		{
			name: "Create",
			fn:   CreateUrm,
		},
		{
			name: "Delete",
			fn:   DeleteUrm,
		},
	}
	for _, tt := range tests {
		for _, resourceType := range resourceTypes {
			for _, userType := range []influxdb.UserType{influxdb.Member, influxdb.Owner} {
				t.Run(tt.name, func(t *testing.T) {
					tt.fn(t, resourceType, userType)
				})
			}
		}
	}
}

func FindUrm(t *testing.T, rt influxdb.ResourceType, ut influxdb.UserType) {
	t.Run("ok", func(t *testing.T) {
		serverFn, clientFn := urmSetup(t, rt, ut)
		server := serverFn()
		defer server.Close()
		client := clientFn(server.URL)

		filter := influxdb.UserResourceMappingFilter{
			ResourceID:   1,
			ResourceType: rt,
			UserType:     ut,
		}
		if _, _, err := client.FindUserResourceMappings(context.Background(), filter); err != nil {
			t.Error(err)
		}
		// Now add the user ID.
		filter.UserID = 1
		if _, _, err := client.FindUserResourceMappings(context.Background(), filter); err == nil {
			t.Error("expected error got none")
		} else if !strings.Contains(err.Error(), "no user found") {
			t.Error(err)
		}
	})

	t.Run("invalid filter", func(t *testing.T) {
		serverFn, clientFn := urmSetup(t, rt, ut)
		server := serverFn()
		defer server.Close()
		client := clientFn(server.URL)

		checkErr := func(t *testing.T, err error) {
			if err == nil {
				t.Error("expected error got none")
			} else if !strings.Contains(err.Error(), "invalid filter") {
				t.Error(err)
			}
		}

		t.Run("all invalid", func(t *testing.T) {
			filter := influxdb.UserResourceMappingFilter{}
			_, _, err := client.FindUserResourceMappings(context.Background(), filter)
			checkErr(t, err)
		})

		t.Run("invalid resource ID", func(t *testing.T) {
			filter := influxdb.UserResourceMappingFilter{
				ResourceType: rt,
				UserType:     ut,
				UserID:       1,
			}
			if _, _, err := client.FindUserResourceMappings(context.Background(), filter); err == nil {
				t.Error("expected error got none")
			} else if !strings.Contains(err.Error(), "invalid filter") {
				t.Error(err)
			}
		})

		t.Run("invalid resource type", func(t *testing.T) {
			filter := influxdb.UserResourceMappingFilter{
				ResourceID: 1,
				UserType:   ut,
				UserID:     1,
			}
			if _, _, err := client.FindUserResourceMappings(context.Background(), filter); err == nil {
				t.Error("expected error got none")
			} else if !strings.Contains(err.Error(), "invalid filter") {
				t.Error(err)
			}
		})

		t.Run("invalid user type", func(t *testing.T) {
			filter := influxdb.UserResourceMappingFilter{
				ResourceID:   1,
				ResourceType: rt,
				UserID:       1,
			}
			if _, _, err := client.FindUserResourceMappings(context.Background(), filter); err == nil {
				t.Error("expected error got none")
			} else if !strings.Contains(err.Error(), "invalid filter") {
				t.Error(err)
			}
		})
	})
}

func CreateUrm(t *testing.T, rt influxdb.ResourceType, ut influxdb.UserType) {
	t.Run("ok", func(t *testing.T) {
		serverFn, clientFn := urmSetup(t, rt, ut)
		server := serverFn()
		defer server.Close()
		client := clientFn(server.URL)

		m := &influxdb.UserResourceMapping{
			UserID:       1,
			UserType:     ut,
			ResourceType: rt,
			ResourceID:   1,
		}
		if err := client.CreateUserResourceMapping(context.Background(), m); err != nil {
			t.Error(err)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		serverFn, clientFn := urmSetup(t, rt, ut)
		server := serverFn()
		defer server.Close()
		client := clientFn(server.URL)

		m := &influxdb.UserResourceMapping{
			UserType:     ut,
			ResourceType: rt,
			ResourceID:   1,
		}
		if err := client.CreateUserResourceMapping(context.Background(), m); err == nil {
			t.Errorf("expected error got none")
		}
	})
}

func DeleteUrm(t *testing.T, rt influxdb.ResourceType, ut influxdb.UserType) {
	t.Run("ok", func(t *testing.T) {
		serverFn, clientFn := urmSetup(t, rt, ut)
		server := serverFn()
		defer server.Close()
		client := clientFn(server.URL)

		if err := client.DeleteUserResourceMapping(context.Background(), 1, 1); err != nil {
			t.Error(err)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		serverFn, clientFn := urmSetup(t, rt, ut)
		server := serverFn()
		defer server.Close()
		client := clientFn(server.URL)

		if err := client.DeleteUserResourceMapping(context.Background(), 0, 1); err == nil {
			t.Error("expected error got none")
		}
	})
}
