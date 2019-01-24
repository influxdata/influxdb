package http

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/mock"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func initUserService(f platformtesting.UserFields, t *testing.T) (platform.UserService, string, func()) {
	t.Helper()
	svc := inmem.NewService()
	svc.IDGenerator = f.IDGenerator

	ctx := context.Background()
	for _, u := range f.Users {
		if err := svc.PutUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}

	handler := NewUserHandler()
	handler.UserService = svc
	server := httptest.NewServer(handler)
	client := UserService{
		Addr:     server.URL,
		OpPrefix: inmem.OpPrefix,
	}

	done := server.Close

	return &client, inmem.OpPrefix, done
}

func TestUserService(t *testing.T) {
	t.Parallel()
	platformtesting.UserService(initUserService, t)
}

func TestService_handleGetUsers(t *testing.T) {
	type fields struct {
		UserService platform.UserService
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
			name: "get all users",
			fields: fields{
				&mock.UserService{
					FindUsersFn: func(ctx context.Context, filter platform.UserFilter, opts ...platform.FindOptions) ([]*platform.User, int, error) {
						return []*platform.User{
							{
								ID:   platformtesting.MustIDBase16("0b501e7e557ab1ed"),
								Name: "abc",
							},
							{
								ID:   platformtesting.MustIDBase16("c0175f0077a77005"),
								Name: "def",
							},
							{
								ID:   platformtesting.MustIDBase16("7365637465747572"),
								Name: "xyz",
							},
						}, 3, nil
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
    "self": "/api/v2/users?descending=false&limit=20&offset=0"
  },
  "users": [
    {
      "links": {
        "self": "/api/v2/users/0b501e7e557ab1ed",
        "log": "/api/v2/users/0b501e7e557ab1ed/log"
      },
      "id": "0b501e7e557ab1ed",
      "name": "abc"
    },
    {
      "links": {
        "self": "/api/v2/users/c0175f0077a77005",
        "log": "/api/v2/users/c0175f0077a77005/log"
      },
      "id": "c0175f0077a77005",
      "name": "def"
    },
    {
      "links": {
        "self": "/api/v2/users/7365637465747572",
        "log": "/api/v2/users/7365637465747572/log"
      },
      "id": "7365637465747572",
      "name": "xyz"
    }
  ]
}
`,
			},
		},
		{
			name: "get users by offset and limit",
			fields: fields{
				&mock.UserService{
					FindUsersFn: func(ctx context.Context, filter platform.UserFilter, opts ...platform.FindOptions) ([]*platform.User, int, error) {
						return []*platform.User{
							{
								ID:   platformtesting.MustIDBase16("c0175f0077a77005"),
								Name: "def",
							},
						}, 1, nil
					},
				},
			},
			args: args{
				map[string][]string{
					"offset": {"1"},
					"limit":  {"1"},
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "prev": "/api/v2/users?descending=false&limit=1&offset=0",
    "self": "/api/v2/users?descending=false&limit=1&offset=1",
    "next": "/api/v2/users?descending=false&limit=1&offset=2"
  },
  "users": [
    {
      "links": {
        "self": "/api/v2/users/c0175f0077a77005",
        "log": "/api/v2/users/c0175f0077a77005/log"
      },
      "id": "c0175f0077a77005",
      "name": "def"
    }
  ]
}
`,
			},
		},
	}

	for _, tt := range tests[1:] {
		t.Run(tt.name, func(t *testing.T) {
			h := NewUserHandler()
			h.UserService = tt.fields.UserService

			r := httptest.NewRequest("GET", "http://any.url", nil)

			qp := r.URL.Query()
			for k, vs := range tt.args.queryParams {
				for _, v := range vs {
					qp.Add(k, v)
				}
			}
			r.URL.RawQuery = qp.Encode()

			w := httptest.NewRecorder()

			h.handleGetUsers(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetBuckets() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetBuckets() = %v, want %v", tt.name, content, tt.wants.contentType)
			}

			if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil || tt.wants.body != "" && !eq {
				t.Errorf("%q. handleGetBuckets() = ***%v***", tt.name, diff)
			}
		})
	}
}
