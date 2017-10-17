package server

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/log"
	"github.com/influxdata/chronograf/mocks"
	"github.com/influxdata/chronograf/oauth2"
)

type MockUsers struct{}

func TestService_Me(t *testing.T) {
	type fields struct {
		UsersStore chronograf.UsersStore
		Logger     chronograf.Logger
		UseAuth    bool
	}
	type args struct {
		w *httptest.ResponseRecorder
		r *http.Request
	}
	tests := []struct {
		name            string
		fields          fields
		args            args
		principal       oauth2.Principal
		wantStatus      int
		wantContentType string
		wantBody        string
	}{
		{
			name: "Existing user",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("GET", "http://example.com/foo", nil),
			},
			fields: fields{
				UseAuth: true,
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, name string) (*chronograf.User, error) {
						return &chronograf.User{
							Name:     "me",
							Provider: "GitHub",
							Passwd:   "hunter2",
						}, nil
					},
					AllF: func(ctx context.Context) ([]chronograf.User, error) {
						return []chronograf.User{
							{
								Name:     "me",
								Provider: "GitHub",
								Passwd:   "hunter2",
							},
							{
								Name:     "billietta",
								Provider: "Google",
								Passwd:   "billiettaspassword",
							},
						}, nil
					},
				},
			},
			principal: oauth2.Principal{
				Subject: "me",
				Issuer:  "GitHub",
			},
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody: `{"name":"me","password":"hunter2","provider":"GitHub","links":{"self":"/chronograf/v1/users/me"}}
`,
		},
		{
			name: "New user",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("GET", "http://example.com/foo", nil),
			},
			fields: fields{
				UseAuth: true,
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, name string) (*chronograf.User, error) {
						return nil, fmt.Errorf("Unknown User")
					},
					AddF: func(ctx context.Context, u *chronograf.User) (*chronograf.User, error) {
						return u, nil
					},
					AllF: func(ctx context.Context) ([]chronograf.User, error) {
						return []chronograf.User{
							{
								Name:     "me",
								Provider: "GitHub",
								Passwd:   "hunter2",
							},
							{
								Name:     "billietta",
								Provider: "Google",
								Passwd:   "billiettaspassword",
							},
						}, nil
					},
				},
			},
			principal: oauth2.Principal{
				Subject: "secret",
				Issuer:  "Auth0",
			},
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody: `{"name":"secret","links":{"self":"/chronograf/v1/users/secret"}}
`,
		},
		{
			name: "Error adding user",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("GET", "http://example.com/foo", nil),
			},
			fields: fields{
				UseAuth: true,
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, name string) (*chronograf.User, error) {
						return nil, fmt.Errorf("Unknown User")
					},
					AddF: func(ctx context.Context, u *chronograf.User) (*chronograf.User, error) {
						return nil, fmt.Errorf("Why Heavy?")
					},
					AllF: func(ctx context.Context) ([]chronograf.User, error) {
						return []chronograf.User{
							{
								Name:     "me",
								Provider: "GitHub",
								Passwd:   "hunter2",
							},
							{
								Name:     "billietta",
								Provider: "Google",
								Passwd:   "billiettaspassword",
							},
						}, nil
					},
				},
				Logger: log.New(log.DebugLevel),
			},
			principal: oauth2.Principal{
				Subject: "secret",
				Issuer:  "Heroku",
			},
			wantStatus:      http.StatusInternalServerError,
			wantContentType: "application/json",
			wantBody:        `{"code":500,"message":"Unknown error: error storing user secret: Why Heavy?"}`,
		},
		{
			name: "No Auth",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("GET", "http://example.com/foo", nil),
			},
			fields: fields{
				UseAuth: false,
				Logger:  log.New(log.DebugLevel),
			},
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
			wantBody: `{"links":{"self":"/chronograf/v1/users/me"}}
`,
		},
		{
			name: "Empty Principal",
			args: args{
				w: httptest.NewRecorder(),
				r: httptest.NewRequest("GET", "http://example.com/foo", nil),
			},
			fields: fields{
				UseAuth: true,
				Logger:  log.New(log.DebugLevel),
			},
			wantStatus: http.StatusUnprocessableEntity,
			principal: oauth2.Principal{
				Subject: "",
				Issuer:  "",
			},
		},
	}
	for _, tt := range tests {
		tt.args.r = tt.args.r.WithContext(context.WithValue(context.Background(), oauth2.PrincipalKey, tt.principal))
		h := &Service{
			UsersStore: tt.fields.UsersStore,
			Logger:     tt.fields.Logger,
			UseAuth:    tt.fields.UseAuth,
		}

		h.Me(tt.args.w, tt.args.r)

		resp := tt.args.w.Result()
		content := resp.Header.Get("Content-Type")
		body, _ := ioutil.ReadAll(resp.Body)

		if resp.StatusCode != tt.wantStatus {
			t.Errorf("%q. Me() = %v, want %v", tt.name, resp.StatusCode, tt.wantStatus)
		}
		if tt.wantContentType != "" && content != tt.wantContentType {
			t.Errorf("%q. Me() = %v, want %v", tt.name, content, tt.wantContentType)
		}
		if tt.wantBody != "" && string(body) != tt.wantBody {
			t.Errorf("%q. Me() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wantBody)
		}
	}
}
