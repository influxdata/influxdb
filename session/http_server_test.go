package session

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/mock"
	"go.uber.org/zap/zaptest"
)

func TestSessionHandler_handleSignin(t *testing.T) {
	type fields struct {
		PasswordsService influxdb.PasswordsService
		SessionService   influxdb.SessionService
	}
	type args struct {
		user     string
		password string
	}
	type wants struct {
		cookie string
		code   int
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "successful compare password",
			fields: fields{
				SessionService: &mock.SessionService{
					CreateSessionFn: func(context.Context, string) (*influxdb.Session, error) {
						return &influxdb.Session{
							ID:        influxdb.ID(0),
							Key:       "abc123xyz",
							CreatedAt: time.Date(2018, 9, 26, 0, 0, 0, 0, time.UTC),
							ExpiresAt: time.Date(2030, 9, 26, 0, 0, 0, 0, time.UTC),
							UserID:    influxdb.ID(1),
						}, nil
					},
				},
				PasswordsService: &mock.PasswordsService{
					ComparePasswordFn: func(context.Context, influxdb.ID, string) error {
						return nil
					},
				},
			},
			args: args{
				user:     "user1",
				password: "supersecret",
			},
			wants: wants{
				cookie: "session=abc123xyz",
				code:   http.StatusNoContent,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			userSVC := mock.NewUserService()
			userSVC.FindUserFn = func(_ context.Context, f influxdb.UserFilter) (*influxdb.User, error) {
				return &influxdb.User{ID: 1}, nil
			}
			h := NewSessionHandler(zaptest.NewLogger(t), tt.fields.SessionService, userSVC, tt.fields.PasswordsService)

			server := httptest.NewServer(h.SignInResourceHandler())
			client := server.Client()

			r, err := http.NewRequest("POST", server.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			r.SetBasicAuth(tt.args.user, tt.args.password)

			resp, err := client.Do(r)
			if err != nil {
				t.Fatal(err)
			}

			if got, want := resp.StatusCode, tt.wants.code; got != want {
				t.Errorf("bad status code: got %d want %d", got, want)
			}

			cookie := resp.Header.Get("Set-Cookie")
			if got, want := cookie, tt.wants.cookie; got != want {
				t.Errorf("expected session cookie to be set: got %q want %q", got, want)
			}
		})
	}
}
