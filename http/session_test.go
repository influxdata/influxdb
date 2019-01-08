package http_test

import (
	"context"
	"go.uber.org/zap"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	platform "github.com/influxdata/influxdb"
	platformhttp "github.com/influxdata/influxdb/http"
	"github.com/influxdata/influxdb/mock"
)

// NewMockSessionBackend returns a SessionBackend with mock services.
func NewMockSessionBackend() *platformhttp.SessionBackend {
	return &platformhttp.SessionBackend{
		Logger: zap.NewNop().With(zap.String("handler", "session")),

		SessionService:   mock.NewSessionService(),
		BasicAuthService: mock.NewBasicAuthService("", ""),
	}
}

func TestBasicAuthHandler_handleSignin(t *testing.T) {
	type fields struct {
		BasicAuthService platform.BasicAuthService
		SessionService   platform.SessionService
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
					CreateSessionFn: func(context.Context, string) (*platform.Session, error) {
						return &platform.Session{
							ID:        platform.ID(0),
							Key:       "abc123xyz",
							CreatedAt: time.Date(2018, 9, 26, 0, 0, 0, 0, time.UTC),
							ExpiresAt: time.Date(2030, 9, 26, 0, 0, 0, 0, time.UTC),
							UserID:    platform.ID(1),
						}, nil
					},
				},
				BasicAuthService: &mock.BasicAuthService{
					ComparePasswordFn: func(context.Context, string, string) error {
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
			b := NewMockSessionBackend()
			b.BasicAuthService = tt.fields.BasicAuthService
			b.SessionService = tt.fields.SessionService
			h := platformhttp.NewSessionHandler(b)

			w := httptest.NewRecorder()
			r := httptest.NewRequest("POST", "http://localhost:9999/api/v2/signin", nil)
			r.SetBasicAuth(tt.args.user, tt.args.password)
			h.ServeHTTP(w, r)

			if got, want := w.Code, tt.wants.code; got != want {
				t.Errorf("bad status code: got %d want %d", got, want)
			}

			headers := w.Header()
			cookie := headers.Get("Set-Cookie")
			if got, want := cookie, tt.wants.cookie; got != want {
				t.Errorf("expected session cookie to be set: got %q want %q", got, want)
			}
		})
	}
}
