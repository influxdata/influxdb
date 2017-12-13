package oauth2

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
	gojwt "github.com/dgrijalva/jwt-go"
)

type MockTokenizer struct {
	Principal Principal
	ValidErr  error
	Token     Token
	CreateErr error
	ExtendErr error
}

func (m *MockTokenizer) ValidPrincipal(ctx context.Context, token Token, duration time.Duration) (Principal, error) {
	return m.Principal, m.ValidErr
}

func (m *MockTokenizer) Create(ctx context.Context, p Principal) (Token, error) {
	return m.Token, m.CreateErr
}

func (m *MockTokenizer) ExtendedPrincipal(ctx context.Context, principal Principal, extension time.Duration) (Principal, error) {
	return principal, m.ExtendErr
}

func (m *MockTokenizer) GetClaims(tokenString string) (gojwt.MapClaims, error) {
    return gojwt.MapClaims{}, nil
}

func TestCookieAuthorize(t *testing.T) {
	var test = []struct {
		Desc      string
		Value     string
		Expected  string
		Err       error
		CreateErr error
	}{
		{
			Desc:      "Unable to create token",
			Err:       ErrAuthentication,
			CreateErr: ErrAuthentication,
		},
		{
			Desc:     "Cookie token extracted",
			Value:    "reallyimportant",
			Expected: "reallyimportant",
			Err:      nil,
		},
	}
	for _, test := range test {
		cook := cookie{
			Lifespan: 1 * time.Second,
			Now: func() time.Time {
				return time.Unix(0, 0)
			},
			Tokens: &MockTokenizer{
				Token:     Token(test.Value),
				CreateErr: test.CreateErr,
			},
		}
		principal := Principal{}
		w := httptest.NewRecorder()
		err := cook.Authorize(context.Background(), w, principal)
		if err != test.Err {
			t.Fatalf("Cookie extract error; expected %v  actual %v", test.Err, err)
		}
		if test.Err != nil {
			continue
		}

		cookies := w.HeaderMap["Set-Cookie"]

		if len(cookies) == 0 {
			t.Fatal("Expected some cookies but got zero")
		}
		log.Printf("%s", cookies[0])
		if !strings.Contains(cookies[0], fmt.Sprintf("%s=%s", DefaultCookieName, test.Expected)) {
			t.Errorf("Token extract error; expected %v  actual %v", test.Expected, principal.Subject)
		}
	}
}

func TestCookieValidate(t *testing.T) {
	var test = []struct {
		Desc     string
		Name     string
		Value    string
		Lookup   string
		Expected string
		Err      error
		ValidErr error
	}{
		{
			Desc:     "No cookie of this name",
			Name:     "Auth",
			Value:    "reallyimportant",
			Lookup:   "Doesntexist",
			Expected: "",
			Err:      ErrAuthentication,
		},
		{
			Desc:     "Unable to create token",
			Name:     "Auth",
			Lookup:   "Auth",
			Err:      ErrAuthentication,
			ValidErr: ErrAuthentication,
		},
		{
			Desc:     "Cookie token extracted",
			Name:     "Auth",
			Value:    "reallyimportant",
			Lookup:   "Auth",
			Expected: "reallyimportant",
			Err:      nil,
		},
	}
	for _, test := range test {
		req, _ := http.NewRequest("", "http://howdy.com", nil)
		req.AddCookie(&http.Cookie{
			Name:  test.Name,
			Value: test.Value,
		})

		cook := cookie{
			Name:       test.Lookup,
			Lifespan:   1 * time.Second,
			Inactivity: DefaultInactivityDuration,
			Now: func() time.Time {
				return time.Unix(0, 0)
			},
			Tokens: &MockTokenizer{
				Principal: Principal{
					Subject: test.Value,
				},
				ValidErr: test.ValidErr,
			},
		}
		principal, err := cook.Validate(context.Background(), req)
		if err != test.Err {
			t.Errorf("Cookie extract error; expected %v  actual %v", test.Err, err)
		}

		if principal.Subject != test.Expected {
			t.Errorf("Token extract error; expected %v  actual %v", test.Expected, principal.Subject)
		}
	}
}

func TestNewCookieJWT(t *testing.T) {
	auth := NewCookieJWT("secret", 2*time.Second)
	if cookie, ok := auth.(*cookie); !ok {
		t.Errorf("NewCookieJWT() did not create cookie Authenticator")
	} else if cookie.Inactivity != time.Second {
		t.Errorf("NewCookieJWT() inactivity was not two seconds: %s", cookie.Inactivity)
	}

	auth = NewCookieJWT("secret", time.Hour)
	if cookie, ok := auth.(*cookie); !ok {
		t.Errorf("NewCookieJWT() did not create cookie Authenticator")
	} else if cookie.Inactivity != DefaultInactivityDuration {
		t.Errorf("NewCookieJWT() inactivity was not five minutes: %s", cookie.Inactivity)
	}

	auth = NewCookieJWT("secret", 0)
	if cookie, ok := auth.(*cookie); !ok {
		t.Errorf("NewCookieJWT() did not create cookie Authenticator")
	} else if cookie.Inactivity != DefaultInactivityDuration {
		t.Errorf("NewCookieJWT() inactivity was not five minutes: %s", cookie.Inactivity)
	}
}

func TestCookieExtend(t *testing.T) {
	history := time.Unix(-446774400, 0)
	type fields struct {
		Name       string
		Lifespan   time.Duration
		Inactivity time.Duration
		Now        func() time.Time
		Tokens     Tokenizer
	}
	type args struct {
		ctx context.Context
		w   *httptest.ResponseRecorder
		p   Principal
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    Principal
		wantErr bool
	}{
		{
			name: "Successful extention",
			want: Principal{
				Subject: "subject",
			},
			fields: fields{
				Name:       "session",
				Lifespan:   time.Second,
				Inactivity: time.Second,
				Now: func() time.Time {
					return history
				},
				Tokens: &MockTokenizer{
					Principal: Principal{
						Subject: "subject",
					},
					Token:     "token",
					ExtendErr: nil,
				},
			},
			args: args{
				ctx: context.Background(),
				w:   httptest.NewRecorder(),
				p: Principal{
					Subject: "subject",
				},
			},
		},
		{
			name:    "Unable to extend",
			wantErr: true,
			fields: fields{
				Tokens: &MockTokenizer{
					ExtendErr: fmt.Errorf("bad extend"),
				},
			},
			args: args{
				ctx: context.Background(),
				w:   httptest.NewRecorder(),
				p: Principal{
					Subject: "subject",
				},
			},
		},
		{
			name:    "Unable to create",
			wantErr: true,
			fields: fields{
				Tokens: &MockTokenizer{
					CreateErr: fmt.Errorf("bad extend"),
				},
			},
			args: args{
				ctx: context.Background(),
				w:   httptest.NewRecorder(),
				p: Principal{
					Subject: "subject",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &cookie{
				Name:       tt.fields.Name,
				Lifespan:   tt.fields.Lifespan,
				Inactivity: tt.fields.Inactivity,
				Now:        tt.fields.Now,
				Tokens:     tt.fields.Tokens,
			}
			got, err := c.Extend(tt.args.ctx, tt.args.w, tt.args.p)
			if (err != nil) != tt.wantErr {
				t.Errorf("cookie.Extend() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr == false {
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("cookie.Extend() = %v, want %v", got, tt.want)
				}

				cookies := tt.args.w.HeaderMap["Set-Cookie"]
				if len(cookies) == 0 {
					t.Fatal("Expected some cookies but got zero")
				}
				log.Printf("%s", cookies)
				want := fmt.Sprintf("%s=%s", DefaultCookieName, "token")
				if !strings.Contains(cookies[0], want) {
					t.Errorf("cookie.Extend() = %v, want %v", cookies[0], want)
				}
			}
		})
	}
}
