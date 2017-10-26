package oauth2_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/chronograf/oauth2"
)

func TestAuthenticate(t *testing.T) {
	history := time.Unix(-446774400, 0)
	var tests = []struct {
		Desc      string
		Secret    string
		Token     oauth2.Token
		Duration  time.Duration
		Principal oauth2.Principal
		Err       error
	}{
		{
			Desc:   "Test bad jwt token",
			Secret: "secret",
			Token:  "badtoken",
			Principal: oauth2.Principal{
				Subject: "",
			},
			Err: errors.New("token contains an invalid number of segments"),
		},
		{
			Desc:     "Test valid jwt token",
			Secret:   "secret",
			Token:    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIvY2hyb25vZ3JhZi92MS91c2Vycy8xIiwibmFtZSI6IkRvYyBCcm93biIsImlhdCI6LTQ0Njc3NDQwMCwiZXhwIjotNDQ2Nzc0Mzk5LCJuYmYiOi00NDY3NzQ0MDB9.Ga0zGXWTT2CBVnnIhIO5tUAuBEVk4bKPaT4t4MU1ngo",
			Duration: time.Second,
			Principal: oauth2.Principal{
				Subject:   "/chronograf/v1/users/1",
				ExpiresAt: history.Add(time.Second),
				IssuedAt:  history,
			},
		},
		{
			Desc:     "Test valid jwt token with organization",
			Secret:   "secret",
			Token:    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIvY2hyb25vZ3JhZi92MS91c2Vycy8xIiwibmFtZSI6IkRvYyBCcm93biIsIm9yZyI6IjEzMzciLCJpYXQiOi00NDY3NzQ0MDAsImV4cCI6LTQ0Njc3NDM5OSwibmJmIjotNDQ2Nzc0NDAwfQ.b38MK5liimWsvvJr4a3GNYRDJOAN7WCrfZ0FfZftqjc",
			Duration: time.Second,
			Principal: oauth2.Principal{
				Subject:      "/chronograf/v1/users/1",
				Organization: "1337",
				ExpiresAt:    history.Add(time.Second),
				IssuedAt:     history,
			},
		},
		{
			Desc:     "Test expired jwt token",
			Secret:   "secret",
			Token:    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIvY2hyb25vZ3JhZi92MS91c2Vycy8xIiwibmFtZSI6IkRvYyBCcm93biIsImlhdCI6LTQ0Njc3NDQwMCwiZXhwIjotNDQ2Nzc0NDAxLCJuYmYiOi00NDY3NzQ0MDB9.vWXdm0-XQ_pW62yBpSISFFJN_yz0vqT9_INcUKTp5Q8",
			Duration: time.Second,
			Principal: oauth2.Principal{
				Subject:   "",
				ExpiresAt: history.Add(time.Second),
				IssuedAt:  history,
			},
			Err: errors.New("token is expired by 1s"),
		},
		{
			Desc:     "Test jwt token not before time",
			Secret:   "secret",
			Token:    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIvY2hyb25vZ3JhZi92MS91c2Vycy8xIiwibmFtZSI6IkRvYyBCcm93biIsImlhdCI6LTQ0Njc3NDQwMCwiZXhwIjotNDQ2Nzc0NDAwLCJuYmYiOi00NDY3NzQzOTl9.TMGAhv57u1aosjc4ywKC7cElP1tKyQH7GmRF2ToAxlE",
			Duration: time.Second,
			Principal: oauth2.Principal{
				Subject:   "",
				ExpiresAt: history.Add(time.Second),
				IssuedAt:  history,
			},
			Err: errors.New("token is not valid yet"),
		},
		{
			Desc:     "Test jwt with empty subject is invalid",
			Secret:   "secret",
			Token:    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOi00NDY3NzQ0MDAsImV4cCI6LTQ0Njc3NDQwMCwibmJmIjotNDQ2Nzc0NDAwfQ.gxsA6_Ei3s0f2I1TAtrrb8FmGiO25OqVlktlF_ylhX4",
			Duration: time.Second,
			Principal: oauth2.Principal{
				Subject:   "",
				ExpiresAt: history.Add(time.Second),
				IssuedAt:  history,
			},
			Err: errors.New("claim has no subject"),
		},
		{
			Desc:     "Test jwt duration matches auth duration",
			Secret:   "secret",
			Token:    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOi00NDY3NzQzMDAsImlhdCI6LTQ0Njc3NDQwMCwiaXNzIjoiaGlsbHZhbGxleSIsIm5iZiI6LTQ0Njc3NDQwMCwic3ViIjoibWFydHlAcGluaGVhZC5uZXQifQ.njEjstpuIDnghSR7VyPPB9QlvJ6Q5JpR3ZEZ_8vGYfA",
			Duration: time.Second,
			Principal: oauth2.Principal{
				Subject:   "marty@pinhead.net",
				ExpiresAt: history,
				IssuedAt:  history.Add(100 * time.Second),
			},
			Err: errors.New("claims duration is different from auth lifespan"),
		},
	}
	for _, test := range tests {
		j := oauth2.JWT{
			Secret: test.Secret,
			Now: func() time.Time {
				return time.Unix(-446774400, 0)
			},
		}
		principal, err := j.ValidPrincipal(context.Background(), test.Token, test.Duration)
		if test.Err != nil && err == nil {
			t.Fatalf("Expected err %s", test.Err.Error())
		}
		if err != nil {
			if test.Err == nil {
				t.Errorf("Error in test %s authenticating with bad token: %v", test.Desc, err)
			} else if err.Error() != test.Err.Error() {
				t.Errorf("Error in test %s expected error: %v actual: %v", test.Desc, test.Err, err)
			}
		} else if test.Principal != principal {
			t.Errorf("Error in test %s; principals different; expected: %v  actual: %v", test.Desc, test.Principal, principal)
		}
	}

}

func TestToken(t *testing.T) {
	expected := oauth2.Token("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOi00NDY3NzQzOTksImlhdCI6LTQ0Njc3NDQwMCwibmJmIjotNDQ2Nzc0NDAwLCJzdWIiOiIvY2hyb25vZ3JhZi92MS91c2Vycy8xIn0.ofQM6yTmrmve5JeEE0RcK4_euLXuZ_rdh6bLAbtbC9M")
	history := time.Unix(-446774400, 0)
	j := oauth2.JWT{
		Secret: "secret",
		Now: func() time.Time {
			return history
		},
	}
	p := oauth2.Principal{
		Subject:   "/chronograf/v1/users/1",
		ExpiresAt: history.Add(time.Second),
		IssuedAt:  history,
	}
	if token, err := j.Create(context.Background(), p); err != nil {
		t.Errorf("Error creating token for principal: %v", err)
	} else if token != expected {
		t.Errorf("Error creating token; expected: %s  actual: %s", expected, token)
	}
}

func TestSigningMethod(t *testing.T) {
	token := oauth2.Token("eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWV9.EkN-DOsnsuRjRO6BxXemmJDm3HbxrbRzXglbN2S4sOkopdU4IsDxTI8jO19W_A4K8ZPJijNLis4EZsHeY559a4DFOd50_OqgHGuERTqYZyuhtF39yxJPAjUESwxk2J5k_4zM3O-vtd1Ghyo4IbqKKSy6J9mTniYJPenn5-HIirE")
	j := oauth2.JWT{}
	if _, err := j.ValidPrincipal(context.Background(), token, 0); err == nil {
		t.Error("Error was expected while validating incorrectly signed token")
	} else if err.Error() != "unexpected signing method: RS256" {
		t.Errorf("Error wanted 'unexpected signing method', got %s", err.Error())
	}
}

func TestJWT_ExtendedPrincipal(t *testing.T) {
	history := time.Unix(-446774400, 0)
	type fields struct {
		Now func() time.Time
	}
	type args struct {
		ctx       context.Context
		principal oauth2.Principal
		extension time.Duration
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    oauth2.Principal
		wantErr bool
	}{
		{
			name: "Extend principal by one hour",
			fields: fields{
				Now: func() time.Time {
					return history
				},
			},
			args: args{
				ctx: context.Background(),
				principal: oauth2.Principal{
					ExpiresAt: history,
				},
				extension: time.Hour,
			},
			want: oauth2.Principal{
				ExpiresAt: history.Add(time.Hour),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := &oauth2.JWT{
				Now: tt.fields.Now,
			}
			got, err := j.ExtendedPrincipal(tt.args.ctx, tt.args.principal, tt.args.extension)
			if (err != nil) != tt.wantErr {
				t.Errorf("JWT.ExtendedPrincipal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("JWT.ExtendedPrincipal() = %v, want %v", got, tt.want)
			}
		})
	}
}
