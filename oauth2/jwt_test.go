package oauth2_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/jwt"
)

func TestAuthenticate(t *testing.T) {
	var tests = []struct {
		Desc   string
		Secret string
		Token  string
		User   Principal
		Err    error
	}{
		{
			Desc:   "Test bad jwt token",
			Secret: "secret",
			Token:  "badtoken",
			User:   "",
			Err:    errors.New("token contains an invalid number of segments"),
		},
		{
			Desc:   "Test valid jwt token",
			Secret: "secret",
			Token:  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIvY2hyb25vZ3JhZi92MS91c2Vycy8xIiwibmFtZSI6IkRvYyBCcm93biIsImlhdCI6LTQ0Njc3NDQwMCwiZXhwIjotNDQ2Nzc0NDAwLCJuYmYiOi00NDY3NzQ0MDB9._rZ4gOIei9PizHOABH6kLcJTA3jm8ls0YnDxtz1qeUI",
			User:   "/chronograf/v1/users/1",
		},
		{
			Desc:   "Test expired jwt token",
			Secret: "secret",
			Token:  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIvY2hyb25vZ3JhZi92MS91c2Vycy8xIiwibmFtZSI6IkRvYyBCcm93biIsImlhdCI6LTQ0Njc3NDQwMCwiZXhwIjotNDQ2Nzc0NDAxLCJuYmYiOi00NDY3NzQ0MDB9.vWXdm0-XQ_pW62yBpSISFFJN_yz0vqT9_INcUKTp5Q8",
			User:   "",
			Err:    errors.New("token is expired by 1s"),
		},
		{
			Desc:   "Test jwt token not before time",
			Secret: "secret",
			Token:  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIvY2hyb25vZ3JhZi92MS91c2Vycy8xIiwibmFtZSI6IkRvYyBCcm93biIsImlhdCI6LTQ0Njc3NDQwMCwiZXhwIjotNDQ2Nzc0NDAwLCJuYmYiOi00NDY3NzQzOTl9.TMGAhv57u1aosjc4ywKC7cElP1tKyQH7GmRF2ToAxlE",
			User:   "",
			Err:    errors.New("token is not valid yet"),
		},
		{
			Desc:   "Test jwt with empty subject is invalid",
			Secret: "secret",
			Token:  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOi00NDY3NzQ0MDAsImV4cCI6LTQ0Njc3NDQwMCwibmJmIjotNDQ2Nzc0NDAwfQ.gxsA6_Ei3s0f2I1TAtrrb8FmGiO25OqVlktlF_ylhX4",
			User:   "",
			Err:    errors.New("claim has no subject"),
		},
	}
	for i, test := range tests {
		j := jwt.JWT{
			Secret: test.Secret,
			Now: func() time.Time {
				return time.Unix(-446774400, 0)
			},
		}
		user, err := j.Authenticate(context.Background(), test.Token)
		if err != nil {
			if test.Err == nil {
				t.Errorf("Error in test %d authenticating with bad token: %v", i, err)
			} else if err.Error() != test.Err.Error() {
				t.Errorf("Error in test %d expected error: %v actual: %v", i, err, test.Err)
			}
		} else if test.User != user {
			t.Errorf("Error in test %d; users different; expected: %v  actual: %v", i, test.User, user)
		}
	}

}

func TestToken(t *testing.T) {
	duration := time.Second
	expected := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOi00NDY3NzQzOTksImlhdCI6LTQ0Njc3NDQwMCwibmJmIjotNDQ2Nzc0NDAwLCJzdWIiOiIvY2hyb25vZ3JhZi92MS91c2Vycy8xIn0.ofQM6yTmrmve5JeEE0RcK4_euLXuZ_rdh6bLAbtbC9M"
	j := jwt.JWT{
		Secret: "secret",
		Now: func() time.Time {
			return time.Unix(-446774400, 0)
		},
	}
	if token, err := j.Token(context.Background(), Principal("/chronograf/v1/users/1"), duration); err != nil {
		t.Errorf("Error creating token for user: %v", err)
	} else if token != expected {
		t.Errorf("Error creating token; expected: %s  actual: %s", "", token)
	}
}
