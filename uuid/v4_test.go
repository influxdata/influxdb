package uuid_test

import (
	"context"
	"testing"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/uuid"
)

func TestAuthenticate(t *testing.T) {
	var tests = []struct {
		Desc   string
		APIKey string
		Key    string
		Err    error
		User   chronograf.Principal
	}{

		{
			Desc:   "Test auth err when keys are different",
			APIKey: "key",
			Key:    "badkey",
			Err:    chronograf.ErrAuthentication,
			User:   "",
		},
		{
			Desc:   "Test that admin user comes back",
			APIKey: "key",
			Key:    "key",
			Err:    nil,
			User:   "admin",
		},
	}

	for _, test := range tests {
		k := uuid.APIKey{
			Key: test.APIKey,
		}
		u, err := k.Authenticate(context.Background(), test.Key)
		if err != test.Err {
			t.Errorf("Auth error different; expected %v  actual %v", test.Err, err)
		}
		if u != test.User {
			t.Errorf("Auth user different; expected %v  actual %v", test.User, u)
		}
	}
}
