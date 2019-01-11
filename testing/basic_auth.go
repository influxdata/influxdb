package testing

import (
	"context"
	"fmt"
	"testing"

	platform "github.com/influxdata/influxdb"
)

// BasicAuth test all the services for basic auth
func BasicAuth(
	init func(UserFields, *testing.T) (platform.BasicAuthService, func()),
	t *testing.T) {
	type args struct {
		name            string
		user            string
		setPassword     string
		comparePassword string
	}
	type wants struct {
		setErr     error
		compareErr error
	}
	tests := []struct {
		fields UserFields
		args   args
		wants  wants
	}{
		{
			fields: UserFields{
				Users: []*platform.User{
					{
						Name: "user1",
						ID:   MustIDBase16(oneID),
					},
				},
			},
			args: args{
				name:            "happy path",
				user:            "user1",
				setPassword:     "hello",
				comparePassword: "hello",
			},
			wants: wants{},
		},
		{
			fields: UserFields{
				Users: []*platform.User{
					{
						Name: "user1",
						ID:   MustIDBase16(oneID),
					},
				},
			},
			args: args{
				name:            "happy path dont match",
				user:            "user1",
				setPassword:     "hello",
				comparePassword: "world",
			},
			wants: wants{
				compareErr: fmt.Errorf("crypto/bcrypt: hashedPassword is not the hash of the given password"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.args.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			err := s.SetPassword(ctx, tt.args.user, tt.args.setPassword)

			if (err != nil && tt.wants.setErr == nil) || (err == nil && tt.wants.setErr != nil) {
				t.Fatalf("expected SetPassword error %v got %v", tt.wants.setErr, err)
				return
			}

			if err != nil {
				if want, got := tt.wants.setErr.Error(), err.Error(); want != got {
					t.Fatalf("expected SetPassword error %v got %v", want, got)
				}
				return
			}

			err = s.ComparePassword(ctx, tt.args.user, tt.args.comparePassword)

			if (err != nil && tt.wants.compareErr == nil) || (err == nil && tt.wants.compareErr != nil) {
				t.Fatalf("expected ComparePassword error %v got %v", tt.wants.compareErr, err)
				return
			}

			if err != nil {
				if want, got := tt.wants.compareErr.Error(), err.Error(); want != got {
					t.Fatalf("expected ComparePassword error %v got %v", tt.wants.compareErr, err)
				}
				return
			}

		})
	}

}

// CompareAndSetPassword test
func CompareAndSetPassword(
	init func(UserFields, *testing.T) (platform.BasicAuthService, func()),
	t *testing.T) {
	type args struct {
		name string
		user string
		old  string
		new  string
	}
	type wants struct {
		err error
	}
	tests := []struct {
		fields UserFields
		args   args
		wants  wants
	}{
		{
			fields: UserFields{
				Users: []*platform.User{
					{
						Name: "user1",
						ID:   MustIDBase16(oneID),
					},
				},
			},
			args: args{
				name: "happy path",
				user: "user1",
				old:  "hello",
				new:  "hello",
			},
			wants: wants{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.args.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			if err := s.SetPassword(ctx, tt.args.user, tt.args.old); err != nil {
				t.Fatalf("unexpected error %v", err)
				return
			}

			err := s.CompareAndSetPassword(ctx, tt.args.user, tt.args.old, tt.args.new)

			if (err != nil && tt.wants.err == nil) || (err == nil && tt.wants.err != nil) {
				t.Fatalf("expected CompareAndSetPassword error %v got %v", tt.wants.err, err)
				return
			}

			if err != nil {
				if want, got := tt.wants.err.Error(), err.Error(); want != got {
					t.Fatalf("expected CompareAndSetPassword error %v got %v", tt.wants.err, err)
				}
				return
			}

		})
	}

}
