package testing

import (
	"context"
	eBase "errors"
	"fmt"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

// PasswordFields will include the IDGenerator, and users and their passwords.
type PasswordFields struct {
	IDGenerator platform.IDGenerator
	Users       []*influxdb.User
	Passwords   []string // passwords are indexed against the Users field
}

// PasswordsService tests all the service functions.
func PasswordsService(
	init func(PasswordFields, *testing.T) (influxdb.PasswordsService, func()), t *testing.T,
) {
	tests := []struct {
		name string
		fn   func(init func(PasswordFields, *testing.T) (influxdb.PasswordsService, func()),
			t *testing.T)
	}{
		{
			name: "SetPassword",
			fn:   SetPassword,
		},
		{
			name: "ComparePassword",
			fn:   ComparePassword,
		},
		{
			name: "CompareAndSetPassword",
			fn:   CompareAndSetPassword,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt := tt
			t.Parallel()
			tt.fn(init, t)
		})
	}
}

// SetPassword tests overriding the password of a known user
func SetPassword(
	init func(PasswordFields, *testing.T) (influxdb.PasswordsService, func()),
	t *testing.T) {
	type args struct {
		user     platform.ID
		password string
	}
	type wants struct {
		err error
	}
	tests := []struct {
		name   string
		fields PasswordFields
		args   args
		wants  wants
	}{
		{
			name: "setting password longer than 8 characters works",
			fields: PasswordFields{
				Users: []*influxdb.User{
					{
						Name: "user1",
						ID:   MustIDBase16(oneID),
					},
				},
			},
			args: args{
				user:     MustIDBase16(oneID),
				password: "howdY&&doody",
			},
			wants: wants{},
		},
		{
			name: "passwords that are too short have errors",
			fields: PasswordFields{
				Users: []*influxdb.User{
					{
						Name: "user1",
						ID:   MustIDBase16(oneID),
					},
				},
			},
			args: args{
				user:     MustIDBase16(oneID),
				password: "A2$u",
			},
			wants: wants{
				err: errors.EPasswordLength,
			},
		},
		{
			name: "setting a password for a non-existent user is a generic-like error",
			fields: PasswordFields{
				Users: []*influxdb.User{
					{
						Name: "user1",
						ID:   MustIDBase16(oneID),
					},
				},
			},
			args: args{
				user:     33,
				password: "Howdy#Doody",
			},
			wants: wants{
				err: fmt.Errorf("your userID is incorrect"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			err := s.SetPassword(ctx, tt.args.user, tt.args.password)

			if (err != nil && tt.wants.err == nil) || (err == nil && tt.wants.err != nil) {
				t.Fatalf("expected SetPassword error %v got %v", tt.wants.err, err)
				return
			}

			if err != nil {
				if want, got := tt.wants.err.Error(), err.Error(); want != got {
					t.Fatalf("expected SetPassword error %v got %v", want, got)
				}
			}
		})
	}
}

// ComparePassword tests setting and comparing passwords.
func ComparePassword(
	init func(PasswordFields, *testing.T) (influxdb.PasswordsService, func()),
	t *testing.T) {
	type args struct {
		user     platform.ID
		password string
	}
	type wants struct {
		err error
	}
	tests := []struct {
		name   string
		fields PasswordFields
		args   args
		wants  wants
	}{
		{
			name: "comparing same password is not an error",
			fields: PasswordFields{
				Users: []*influxdb.User{
					{
						Name: "user1",
						ID:   MustIDBase16(oneID),
					},
				},
				Passwords: []string{"Howdy%doody"},
			},
			args: args{
				user:     MustIDBase16(oneID),
				password: "Howdy%doody",
			},
			wants: wants{},
		},
		{
			name: "comparing same weak password forces change",
			fields: PasswordFields{
				Users: []*influxdb.User{
					{
						Name: "user1",
						ID:   MustIDBase16(oneID),
					},
				},
				Passwords: []string{"Howdydoody"},
			},
			args: args{
				user:     MustIDBase16(oneID),
				password: "Howdydoody",
			},
			wants: wants{eBase.Join(errors.EPasswordChangeRequired, errors.EPasswordChars)},
		},
		{
			name: "comparing different password is an error",
			fields: PasswordFields{
				Users: []*influxdb.User{
					{
						Name: "user1",
						ID:   MustIDBase16(oneID),
					},
				},
				Passwords: []string{"howdydoody"},
			},
			args: args{
				user:     MustIDBase16(oneID),
				password: "wrongpassword",
			},
			wants: wants{
				err: fmt.Errorf("your username or password is incorrect"),
			},
		},
		{
			name: "comparing a password to a non-existent user is a generic-like error",
			fields: PasswordFields{
				Users: []*influxdb.User{
					{
						Name: "user1",
						ID:   MustIDBase16(oneID),
					},
				},
				Passwords: []string{"howdydoody"},
			},
			args: args{
				user:     1,
				password: "howdydoody",
			},
			wants: wants{
				err: fmt.Errorf("your userID is incorrect"),
			},
		},
		{
			name: "user exists but no password has been set",
			fields: PasswordFields{
				Users: []*influxdb.User{
					{
						Name: "user1",
						ID:   MustIDBase16(oneID),
					},
				},
			},
			args: args{
				user:     MustIDBase16(oneID),
				password: "howdydoody",
			},
			wants: wants{
				err: fmt.Errorf("your username or password is incorrect"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			err := s.ComparePassword(ctx, tt.args.user, tt.args.password)

			if (err != nil && tt.wants.err == nil) || (err == nil && tt.wants.err != nil) {
				t.Fatalf("expected ComparePassword error %v got %v", tt.wants.err, err)
				return
			}

			if err != nil {
				if want, got := tt.wants.err.Error(), err.Error(); want != got {
					t.Fatalf("expected ComparePassword error %v got %v", tt.wants.err, err)
				}
				return
			}

		})
	}
}

// CompareAndSetPassword tests implementations of PasswordsService.
func CompareAndSetPassword(
	init func(PasswordFields, *testing.T) (influxdb.PasswordsService, func()),
	t *testing.T) {
	type args struct {
		user platform.ID
		old  string
		new  string
	}
	type wants struct {
		err error
	}
	tests := []struct {
		name   string
		fields PasswordFields
		args   args
		wants  wants
	}{
		{
			name: "setting a password to the existing password is valid",
			fields: PasswordFields{
				Users: []*influxdb.User{
					{
						Name: "user1",
						ID:   MustIDBase16(oneID),
					},
				},
				Passwords: []string{"howdY&doody"},
			},
			args: args{
				user: MustIDBase16(oneID),
				old:  "howdY&doody",
				new:  "howdY&doody",
			},
			wants: wants{},
		},
		{
			name: "providing an incorrect old password is an error",
			fields: PasswordFields{
				Users: []*influxdb.User{
					{
						Name: "user1",
						ID:   MustIDBase16(oneID),
					},
				},
				Passwords: []string{"howdydoody"},
			},
			args: args{
				user: MustIDBase16(oneID),
				old:  "invalid",
				new:  "not used",
			},
			wants: wants{
				err: fmt.Errorf("your username or password is incorrect"),
			},
		},
		{
			name: "<invalid> a new password that is less than 8 characters is an error",
			fields: PasswordFields{
				Users: []*influxdb.User{
					{
						Name: "user1",
						ID:   MustIDBase16(oneID),
					},
				},
				Passwords: []string{"howdydoody"},
			},
			args: args{
				user: MustIDBase16(oneID),
				old:  "howdydoody",
				new:  "short",
			},
			wants: wants{
				err: eBase.Join(errors.EPasswordLength, errors.EPasswordChars),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

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
