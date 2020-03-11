package testing

import (
	"context"
	"fmt"
	"testing"

	"github.com/influxdata/influxdb"
	pk "github.com/influxdata/influxdb/kit/password"
)

// PasswordFields will include the IDGenerator, and users and their passwords.
type PasswordFields struct {
	IDGenerator influxdb.IDGenerator
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
			tt.fn(init, t)
		})
	}
}

// SetPassword tests overriding the password of a known user
func SetPassword(
	init func(PasswordFields, *testing.T) (influxdb.PasswordsService, func()),
	t *testing.T) {
	type args struct {
		user     influxdb.ID
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
			name: "setting password that matches minimum requirements works",
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
				password: "passWord1",
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
				password: "short",
			},
			wants: wants{
				err: pk.EPasswordTooShort,
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
				password: "passWord1",
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
		user     influxdb.ID
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
				Passwords: []string{"passWord1"},
			},
			args: args{
				user:     MustIDBase16(oneID),
				password: "passWord1",
			},
			wants: wants{},
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
				Passwords: []string{"passWord1"},
			},
			args: args{
				user:     MustIDBase16(oneID),
				password: "passWord2",
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
				Passwords: []string{"passWord1"},
			},
			args: args{
				user:     1,
				password: "passWord1",
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
				password: "passWord1",
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
		user influxdb.ID
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
				Passwords: []string{"passWord1"},
			},
			args: args{
				user: MustIDBase16(oneID),
				old:  "passWord1",
				new:  "passWord1",
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
				Passwords: []string{"passWord1"},
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
			name: "<invalid> a new password that doesn't match minimum requirements (length)",
			fields: PasswordFields{
				Users: []*influxdb.User{
					{
						Name: "user1",
						ID:   MustIDBase16(oneID),
					},
				},
				Passwords: []string{"passWord1"},
			},
			args: args{
				user: MustIDBase16(oneID),
				old:  "passWord1",
				new:  "pass",
			},
			wants: wants{
				err: pk.EPasswordTooShort,
			},
		},
		{
			name: "<invalid> a new password that doesn't match minimum requirements (classes)",
			fields: PasswordFields{
				Users: []*influxdb.User{
					{
						Name: "user1",
						ID:   MustIDBase16(oneID),
					},
				},
				Passwords: []string{"passWord1"},
			},
			args: args{
				user: MustIDBase16(oneID),
				old:  "passWord1",
				new:  "password1",
			},
			wants: wants{
				err: pk.ENotEnoughCharacterClasses,
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
