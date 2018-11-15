package testing

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform"
)

// A secret is a comparable data structure that is used for testing
type Secret struct {
	OrganizationID platform.ID
	Env            map[string]string
}

// SecretServiceFields contain the
type SecretServiceFields struct {
	Secrets []Secret
}

// SecretService will test all methods for the secrets service.
func SecretService(
	init func(SecretServiceFields, *testing.T) (platform.SecretService, func()),
	t *testing.T,
) {

	tests := []struct {
		name string
		fn   func(
			init func(SecretServiceFields, *testing.T) (platform.SecretService, func()),
			t *testing.T,
		)
	}{
		{
			name: "LoadSecret",
			fn:   LoadSecret,
		},
		{
			name: "PutSecret",
			fn:   PutSecret,
		},
		{
			name: "GetSecretKeys",
			fn:   GetSecretKeys,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(init, t)
		})
	}
}

// LoadSecret tests the LoadSecret method for the SecretService interface.
func LoadSecret(
	init func(f SecretServiceFields, t *testing.T) (platform.SecretService, func()),
	t *testing.T,
) {
	type args struct {
		orgID platform.ID
		key   string
	}
	type wants struct {
		value string
		err   error
	}

	tests := []struct {
		name   string
		fields SecretServiceFields
		args   args
		wants  wants
	}{
		{
			name: "load secret field",
			fields: SecretServiceFields{
				Secrets: []Secret{
					{
						OrganizationID: platform.ID(1),
						Env: map[string]string{
							"api_key": "abc123xyz",
						},
					},
				},
			},
			args: args{
				orgID: platform.ID(1),
				key:   "api_key",
			},
			wants: wants{
				value: "abc123xyz",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			val, err := s.LoadSecret(ctx, tt.args.orgID, tt.args.key)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			if want, got := tt.wants.value, val; want != got {
				t.Errorf("expected value to be %s, got %s", want, got)
			}
		})
	}
}

// PutSecret tests the PutSecret method for the SecretService interface.
func PutSecret(
	init func(f SecretServiceFields, t *testing.T) (platform.SecretService, func()),
	t *testing.T,
) {
	type args struct {
		orgID platform.ID
		key   string
		value string
	}
	type wants struct {
		err error
	}

	tests := []struct {
		name   string
		fields SecretServiceFields
		args   args
		wants  wants
	}{
		{
			name:   "put secret",
			fields: SecretServiceFields{},
			args: args{
				orgID: platform.ID(1),
				key:   "api_key",
				value: "abc123xyz",
			},
			wants: wants{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			err := s.PutSecret(ctx, tt.args.orgID, tt.args.key, tt.args.value)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			val, err := s.LoadSecret(ctx, tt.args.orgID, tt.args.key)
			if err != nil {
				t.Fatalf("unexpected error %v", err)
			}

			if want, got := tt.args.value, val; want != got {
				t.Errorf("expected value to be %s, got %s", want, got)
			}
		})
	}
}

// GetSecretKeys tests the GetSecretKeys method for the SecretService interface.
func GetSecretKeys(
	init func(f SecretServiceFields, t *testing.T) (platform.SecretService, func()),
	t *testing.T,
) {
	type args struct {
		orgID platform.ID
	}
	type wants struct {
		keys []string
		err  error
	}

	tests := []struct {
		name   string
		fields SecretServiceFields
		args   args
		wants  wants
	}{
		{
			name: "get secret keys for one org",
			fields: SecretServiceFields{
				Secrets: []Secret{
					{
						OrganizationID: platform.ID(1),
						Env: map[string]string{
							"api_key": "abc123xyz",
						},
					},
					{
						OrganizationID: platform.ID(2),
						Env: map[string]string{
							"api_key": "zyx321cba",
						},
					},
				},
			},
			args: args{
				orgID: platform.ID(1),
			},
			wants: wants{
				keys: []string{"api_key"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			keys, err := s.GetSecretKeys(ctx, tt.args.orgID)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			if diff := cmp.Diff(keys, tt.wants.keys); diff != "" {
				t.Errorf("keys are different -got/+want\ndiff %s", diff)
			}
		})
	}
}
