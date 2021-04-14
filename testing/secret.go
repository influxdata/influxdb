package testing

import (
	"bytes"
	"context"
	"sort"
	"testing"

	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/google/go-cmp/cmp"
	platform "github.com/influxdata/influxdb/v2"
)

var secretCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []string) []string {
		out := append([]string(nil), in...) // Copy input to avoid mutating it
		sort.Strings(out)
		return out
	}),
}

// A secret is a comparable data structure that is used for testing
type Secret struct {
	OrganizationID platform2.ID
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
			name: "PutSecrets",
			fn:   PutSecrets,
		},
		{
			name: "PatchSecrets",
			fn:   PatchSecrets,
		},
		{
			name: "GetSecretKeys",
			fn:   GetSecretKeys,
		},
		{
			name: "DeleteSecrets",
			fn:   DeleteSecrets,
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

// LoadSecret tests the LoadSecret method for the SecretService interface.
func LoadSecret(
	init func(f SecretServiceFields, t *testing.T) (platform.SecretService, func()),
	t *testing.T,
) {
	type args struct {
		orgID platform2.ID
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
						OrganizationID: platform2.ID(1),
						Env: map[string]string{
							"api_key": "abc123xyz",
						},
					},
				},
			},
			args: args{
				orgID: platform2.ID(1),
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
		orgID platform2.ID
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
				orgID: platform2.ID(1),
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

// PutSecrets tests the PutSecrets method for the SecretService interface.
func PutSecrets(
	init func(f SecretServiceFields, t *testing.T) (platform.SecretService, func()),
	t *testing.T,
) {
	type args struct {
		orgID   platform2.ID
		secrets map[string]string
	}
	type wants struct {
		err  error
		keys []string
	}

	tests := []struct {
		name   string
		fields SecretServiceFields
		args   args
		wants  wants
	}{
		{
			name: "put secrets",
			fields: SecretServiceFields{
				Secrets: []Secret{
					{
						OrganizationID: platform2.ID(1),
						Env: map[string]string{
							"api_key": "abc123xyz",
						},
					},
				},
			},
			args: args{
				orgID: platform2.ID(1),
				secrets: map[string]string{
					"api_key2": "abc123xyz",
					"batman":   "potato",
				},
			},
			wants: wants{
				keys: []string{"api_key2", "batman"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			err := s.PutSecrets(ctx, tt.args.orgID, tt.args.secrets)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			for k, v := range tt.args.secrets {
				val, err := s.LoadSecret(ctx, tt.args.orgID, k)
				if err != nil {
					t.Fatalf("unexpected error %v", err)
				}

				if want, got := v, val; want != got {
					t.Errorf("expected value to be %s, got %s", want, got)
				}
			}

			keys, err := s.GetSecretKeys(ctx, tt.args.orgID)
			if err != nil {
				t.Fatalf("unexpected error %v", err)
			}

			if diff := cmp.Diff(keys, tt.wants.keys, secretCmpOptions...); diff != "" {
				t.Errorf("keys are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// PatchSecrets tests the PatchSecrets method for the SecretService interface.
func PatchSecrets(
	init func(f SecretServiceFields, t *testing.T) (platform.SecretService, func()),
	t *testing.T,
) {
	type args struct {
		orgID   platform2.ID
		secrets map[string]string
	}
	type wants struct {
		err  error
		keys []string
	}

	tests := []struct {
		name   string
		fields SecretServiceFields
		args   args
		wants  wants
	}{
		{
			name: "patch secrets",
			fields: SecretServiceFields{
				Secrets: []Secret{
					{
						OrganizationID: platform2.ID(1),
						Env: map[string]string{
							"api_key": "abc123xyz",
						},
					},
				},
			},
			args: args{
				orgID: platform2.ID(1),
				secrets: map[string]string{
					"api_key2": "abc123xyz",
					"batman":   "potato",
				},
			},
			wants: wants{
				keys: []string{"api_key", "api_key2", "batman"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()

			ctx := context.Background()

			err := s.PatchSecrets(ctx, tt.args.orgID, tt.args.secrets)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			for k, v := range tt.args.secrets {
				val, err := s.LoadSecret(ctx, tt.args.orgID, k)
				if err != nil {
					if errors.ErrorCode(err) == errors.EMethodNotAllowed {
						// skip value checking for http service testing
						break
					}
					t.Fatalf("unexpected error %v", err)
				}

				if want, got := v, val; want != got {
					t.Errorf("expected value to be %s, got %s", want, got)
				}
			}

			keys, err := s.GetSecretKeys(ctx, tt.args.orgID)
			if err != nil {
				t.Fatalf("unexpected error %v", err)
			}

			if diff := cmp.Diff(keys, tt.wants.keys, secretCmpOptions...); diff != "" {
				t.Errorf("keys are different -got/+want\ndiff %s", diff)
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
		orgID platform2.ID
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
						OrganizationID: platform2.ID(1),
						Env: map[string]string{
							"api_key": "abc123xyz",
						},
					},
					{
						OrganizationID: platform2.ID(2),
						Env: map[string]string{
							"api_key": "zyx321cba",
						},
					},
				},
			},
			args: args{
				orgID: platform2.ID(1),
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

			if diff := cmp.Diff(keys, tt.wants.keys, secretCmpOptions...); diff != "" {
				t.Errorf("keys are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// DeleteSecrets tests the DeleteSecrets method for the SecretService interface.
func DeleteSecrets(
	init func(f SecretServiceFields, t *testing.T) (platform.SecretService, func()),
	t *testing.T,
) {
	type args struct {
		orgID platform2.ID
		keys  []string
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
			name: "delete secret keys",
			fields: SecretServiceFields{
				Secrets: []Secret{
					{
						OrganizationID: platform2.ID(1),
						Env: map[string]string{
							"api_key":  "abc123xyz",
							"api_key2": "potato",
							"batman":   "foo",
						},
					},
				},
			},
			args: args{
				orgID: platform2.ID(1),
				keys:  []string{"api_key2", "batman"},
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

			err := s.DeleteSecret(ctx, tt.args.orgID, tt.args.keys...)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			keys, err := s.GetSecretKeys(ctx, tt.args.orgID)
			if err != nil {
				t.Fatalf("unexpected error %v", err)
			}

			if diff := cmp.Diff(keys, tt.wants.keys, secretCmpOptions...); diff != "" {
				t.Errorf("keys are different -got/+want\ndiff %s", diff)
			}
		})
	}
}
