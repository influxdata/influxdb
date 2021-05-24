package authorizer_test

import (
	"bytes"
	"context"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
	influxdbcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/mock"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
)

var secretCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
}

func TestSecretService_LoadSecret(t *testing.T) {
	type fields struct {
		SecretService influxdb.SecretService
	}
	type args struct {
		permission influxdb.Permission
		org        platform.ID
		key        string
	}
	type wants struct {
		err error
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to access secret within org",
			fields: fields{
				SecretService: &mock.SecretService{
					LoadSecretFn: func(ctx context.Context, orgID platform.ID, k string) (string, error) {
						if k == "key" {
							return "val", nil
						}
						return "", &errors.Error{
							Code: errors.ENotFound,
							Msg:  influxdb.ErrSecretNotFound,
						}
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						Type:  influxdb.SecretsResourceType,
						OrgID: influxdbtesting.IDPtr(10),
					},
				},
				key: "key",
				org: platform.ID(10),
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "cannot access not existing secret",
			fields: fields{
				SecretService: &mock.SecretService{
					LoadSecretFn: func(ctx context.Context, orgID platform.ID, k string) (string, error) {
						if k == "key" {
							return "val", nil
						}
						return "", &errors.Error{
							Code: errors.ENotFound,
							Msg:  influxdb.ErrSecretNotFound,
						}
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						Type:  influxdb.SecretsResourceType,
						OrgID: influxdbtesting.IDPtr(10),
					},
				},
				key: "not existing",
				org: platform.ID(10),
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.ENotFound,
					Msg:  influxdb.ErrSecretNotFound,
				},
			},
		},
		{
			name: "unauthorized to access secret within org",
			fields: fields{
				SecretService: &mock.SecretService{
					LoadSecretFn: func(ctx context.Context, orgID platform.ID, k string) (string, error) {
						if k == "key" {
							return "val", nil
						}
						return "", &errors.Error{
							Code: errors.ENotFound,
							Msg:  influxdb.ErrSecretNotFound,
						}
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						Type: influxdb.SecretsResourceType,
						ID:   influxdbtesting.IDPtr(10),
					},
				},
				org: platform.ID(2),
				key: "key",
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "read:orgs/0000000000000002/secrets is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewSecretService(tt.fields.SecretService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			_, err := s.LoadSecret(ctx, tt.args.org, tt.args.key)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestSecretService_GetSecretKeys(t *testing.T) {
	type fields struct {
		SecretService influxdb.SecretService
	}
	type args struct {
		permission influxdb.Permission
		org        platform.ID
	}
	type wants struct {
		err     error
		secrets []string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to see all secrets within an org",
			fields: fields{
				SecretService: &mock.SecretService{
					GetSecretKeysFn: func(ctx context.Context, orgID platform.ID) ([]string, error) {
						return []string{
							"0000000000000001secret1",
							"0000000000000001secret2",
							"0000000000000001secret3",
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						Type:  influxdb.SecretsResourceType,
						OrgID: influxdbtesting.IDPtr(1),
					},
				},
				org: platform.ID(1),
			},
			wants: wants{
				secrets: []string{
					"0000000000000001secret1",
					"0000000000000001secret2",
					"0000000000000001secret3",
				},
			},
		},
		{
			name: "unauthorized to see all secrets within an org",
			fields: fields{
				SecretService: &mock.SecretService{
					GetSecretKeysFn: func(ctx context.Context, orgID platform.ID) ([]string, error) {
						return []string{
							"0000000000000002secret1",
							"0000000000000002secret2",
							"0000000000000002secret3",
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						Type:  influxdb.SecretsResourceType,
						OrgID: influxdbtesting.IDPtr(1),
					},
				},
				org: platform.ID(2),
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.EUnauthorized,
					Msg:  "read:orgs/0000000000000002/secrets is unauthorized",
				},
				secrets: []string{},
			},
		},
		{
			name: "errors when there are not secret into an org",
			fields: fields{
				SecretService: &mock.SecretService{
					GetSecretKeysFn: func(ctx context.Context, orgID platform.ID) ([]string, error) {
						return []string(nil), &errors.Error{
							Code: errors.ENotFound,
							Msg:  "organization has no secret keys",
						}
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: influxdb.ReadAction,
					Resource: influxdb.Resource{
						Type:  influxdb.SecretsResourceType,
						OrgID: influxdbtesting.IDPtr(10),
					},
				},
				org: platform.ID(10),
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.ENotFound,
					Msg:  "organization has no secret keys",
				},
				secrets: []string{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewSecretService(tt.fields.SecretService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			secrets, err := s.GetSecretKeys(ctx, tt.args.org)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)

			if diff := cmp.Diff(secrets, tt.wants.secrets, secretCmpOptions...); diff != "" {
				t.Errorf("secrets are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func TestSecretService_PatchSecrets(t *testing.T) {
	type fields struct {
		SecretService influxdb.SecretService
	}
	type args struct {
		org         platform.ID
		permissions []influxdb.Permission
	}
	type wants struct {
		err error
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to patch secrets",
			fields: fields{
				SecretService: &mock.SecretService{
					PatchSecretsFn: func(ctx context.Context, orgID platform.ID, m map[string]string) error {
						return nil
					},
				},
			},
			args: args{
				org: platform.ID(1),
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type:  influxdb.SecretsResourceType,
							OrgID: influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to update secret",
			fields: fields{
				SecretService: &mock.SecretService{
					PatchSecretsFn: func(ctx context.Context, orgID platform.ID, m map[string]string) error {
						return nil
					},
				},
			},
			args: args{
				org: platform.ID(1),
				permissions: []influxdb.Permission{
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type:  influxdb.SecretsResourceType,
							OrgID: influxdbtesting.IDPtr(10),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/0000000000000001/secrets is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewSecretService(tt.fields.SecretService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			patches := make(map[string]string)
			err := s.PatchSecrets(ctx, tt.args.org, patches)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestSecretService_DeleteSecret(t *testing.T) {
	type fields struct {
		SecretService influxdb.SecretService
	}
	type args struct {
		org         platform.ID
		permissions []influxdb.Permission
	}
	type wants struct {
		err error
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to delete secret",
			fields: fields{
				SecretService: &mock.SecretService{
					DeleteSecretFn: func(ctx context.Context, orgID platform.ID, keys ...string) error {
						return nil
					},
				},
			},
			args: args{
				org: platform.ID(1),
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type:  influxdb.SecretsResourceType,
							OrgID: influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to delete secret",
			fields: fields{
				SecretService: &mock.SecretService{
					DeleteSecretFn: func(ctx context.Context, orgID platform.ID, keys ...string) error {
						return nil
					},
				},
			},
			args: args{
				org: 10,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type:  influxdb.SecretsResourceType,
							OrgID: influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/secrets is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewSecretService(tt.fields.SecretService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			err := s.DeleteSecret(ctx, tt.args.org)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestSecretService_PutSecret(t *testing.T) {
	type fields struct {
		SecretService influxdb.SecretService
	}
	type args struct {
		permission influxdb.Permission
		orgID      platform.ID
	}
	type wants struct {
		err error
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to put a secret",
			fields: fields{
				SecretService: &mock.SecretService{
					PutSecretFn: func(ctx context.Context, orgID platform.ID, key string, val string) error {
						return nil
					},
				},
			},
			args: args{
				orgID: platform.ID(10),
				permission: influxdb.Permission{
					Action: influxdb.WriteAction,
					Resource: influxdb.Resource{
						Type:  influxdb.SecretsResourceType,
						OrgID: influxdbtesting.IDPtr(10),
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to put a secret",
			fields: fields{
				SecretService: &mock.SecretService{
					PutSecretFn: func(ctx context.Context, orgID platform.ID, key string, val string) error {
						return nil
					},
				},
			},
			args: args{
				orgID: 10,
				permission: influxdb.Permission{
					Action: influxdb.WriteAction,
					Resource: influxdb.Resource{
						Type: influxdb.SecretsResourceType,
						ID:   influxdbtesting.IDPtr(1),
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/secrets is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewSecretService(tt.fields.SecretService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			err := s.PutSecret(ctx, tt.args.orgID, "", "")
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestSecretService_PutSecrets(t *testing.T) {
	type fields struct {
		SecretService influxdb.SecretService
	}
	type args struct {
		permissions []influxdb.Permission
		orgID       platform.ID
	}
	type wants struct {
		err error
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to put secrets",
			fields: fields{
				SecretService: &mock.SecretService{
					PutSecretsFn: func(ctx context.Context, orgID platform.ID, m map[string]string) error {
						return nil
					},
				},
			},
			args: args{
				orgID: platform.ID(10),
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type:  influxdb.SecretsResourceType,
							OrgID: influxdbtesting.IDPtr(10),
						},
					},
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type:  influxdb.SecretsResourceType,
							OrgID: influxdbtesting.IDPtr(10),
						},
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to put secrets",
			fields: fields{
				SecretService: &mock.SecretService{
					PutSecretsFn: func(ctx context.Context, orgID platform.ID, m map[string]string) error {
						return nil
					},
				},
			},
			args: args{
				orgID: platform.ID(2),
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type:  influxdb.SecretsResourceType,
							OrgID: influxdbtesting.IDPtr(1),
						},
					},
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type:  influxdb.SecretsResourceType,
							OrgID: influxdbtesting.IDPtr(2),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/0000000000000002/secrets is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
		{
			name: "unauthorized to put secrets without read access to their org",
			fields: fields{
				SecretService: &mock.SecretService{
					PutSecretFn: func(ctx context.Context, orgID platform.ID, key string, val string) error {
						return nil
					},
					PutSecretsFn: func(ctx context.Context, orgID platform.ID, m map[string]string) error {
						return nil
					},
				},
			},
			args: args{
				orgID: 10,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type:  influxdb.SecretsResourceType,
							OrgID: influxdbtesting.IDPtr(10),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "read:orgs/000000000000000a/secrets is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
		{
			name: "unauthorized to put secrets without write access to their org",
			fields: fields{
				SecretService: &mock.SecretService{
					PutSecretFn: func(ctx context.Context, orgID platform.ID, key string, val string) error {
						return nil
					},
					PutSecretsFn: func(ctx context.Context, orgID platform.ID, m map[string]string) error {
						return nil
					},
				},
			},
			args: args{
				orgID: 10,
				permissions: []influxdb.Permission{
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type:  influxdb.SecretsResourceType,
							OrgID: influxdbtesting.IDPtr(10),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/000000000000000a/secrets is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewSecretService(tt.fields.SecretService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			secrets := make(map[string]string)
			err := s.PutSecrets(ctx, tt.args.orgID, secrets)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}
