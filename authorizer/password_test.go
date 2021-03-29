package authorizer_test

import (
	"context"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
	icontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/stretchr/testify/require"
)

func TestPasswordService(t *testing.T) {
	t.Run("SetPassword", func(t *testing.T) {
		t.Run("user with permissions should proceed", func(t *testing.T) {
			userID := platform.ID(1)

			permission := influxdb.Permission{
				Action: influxdb.WriteAction,
				Resource: influxdb.Resource{
					Type: influxdb.UsersResourceType,
					ID:   &userID,
				},
			}

			fakeSVC := mock.NewPasswordsService()
			fakeSVC.SetPasswordFn = func(_ context.Context, _ platform.ID, _ string) error {
				return nil
			}
			s := authorizer.NewPasswordService(fakeSVC)

			ctx := icontext.SetAuthorizer(context.Background(), mock.NewMockAuthorizer(false, []influxdb.Permission{permission}))

			err := s.SetPassword(ctx, 1, "password")
			require.NoError(t, err)
		})

		t.Run("user without permissions should proceed", func(t *testing.T) {
			goodUserID := platform.ID(1)
			badUserID := platform.ID(3)

			tests := []struct {
				name          string
				badPermission influxdb.Permission
			}{
				{
					name: "has no access",
				},
				{
					name: "has read only access on correct resource",
					badPermission: influxdb.Permission{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type: influxdb.UsersResourceType,
							ID:   &goodUserID,
						},
					},
				},
				{
					name: "has write access on incorrect resource",
					badPermission: influxdb.Permission{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.OrgsResourceType,
							ID:   &goodUserID,
						},
					},
				},
				{
					name: "user accessing user that is not self",
					badPermission: influxdb.Permission{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.UsersResourceType,
							ID:   &badUserID,
						},
					},
				},
			}

			for _, tt := range tests {
				fn := func(t *testing.T) {
					fakeSVC := &mock.PasswordsService{
						SetPasswordFn: func(_ context.Context, _ platform.ID, _ string) error {
							return nil
						},
					}
					s := authorizer.NewPasswordService(fakeSVC)

					ctx := icontext.SetAuthorizer(context.Background(), mock.NewMockAuthorizer(false, []influxdb.Permission{tt.badPermission}))

					err := s.SetPassword(ctx, goodUserID, "password")
					require.Error(t, err)
				}

				t.Run(tt.name, fn)
			}
		})
	})
}
