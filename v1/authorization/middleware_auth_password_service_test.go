package authorization_test

import (
	"context"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/influxdata/influxdb/v2"
	icontext "github.com/influxdata/influxdb/v2/context"
	itest "github.com/influxdata/influxdb/v2/testing"
	"github.com/influxdata/influxdb/v2/v1/authorization"
	"github.com/influxdata/influxdb/v2/v1/authorization/mocks"
	"github.com/stretchr/testify/assert"
)

func TestAuthedPasswordService_SetPassword(t *testing.T) {
	var (
		authID = itest.MustIDBase16("0000000000001000")
		userID = itest.MustIDBase16("0000000000002000")
		orgID  = itest.MustIDBase16("0000000000003000")
	)
	t.Run("error when auth not found", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := context.Background()

		af := mocks.NewMockAuthFinder(ctrl)
		af.EXPECT().
			FindAuthorizationByID(ctx, authID).
			Return(nil, &errors.Error{})

		ps := authorization.NewAuthedPasswordService(af, nil)
		err := ps.SetPassword(ctx, authID, "foo")
		assert.EqualError(t, err, authorization.ErrAuthNotFound.Error())
	})

	t.Run("error when no authorizer", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := context.Background()
		auth := influxdb.Authorization{
			ID:     authID,
			OrgID:  orgID,
			UserID: userID,
		}

		af := mocks.NewMockAuthFinder(ctrl)
		af.EXPECT().
			FindAuthorizationByID(ctx, authID).
			Return(&auth, nil)

		ps := authorization.NewAuthedPasswordService(af, nil)
		err := ps.SetPassword(ctx, authID, "foo")
		assert.EqualError(t, err, "authorizer not found on context")
	})

	t.Run("error with restricted permission", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		influxdb.OperPermissions()
		auth := influxdb.Authorization{
			ID:     authID,
			OrgID:  orgID,
			UserID: userID,
			Status: influxdb.Active,
		}
		ctx := context.Background()
		ctx = icontext.SetAuthorizer(ctx, &auth)

		af := mocks.NewMockAuthFinder(ctrl)
		af.EXPECT().
			FindAuthorizationByID(ctx, authID).
			Return(&auth, nil)

		ps := authorization.NewAuthedPasswordService(af, nil)
		err := ps.SetPassword(ctx, authID, "foo")
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		influxdb.OperPermissions()
		auth := influxdb.Authorization{
			ID:          authID,
			OrgID:       orgID,
			UserID:      userID,
			Status:      influxdb.Active,
			Permissions: influxdb.MePermissions(userID),
		}
		ctx := context.Background()
		ctx = icontext.SetAuthorizer(ctx, &auth)

		af := mocks.NewMockAuthFinder(ctrl)
		af.EXPECT().
			FindAuthorizationByID(ctx, authID).
			Return(&auth, nil)

		inner := mocks.NewMockPasswordService(ctrl)
		inner.EXPECT().
			SetPassword(ctx, authID, "foo").
			Return(nil)

		ps := authorization.NewAuthedPasswordService(af, inner)
		err := ps.SetPassword(ctx, authID, "foo")
		assert.NoError(t, err)
	})
}
