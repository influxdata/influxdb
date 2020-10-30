package authorization

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/influxdata/influxdb/v2"
	itesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/influxdata/influxdb/v2/v1/authorization/mocks"
	"github.com/stretchr/testify/assert"
)

func TestAuthorizer_Authorize(t *testing.T) {
	var (
		username   = "foo"
		token      = "bar"
		authID     = itesting.MustIDBase16("0000000000001234")
		userID     = itesting.MustIDBase16("000000000000fefe")
		expAuthErr = influxdb.ErrCredentialsUnauthorized.Error()

		auth = &influxdb.Authorization{
			ID:     authID,
			UserID: userID,
			Token:  username,
			Status: influxdb.Active,
		}

		user = &influxdb.User{
			ID:     userID,
			Status: influxdb.Active,
		}
	)

	t.Run("invalid scheme returns error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		ctx := context.Background()

		authz := Authorizer{}

		cred := influxdb.CredentialsV1{
			Scheme: "foo",
		}

		gotAuth, gotErr := authz.Authorize(ctx, cred)
		assert.Nil(t, gotAuth)
		assert.EqualError(t, gotErr, ErrUnsupportedScheme.Error())
	})

	tests := func(t *testing.T, scheme influxdb.SchemeV1) {
		t.Run("invalid v1 and v2 token returns expected error", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			ctx := context.Background()

			v1 := mocks.NewMockAuthTokenFinder(ctrl)
			v1.EXPECT().
				FindAuthorizationByToken(ctx, username).
				Return(nil, ErrAuthNotFound)

			v2 := mocks.NewMockAuthTokenFinder(ctrl)
			v2.EXPECT().
				FindAuthorizationByToken(ctx, token).
				Return(nil, ErrAuthNotFound)

			authz := Authorizer{
				AuthV1: v1,
				AuthV2: v2,
			}

			cred := influxdb.CredentialsV1{
				Scheme:   scheme,
				Username: username,
				Token:    token,
			}

			gotAuth, gotErr := authz.Authorize(ctx, cred)
			assert.Nil(t, gotAuth)
			assert.EqualError(t, gotErr, expAuthErr)
		})

		t.Run("valid v1 token and invalid password returns expected error", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			ctx := context.Background()

			v1 := mocks.NewMockAuthTokenFinder(ctrl)
			v1.EXPECT().
				FindAuthorizationByToken(ctx, username).
				Return(auth, nil)

			pw := mocks.NewMockPasswordComparer(ctrl)
			pw.EXPECT().
				ComparePassword(ctx, authID, token).
				Return(EIncorrectPassword)

			authz := Authorizer{
				AuthV1:   v1,
				Comparer: pw,
			}

			cred := influxdb.CredentialsV1{
				Scheme:   scheme,
				Username: username,
				Token:    token,
			}

			gotAuth, gotErr := authz.Authorize(ctx, cred)
			assert.Nil(t, gotAuth)
			assert.EqualError(t, gotErr, expAuthErr)
		})

		t.Run("valid v1 token and password returns authorization", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			ctx := context.Background()

			v1 := mocks.NewMockAuthTokenFinder(ctrl)
			v1.EXPECT().
				FindAuthorizationByToken(ctx, username).
				Return(auth, nil)

			pw := mocks.NewMockPasswordComparer(ctrl)
			pw.EXPECT().
				ComparePassword(ctx, authID, token).
				Return(nil)

			uf := mocks.NewMockUserFinder(ctrl)
			uf.EXPECT().
				FindUserByID(ctx, userID).
				Return(user, nil)

			authz := Authorizer{
				AuthV1:   v1,
				Comparer: pw,
				User:     uf,
			}

			cred := influxdb.CredentialsV1{
				Scheme:   scheme,
				Username: username,
				Token:    token,
			}

			gotAuth, gotErr := authz.Authorize(ctx, cred)
			assert.NoError(t, gotErr)
			assert.Equal(t, auth, gotAuth)
		})

		t.Run("invalid v1 token and valid v2 token returns authorization", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			ctx := context.Background()

			v1 := mocks.NewMockAuthTokenFinder(ctrl)
			v1.EXPECT().
				FindAuthorizationByToken(ctx, username).
				Return(nil, ErrAuthNotFound)

			v2 := mocks.NewMockAuthTokenFinder(ctrl)
			v2.EXPECT().
				FindAuthorizationByToken(ctx, token).
				Return(auth, nil)

			uf := mocks.NewMockUserFinder(ctrl)
			uf.EXPECT().
				FindUserByID(ctx, userID).
				Return(user, nil)

			authz := Authorizer{
				AuthV1: v1,
				AuthV2: v2,
				User:   uf,
			}

			cred := influxdb.CredentialsV1{
				Scheme:   scheme,
				Username: username,
				Token:    token,
			}

			gotAuth, gotErr := authz.Authorize(ctx, cred)
			assert.NoError(t, gotErr)
			assert.Equal(t, auth, gotAuth)
		})
	}

	t.Run("using Basic scheme", func(t *testing.T) {
		tests(t, influxdb.SchemeV1Basic)
	})

	t.Run("using URL scheme", func(t *testing.T) {
		tests(t, influxdb.SchemeV1URL)
	})

	t.Run("using Token scheme", func(t *testing.T) {
		t.Run("invalid v2 token returns expected error", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			ctx := context.Background()

			v2 := mocks.NewMockAuthTokenFinder(ctrl)
			v2.EXPECT().
				FindAuthorizationByToken(ctx, token).
				Return(nil, ErrAuthNotFound)

			authz := Authorizer{
				AuthV2: v2,
			}

			cred := influxdb.CredentialsV1{
				Scheme:   influxdb.SchemeV1Token,
				Username: username,
				Token:    token,
			}

			gotAuth, gotErr := authz.Authorize(ctx, cred)
			assert.Nil(t, gotAuth)
			assert.EqualError(t, gotErr, expAuthErr)
		})

		t.Run("valid v2 token returns authorization", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			ctx := context.Background()

			v2 := mocks.NewMockAuthTokenFinder(ctrl)
			v2.EXPECT().
				FindAuthorizationByToken(ctx, token).
				Return(auth, nil)

			uf := mocks.NewMockUserFinder(ctrl)
			uf.EXPECT().
				FindUserByID(ctx, userID).
				Return(user, nil)

			authz := Authorizer{
				AuthV2: v2,
				User:   uf,
			}

			cred := influxdb.CredentialsV1{
				Scheme:   influxdb.SchemeV1Token,
				Username: username,
				Token:    token,
			}

			gotAuth, gotErr := authz.Authorize(ctx, cred)
			assert.NoError(t, gotErr)
			assert.Equal(t, auth, gotAuth)
		})
	})

	// test inactive user and inactive token

	t.Run("inactive user returns error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		ctx := context.Background()

		v1 := mocks.NewMockAuthTokenFinder(ctrl)
		v1.EXPECT().
			FindAuthorizationByToken(ctx, username).
			Return(auth, nil)

		pw := mocks.NewMockPasswordComparer(ctrl)
		pw.EXPECT().
			ComparePassword(ctx, authID, token).
			Return(nil)

		user := *user
		user.Status = influxdb.Inactive

		uf := mocks.NewMockUserFinder(ctrl)
		uf.EXPECT().
			FindUserByID(ctx, userID).
			Return(&user, nil)

		authz := Authorizer{
			AuthV1:   v1,
			Comparer: pw,
			User:     uf,
		}

		cred := influxdb.CredentialsV1{
			Scheme:   influxdb.SchemeV1Basic,
			Username: username,
			Token:    token,
		}

		gotAuth, gotErr := authz.Authorize(ctx, cred)
		assert.Nil(t, gotAuth)
		assert.EqualError(t, gotErr, expAuthErr)
	})

	t.Run("inactive token returns error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		ctx := context.Background()

		auth := *auth
		auth.Status = influxdb.Inactive

		v1 := mocks.NewMockAuthTokenFinder(ctrl)
		v1.EXPECT().
			FindAuthorizationByToken(ctx, username).
			Return(&auth, nil)

		pw := mocks.NewMockPasswordComparer(ctrl)
		pw.EXPECT().
			ComparePassword(ctx, authID, token).
			Return(nil)

		authz := Authorizer{
			AuthV1:   v1,
			Comparer: pw,
		}

		cred := influxdb.CredentialsV1{
			Scheme:   influxdb.SchemeV1Basic,
			Username: username,
			Token:    token,
		}

		gotAuth, gotErr := authz.Authorize(ctx, cred)
		assert.Nil(t, gotAuth)
		assert.EqualError(t, gotErr, expAuthErr)
	})
}
