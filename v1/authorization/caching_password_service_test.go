package authorization

import (
	"context"
	"strings"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/golang/mock/gomock"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/stretchr/testify/assert"
)

func TestCachingPasswordsService(t *testing.T) {
	const (
		user1 = platform.ID(1)
		user2 = platform.ID(2)
	)

	makeUser := func(salt, pass string) authUser {
		if len(salt) != SaltBytes {
			panic("invalid salt")
		}

		var ps CachingPasswordsService
		return authUser{salt: []byte(salt), hash: ps.hashWithSalt([]byte(salt), pass)}
	}

	var (
		userE1 = makeUser(strings.Repeat("salt---1", 4), "foo")
		userE2 = makeUser(strings.Repeat("salt---2", 4), "bar")
	)

	t.Run("SetPassword deletes cached user", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		inner := mock.NewMockPasswordsService(ctrl)
		inner.EXPECT().
			SetPassword(gomock.Any(), user1, "foo").
			Return(nil)

		s := NewCachingPasswordsService(inner)
		s.authCache[user1] = userE1
		s.authCache[user2] = userE2

		ctx := context.Background()

		_, ok := s.authCache[user1]
		assert.True(t, ok)
		assert.NoError(t, s.SetPassword(ctx, user1, "foo"))
		_, ok = s.authCache[user1]
		assert.False(t, ok)
		_, ok = s.authCache[user2]
		assert.True(t, ok)
	})

	t.Run("ComparePassword adds cached user", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		inner := mock.NewMockPasswordsService(ctrl)
		inner.EXPECT().
			ComparePassword(gomock.Any(), user1, "foo").
			Return(nil)

		s := NewCachingPasswordsService(inner)
		s.authCache[user2] = userE2

		ctx := context.Background()

		assert.NoError(t, s.ComparePassword(ctx, user1, "foo"))
		_, ok := s.authCache[user1]
		assert.True(t, ok)
		_, ok = s.authCache[user2]
		assert.True(t, ok)
	})

	t.Run("ComparePassword does not add cached user when inner errors", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		inner := mock.NewMockPasswordsService(ctrl)
		inner.EXPECT().
			ComparePassword(gomock.Any(), user1, "foo").
			Return(tenant.EShortPassword)

		s := NewCachingPasswordsService(inner)
		s.authCache[user2] = userE2

		ctx := context.Background()

		assert.Error(t, s.ComparePassword(ctx, user1, "foo"))
		_, ok := s.authCache[user1]
		assert.False(t, ok)
		_, ok = s.authCache[user2]
		assert.True(t, ok)
	})

	t.Run("ComparePassword uses cached password when context option set", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		inner := mock.NewMockPasswordsService(ctrl)
		s := NewCachingPasswordsService(inner)
		s.authCache[user1] = userE1
		s.authCache[user2] = userE2

		ctx := context.Background()
		assert.NoError(t, s.ComparePassword(ctx, user1, "foo"))
	})

	t.Run("CompareAndSetPassword deletes cached user", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		inner := mock.NewMockPasswordsService(ctrl)
		inner.EXPECT().
			CompareAndSetPassword(gomock.Any(), user1, "foo", "foo2").
			Return(nil)

		s := NewCachingPasswordsService(inner)
		s.authCache[user1] = userE1
		s.authCache[user2] = userE2

		ctx := context.Background()

		assert.NoError(t, s.CompareAndSetPassword(ctx, user1, "foo", "foo2"))
		_, ok := s.authCache[user1]
		assert.False(t, ok)
		_, ok = s.authCache[user2]
		assert.True(t, ok)
	})

	// The following tests ensure the service does not change state for invalid
	// requests, which may permit a certain class of attacks.

	t.Run("SetPassword does not delete cached user when inner errors", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		inner := mock.NewMockPasswordsService(ctrl)
		inner.EXPECT().
			SetPassword(gomock.Any(), user1, "foo").
			Return(tenant.EShortPassword)

		s := NewCachingPasswordsService(inner)
		s.authCache[user1] = userE1
		s.authCache[user2] = userE2

		ctx := context.Background()

		_, ok := s.authCache[user1]
		assert.True(t, ok)
		assert.EqualError(t, s.SetPassword(ctx, user1, "foo"), tenant.EShortPassword.Error())
		_, ok = s.authCache[user1]
		assert.True(t, ok)
	})

	t.Run("CompareAndSetPassword does not delete cached user when inner errors", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		inner := mock.NewMockPasswordsService(ctrl)
		inner.EXPECT().
			CompareAndSetPassword(gomock.Any(), user1, "foo", "foo2").
			Return(tenant.EShortPassword)

		s := NewCachingPasswordsService(inner)
		s.authCache[user1] = userE1
		s.authCache[user2] = userE2

		ctx := context.Background()

		assert.Error(t, s.CompareAndSetPassword(ctx, user1, "foo", "foo2"))
		_, ok := s.authCache[user1]
		assert.True(t, ok)
		_, ok = s.authCache[user2]
		assert.True(t, ok)
	})

}
