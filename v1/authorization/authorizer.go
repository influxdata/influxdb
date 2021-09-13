package authorization

import (
	"context"
	"errors"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	errors2 "github.com/influxdata/influxdb/v2/kit/platform/errors"
)

var (
	ErrUnsupportedScheme = &errors2.Error{
		Code: errors2.EInternal,
		Msg:  "unsupported authorization scheme",
	}
)

type UserFinder interface {
	// Returns a single user by ID.
	FindUserByID(ctx context.Context, id platform.ID) (*influxdb.User, error)
}

type PasswordComparer interface {
	ComparePassword(ctx context.Context, authID platform.ID, password string) error
}

type AuthTokenFinder interface {
	FindAuthorizationByToken(ctx context.Context, token string) (*influxdb.Authorization, error)
}

// A type that is used to verify credentials.
type Authorizer struct {
	AuthV1   AuthTokenFinder  // A service to find V1 tokens
	AuthV2   AuthTokenFinder  // A service to find V2 tokens
	Comparer PasswordComparer // A service to compare passwords for V1 tokens
	User     UserFinder       // A service to find users
}

// Authorize returns an influxdb.Authorization if c can be verified; otherwise, an error.
// influxdb.ErrCredentialsUnauthorized will be returned if the credentials are invalid.
func (v *Authorizer) Authorize(ctx context.Context, c influxdb.CredentialsV1) (auth *influxdb.Authorization, err error) {
	defer func() {
		auth, err = v.checkAuthError(ctx, auth, err)
	}()

	switch c.Scheme {
	case influxdb.SchemeV1Basic, influxdb.SchemeV1URL:
		auth, err = v.tryV1Authorization(ctx, c)
		if errors.Is(err, ErrAuthNotFound) {
			return v.tryV2Authorization(ctx, c)
		}

		if err != nil {
			return nil, v.normalizeError(err)
		}
		return

	case influxdb.SchemeV1Token:
		return v.tryV2Authorization(ctx, c)

	default:
		// this represents a programmer error
		return nil, ErrUnsupportedScheme
	}
}

func (v *Authorizer) checkAuthError(ctx context.Context, auth *influxdb.Authorization, err error) (*influxdb.Authorization, error) {
	if err != nil {
		return nil, err
	}

	if auth == nil {
		return nil, influxdb.ErrCredentialsUnauthorized
	}

	if auth.Status != influxdb.Active {
		return nil, influxdb.ErrCredentialsUnauthorized
	}

	// check the user is still active
	if user, userErr := v.User.FindUserByID(ctx, auth.UserID); userErr != nil {
		return nil, v.normalizeError(userErr)
	} else if user == nil || user.Status != influxdb.Active {
		return nil, influxdb.ErrCredentialsUnauthorized
	}

	return auth, nil
}

func (v *Authorizer) tryV1Authorization(ctx context.Context, c influxdb.CredentialsV1) (auth *influxdb.Authorization, err error) {
	auth, err = v.AuthV1.FindAuthorizationByToken(ctx, c.Username)
	if err != nil {
		return nil, err
	}

	if err := v.Comparer.ComparePassword(ctx, auth.ID, c.Token); err != nil {
		return nil, err
	}

	return auth, nil
}

func (v *Authorizer) tryV2Authorization(ctx context.Context, c influxdb.CredentialsV1) (auth *influxdb.Authorization, err error) {
	auth, err = v.AuthV2.FindAuthorizationByToken(ctx, c.Token)
	if err != nil {
		return nil, v.normalizeError(err)
	}
	return auth, nil
}

func (v *Authorizer) normalizeError(err error) error {
	if err == nil {
		return nil
	}

	var erri *errors2.Error
	if errors.As(err, &erri) {
		switch erri.Code {
		case errors2.ENotFound, errors2.EForbidden:
			return influxdb.ErrCredentialsUnauthorized
		}
	}

	return err
}
