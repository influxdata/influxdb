package influxdb

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

var (
	// ErrCredentialsUnauthorized is the error returned when CredentialsV1 cannot be
	// authorized.
	ErrCredentialsUnauthorized = &errors.Error{
		Code: errors.EUnauthorized,
		Msg:  "Unauthorized",
	}
)

// SchemeV1 is an enumeration of supported authorization types
type SchemeV1 string

const (
	// SchemeV1Basic indicates the credentials came from an Authorization header using the BASIC scheme
	SchemeV1Basic SchemeV1 = "basic"

	// SchemeV1Token indicates the credentials came from an Authorization header using the Token scheme
	SchemeV1Token SchemeV1 = "token"

	// SchemeV1URL indicates the credentials came from the u and p query parameters
	SchemeV1URL SchemeV1 = "url"
)

// CredentialsV1 encapsulates the required credentials to authorize a v1 HTTP request.
type CredentialsV1 struct {
	Scheme   SchemeV1
	Username string
	Token    string
}

type AuthorizerV1 interface {
	Authorize(ctx context.Context, v1 CredentialsV1) (*Authorization, error)
}
