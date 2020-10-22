package influxdb

import (
	"encoding/json"
	"errors"
	"fmt"
)

type AuthorizationType string

const (
	// AuthorizationTypePlain is the default authorization type that stores
	// the Token in plain text.
	AuthorizationTypePlain AuthorizationType = "plain"

	// AuthorizationTypeV1User is reserved for authorizing InfluxDB v1.x requests only.
	AuthorizationTypeV1User AuthorizationType = "v1_user"
)

// JSON representations of AuthorizationType for marshaling
const (
	authorizationTypePlainJSON  = `"plain"`
	authorizationTypeV1UserJSON = `"v1_user"`
)

func (a AuthorizationType) String() string {
	switch a {
	case "", AuthorizationTypePlain:
		return string(AuthorizationTypePlain)
	case AuthorizationTypeV1User:
		return string(AuthorizationTypeV1User)
	default:
		return "invalid"
	}
}

func (a AuthorizationType) MarshalJSON() ([]byte, error) {
	switch a {
	case "", AuthorizationTypePlain:
		return []byte(authorizationTypePlainJSON), nil
	case AuthorizationTypeV1User:
		return []byte(authorizationTypeV1UserJSON), nil
	default:
		return nil, errors.New("authorizationType: invalid")
	}
}

func (a *AuthorizationType) UnmarshalJSON(bytes []byte) error {
	var val string
	err := json.Unmarshal(bytes, &val)
	if err != nil {
		return err
	}

	switch AuthorizationType(val) {
	case AuthorizationTypePlain:
		*a = AuthorizationTypePlain
		return nil
	case AuthorizationTypeV1User:
		*a = AuthorizationTypeV1User
		return nil
	}

	return errors.New("authorizationType: invalid")
}

func (a *AuthorizationType) UnmarshalText(text []byte) error {
	if err := AuthorizationType(text).Valid(); err != nil {
		return err
	}

	*a = AuthorizationType(text)
	return nil
}

func (a AuthorizationType) Valid() error {
	switch a {
	case "", AuthorizationTypePlain, AuthorizationTypeV1User:
		return nil
	default:
		return &Error{
			Code: EInvalid,
			Msg:  fmt.Sprintf("invalid authorizationType: must be %v or %v", AuthorizationTypePlain, AuthorizationTypeV1User),
		}
	}
}

// Ptr returns a pointer to a.
func (a AuthorizationType) Ptr() *AuthorizationType {
	return &a
}
