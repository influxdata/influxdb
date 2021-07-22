package jsonweb

import (
	"errors"

	"github.com/golang-jwt/jwt"
	"github.com/influxdata/influxdb/v2"
)

const kind = "jwt"

var (
	// ErrKeyNotFound should be returned by a KeyStore when
	// a key cannot be located for the provided key ID
	ErrKeyNotFound = errors.New("key not found")

	// EmptyKeyStore is a KeyStore implementation which contains no keys
	EmptyKeyStore = KeyStoreFunc(func(string) ([]byte, error) {
		return nil, ErrKeyNotFound
	})
)

// KeyStore is a type which holds a set of keys accessed
// via an id
type KeyStore interface {
	Key(string) ([]byte, error)
}

// KeyStoreFunc is a function which can be used as a KeyStore
type KeyStoreFunc func(string) ([]byte, error)

// Key delegates to the receiver KeyStoreFunc
func (k KeyStoreFunc) Key(v string) ([]byte, error) { return k(v) }

// TokenParser is a type which can parse and validate tokens
type TokenParser struct {
	keyStore KeyStore
	parser   *jwt.Parser
}

// NewTokenParser returns a configured token parser used to
// parse Token types from strings
func NewTokenParser(keyStore KeyStore) *TokenParser {
	return &TokenParser{
		keyStore: keyStore,
		parser: &jwt.Parser{
			ValidMethods: []string{jwt.SigningMethodHS256.Alg()},
		},
	}
}

// Parse takes a string then parses and validates it as a jwt based on
// the key described within the token
func (t *TokenParser) Parse(v string) (*Token, error) {
	jwt, err := t.parser.ParseWithClaims(v, &Token{}, func(jwt *jwt.Token) (interface{}, error) {
		token, ok := jwt.Claims.(*Token)
		if !ok {
			return nil, errors.New("missing kid in token claims")
		}

		// fetch key for "kid" from key store
		return t.keyStore.Key(token.KeyID)
	})

	if err != nil {
		return nil, err
	}

	token, ok := jwt.Claims.(*Token)
	if !ok {
		return nil, errors.New("token is unexpected type")
	}

	return token, nil
}

// IsMalformedError returns true if the error returned represents
// a jwt malformed token error
func IsMalformedError(err error) bool {
	verr, ok := err.(*jwt.ValidationError)
	return ok && verr.Errors&jwt.ValidationErrorMalformed > 0
}

// Token is a structure which is serialized as a json web token
// It contains the necessary claims required to authorize
type Token struct {
	jwt.StandardClaims
	// KeyID is the identifier of the key used to sign the token
	KeyID string `json:"kid"`
	// Permissions is the set of authorized permissions for the token
	Permissions []influxdb.Permission `json:"permissions"`
	// UserID for the token
	UserID string `json:"uid,omitempty"`
}

// PermissionSet returns the set of permissions associated with the token.
func (t *Token) PermissionSet() (influxdb.PermissionSet, error) {
	return t.Permissions, nil
}

// Identifier returns the identifier for this Token
// as found in the standard claims
func (t *Token) Identifier() influxdb.ID {
	id, err := influxdb.IDFromString(t.Id)
	if err != nil || id == nil {
		return influxdb.ID(1)
	}

	return *id
}

// GetUserID returns an invalid id as tokens are generated
// with permissions rather than for or by a particular user
func (t *Token) GetUserID() influxdb.ID {
	id, err := influxdb.IDFromString(t.UserID)
	if err != nil {
		return influxdb.InvalidID()
	}
	return *id
}

// Kind returns the string "jwt" which is used for auditing
func (t *Token) Kind() string {
	return kind
}

// EphemeralAuth creates a influxdb Auth form a jwt token
func (t *Token) EphemeralAuth(orgID influxdb.ID) *influxdb.Authorization {
	return &influxdb.Authorization{
		ID:          t.Identifier(),
		OrgID:       orgID,
		Status:      influxdb.Active,
		Permissions: t.Permissions,
	}
}
