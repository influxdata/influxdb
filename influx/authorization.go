package influx

import (
	"fmt"
	"net/http"
	"time"

	jwt "github.com/dgrijalva/jwt-go"
	"github.com/influxdata/chronograf"
)

// Authorizer adds optional authorization header to request
type Authorizer interface {
	// Set may manipulate the request by adding the Authorization header
	Set(req *http.Request) error
}

// NoAuthorization does not add any authorization headers
type NoAuthorization struct{}

// Set does not add authorization
func (n *NoAuthorization) Set(req *http.Request) error { return nil }

// DefaultAuthorization creates either a shared JWT builder, basic auth or Noop
func DefaultAuthorization(src *chronograf.Source) Authorizer {
	// Optionally, add the shared secret JWT token creation
	if src.Username != "" && src.SharedSecret != "" {
		return &BearerJWT{
			Username:     src.Username,
			SharedSecret: src.SharedSecret,
		}
	} else if src.Username != "" && src.Password != "" {
		return &BasicAuth{
			Username: src.Username,
			Password: src.Password,
		}
	}
	return &NoAuthorization{}
}

// BasicAuth adds Authorization: Basic to the request header
type BasicAuth struct {
	Username string
	Password string
}

// Set adds the basic auth headers to the request
func (b *BasicAuth) Set(r *http.Request) error {
	r.SetBasicAuth(b.Username, b.Password)
	return nil
}

// BearerJWT is the default Bearer for InfluxDB
type BearerJWT struct {
	Username     string
	SharedSecret string
}

// Set adds an Authorization Bearer to the request if has a shared secret
func (b *BearerJWT) Set(r *http.Request) error {
	if b.SharedSecret != "" && b.Username != "" {
		token, err := b.Token(b.Username)
		if err != nil {
			return fmt.Errorf("Unable to create token")
		}
		r.Header.Set("Authorization", "Bearer "+token)
	}
	return nil
}

// Token returns the expected InfluxDB JWT signed with the sharedSecret
func (b *BearerJWT) Token(username string) (string, error) {
	return JWT(username, b.SharedSecret, time.Now)
}

// Now returns the current time
type Now func() time.Time

// JWT returns a token string accepted by InfluxDB using the sharedSecret as an Authorization: Bearer header
func JWT(username, sharedSecret string, now Now) (string, error) {
	token := &jwt.Token{
		Header: map[string]interface{}{
			"typ": "JWT",
			"alg": jwt.SigningMethodHS512.Alg(),
		},
		Claims: jwt.MapClaims{
			"username": username,
			"exp":      now().Add(time.Minute).Unix(),
		},
		Method: jwt.SigningMethodHS512,
	}
	return token.SignedString([]byte(sharedSecret))
}
