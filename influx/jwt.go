package influx

import (
	"time"

	jwt "github.com/dgrijalva/jwt-go"
)

// Bearer generates tokens for Authorization: Bearer
type Bearer interface {
	Token(username string) (string, error)
}

// BearerJWT is the default Bearer for InfluxDB
type BearerJWT struct {
	SharedSecret string
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
