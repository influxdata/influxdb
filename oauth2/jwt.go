package oauth2

import (
	"context"
	"fmt"
	"time"

	gojwt "github.com/dgrijalva/jwt-go"
)

// Test if JWT implements Authenticator
var _ Authenticator = &JWT{}

// JWT represents a javascript web token that can be validated or marshaled into string.
type JWT struct {
	Secret string
	Now    func() time.Time
}

// NewJWT creates a new JWT using time.Now; secret is used for signing and validating.
func NewJWT(secret string) JWT {
	return JWT{
		Secret: secret,
		Now:    time.Now,
	}
}

// Ensure Claims implements the jwt.Claims interface
var _ gojwt.Claims = &Claims{}

// Claims extends jwt.StandardClaims Valid to make sure claims has a subject.
type Claims struct {
	gojwt.StandardClaims
}

// Valid adds an empty subject test to the StandardClaims checks.
func (c *Claims) Valid() error {
	if err := c.StandardClaims.Valid(); err != nil {
		return err
	} else if c.StandardClaims.Subject == "" {
		return fmt.Errorf("claim has no subject")
	}
	return nil
}

// Authenticate checks if the jwtToken is signed correctly and validates with Claims.
func (j *JWT) Authenticate(ctx context.Context, jwtToken string) (Principal, error) {
	gojwt.TimeFunc = j.Now

	// Check for expected signing method.
	alg := func(token *gojwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*gojwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(j.Secret), nil
	}

	// 1. Checks for expired tokens
	// 2. Checks if time is after the issued at
	// 3. Check if time is after not before (nbf)
	// 4. Check if subject is not empty
	token, err := gojwt.ParseWithClaims(jwtToken, &Claims{}, alg)
	if err != nil {
		return Principal{}, err
	} else if !token.Valid {
		return Principal{}, err
	}

	claims, ok := token.Claims.(*Claims)
	if !ok {
		return Principal{}, fmt.Errorf("unable to convert claims to standard claims")
	}

	return Principal{
		Subject: claims.Subject,
		Issuer:  claims.Issuer,
	}, nil
}

// Token creates a signed JWT token from user that expires at Now + duration
func (j *JWT) Token(ctx context.Context, user Principal, duration time.Duration) (string, error) {
	// Create a new token object, specifying signing method and the claims
	// you would like it to contain.
	now := j.Now().UTC()
	claims := &Claims{
		gojwt.StandardClaims{
			Subject:   user.Subject,
			Issuer:    user.Issuer,
			ExpiresAt: now.Add(duration).Unix(),
			IssuedAt:  now.Unix(),
			NotBefore: now.Unix(),
		},
	}
	token := gojwt.NewWithClaims(gojwt.SigningMethodHS256, claims)

	// Sign and get the complete encoded token as a string using the secret
	return token.SignedString([]byte(j.Secret))
}
