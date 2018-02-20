package oauth2

import (
	"context"
	"fmt"
	"time"

	gojwt "github.com/dgrijalva/jwt-go"
)

// Ensure JWT conforms to the Tokenizer interface
var _ Tokenizer = &JWT{}

// JWT represents a javascript web token that can be validated or marshaled into string.
type JWT struct {
	Secret string
	Now    func() time.Time
}

// NewJWT creates a new JWT using time.Now; secret is used for signing and validating.
func NewJWT(secret string) *JWT {
	return &JWT{
		Secret: secret,
		Now:    DefaultNowTime,
	}
}

// Ensure Claims implements the jwt.Claims interface
var _ gojwt.Claims = &Claims{}

// Claims extends jwt.StandardClaims' Valid to make sure claims has a subject.
type Claims struct {
	gojwt.StandardClaims
	// We were unable to find a standard claim at https://www.iana.org/assignments/jwt/jwt.xhtml
	// that felt appropriate for Organization. As a result, we added a custom `org` field.
	Organization string `json:"org,omitempty"`
	// We were unable to find a standard claim at https://www.iana.org/assignments/jwt/jwt.xhtml
	// that felt appropriate for a users Group(s). As a result we added a custom `grp` field.
	// Multiple groups may be specified by comma delimiting the various group.
	//
	// The singlular `grp` was chosen over the `grps` to keep consistent with the JWT naming
	// convention (it is common for singlularly named values to actually be arrays, see `given_name`,
	// `family_name`, and `middle_name` in the iana link provided above). I should add the discalimer
	// I'm currently sick, so this thought process might be off.
	Group string `json:"grp,omitempty"`
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

// ValidPrincipal checks if the jwtToken is signed correctly and validates with Claims.  lifespan is the
// maximum valid lifetime of a token.  If the lifespan is 0 then the auth lifespan duration is not checked.
func (j *JWT) ValidPrincipal(ctx context.Context, jwtToken Token, lifespan time.Duration) (Principal, error) {
	gojwt.TimeFunc = j.Now

	// Check for expected signing method.
	alg := func(token *gojwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*gojwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(j.Secret), nil
	}

	return j.ValidClaims(jwtToken, lifespan, alg)
}

// ValidClaims validates a token with StandardClaims
func (j *JWT) ValidClaims(jwtToken Token, lifespan time.Duration, alg gojwt.Keyfunc) (Principal, error) {
	// 1. Checks for expired tokens
	// 2. Checks if time is after the issued at
	// 3. Check if time is after not before (nbf)
	// 4. Check if subject is not empty
	// 5. Check if duration less than auth lifespan
	token, err := gojwt.ParseWithClaims(string(jwtToken), &Claims{}, alg)
	if err != nil {
		return Principal{}, err
		// at time of this writing and researching the docs, token.Valid seems to be always true
	} else if !token.Valid {
		return Principal{}, err
	}

	// at time of this writing and researching the docs, there will always be claims
	claims, ok := token.Claims.(*Claims)
	if !ok {
		return Principal{}, fmt.Errorf("unable to convert claims to standard claims")
	}

	exp := time.Unix(claims.ExpiresAt, 0)
	iat := time.Unix(claims.IssuedAt, 0)

	// If the duration of the claim is longer than the auth lifespan then this is
	// an invalid claim because server assumes that lifespan is the maximum possible
	// duration.  However, a lifespan of zero means that the duration comparison
	// against the auth duration is not needed.
	if lifespan > 0 && exp.Sub(iat) > lifespan {
		return Principal{}, fmt.Errorf("claims duration is different from auth lifespan")
	}

	return Principal{
		Subject:      claims.Subject,
		Issuer:       claims.Issuer,
		Organization: claims.Organization,
		Group:        claims.Group,
		ExpiresAt:    exp,
		IssuedAt:     iat,
	}, nil
}

// Create creates a signed JWT token from user that expires at Principal's ExpireAt time.
func (j *JWT) Create(ctx context.Context, user Principal) (Token, error) {
	// Create a new token object, specifying signing method and the claims
	// you would like it to contain.
	claims := &Claims{
		StandardClaims: gojwt.StandardClaims{
			Subject:   user.Subject,
			Issuer:    user.Issuer,
			ExpiresAt: user.ExpiresAt.Unix(),
			IssuedAt:  user.IssuedAt.Unix(),
			NotBefore: user.IssuedAt.Unix(),
		},
		Organization: user.Organization,
		Group:        user.Group,
	}
	token := gojwt.NewWithClaims(gojwt.SigningMethodHS256, claims)
	// Sign and get the complete encoded token as a string using the secret
	t, err := token.SignedString([]byte(j.Secret))
	// this will only fail if the JSON can't be encoded correctly
	if err != nil {
		return "", err
	}
	return Token(t), nil
}

// ExtendedPrincipal sets the expires at to be the current time plus the extention into the future
func (j *JWT) ExtendedPrincipal(ctx context.Context, principal Principal, extension time.Duration) (Principal, error) {
	// Extend the time of expiration.  Do not change IssuedAt as the
	// lifetime of the token is extended, but, NOT the original time
	// of issue. This is used to enforce a maximum lifetime of a token
	principal.ExpiresAt = j.Now().Add(extension)
	return principal, nil
}
