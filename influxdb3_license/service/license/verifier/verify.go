package verifier

import (
	"errors"
	"fmt"

	"github.com/golang-jwt/jwt/v5"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/keyring"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/license"
)

type Verifier struct {
	method  jwt.SigningMethod
	keyring *keyring.Keyring
}

// NewVerifier creates a new Verifier instance.
func NewVerifier(method jwt.SigningMethod) (*Verifier, error) {
	keyring, err := keyring.LoadKeys()
	if err != nil {
		return nil, fmt.Errorf("failed to load keys: %w", err)
	}

	return &Verifier{
		method:  method,
		keyring: keyring,
	}, nil
}

// Verify implements the Verifier interface, which verifies the license and
// returns the jwt.Token containing the claims.
func (v *Verifier) Verify(tokenString string) (*jwt.Token, error) {
	token, err := jwt.ParseWithClaims(tokenString, &license.Claims{}, func(token *jwt.Token) (interface{}, error) {
		// Validate signing method
		if token.Method.Alg() != v.method.Alg() {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Method.Alg())
		}

		// Get key ID from token header
		kid, ok := token.Header["kid"].(string)
		if !ok {
			return nil, errors.New("no key ID in token header")
		}

		// Find matching public key
		pubKeyBytes, ok := v.keyring.Keys[kid]
		if !ok {
			return nil, fmt.Errorf("no public key found for key ID: %s", kid)
		}

		// Parse the public key
		pubKey, err := jwt.ParseECPublicKeyFromPEM(pubKeyBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse public key: %w", err)
		}

		return pubKey, nil
	})

	if err != nil {
		return nil, err
	}

	// Type assert and validate claims
	claims, ok := token.Claims.(*license.Claims)
	if !ok {
		return nil, errors.New("invalid claims type")
	}

	// Verify required custom claims are present
	if claims.Email == "" {
		return nil, errors.New("missing email claim")
	}
	if claims.HostID == "" {
		return nil, errors.New("missing host_id claim")
	}
	if claims.InstanceID == "" {
		return nil, errors.New("missing instance_id claim")
	}

	return token, nil
}
