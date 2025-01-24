package license

import (
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
)

var (
	ErrEmailEmpty      = errors.New("email cannot be empty")
	ErrInstanceIDEmpty = errors.New("instance ID cannot be empty")
	ErrNodeIDEmpty     = errors.New("node ID cannot be empty")
	ErrDuration        = errors.New("duration must be >= 1 day")

	IssuerDefault = "InfluxData - InfluxDB Pro Licensning Server"
)

type TimeFunc func() time.Time

type Signer interface {
	jwt.SigningMethod

	Kid() string
}

// Creator represents a license creator.
type Creator struct {
	TimeFn TimeFunc
	signer Signer
}

// NewCreator creates a new license creator.
func NewCreator(signer Signer) (*Creator, error) {
	return &Creator{
		TimeFn: time.Now,
		signer: signer,
	}, nil
}

// Claims is a custom claims struct that extends the standard JWT claims
// with additional fields needed for Pro licensing.
type Claims struct {
	jwt.RegisteredClaims
	LicenseExp *jwt.NumericDate `json:"license_exp"`
	Email      string           `json:"email"`
	NodeID     string           `json:"node_id"`
	InstanceID string           `json:"instance_id"`
}

// Create creates a new signed license.
func (c *Creator) Create(email, nodeID, instanceID string, d time.Duration) (string, error) {
	// Validate input
	if email == "" {
		return "", ErrEmailEmpty
	} else if nodeID == "" {
		return "", ErrNodeIDEmpty
	} else if instanceID == "" {
		return "", ErrInstanceIDEmpty
	}

	// Create the claims for the new license
	now := c.TimeFn()

	claims := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "InfluxData - InfluxDB Pro Licensing Server",
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(d)),
		},
		LicenseExp: jwt.NewNumericDate(now.Add(d)),
		Email:      email,
		NodeID:     nodeID,
		InstanceID: instanceID,
	}

	// Create the token with the claims
	tok := jwt.NewWithClaims(c.signer, claims)

	// Set the public key ID so the verifier can find the correct public key
	// to verify the signature.
	tok.Header["kid"] = c.signer.Kid()

	// Sign the token with the private key
	signingString, err := tok.SigningString()
	if err != nil {
		return "", fmt.Errorf("license creator failed to create a signing string: %w", err)
	}

	sig, err := c.signer.Sign(signingString, nil)
	if err != nil {
		return "", err
	}

	// Encode the signature
	sigEnc := base64.RawURLEncoding.EncodeToString(sig)

	// Create the license, which is a JWT token string of the format:
	// <header>.<claims>.<signature>
	lic := signingString + "." + string(sigEnc)

	return lic, nil
}

// Verifier parses a license token string, verifies the signature, and returns
// a JWT token with the claims.
type Verifier interface {
	Verify(token string) (*jwt.Token, error)
}
