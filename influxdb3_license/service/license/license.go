package license

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/keyring"
)

var (
	ErrEmailEmpty      = errors.New("email cannot be empty")
	ErrInstanceIDEmpty = errors.New("instance ID cannot be empty")
	ErrWriterIDEmpty   = errors.New("writer ID cannot be empty")
	ErrDuration        = errors.New("duration must be >= 1 day")

	IssuerDefault = "InfluxData - InfluxDB Pro Licensning Server"
)

type TimeFunc func() time.Time

// Creator represents a license creator.
type Creator struct {
	TimeFn  TimeFunc
	signer  jwt.SigningMethod
	privKey string
	pubKey  string
}

// NewCreator creates a new license creator.
func NewCreator(signer jwt.SigningMethod, privKey, pubKey string) (*Creator, error) {
	// Make sure the public key is available in the local keyring. Otherwise,
	// the license cannot be verified.
	kr, err := keyring.LoadKeys()
	if err != nil {
		return nil, fmt.Errorf("license creator failed to load local keyring: %w", err)
	}

	if _, err := kr.GetKey(pubKey); err != nil {
		return nil, fmt.Errorf("could not find pubKey in local keyring: %w", err)
	}

	// If the pub key is for Google Cloud KMS, make sure it is in the correct format
	// to convert to a KMS key path.
	if strings.HasPrefix(pubKey, "gcloud-kms") {
		_, err := kr.KMSPrivateKey("any", pubKey)
		if err != nil {
			return nil, fmt.Errorf("pubKey appears to be a Google KMS key but couldn't derive path to KMS privKey resrource: %w", err)
		}
	}

	return &Creator{
		TimeFn:  time.Now,
		signer:  signer,
		privKey: privKey,
		pubKey:  pubKey,
	}, nil
}

// Claims is a custom claims struct that extends the standard JWT claims
// with additional fields needed for Pro licensing.
type Claims struct {
	jwt.RegisteredClaims
	LicenseExp *jwt.NumericDate `json:"license_exp"`
	Email      string           `json:"email"`
	WriterID   string           `json:"writer_id"`
	InstanceID string           `json:"instance_id"`
}

// Create creates a new signed license.
func (c *Creator) Create(email, writerID, instanceID string, d time.Duration) (string, error) {
	// Validate input
	if email == "" {
		return "", ErrEmailEmpty
	} else if writerID == "" {
		return "", ErrWriterIDEmpty
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
		WriterID:   writerID,
		InstanceID: instanceID,
	}

	// Create the token with the claims
	tok := jwt.NewWithClaims(c.signer, claims)

	// Set the public key ID so the verifier can find the correct public key
	// to verify the signature.
	tok.Header["kid"] = c.pubKey

	// Sign the token with the private key
	signingString, err := tok.SigningString()
	if err != nil {
		return "", fmt.Errorf("license creator failed to create a signing string: %w", err)
	}

	sig, err := c.signer.Sign(signingString, c.privKey)
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
