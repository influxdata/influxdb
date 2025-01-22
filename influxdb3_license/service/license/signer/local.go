package signer

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"fmt"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/keyring"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/license"
)

// LocalSigingMethod is a custom JWT signing method that uses a local private key.
type LocalSigingMethod struct {
	jwt.SigningMethod
	privKey string
	pubKey  string
	keyring *keyring.Keyring
}

// NewLocalSigningMethod creates a new LocalSigingMethod instance.
func NewLocalSigningMethod(privKey, pubKey string) (license.Signer, error) {
	keyring, err := keyring.LoadKeys()
	if err != nil {
		return nil, fmt.Errorf("local signer failed to load keys: %w", err)
	}

	_, err = keyring.GetKey(privKey)
	if err != nil {
		return nil, fmt.Errorf("failed to load private key: %w", err)
	}

	_, err = keyring.GetKey(pubKey)
	if err != nil {
		return nil, fmt.Errorf("failed to load public key: %w", err)
	}

	return &LocalSigingMethod{
		SigningMethod: jwt.SigningMethodES256,
		privKey:       privKey,
		pubKey:        pubKey,
		keyring:       keyring,
	}, nil
}

// Sign implements the jwt.SigningMethod interface, which signs the given
// string using the ES256 algorithm and the private key.
func (lsm *LocalSigingMethod) Sign(signingString string, _privKey interface{}) ([]byte, error) {
	// Convert string to bytes for hashing
	data := []byte(signingString)

	// Compute SHA256 hash of the input
	hash := sha256.Sum256(data)

	// Get the private key from the keyring
	privKeyBytes, err := lsm.keyring.GetKey(lsm.privKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get private key: %w", err)
	}

	// Parse the private key
	ecdsaKey, err := jwt.ParseECPrivateKeyFromPEM(privKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	// Sign the hash
	r, s, err := ecdsa.Sign(rand.Reader, ecdsaKey, hash[:])
	if err != nil {
		return nil, fmt.Errorf("failed to sign: %w", err)
	}

	// P-256 signatures must be 32 bytes for R and 32 for S
	const size = 32
	rBytes := r.FillBytes(make([]byte, size))
	sBytes := s.FillBytes(make([]byte, size))

	// Concatenate R and S
	sig := append(rBytes, sBytes...)

	return sig, nil
}

func (lsm *LocalSigingMethod) Kid() string {
	return lsm.pubKey
}
