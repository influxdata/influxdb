package rand

import (
	"crypto/rand"
	"encoding/base64"

	platform "github.com/influxdata/influxdb"
)

// TODO: rename to token.go

// TokenGenerator implements platform.TokenGenerator.
type TokenGenerator struct {
	size int
}

// NewTokenGenerator creates an instance of an platform.TokenGenerator.
func NewTokenGenerator(n int) platform.TokenGenerator {
	return &TokenGenerator{
		size: n,
	}
}

// Token returns a new string token of size t.size.
func (t *TokenGenerator) Token() (string, error) {
	return generateRandomString(t.size)
}

func generateRandomString(s int) (string, error) {
	b, err := generateRandomBytes(s)
	return base64.URLEncoding.EncodeToString(b), err
}

func generateRandomBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}

	return b, nil
}
