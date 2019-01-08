package mock

import (
	"testing"

	platform "github.com/influxdata/influxdb"
)

// IDGenerator is mock implementation of platform.IDGenerator.
type IDGenerator struct {
	IDFn func() platform.ID
}

// ID generates a new platform.ID from a mock function.
func (g IDGenerator) ID() platform.ID {
	return g.IDFn()
}

// NewIDGenerator is a simple way to create immutable id generator
func NewIDGenerator(s string, t *testing.T) IDGenerator {
	return IDGenerator{
		IDFn: func() platform.ID {
			id, err := platform.IDFromString(s)
			if err != nil {
				t.Fatal(err)
			}
			return *id
		},
	}
}

// NewTokenGenerator is a simple way to create immutable token generator.
func NewTokenGenerator(s string, err error) TokenGenerator {
	return TokenGenerator{
		TokenFn: func() (string, error) {
			return s, err
		},
	}
}

// TokenGenerator is mock implementation of platform.TokenGenerator.
type TokenGenerator struct {
	TokenFn func() (string, error)
}

// Token generates a new platform.Token from a mock function.
func (g TokenGenerator) Token() (string, error) {
	return g.TokenFn()
}
