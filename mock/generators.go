package mock

import (
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
)

// IDGenerator is mock implementation of influxdb.IDGenerator.
type IDGenerator struct {
	IDFn func() platform.ID
}

// ID generates a new influxdb.ID from a mock function.
func (g IDGenerator) ID() platform.ID {
	return g.IDFn()
}

// NewIDGenerator is a simple way to create immutable id generator
func NewIDGenerator(s string, t *testing.T) IDGenerator {
	t.Helper()

	id, err := platform.IDFromString(s)
	if err != nil {
		t.Fatal(err)
	}

	return NewStaticIDGenerator(*id)
}

// NewStaticIDGenerator returns an IDGenerator which produces the ID
// provided to this function on a call to ID().
func NewStaticIDGenerator(id platform.ID) IDGenerator {
	return IDGenerator{
		IDFn: func() platform.ID {
			return id
		},
	}
}

// NewIncrementingIDGenerator returns an ID generator which starts at the
// provided ID and increments on each call to ID().
func NewIncrementingIDGenerator(start platform.ID) IDGenerator {
	return IDGenerator{
		IDFn: func() platform.ID {
			defer func() { start++ }()
			return start
		},
	}
}

// SetIDForFunc replaces the id generator at the end of the pointer with
// one which returns the provided id. It then invokes the provided function before
// restoring the original value at the end of the pointer.
func SetIDForFunc(gen *platform.IDGenerator, id platform.ID, fn func()) {
	backup := *gen
	defer func() { *gen = backup }()

	*gen = NewStaticIDGenerator(id)

	fn()
}

type MockIDGenerator struct {
	Last  *platform.ID
	Count int
}

const FirstMockID int = 65536

func NewMockIDGenerator() *MockIDGenerator {
	return &MockIDGenerator{
		Count: FirstMockID,
	}
}

func (g *MockIDGenerator) ID() platform.ID {
	id := platform.ID(g.Count)
	g.Count++

	g.Last = &id

	return id
}

// NewTokenGenerator is a simple way to create immutable token generator.
func NewTokenGenerator(s string, err error) TokenGenerator {
	return TokenGenerator{
		TokenFn: func() (string, error) {
			return s, err
		},
	}
}

// TokenGenerator is mock implementation of influxdb.TokenGenerator.
type TokenGenerator struct {
	TokenFn func() (string, error)
}

// Token generates a new influxdb.Token from a mock function.
func (g TokenGenerator) Token() (string, error) {
	return g.TokenFn()
}

// TimeGenerator stores a fake value of time.
type TimeGenerator struct {
	FakeValue time.Time
}

// Now will return the FakeValue stored in the struct.
func (g TimeGenerator) Now() time.Time {
	return g.FakeValue
}
