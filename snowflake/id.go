package snowflake

import (
	"math/rand"
	"time"

	"github.com/influxdata/influxdb/pkg/snowflake"
	"github.com/influxdata/platform"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// IDGenerator holds the ID generator.
type IDGenerator struct {
	Generator *snowflake.Generator
}

// NewIDGenerator creates a new ID generator.
func NewIDGenerator() *IDGenerator {
	return &IDGenerator{
		// Maximum machine id is 1023
		Generator: snowflake.New(rand.Intn(1023)),
	}
}

// ID returns a pointer to a generated ID.
func (g *IDGenerator) ID() *platform.ID {
	id := platform.ID(g.Generator.Next())
	return &id
}
