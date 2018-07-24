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

type IDGenerator struct {
	Generator *snowflake.Generator
}

func NewIDGenerator() *IDGenerator {
	return &IDGenerator{
		// Maximum machine id is 1023
		Generator: snowflake.New(rand.Intn(1023)),
	}
}

func (g *IDGenerator) ID() platform.ID {
	return platform.ID(g.Generator.Next())
}
