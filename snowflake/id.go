package snowflake

import (
	"encoding/binary"
	"math/rand"
	"time"

	"github.com/influxdata/influxdb/pkg/snowflake"
	"github.com/influxdata/platform"
)

// TODO: rename to id.go

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
	id := make(platform.ID, 8)
	i := g.Generator.Next()
	binary.BigEndian.PutUint64(id, i)
	return id
}
