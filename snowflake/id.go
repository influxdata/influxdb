package snowflake

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	platform2 "github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2/pkg/snowflake"
)

func init() {
	rand.Seed(time.Now().UnixNano())
	SetGlobalMachineID(rand.Intn(1023))
}

var globalmachineID struct {
	id  int
	set bool
	sync.RWMutex
}

// ErrGlobalIDBadVal means that the global machine id value wasn't properly set.
var ErrGlobalIDBadVal = errors.New("globalID must be a number between (inclusive) 0 and 1023")

// SetGlobalMachineID returns the global machine id.  This number is limited to a number between 0 and 1023 inclusive.
func SetGlobalMachineID(id int) error {
	if id > 1023 || id < 0 {
		return ErrGlobalIDBadVal
	}
	globalmachineID.Lock()
	globalmachineID.id = id
	globalmachineID.set = true
	globalmachineID.Unlock()
	return nil
}

// GlobalMachineID returns the global machine id.  This number is limited to a number between 0 and 1023 inclusive.
func GlobalMachineID() int {
	var id int
	globalmachineID.RLock()
	id = int(globalmachineID.id)
	globalmachineID.RUnlock()
	return id
}

// NewDefaultIDGenerator returns an *IDGenerator that uses the currently set global machine ID.
// If you change the global machine id, it will not change the id in any generators that have already been created.
func NewDefaultIDGenerator() *IDGenerator {
	globalmachineID.RLock()
	defer globalmachineID.RUnlock()
	if globalmachineID.set {
		return NewIDGenerator(WithMachineID(globalmachineID.id))
	}
	return NewIDGenerator()
}

// IDGenerator holds the ID generator.
type IDGenerator struct {
	Generator *snowflake.Generator
}

// IDGeneratorOp is an option for an IDGenerator.
type IDGeneratorOp func(*IDGenerator)

// WithMachineID uses the low 12 bits of machineID to set the machine ID for the snowflake ID.
func WithMachineID(machineID int) IDGeneratorOp {
	return func(g *IDGenerator) {
		g.Generator = snowflake.New(machineID & 1023)
	}
}

// NewIDGenerator returns a new IDGenerator.  Optionally you can use an IDGeneratorOp.
// to use a specific Generator
func NewIDGenerator(opts ...IDGeneratorOp) *IDGenerator {
	gen := &IDGenerator{}
	for _, f := range opts {
		f(gen)
	}
	if gen.Generator == nil {
		gen.Generator = snowflake.New(rand.Intn(1023))
	}
	return gen
}

// ID returns the next platform.ID from an IDGenerator.
func (g *IDGenerator) ID() platform2.ID {
	var id platform2.ID
	for !id.Valid() {
		id = platform2.ID(g.Generator.Next())
	}
	return id
}
