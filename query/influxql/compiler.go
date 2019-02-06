package influxql

import (
	"context"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/interpreter"
	"github.com/influxdata/flux/semantic"
	platform "github.com/influxdata/influxdb"
)

const CompilerType = "influxql"

// AddCompilerMappings adds the influxql specific compiler mappings.
func AddCompilerMappings(mappings flux.CompilerMappings, dbrpMappingSvc platform.DBRPMappingService) error {
	return mappings.Add(CompilerType, func() flux.Compiler {
		return NewCompiler(dbrpMappingSvc)
	})
}

// Compiler is the transpiler to convert InfluxQL to a Flux specification.
type Compiler struct {
	Cluster string     `json:"cluster,omitempty"`
	DB      string     `json:"db,omitempty"`
	RP      string     `json:"rp,omitempty"`
	Query   string     `json:"query"`
	Now     *time.Time `json:"now,omitempty"`

	dbrpMappingSvc platform.DBRPMappingService
}

func NewCompiler(dbrpMappingSvc platform.DBRPMappingService) *Compiler {
	return &Compiler{
		dbrpMappingSvc: dbrpMappingSvc,
	}
}

// Compile tranpiles the query into a specification.
func (c *Compiler) Compile(ctx context.Context) (*flux.Spec, error) {
	var now time.Time
	if c.Now != nil {
		now = *c.Now
	} else {
		now = time.Now()
	}
	transpiler := NewTranspilerWithConfig(
		c.dbrpMappingSvc,
		Config{
			Cluster:                c.Cluster,
			DefaultDatabase:        c.DB,
			DefaultRetentionPolicy: c.RP,
			Now:                    now,
		},
	)
	astPkg, err := transpiler.Transpile(ctx, c.Query)
	if err != nil {
		return nil, err
	}

	semPkg, err := semantic.New(astPkg)
	if err != nil {
		return nil, err
	}

	itrp := interpreter.NewInterpreter()
	universe := flux.Prelude()

	sideEffects, err := itrp.Eval(semPkg, universe, flux.StdLib())
	if err != nil {
		return nil, err
	}

	spec, err := flux.ToSpec(sideEffects, now)
	if err != nil {
		return nil, err
	}
	return spec, nil
}
func (c *Compiler) CompilerType() flux.CompilerType {
	return CompilerType
}
