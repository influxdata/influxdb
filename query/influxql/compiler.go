package influxql

import (
	"context"

	"github.com/influxdata/flux"
	"github.com/influxdata/platform"
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
	Cluster string `json:"cluster,omitempty"`
	DB      string `json:"db,omitempty"`
	RP      string `json:"rp,omitempty"`
	Query   string `json:"query"`

	dbrpMappingSvc platform.DBRPMappingService
}

func NewCompiler(dbrpMappingSvc platform.DBRPMappingService) *Compiler {
	return &Compiler{
		dbrpMappingSvc: dbrpMappingSvc,
	}
}

// Compile tranpiles the query into a specification.
func (c *Compiler) Compile(ctx context.Context) (*flux.Spec, error) {
	transpiler := NewTranspilerWithConfig(
		c.dbrpMappingSvc,
		Config{
			Cluster:                c.Cluster,
			DefaultDatabase:        c.DB,
			DefaultRetentionPolicy: c.RP,
		},
	)
	return transpiler.Transpile(ctx, c.Query)
}
func (c *Compiler) CompilerType() flux.CompilerType {
	return CompilerType
}
