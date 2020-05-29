package influxql

import (
	"context"
	"encoding/json"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/flux/plan"
	platform "github.com/influxdata/influxdb/v2"
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
	Bucket  string     `json:"bucket,omitempty"`
	Query   string     `json:"query"`
	Now     *time.Time `json:"now,omitempty"`

	logicalPlannerOptions []plan.LogicalOption

	dbrpMappingSvc platform.DBRPMappingService
}

var _ flux.Compiler = &Compiler{}

func NewCompiler(dbrpMappingSvc platform.DBRPMappingService) *Compiler {
	return &Compiler{
		dbrpMappingSvc: dbrpMappingSvc,
	}
}

// Compile transpiles the query into a Program.
func (c *Compiler) Compile(ctx context.Context, runtime flux.Runtime) (flux.Program, error) {
	var now time.Time
	if c.Now != nil {
		now = *c.Now
	} else {
		now = time.Now()
	}
	transpiler := NewTranspilerWithConfig(
		c.dbrpMappingSvc,
		Config{
			Bucket:                 c.Bucket,
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
	compileOptions := lang.WithLogPlanOpts(c.logicalPlannerOptions...)
	bs, err := json.Marshal(astPkg)
	if err != nil {
		return nil, err
	}
	hdl, err := runtime.JSONToHandle(bs)
	if err != nil {
		return nil, err
	}
	return lang.CompileAST(hdl, runtime, now, compileOptions), nil
}

func (c *Compiler) CompilerType() flux.CompilerType {
	return CompilerType
}

func (c *Compiler) WithLogicalPlannerOptions(opts ...plan.LogicalOption) {
	c.logicalPlannerOptions = opts
}
