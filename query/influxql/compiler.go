package influxql

import (
	"context"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/flux/plan"
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
func (c *Compiler) Compile(ctx context.Context) (flux.Program, error) {
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

	fs, err := flux.CompileAST(ctx, astPkg, now)
	if err != nil {
		return nil, err
	}

	pb := plan.PlannerBuilder{}
	pb.AddLogicalOptions(c.logicalPlannerOptions...)
	planner := pb.Build()
	ps, err := planner.Plan(fs)
	if err != nil {
		return nil, err
	}
	return lang.Program{
		PlanSpec: ps,
	}, nil
}

func (c *Compiler) CompilerType() flux.CompilerType {
	return CompilerType
}

func (c *Compiler) WithLogicalPlannerOptions(opts ...plan.LogicalOption) {
	c.logicalPlannerOptions = opts
}
