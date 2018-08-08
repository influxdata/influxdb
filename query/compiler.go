package query

import (
	"context"
	"time"
)

const (
	FluxCompilerType = "flux"
	SpecCompilerType = "spec"
)

// AddCompilerMappings adds the Flux specific compiler mappings.
func AddCompilerMappings(mappings CompilerMappings) error {
	if err := mappings.Add(FluxCompilerType, func() Compiler {
		return new(FluxCompiler)

	}); err != nil {
		return err
	}
	return mappings.Add(SpecCompilerType, func() Compiler {
		return new(SpecCompiler)
	})
}

// FluxCompiler compiles a Flux script into a spec.
type FluxCompiler struct {
	Query string `json:"query"`
}

func (c FluxCompiler) Compile(ctx context.Context) (*Spec, error) {
	return Compile(ctx, c.Query, time.Now())
}

func (c FluxCompiler) CompilerType() CompilerType {
	return FluxCompilerType
}

// SpecCompiler implements Compiler by returning a known spec.
type SpecCompiler struct {
	Spec *Spec `json:"spec"`
}

func (c SpecCompiler) Compile(ctx context.Context) (*Spec, error) {
	return c.Spec, nil
}
func (c SpecCompiler) CompilerType() CompilerType {
	return SpecCompilerType
}
