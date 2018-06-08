// +build parser_debug

package parser

//go:generate pigeon -optimize-grammar -o flux.go flux.peg

import (
	"github.com/influxdata/platform/query/ast"
)

// NewAST parses Flux query and produces an ast.Program
func NewAST(flux string, opts ...Option) (*ast.Program, error) {
	// Turn on Debugging
	opts = append(opts, Debug(true))

	f, err := Parse("", []byte(flux), opts...)
	if err != nil {
		return nil, err
	}
	return f.(*ast.Program), nil
}
