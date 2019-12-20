package notification

import (
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/notification/flux"
)

// TagRule is the struct of tag rule.
type TagRule influxdb.TagRule

// Valid returns error for invalid operators.
func (tr TagRule) Valid() error {
	return influxdb.TagRule(tr).Valid()
}

// GenerateFluxAST generates the AST expression for a tag rule.
func (tr TagRule) GenerateFluxAST() ast.Expression {
	k := flux.Member("r", tr.Key)
	v := flux.String(tr.Value)

	switch tr.Operator {
	case influxdb.Equal:
		return flux.Equal(k, v)
		// TODO(desa): have this work for all operator types
	}

	return flux.Equal(k, v)
}
