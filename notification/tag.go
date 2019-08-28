package notification

import (
	"fmt"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/notification/flux"
)

// TagRule is the struct of tag rule.
type TagRule struct {
	influxdb.Tag
	Operator `json:"operator"`
}

// GenerateFluxAST generates the AST expression for a tag rule.
func (tr TagRule) GenerateFluxAST() ast.Expression {
	k := flux.Member("r", tr.Key)
	v := flux.String(tr.Value)

	switch tr.Operator {
	case Equal:
		return flux.Equal(k, v)
		// TODO(desa): have this work for all operator types
	}

	return flux.Equal(k, v)
}

// Operator is an Enum value of
type Operator string

// operators
const (
	Equal         Operator = "equal"
	NotEqual      Operator = "notequal"
	RegexEqual    Operator = "equalregex"
	NotRegexEqual Operator = "notequalregex"
)

var availableOperator = map[Operator]bool{
	Equal:         false,
	NotEqual:      false,
	RegexEqual:    false,
	NotRegexEqual: false,
}

// Valid returns error for invalid operators.
func (tr TagRule) Valid() error {
	if err := tr.Tag.Valid(); err != nil {
		return err
	}
	if _, ok := availableOperator[tr.Operator]; !ok {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  fmt.Sprintf(`Operator %q is invalid`, tr.Operator),
		}
	}
	return nil
}
