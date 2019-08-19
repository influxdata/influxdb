package notification

import (
	"fmt"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/notification/flux"
)

// Tag is k/v pair.
type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Valid returns an error if the tag is missing fields
func (t Tag) Valid() error {
	if t.Key == "" || t.Value == "" {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "tag must contain a key and a value",
		}
	}
	return nil
}

// TagRule is the struct of tag rule.
type TagRule struct {
	Tag
	Operator `json:"operator"`
}

// GenerateFluxAST generates the AST expression for a tag rule.
func (r TagRule) GenerateFluxAST() ast.Expression {
	k := flux.Member("r", r.Key)
	v := flux.String(r.Value)

	switch r.Operator {
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
