package notification

import (
	"fmt"

	"github.com/influxdata/influxdb"
)

// Tag is k/v pair.
type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// TagRule is the struct of tag rule.
type TagRule struct {
	Tag
	Operator `json:"operator"`
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
	if _, ok := availableOperator[tr.Operator]; !ok {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  fmt.Sprintf(`Operator %q is invalid`, tr.Operator),
		}
	}
	return nil
}
