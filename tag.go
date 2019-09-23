package influxdb

import (
	"fmt"
	"regexp"
	"strings"
)

// Operator is an Enum value of operators.
type Operator string

// Valid returns invalid error if the operator is invalid.
func (op Operator) Valid() error {
	if good := availableOperator[op]; !good {
		return &Error{
			Code: EInvalid,
			Msg:  fmt.Sprintf(`Operator %q is invalid`, op),
		}
	}
	return nil
}

// operators
const (
	Equal         Operator = "equal"
	NotEqual      Operator = "notequal"
	RegexEqual    Operator = "equalregex"
	NotRegexEqual Operator = "notequalregex"
)

var availableOperator = map[Operator]bool{
	Equal:         true,
	NotEqual:      true,
	RegexEqual:    true,
	NotRegexEqual: true,
}

// Tag is a tag key-value pair.
type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// NewTag generates a tag pair from a string in the format key:value.
func NewTag(s string) (Tag, error) {
	var tagPair Tag

	matched, err := regexp.MatchString(`^[a-zA-Z0-9_]+:[a-zA-Z0-9_]+$`, s)
	if !matched || err != nil {
		return tagPair, &Error{
			Code: EInvalid,
			Msg:  `tag must be in form key:value`,
		}
	}

	slice := strings.Split(s, ":")
	tagPair.Key = slice[0]
	tagPair.Value = slice[1]

	return tagPair, nil
}

// Valid returns an error if the tagpair is missing fields
func (t Tag) Valid() error {
	if t.Key == "" || t.Value == "" {
		return &Error{
			Code: EInvalid,
			Msg:  "tag must contain a key and a value",
		}
	}
	return nil
}

// QueryParam converts a Tag to a string query parameter
func (tp *Tag) QueryParam() string {
	return strings.Join([]string{tp.Key, tp.Value}, ":")
}

// TagRule is the struct of tag rule.
type TagRule struct {
	Tag
	Operator `json:"operator"`
}

// Valid returns error for invalid operators.
func (tr TagRule) Valid() error {
	if err := tr.Tag.Valid(); err != nil {
		return err
	}

	return tr.Operator.Valid()
}
