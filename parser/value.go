package parser

// #include "query_types.h"
// #include <stdlib.h>
import "C"

import (
	"bytes"
	"fmt"
	"regexp"
)

type ValueType int

const (
	ValueRegex        ValueType = C.VALUE_REGEX
	ValueInt                    = C.VALUE_INT
	ValueBool                   = C.VALUE_BOOLEAN
	ValueFloat                  = C.VALUE_FLOAT
	ValueString                 = C.VALUE_STRING
	ValueIntoName               = C.VALUE_INTO_NAME
	ValueTableName              = C.VALUE_TABLE_NAME
	ValueSimpleName             = C.VALUE_SIMPLE_NAME
	ValueDuration               = C.VALUE_DURATION
	ValueWildcard               = C.VALUE_WILDCARD
	ValueFunctionCall           = C.VALUE_FUNCTION_CALL
	ValueExpression             = C.VALUE_EXPRESSION
)

type Value struct {
	Name          string
	Alias         string
	Type          ValueType
	Elems         []*Value
	compiledRegex *regexp.Regexp
	IsInsensitive bool
}

func (self *Value) IsFunctionCall() bool {
	return self.Type == ValueFunctionCall
}

func (self *Value) GetCompiledRegex() (*regexp.Regexp, bool) {
	return self.compiledRegex, self.Type == ValueRegex
}

func (self *Value) GetString() string {
	buffer := bytes.NewBufferString("")
	switch self.Type {
	case ValueExpression:
		fmt.Fprintf(buffer, "%s %s %s", self.Elems[0].GetString(), self.Name, self.Elems[1].GetString())
	case ValueFunctionCall:
		fmt.Fprintf(buffer, "%s(%s)", self.Name, Values(self.Elems).GetString())
	case ValueString:
		fmt.Fprintf(buffer, "'%s'", self.Name)
	case ValueRegex:
		fmt.Fprintf(buffer, "/%s/", self.Name)
		if self.IsInsensitive {
			buffer.WriteString("i")
		}
	default:
		buffer.WriteString(self.Name)
	}

	if self.Alias != "" {
		fmt.Fprintf(buffer, " as %s", self.Alias)
	}

	return buffer.String()
}
