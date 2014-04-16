package parser

import (
	"fmt"
)

type WhereCondition struct {
	isBooleanExpression bool
	Left                interface{}
	Operation           string
	Right               *WhereCondition
}

func (self *WhereCondition) GetBoolExpression() (*Value, bool) {
	if self.isBooleanExpression {
		return self.Left.(*Value), true
	}
	return nil, false
}

func (self *WhereCondition) GetLeftWhereCondition() (*WhereCondition, bool) {
	if !self.isBooleanExpression {
		return self.Left.(*WhereCondition), true
	}
	return nil, false
}

func (self *WhereCondition) GetString() string {
	if expr, ok := self.GetBoolExpression(); ok {
		return expr.GetString()
	}

	return fmt.Sprintf("(%s) %s (%s)", self.Left.(*WhereCondition).GetString(), self.Operation, self.Right.GetString())
}
