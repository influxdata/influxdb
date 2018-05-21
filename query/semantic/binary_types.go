package semantic

import "github.com/influxdata/ifql/ast"

type binarySignature struct {
	operator    ast.OperatorKind
	left, right Kind
}

var binaryTypesLookup = map[binarySignature]Kind{
	//---------------
	// Math Operators
	//---------------
	{operator: ast.AdditionOperator, left: Int, right: Int}:           Int,
	{operator: ast.AdditionOperator, left: UInt, right: UInt}:         UInt,
	{operator: ast.AdditionOperator, left: Float, right: Float}:       Float,
	{operator: ast.SubtractionOperator, left: Int, right: Int}:        Int,
	{operator: ast.SubtractionOperator, left: UInt, right: UInt}:      UInt,
	{operator: ast.SubtractionOperator, left: Float, right: Float}:    Float,
	{operator: ast.MultiplicationOperator, left: Int, right: Int}:     Int,
	{operator: ast.MultiplicationOperator, left: UInt, right: UInt}:   UInt,
	{operator: ast.MultiplicationOperator, left: Float, right: Float}: Float,
	{operator: ast.DivisionOperator, left: Int, right: Int}:           Int,
	{operator: ast.DivisionOperator, left: UInt, right: UInt}:         UInt,
	{operator: ast.DivisionOperator, left: Float, right: Float}:       Float,

	//---------------------
	// Comparison Operators
	//---------------------

	// LessThanEqualOperator

	{operator: ast.LessThanEqualOperator, left: Int, right: Int}:     Bool,
	{operator: ast.LessThanEqualOperator, left: Int, right: UInt}:    Bool,
	{operator: ast.LessThanEqualOperator, left: Int, right: Float}:   Bool,
	{operator: ast.LessThanEqualOperator, left: UInt, right: Int}:    Bool,
	{operator: ast.LessThanEqualOperator, left: UInt, right: UInt}:   Bool,
	{operator: ast.LessThanEqualOperator, left: UInt, right: Float}:  Bool,
	{operator: ast.LessThanEqualOperator, left: Float, right: Int}:   Bool,
	{operator: ast.LessThanEqualOperator, left: Float, right: UInt}:  Bool,
	{operator: ast.LessThanEqualOperator, left: Float, right: Float}: Bool,

	// LessThanOperator

	{operator: ast.LessThanOperator, left: Int, right: Int}:     Bool,
	{operator: ast.LessThanOperator, left: Int, right: UInt}:    Bool,
	{operator: ast.LessThanOperator, left: Int, right: Float}:   Bool,
	{operator: ast.LessThanOperator, left: UInt, right: Int}:    Bool,
	{operator: ast.LessThanOperator, left: UInt, right: UInt}:   Bool,
	{operator: ast.LessThanOperator, left: UInt, right: Float}:  Bool,
	{operator: ast.LessThanOperator, left: Float, right: Int}:   Bool,
	{operator: ast.LessThanOperator, left: Float, right: UInt}:  Bool,
	{operator: ast.LessThanOperator, left: Float, right: Float}: Bool,

	// GreaterThanEqualOperator

	{operator: ast.GreaterThanEqualOperator, left: Int, right: Int}:     Bool,
	{operator: ast.GreaterThanEqualOperator, left: Int, right: UInt}:    Bool,
	{operator: ast.GreaterThanEqualOperator, left: Int, right: Float}:   Bool,
	{operator: ast.GreaterThanEqualOperator, left: UInt, right: Int}:    Bool,
	{operator: ast.GreaterThanEqualOperator, left: UInt, right: UInt}:   Bool,
	{operator: ast.GreaterThanEqualOperator, left: UInt, right: Float}:  Bool,
	{operator: ast.GreaterThanEqualOperator, left: Float, right: Int}:   Bool,
	{operator: ast.GreaterThanEqualOperator, left: Float, right: UInt}:  Bool,
	{operator: ast.GreaterThanEqualOperator, left: Float, right: Float}: Bool,

	// GreaterThanOperator

	{operator: ast.GreaterThanOperator, left: Int, right: Int}:     Bool,
	{operator: ast.GreaterThanOperator, left: Int, right: UInt}:    Bool,
	{operator: ast.GreaterThanOperator, left: Int, right: Float}:   Bool,
	{operator: ast.GreaterThanOperator, left: UInt, right: Int}:    Bool,
	{operator: ast.GreaterThanOperator, left: UInt, right: UInt}:   Bool,
	{operator: ast.GreaterThanOperator, left: UInt, right: Float}:  Bool,
	{operator: ast.GreaterThanOperator, left: Float, right: Int}:   Bool,
	{operator: ast.GreaterThanOperator, left: Float, right: UInt}:  Bool,
	{operator: ast.GreaterThanOperator, left: Float, right: Float}: Bool,

	// EqualOperator

	{operator: ast.EqualOperator, left: Int, right: Int}:       Bool,
	{operator: ast.EqualOperator, left: Int, right: UInt}:      Bool,
	{operator: ast.EqualOperator, left: Int, right: Float}:     Bool,
	{operator: ast.EqualOperator, left: UInt, right: Int}:      Bool,
	{operator: ast.EqualOperator, left: UInt, right: UInt}:     Bool,
	{operator: ast.EqualOperator, left: UInt, right: Float}:    Bool,
	{operator: ast.EqualOperator, left: Float, right: Int}:     Bool,
	{operator: ast.EqualOperator, left: Float, right: UInt}:    Bool,
	{operator: ast.EqualOperator, left: Float, right: Float}:   Bool,
	{operator: ast.EqualOperator, left: String, right: String}: Bool,

	// NotEqualOperator

	{operator: ast.NotEqualOperator, left: Int, right: Int}:       Bool,
	{operator: ast.NotEqualOperator, left: Int, right: UInt}:      Bool,
	{operator: ast.NotEqualOperator, left: Int, right: Float}:     Bool,
	{operator: ast.NotEqualOperator, left: UInt, right: Int}:      Bool,
	{operator: ast.NotEqualOperator, left: UInt, right: UInt}:     Bool,
	{operator: ast.NotEqualOperator, left: UInt, right: Float}:    Bool,
	{operator: ast.NotEqualOperator, left: Float, right: Int}:     Bool,
	{operator: ast.NotEqualOperator, left: Float, right: UInt}:    Bool,
	{operator: ast.NotEqualOperator, left: Float, right: Float}:   Bool,
	{operator: ast.NotEqualOperator, left: String, right: String}: Bool,

	//---------------
	// Regexp Operators
	//---------------

	// RegexpMatchOperator

	{operator: ast.RegexpMatchOperator, left: String, right: Regexp}: Bool,
	{operator: ast.RegexpMatchOperator, left: Regexp, right: String}: Bool,

	// NotRegexpMatchOperator

	{operator: ast.NotRegexpMatchOperator, left: String, right: Regexp}: Bool,
	{operator: ast.NotRegexpMatchOperator, left: Regexp, right: String}: Bool,

	{operator: ast.AdditionOperator, left: String, right: String}: String,
}
