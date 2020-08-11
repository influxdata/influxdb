package ast

import (
	"regexp"
	"time"

	"github.com/influxdata/flux/ast"
)

func IntegerLiteralFromValue(v int64) *ast.IntegerLiteral {
	return &ast.IntegerLiteral{Value: v}
}
func UnsignedIntegerLiteralFromValue(v uint64) *ast.UnsignedIntegerLiteral {
	return &ast.UnsignedIntegerLiteral{Value: v}
}
func FloatLiteralFromValue(v float64) *ast.FloatLiteral {
	return &ast.FloatLiteral{Value: v}
}
func StringLiteralFromValue(v string) *ast.StringLiteral {
	return &ast.StringLiteral{Value: v}
}
func BooleanLiteralFromValue(v bool) *ast.BooleanLiteral {
	return &ast.BooleanLiteral{Value: v}
}
func DateTimeLiteralFromValue(v time.Time) *ast.DateTimeLiteral {
	return &ast.DateTimeLiteral{Value: v}
}
func RegexpLiteralFromValue(v *regexp.Regexp) *ast.RegexpLiteral {
	return &ast.RegexpLiteral{Value: v}
}

func IntegerFromLiteral(lit *ast.IntegerLiteral) int64 {
	return lit.Value
}
func UnsignedIntegerFromLiteral(lit *ast.UnsignedIntegerLiteral) uint64 {
	return lit.Value
}
func FloatFromLiteral(lit *ast.FloatLiteral) float64 {
	return lit.Value
}
func StringFromLiteral(lit *ast.StringLiteral) string {
	return lit.Value
}
func BooleanFromLiteral(lit *ast.BooleanLiteral) bool {
	return lit.Value
}
func DateTimeFromLiteral(lit *ast.DateTimeLiteral) time.Time {
	return lit.Value
}
func RegexpFromLiteral(lit *ast.RegexpLiteral) *regexp.Regexp {
	return lit.Value
}
