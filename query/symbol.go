package query

import (
	"github.com/influxdata/influxdb/influxql"
)

type Symbol interface {
	// resolve will resolve the Symbol with a Node and attach it to the WriteEdge.
	resolve(s storage, c *compiledField, out *WriteEdge)
}

type SymbolTable struct {
	Table map[*WriteEdge]Symbol
}

type AuxiliarySymbol struct {
	Ref *influxql.VarRef
}

func (s *AuxiliarySymbol) resolve(storage storage, c *compiledField, out *WriteEdge) {
	// Determine the type for the reference if it hasn't been resolved yet.
	ref := *s.Ref
	if ref.Type == influxql.Unknown || ref.Type == influxql.AnyField {
		ref.Type = storage.MapType(ref.Val)
	}
	c.global.AuxiliaryFields.Iterator(&ref, out)
}

type AuxiliaryWildcardSymbol struct{}

func (s *AuxiliaryWildcardSymbol) resolve(storage storage, c *compiledField, out *WriteEdge) {
	symbol := AuxiliarySymbol{Ref: c.WildcardRef}
	symbol.resolve(storage, c, out)
}

type VarRefSymbol struct {
	Ref *influxql.VarRef
}

func (s *VarRefSymbol) resolve(storage storage, c *compiledField, out *WriteEdge) {
	// Determine the type for the reference if it hasn't been resolved yet.
	ref := *s.Ref
	ref.Type = storage.MapType(ref.Val)
	storage.resolve(&ref, out)
}

type WildcardSymbol struct{}

func (s *WildcardSymbol) resolve(storage storage, c *compiledField, out *WriteEdge) {
	symbol := VarRefSymbol{Ref: c.WildcardRef}
	symbol.resolve(storage, c, out)
}
