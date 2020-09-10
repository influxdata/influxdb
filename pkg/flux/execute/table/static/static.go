// Package static provides utilities for easily constructing static
// tables that are meant for tests.
//
// The primary type is Table which will be a mapping of columns to their data.
// The data is defined in a columnar format instead of a row-based one.
//
// The implementations in this package are not performant and are not meant
// to be used in production code. They are good enough for small datasets that
// are present in tests to ensure code correctness.
package static

import (
	"fmt"
	"time"

	stdarrow "github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/arrow"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/v2/pkg/flux/internal/errors"
	"github.com/influxdata/influxdb/v2/pkg/flux/internal/execute/table"
)

// Table is a statically constructed table.
// It is a mapping between column names and the column.
//
// This is not a performant section of code and it is primarily
// meant to make writing unit tests easily. Do not use in
// production code.
//
// The Table struct implements the TableIterator interface
// and not the Table interface. To retrieve a flux.Table compatible
// implementation, the Table() method can be used.
type Table []Column

// Do will produce the Table and then invoke the function
// on that flux.Table.
//
// If the produced Table is invalid, then this method
// will panic.
func (s Table) Do(f func(flux.Table) error) error {
	return f(s.Table())
}

func (s Table) Build(template *[]Column) []flux.Table {
	t := make(Table, 0, len(*template)+len(s))
	t = append(t, *template...)
	t = append(t, s...)
	return []flux.Table{t.Table()}
}

// Table will produce a flux.Table using the Column values
// that are part of this Table.
//
// If the Table produces an invalid buffer, then this method
// will panic.
func (s Table) Table() flux.Table {
	if len(s) == 0 {
		panic(errors.New(codes.Internal, "static table has no columns"))
	}

	key, cols := s.buildSchema()
	buffer := &arrow.TableBuffer{
		GroupKey: key,
		Columns:  cols,
	}

	// Determine the size by looking at the first non-key column.
	n := 0
	for _, c := range s {
		if c.IsKey() {
			continue
		}
		n = c.Len()
		break
	}

	// Construct each of the buffers.
	buffer.Values = make([]array.Interface, len(buffer.Columns))
	for i, c := range s {
		buffer.Values[i] = c.Make(n)
	}

	if err := buffer.Validate(); err != nil {
		panic(err)
	}
	return table.FromBuffer(buffer)
}

// buildSchema will construct the schema from the columns.
func (s Table) buildSchema() (flux.GroupKey, []flux.ColMeta) {
	var (
		keyCols []flux.ColMeta
		keyVals []values.Value
		cols    []flux.ColMeta
	)
	for _, c := range s {
		col := flux.ColMeta{Label: c.Label(), Type: c.Type()}
		if c.IsKey() {
			keyCols = append(keyCols, col)
			keyVals = append(keyVals, c.KeyValue())
		}
		cols = append(cols, col)
	}
	return execute.NewGroupKey(keyCols, keyVals), cols
}

// Column is the definition for how to construct a column for the table.
type Column interface {
	// Label returns the label associated with this column.
	Label() string

	// Type returns the column type for this column.
	Type() flux.ColType

	// Make will construct an array with the given length
	// if it is possible.
	Make(n int) array.Interface

	// Len will return the length of this column.
	// If no length is known, this will return -1.
	Len() int

	// IsKey will return true if this is part of the group key.
	IsKey() bool

	// KeyValue will return the key value if this column is part
	// of the group key.
	KeyValue() values.Value

	// TableBuilder allows this column to add itself to a template.
	TableBuilder
}

// IntKey will construct a group key with the integer type.
// The value can be an int, int64, or nil.
func IntKey(k string, v interface{}) KeyColumn {
	if iv, ok := mustIntValue(v); ok {
		return KeyColumn{k: k, v: iv, t: flux.TInt}
	}
	return KeyColumn{k: k, t: flux.TInt}
}

// UintKey will construct a group key with the unsigned type.
// The value can be a uint, uint64, int, int64, or nil.
func UintKey(k string, v interface{}) KeyColumn {
	if iv, ok := mustUintValue(v); ok {
		return KeyColumn{k: k, v: iv, t: flux.TUInt}
	}
	return KeyColumn{k: k, t: flux.TUInt}
}

// FloatKey will construct a group key with the float type.
// The value can be a float64, int, int64, or nil.
func FloatKey(k string, v interface{}) KeyColumn {
	if iv, ok := mustFloatValue(v); ok {
		return KeyColumn{k: k, v: iv, t: flux.TFloat}
	}
	return KeyColumn{k: k, t: flux.TFloat}
}

// StringKey will construct a group key with the string type.
// The value can be a string or nil.
func StringKey(k string, v interface{}) KeyColumn {
	if iv, ok := mustStringValue(v); ok {
		return KeyColumn{k: k, v: iv, t: flux.TString}
	}
	return KeyColumn{k: k, t: flux.TString}
}

// BooleanKey will construct a group key with the boolean type.
// The value can be a bool or nil.
func BooleanKey(k string, v interface{}) KeyColumn {
	if iv, ok := mustBooleanValue(v); ok {
		return KeyColumn{k: k, v: iv, t: flux.TBool}
	}
	return KeyColumn{k: k, t: flux.TBool}
}

// TimeKey will construct a group key with the given time using either a
// string or an integer. If an integer is used, then it is in seconds.
func TimeKey(k string, v interface{}) KeyColumn {
	if iv, _, ok := mustTimeValue(v, 0, time.Second); ok {
		return KeyColumn{k: k, v: execute.Time(iv), t: flux.TTime}
	}
	return KeyColumn{k: k, t: flux.TTime}
}

type KeyColumn struct {
	k string
	v interface{}
	t flux.ColType
}

func (s KeyColumn) Make(n int) array.Interface {
	return arrow.Repeat(s.KeyValue(), n, memory.DefaultAllocator)
}

func (s KeyColumn) Label() string          { return s.k }
func (s KeyColumn) Type() flux.ColType     { return s.t }
func (s KeyColumn) Len() int               { return -1 }
func (s KeyColumn) IsKey() bool            { return true }
func (s KeyColumn) KeyValue() values.Value { return values.New(s.v) }

func (s KeyColumn) Build(template *[]Column) []flux.Table {
	*template = append(*template, s)
	return nil
}

// Ints will construct an array of integers.
// Each value can be an int, int64, or nil.
func Ints(k string, v ...interface{}) Column {
	c := intColumn{
		column: column{k: k},
		v:      make([]int64, len(v)),
	}
	for i, iv := range v {
		val, ok := mustIntValue(iv)
		if !ok {
			if c.valid == nil {
				c.valid = make([]bool, len(v))
				for i := range c.valid {
					c.valid[i] = true
				}
			}
			c.valid[i] = false
		}
		c.v[i] = val
	}
	return c
}

type column struct {
	k     string
	valid []bool
}

func (s column) Label() string { return s.k }
func (s column) IsKey() bool   { return false }

type intColumn struct {
	column
	v []int64
}

func (s intColumn) Make(n int) array.Interface {
	b := array.NewInt64Builder(memory.DefaultAllocator)
	b.Resize(len(s.v))
	b.AppendValues(s.v, s.valid)
	return b.NewArray()
}

func (s intColumn) Type() flux.ColType     { return flux.TInt }
func (s intColumn) Len() int               { return len(s.v) }
func (s intColumn) KeyValue() values.Value { return values.InvalidValue }

func (s intColumn) Build(template *[]Column) []flux.Table {
	*template = append(*template, s)
	return nil
}

func mustIntValue(v interface{}) (int64, bool) {
	if v == nil {
		return 0, false
	}

	switch v := v.(type) {
	case int:
		return int64(v), true
	case int64:
		return v, true
	default:
		panic(fmt.Sprintf("unable to convert type %T to an int value", v))
	}
}

// Uints will construct an array of unsigned integers.
// Each value can be a uint, uint64, int, int64, or nil.
func Uints(k string, v ...interface{}) Column {
	c := uintColumn{
		column: column{k: k},
		v:      make([]uint64, len(v)),
	}
	for i, iv := range v {
		val, ok := mustUintValue(iv)
		if !ok {
			if c.valid == nil {
				c.valid = make([]bool, len(v))
				for i := range c.valid {
					c.valid[i] = true
				}
			}
			c.valid[i] = false
		}
		c.v[i] = val
	}
	return c
}

type uintColumn struct {
	column
	v []uint64
}

func (s uintColumn) Make(n int) array.Interface {
	b := array.NewUint64Builder(memory.DefaultAllocator)
	b.Resize(len(s.v))
	b.AppendValues(s.v, s.valid)
	return b.NewArray()
}

func (s uintColumn) Type() flux.ColType     { return flux.TUInt }
func (s uintColumn) Len() int               { return len(s.v) }
func (s uintColumn) KeyValue() values.Value { return values.InvalidValue }

func (s uintColumn) Build(template *[]Column) []flux.Table {
	*template = append(*template, s)
	return nil
}

func mustUintValue(v interface{}) (uint64, bool) {
	if v == nil {
		return 0, false
	}

	switch v := v.(type) {
	case int:
		return uint64(v), true
	case int64:
		return uint64(v), true
	case uint:
		return uint64(v), true
	case uint64:
		return v, true
	default:
		panic(fmt.Sprintf("unable to convert type %T to a uint value", v))
	}
}

// Floats will construct an array of floats.
// Each value can be a float64, int, int64, or nil.
func Floats(k string, v ...interface{}) Column {
	c := floatColumn{
		column: column{k: k},
		v:      make([]float64, len(v)),
	}
	for i, iv := range v {
		val, ok := mustFloatValue(iv)
		if !ok {
			if c.valid == nil {
				c.valid = make([]bool, len(v))
				for i := range c.valid {
					c.valid[i] = true
				}
			}
			c.valid[i] = false
		}
		c.v[i] = val
	}
	return c
}

type floatColumn struct {
	column
	v []float64
}

func (s floatColumn) Make(n int) array.Interface {
	b := array.NewFloat64Builder(memory.DefaultAllocator)
	b.Resize(len(s.v))
	b.AppendValues(s.v, s.valid)
	return b.NewArray()
}

func (s floatColumn) Type() flux.ColType     { return flux.TFloat }
func (s floatColumn) Len() int               { return len(s.v) }
func (s floatColumn) KeyValue() values.Value { return values.InvalidValue }

func (s floatColumn) Build(template *[]Column) []flux.Table {
	*template = append(*template, s)
	return nil
}

func mustFloatValue(v interface{}) (float64, bool) {
	if v == nil {
		return 0, false
	}

	switch v := v.(type) {
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case float64:
		return v, true
	default:
		panic(fmt.Sprintf("unable to convert type %T to a float value", v))
	}
}

// Strings will construct an array of strings.
// Each value can be a string or nil.
func Strings(k string, v ...interface{}) Column {
	c := stringColumn{
		column: column{k: k},
		v:      make([]string, len(v)),
	}
	for i, iv := range v {
		val, ok := mustStringValue(iv)
		if !ok {
			if c.valid == nil {
				c.valid = make([]bool, len(v))
				for i := range c.valid {
					c.valid[i] = true
				}
			}
			c.valid[i] = false
		}
		c.v[i] = val
	}
	return c
}

type stringColumn struct {
	column
	v []string
}

func (s stringColumn) Make(n int) array.Interface {
	b := array.NewBinaryBuilder(memory.DefaultAllocator, stdarrow.BinaryTypes.String)
	b.Resize(len(s.v))
	b.AppendStringValues(s.v, s.valid)
	return b.NewArray()
}

func (s stringColumn) Type() flux.ColType     { return flux.TString }
func (s stringColumn) Len() int               { return len(s.v) }
func (s stringColumn) KeyValue() values.Value { return values.InvalidValue }

func (s stringColumn) Build(template *[]Column) []flux.Table {
	*template = append(*template, s)
	return nil
}

func mustStringValue(v interface{}) (string, bool) {
	if v == nil {
		return "", false
	}

	switch v := v.(type) {
	case string:
		return v, true
	default:
		panic(fmt.Sprintf("unable to convert type %T to a string value", v))
	}
}

// Booleans will construct an array of booleans.
// Each value can be a bool or nil.
func Booleans(k string, v ...interface{}) Column {
	c := booleanColumn{
		column: column{k: k},
		v:      make([]bool, len(v)),
	}
	for i, iv := range v {
		val, ok := mustBooleanValue(iv)
		if !ok {
			if c.valid == nil {
				c.valid = make([]bool, len(v))
				for i := range c.valid {
					c.valid[i] = true
				}
			}
			c.valid[i] = false
		}
		c.v[i] = val
	}
	return c
}

type booleanColumn struct {
	column
	v []bool
}

func (s booleanColumn) Make(n int) array.Interface {
	b := array.NewBooleanBuilder(memory.DefaultAllocator)
	b.Resize(len(s.v))
	b.AppendValues(s.v, s.valid)
	return b.NewArray()
}

func (s booleanColumn) Type() flux.ColType     { return flux.TBool }
func (s booleanColumn) Len() int               { return len(s.v) }
func (s booleanColumn) KeyValue() values.Value { return values.InvalidValue }

func (s booleanColumn) Build(template *[]Column) []flux.Table {
	*template = append(*template, s)
	return nil
}

func mustBooleanValue(v interface{}) (bool, bool) {
	if v == nil {
		return false, false
	}

	switch v := v.(type) {
	case bool:
		return v, true
	default:
		panic(fmt.Sprintf("unable to convert type %T to a boolean value", v))
	}
}

// Times will construct an array of times with the given time using either a
// string or an integer. If an integer is used, then it is in seconds.
//
// If strings and integers are mixed, the integers will be treates as offsets
// from the last string time that was used.
func Times(k string, v ...interface{}) Column {
	var offset int64
	c := timeColumn{
		column: column{k: k},
		v:      make([]int64, len(v)),
	}
	for i, iv := range v {
		val, abs, ok := mustTimeValue(iv, offset, time.Second)
		if !ok {
			if c.valid == nil {
				c.valid = make([]bool, len(v))
				for i := range c.valid {
					c.valid[i] = true
				}
			}
			c.valid[i] = false
		}
		if abs {
			offset = val
		}
		c.v[i] = val
	}
	return c
}

type timeColumn struct {
	column
	v []int64
}

func (s timeColumn) Make(n int) array.Interface {
	b := array.NewInt64Builder(memory.DefaultAllocator)
	b.Resize(len(s.v))
	b.AppendValues(s.v, s.valid)
	return b.NewArray()
}

func (s timeColumn) Type() flux.ColType     { return flux.TTime }
func (s timeColumn) Len() int               { return len(s.v) }
func (s timeColumn) KeyValue() values.Value { return values.InvalidValue }

func (s timeColumn) Build(template *[]Column) []flux.Table {
	*template = append(*template, s)
	return nil
}

// mustTimeValue will convert the interface into a time value.
// This must either be an int-like value or a string that can be
// parsed as a time in RFC3339 format.
//
// This will panic otherwise.
func mustTimeValue(v interface{}, offset int64, unit time.Duration) (t int64, abs, ok bool) {
	if v == nil {
		return 0, false, false
	}

	switch v := v.(type) {
	case int:
		return offset + int64(v)*int64(unit), false, true
	case int64:
		return offset + v*int64(unit), false, true
	case string:
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			if t, err = time.Parse(time.RFC3339Nano, v); err != nil {
				panic(err)
			}
		}
		return t.UnixNano(), true, true
	default:
		panic(fmt.Sprintf("unable to convert type %T to a time value", v))
	}
}

// TableBuilder is used to construct a set of Tables.
type TableBuilder interface {
	// Build will construct a set of tables using the
	// template as input.
	//
	// The template is a pointer as a builder is allowed
	// to modify the template. For implementors, the
	// template pointer must be non-nil.
	Build(template *[]Column) []flux.Table
}

// TableGroup will construct a group of Tables
// that have common values. It includes any TableBuilder
// values.
type TableGroup []TableBuilder

func (t TableGroup) Do(f func(flux.Table) error) error {
	// Use an empty template.
	var template []Column
	tables := t.Build(&template)
	return table.Iterator(tables).Do(f)
}

// Build will construct Tables using the given template.
func (t TableGroup) Build(template *[]Column) []flux.Table {
	// Copy over the template.
	gtemplate := make([]Column, len(*template))
	copy(gtemplate, *template)

	var tables []flux.Table
	for _, tb := range t {
		tables = append(tables, tb.Build(&gtemplate)...)
	}
	return tables
}

// TableList will produce a Table using the template and
// each of the table builders.
//
// Changes to the template are not shared between each of the
// entries. If the TableBuilder does not produce tables,
// this will force a single Table to be created.
type TableList []TableBuilder

func (t TableList) Build(template *[]Column) []flux.Table {
	var tables []flux.Table
	for _, tb := range t {
		// Copy over the group template for each of these.
		gtemplate := make([]Column, len(*template), len(*template)+1)
		copy(gtemplate, *template)

		if ntables := tb.Build(&gtemplate); len(ntables) > 0 {
			tables = append(tables, ntables...)
		} else {
			tables = append(tables, Table(gtemplate).Table())
		}
	}
	return tables
}

// StringKeys creates a TableList with the given key values.
func StringKeys(k string, v ...interface{}) TableList {
	list := make(TableList, len(v))
	for i := range v {
		list[i] = StringKey(k, v[i])
	}
	return list
}

// TableMatrix will produce a set of Tables by producing the
// cross product of each of the TableBuilders with each other.
type TableMatrix []TableList

func (t TableMatrix) Build(template *[]Column) []flux.Table {
	if len(t) == 0 {
		return nil
	} else if len(t) == 1 {
		return t[0].Build(template)
	}

	// Split the TableList into their own distinct TableGroups
	// so we can produce a cross product of groups.
	builders := make([]TableGroup, len(t[0]))
	for i, b := range t[0] {
		builders[i] = append(builders[i], b)
	}

	for i := 1; i < len(t); i++ {
		product := make([]TableGroup, 0, len(builders)*len(t[i]))
		for _, bs := range t[i] {
			a := make([]TableGroup, len(builders))
			copy(a, builders)
			for j := range a {
				a[j] = append(a[j], bs)
			}
			product = append(product, a...)
		}
		builders = product
	}

	var tables []flux.Table
	for _, b := range builders {
		tables = append(tables, b.Build(template)...)
	}
	return tables
}
