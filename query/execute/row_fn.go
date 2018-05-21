package execute

import (
	"fmt"
	"regexp"

	"github.com/influxdata/platform/query/compiler"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/semantic"
	"github.com/influxdata/platform/query/values"
	"github.com/pkg/errors"
)

type rowFn struct {
	fn               *semantic.FunctionExpression
	compilationCache *compiler.CompilationCache
	scope            compiler.Scope

	preparedFn compiler.Func

	recordName string
	record     *Record

	recordCols map[string]int
	references []string
}

func newRowFn(fn *semantic.FunctionExpression) (rowFn, error) {
	if len(fn.Params) != 1 {
		return rowFn{}, fmt.Errorf("function should only have a single parameter, got %d", len(fn.Params))
	}
	scope, decls := query.BuiltIns()
	return rowFn{
		compilationCache: compiler.NewCompilationCache(fn, scope, decls),
		scope:            make(compiler.Scope, 1),
		recordName:       fn.Params[0].Key.Name,
		references:       findColReferences(fn),
		recordCols:       make(map[string]int),
	}, nil
}

func (f *rowFn) prepare(cols []ColMeta) error {
	// Prepare types and recordCols
	propertyTypes := make(map[string]semantic.Type, len(f.references))
	for _, r := range f.references {
		found := false
		for j, c := range cols {
			if r == c.Label {
				f.recordCols[r] = j
				found = true
				propertyTypes[r] = ConvertToKind(c.Type)
				break
			}
		}
		if !found {
			return fmt.Errorf("function references unknown column %q", r)
		}
	}
	f.record = NewRecord(semantic.NewObjectType(propertyTypes))
	// Compile fn for given types
	fn, err := f.compilationCache.Compile(map[string]semantic.Type{
		f.recordName: f.record.Type(),
	})
	if err != nil {
		return err
	}
	f.preparedFn = fn
	return nil
}

func ConvertToKind(t DataType) semantic.Kind {
	// TODO make this an array lookup.
	switch t {
	case TInvalid:
		return semantic.Invalid
	case TBool:
		return semantic.Bool
	case TInt:
		return semantic.Int
	case TUInt:
		return semantic.UInt
	case TFloat:
		return semantic.Float
	case TString:
		return semantic.String
	case TTime:
		return semantic.Time
	default:
		return semantic.Invalid
	}
}

func ConvertFromKind(k semantic.Kind) DataType {
	// TODO make this an array lookup.
	switch k {
	case semantic.Invalid:
		return TInvalid
	case semantic.Bool:
		return TBool
	case semantic.Int:
		return TInt
	case semantic.UInt:
		return TUInt
	case semantic.Float:
		return TFloat
	case semantic.String:
		return TString
	case semantic.Time:
		return TTime
	default:
		return TInvalid
	}
}

func (f *rowFn) eval(row int, cr ColReader) (values.Value, error) {
	for _, r := range f.references {
		f.record.Set(r, ValueForRow(row, f.recordCols[r], cr))
	}
	f.scope[f.recordName] = f.record
	return f.preparedFn.Eval(f.scope)
}

type RowPredicateFn struct {
	rowFn
}

func NewRowPredicateFn(fn *semantic.FunctionExpression) (*RowPredicateFn, error) {
	r, err := newRowFn(fn)
	if err != nil {
		return nil, err
	}
	return &RowPredicateFn{
		rowFn: r,
	}, nil
}

func (f *RowPredicateFn) Prepare(cols []ColMeta) error {
	err := f.rowFn.prepare(cols)
	if err != nil {
		return err
	}
	if f.preparedFn.Type() != semantic.Bool {
		return errors.New("row predicate function does not evaluate to a boolean")
	}
	return nil
}

func (f *RowPredicateFn) Eval(row int, cr ColReader) (bool, error) {
	v, err := f.rowFn.eval(row, cr)
	if err != nil {
		return false, err
	}
	return v.Bool(), nil
}

type RowMapFn struct {
	rowFn

	isWrap  bool
	wrapObj *Record
}

func NewRowMapFn(fn *semantic.FunctionExpression) (*RowMapFn, error) {
	r, err := newRowFn(fn)
	if err != nil {
		return nil, err
	}
	return &RowMapFn{
		rowFn: r,
	}, nil
}

func (f *RowMapFn) Prepare(cols []ColMeta) error {
	err := f.rowFn.prepare(cols)
	if err != nil {
		return err
	}
	k := f.preparedFn.Type().Kind()
	f.isWrap = k != semantic.Object
	if f.isWrap {
		f.wrapObj = NewRecord(semantic.NewObjectType(map[string]semantic.Type{
			DefaultValueColLabel: f.preparedFn.Type(),
		}))
	}
	return nil
}

func (f *RowMapFn) Type() semantic.Type {
	if f.isWrap {
		return f.wrapObj.Type()
	}
	return f.preparedFn.Type()
}

func (f *RowMapFn) Eval(row int, cr ColReader) (values.Object, error) {
	v, err := f.rowFn.eval(row, cr)
	if err != nil {
		return nil, err
	}
	if f.isWrap {
		f.wrapObj.Set(DefaultValueColLabel, v)
		return f.wrapObj, nil
	}
	return v.Object(), nil
}

func ValueForRow(i, j int, cr ColReader) values.Value {
	t := cr.Cols()[j].Type
	switch t {
	case TString:
		return values.NewStringValue(cr.Strings(j)[i])
	case TInt:
		return values.NewIntValue(cr.Ints(j)[i])
	case TUInt:
		return values.NewUIntValue(cr.UInts(j)[i])
	case TFloat:
		return values.NewFloatValue(cr.Floats(j)[i])
	case TBool:
		return values.NewBoolValue(cr.Bools(j)[i])
	case TTime:
		return values.NewTimeValue(cr.Times(j)[i])
	default:
		PanicUnknownType(t)
		return nil
	}
}

func AppendValue(builder BlockBuilder, j int, v values.Value) {
	switch k := v.Type().Kind(); k {
	case semantic.Bool:
		builder.AppendBool(j, v.Bool())
	case semantic.Int:
		builder.AppendInt(j, v.Int())
	case semantic.UInt:
		builder.AppendUInt(j, v.UInt())
	case semantic.Float:
		builder.AppendFloat(j, v.Float())
	case semantic.String:
		builder.AppendString(j, v.Str())
	case semantic.Time:
		builder.AppendTime(j, v.Time())
	default:
		PanicUnknownType(ConvertFromKind(k))
	}
}

func findColReferences(fn *semantic.FunctionExpression) []string {
	v := &colReferenceVisitor{
		recordName: fn.Params[0].Key.Name,
	}
	semantic.Walk(v, fn)
	return v.refs
}

type colReferenceVisitor struct {
	recordName string
	refs       []string
}

func (c *colReferenceVisitor) Visit(node semantic.Node) semantic.Visitor {
	if me, ok := node.(*semantic.MemberExpression); ok {
		if obj, ok := me.Object.(*semantic.IdentifierExpression); ok && obj.Name == c.recordName {
			c.refs = append(c.refs, me.Property)
		}
	}
	return c
}

func (c *colReferenceVisitor) Done() {}

type Record struct {
	t      semantic.Type
	values map[string]values.Value
}

func NewRecord(t semantic.Type) *Record {
	return &Record{
		t:      t,
		values: make(map[string]values.Value),
	}
}
func (r *Record) Type() semantic.Type {
	return r.t
}

func (r *Record) Str() string {
	panic(values.UnexpectedKind(semantic.Object, semantic.String))
}
func (r *Record) Int() int64 {
	panic(values.UnexpectedKind(semantic.Object, semantic.Int))
}
func (r *Record) UInt() uint64 {
	panic(values.UnexpectedKind(semantic.Object, semantic.UInt))
}
func (r *Record) Float() float64 {
	panic(values.UnexpectedKind(semantic.Object, semantic.Float))
}
func (r *Record) Bool() bool {
	panic(values.UnexpectedKind(semantic.Object, semantic.Bool))
}
func (r *Record) Time() values.Time {
	panic(values.UnexpectedKind(semantic.Object, semantic.Time))
}
func (r *Record) Duration() values.Duration {
	panic(values.UnexpectedKind(semantic.Object, semantic.Duration))
}
func (r *Record) Regexp() *regexp.Regexp {
	panic(values.UnexpectedKind(semantic.Object, semantic.Regexp))
}
func (r *Record) Array() values.Array {
	panic(values.UnexpectedKind(semantic.Object, semantic.Array))
}
func (r *Record) Object() values.Object {
	return r
}
func (r *Record) Function() values.Function {
	panic(values.UnexpectedKind(semantic.Object, semantic.Function))
}

func (r *Record) Set(name string, v values.Value) {
	r.values[name] = v
}
func (r *Record) Get(name string) (values.Value, bool) {
	v, ok := r.values[name]
	return v, ok
}
func (r *Record) Len() int {
	return len(r.values)
}

func (r *Record) Range(f func(name string, v values.Value)) {
	for k, v := range r.values {
		f(k, v)
	}
}
