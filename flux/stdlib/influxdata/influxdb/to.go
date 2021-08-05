package influxdb

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/compiler"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/interpreter"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/runtime"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/stdlib/influxdata/influxdb"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxql"
)

const (
	ToKind                     = "influx1x/toKind"
	DefaultBufferSize          = 5000
	DefaultFieldColLabel       = "_field"
	DefaultMeasurementColLabel = "_measurement"
)

// ToOpSpec is the flux.OperationSpec for the `to` flux function.
type ToOpSpec struct {
	Bucket            string                       `json:"bucket"`
	TimeColumn        string                       `json:"timeColumn"`
	MeasurementColumn string                       `json:"measurementColumn"`
	TagColumns        []string                     `json:"tagColumns"`
	FieldFn           interpreter.ResolvedFunction `json:"fieldFn"`
}

func init() {
	toSignature := runtime.MustLookupBuiltinType("influxdata/influxdb", influxdb.ToKind)
	runtime.ReplacePackageValue("influxdata/influxdb", "to", flux.MustValue(flux.FunctionValueWithSideEffect(ToKind, createToOpSpec, toSignature)))
	flux.RegisterOpSpec(ToKind, func() flux.OperationSpec { return &ToOpSpec{} })
	plan.RegisterProcedureSpecWithSideEffect(ToKind, newToProcedure, ToKind)
	execute.RegisterTransformation(ToKind, createToTransformation)
}

var unsupportedToArgs = []string{"bucketID", "orgID", "host", "url", "brokers"}

func (o *ToOpSpec) ReadArgs(args flux.Arguments) error {
	var err error
	var ok bool

	for _, arg := range unsupportedToArgs {
		if _, ok = args.Get(arg); ok {
			return &flux.Error{
				Code: codes.Invalid,
				Msg:  fmt.Sprintf("argument %s for 'to' not supported in 1.x flux engine", arg),
			}
		}
	}

	if o.Bucket, err = args.GetRequiredString("bucket"); err != nil {
		return err
	}
	if o.TimeColumn, ok, _ = args.GetString("timeColumn"); !ok {
		o.TimeColumn = execute.DefaultTimeColLabel
	}

	if o.MeasurementColumn, ok, _ = args.GetString("measurementColumn"); !ok {
		o.MeasurementColumn = DefaultMeasurementColLabel
	}

	if tags, ok, _ := args.GetArray("tagColumns", semantic.String); ok {
		o.TagColumns = make([]string, tags.Len())
		tags.Sort(func(i, j values.Value) bool {
			return i.Str() < j.Str()
		})
		tags.Range(func(i int, v values.Value) {
			o.TagColumns[i] = v.Str()
		})
	}

	if fieldFn, ok, _ := args.GetFunction("fieldFn"); ok {
		if o.FieldFn, err = interpreter.ResolveFunction(fieldFn); err != nil {
			return err
		}
	}
	return nil
}

func createToOpSpec(args flux.Arguments, a *flux.Administration) (flux.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}
	s := &ToOpSpec{}
	if err := s.ReadArgs(args); err != nil {
		return nil, err
	}
	return s, nil
}

func (ToOpSpec) Kind() flux.OperationKind {
	return ToKind
}

// ToProcedureSpec is the procedure spec for the `to` flux function.
type ToProcedureSpec struct {
	plan.DefaultCost
	Spec *ToOpSpec
}

// Kind returns the kind for the procedure spec for the `to` flux function.
func (o *ToProcedureSpec) Kind() plan.ProcedureKind {
	return ToKind
}

// Copy clones the procedure spec for `to` flux function.
func (o *ToProcedureSpec) Copy() plan.ProcedureSpec {
	s := o.Spec
	res := &ToProcedureSpec{
		Spec: &ToOpSpec{
			Bucket:            s.Bucket,
			TimeColumn:        s.TimeColumn,
			MeasurementColumn: s.MeasurementColumn,
			TagColumns:        append([]string(nil), s.TagColumns...),
			FieldFn:           s.FieldFn.Copy(),
		},
	}
	return res
}

func newToProcedure(qs flux.OperationSpec, a plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*ToOpSpec)
	if !ok && spec != nil {
		return nil, &flux.Error{
			Code: codes.Internal,
			Msg:  fmt.Sprintf("invalid spec type %T", qs),
		}
	}
	return &ToProcedureSpec{Spec: spec}, nil
}

func createToTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*ToProcedureSpec)
	if !ok {
		return nil, nil, &flux.Error{
			Code: codes.Internal,
			Msg:  fmt.Sprintf("invalid spec type %T", spec),
		}
	}
	cache := execute.NewTableBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	deps := GetStorageDependencies(a.Context())
	if deps == (StorageDependencies{}) {
		return nil, nil, &flux.Error{
			Code: codes.Unimplemented,
			Msg:  "cannot return storage dependencies; storage dependencies are unimplemented",
		}
	}
	t, err := NewToTransformation(a.Context(), d, cache, s, deps)
	if err != nil {
		return nil, nil, err
	}
	return t, d, nil
}

type ToTransformation struct {
	execute.ExecutionNode
	Ctx         context.Context
	DB          string
	RP          string
	spec        *ToProcedureSpec
	isTagColumn func(column flux.ColMeta) bool
	fn          *execute.RowMapFn
	d           execute.Dataset
	cache       execute.TableBuilderCache
	deps        StorageDependencies
	buf         *coordinator.BufferedPointsWriter
}

func NewToTransformation(ctx context.Context, d execute.Dataset, cache execute.TableBuilderCache, toSpec *ToProcedureSpec, deps StorageDependencies) (*ToTransformation, error) {
	spec := toSpec.Spec
	var db, rp string
	db, rp, err := lookupDatabase(ctx, spec.Bucket, deps, influxql.WritePrivilege)
	if err != nil {
		return nil, err
	}
	var fn *execute.RowMapFn
	if spec.FieldFn.Fn != nil {
		fn = execute.NewRowMapFn(spec.FieldFn.Fn, compiler.ToScope(spec.FieldFn.Scope))
	}
	var isTagColumn func(column flux.ColMeta) bool
	if toSpec.Spec.TagColumns == nil {
		// If no tag columns are specified, by default we exclude
		// _field, _value and _measurement from being tag columns.
		excludeColumns := map[string]bool{
			execute.DefaultValueColLabel: true,
			DefaultFieldColLabel:         true,
			DefaultMeasurementColLabel:   true,
		}

		// Also exclude the overridden measurement column
		excludeColumns[toSpec.Spec.MeasurementColumn] = true

		// If a field function is specified then we exclude any column that
		// is referenced in the function expression from being a tag column.
		if toSpec.Spec.FieldFn.Fn != nil {
			recordParam := toSpec.Spec.FieldFn.Fn.Parameters.List[0].Key.Name
			exprNode := toSpec.Spec.FieldFn.Fn
			colVisitor := newFieldFunctionVisitor(recordParam)

			// Walk the field function expression and record which columns
			// are referenced. None of these columns will be used as tag columns.
			semantic.Walk(colVisitor, exprNode)
			for k, v := range colVisitor.captured {
				excludeColumns[k] = v
			}
		}
		isTagColumn = func(column flux.ColMeta) bool {
			return column.Type == flux.TString && !excludeColumns[column.Label]
		}
	} else {
		// Simply check if column is in the sorted list of tag values
		isTagColumn = func(column flux.ColMeta) bool {
			i := sort.SearchStrings(toSpec.Spec.TagColumns, column.Label)
			if i >= len(toSpec.Spec.TagColumns) {
				return false
			}
			return toSpec.Spec.TagColumns[i] == column.Label
		}
	}
	return &ToTransformation{
		Ctx:         ctx,
		DB:          db,
		RP:          rp,
		spec:        toSpec,
		isTagColumn: isTagColumn,
		fn:          fn,
		d:           d,
		cache:       cache,
		deps:        deps,
		buf:         coordinator.NewBufferedPointsWriter(deps.PointsWriter, db, rp, DefaultBufferSize),
	}, nil
}

func (t *ToTransformation) RetractTable(id execute.DatasetID, key flux.GroupKey) error {
	return t.d.RetractTable(key)
}

// Process does the actual work for the ToTransformation.
func (t *ToTransformation) Process(id execute.DatasetID, tbl flux.Table) error {
	columns := tbl.Cols()
	isTag := make([]bool, len(columns))
	numTags := 0
	for i, col := range columns {
		isTag[i] = t.isTagColumn(col)
		if isTag[i] {
			numTags++
		}
	}

	timeColLabel := t.spec.Spec.TimeColumn
	timeColIdx := execute.ColIdx(timeColLabel, columns)
	if timeColIdx < 0 {
		return &flux.Error{
			Code: codes.Invalid,
			Msg:  "no time column detected",
		}
	}
	if columns[timeColIdx].Type != flux.TTime {
		return &flux.Error{
			Code: codes.Invalid,
			Msg:  fmt.Sprintf("column %s of type %s is not of type %s", timeColLabel, columns[timeColIdx].Type, flux.TTime),
		}
	}

	// prepare field function
	var fn *execute.RowMapPreparedFn
	if t.fn != nil {
		var err error
		if fn, err = t.fn.Prepare(columns); err != nil {
			return err
		}
	}

	builder, new := t.cache.TableBuilder(tbl.Key())
	if new {
		if err := execute.AddTableCols(tbl, builder); err != nil {
			return err
		}
	}

	return tbl.Do(func(er flux.ColReader) error {
		kv := make([][]byte, 0, numTags*2)
		var tags models.Tags
		var points models.Points
	outer:
		for i := 0; i < er.Len(); i++ {
			measurementName := ""
			fields := make(models.Fields)
			var pointTime time.Time
			kv = kv[:0]

			// get the non-field values
			for j, col := range er.Cols() {
				switch {
				case col.Label == t.spec.Spec.MeasurementColumn:
					measurementName = string(er.Strings(j).Value(i))
				case col.Label == timeColLabel:
					valueTime := execute.ValueForRow(er, i, j)
					if valueTime.IsNull() {
						// skip rows with null timestamp
						continue outer
					}
					pointTime = valueTime.Time().Time()
				case isTag[j]:
					if col.Type != flux.TString {
						return &flux.Error{
							Code: codes.Invalid,
							Msg:  "Invalid type for tag column",
						}
					}
					kv = append(kv, []byte(col.Label), []byte(er.Strings(j).Value(i)))
				}
			}

			// validate the point
			if pointTime.IsZero() {
				return &flux.Error{
					Code: codes.Invalid,
					Msg:  "timestamp missing from block",
				}
			}
			if measurementName == "" {
				return &flux.Error{
					Code: codes.Invalid,
					Msg:  fmt.Sprintf("no column with label %s exists", t.spec.Spec.MeasurementColumn),
				}
			}

			var fieldValues values.Object
			var err error
			if fn == nil {
				if fieldValues, err = defaultFieldMapping(er, i); err != nil {
					return err
				}
			} else if fieldValues, err = fn.Eval(t.Ctx, i, er); err != nil {
				return err
			}

			fieldValues.Range(func(k string, v values.Value) {
				if v.IsNull() {
					fields[k] = nil
					return
				}
				switch v.Type().Nature() {
				case semantic.Float:
					fields[k] = v.Float()
				case semantic.Int:
					fields[k] = v.Int()
				case semantic.UInt:
					fields[k] = v.UInt()
				case semantic.String:
					fields[k] = v.Str()
				case semantic.Time:
					fields[k] = v.Time()
				case semantic.Bool:
					fields[k] = v.Bool()
				}
			})

			tags, _ = models.NewTagsKeyValues(tags, kv...)
			pt, err := models.NewPoint(measurementName, tags, fields, pointTime)
			if err != nil {
				return err
			}
			points = append(points, pt)
			if err := execute.AppendRecord(i, er, builder); err != nil {
				return err
			}
		}
		return t.buf.WritePointsInto(&coordinator.IntoWriteRequest{
			Database:        t.DB,
			RetentionPolicy: t.RP,
			Points:          points,
		})
	})
}

// fieldFunctionVisitor implements semantic.Visitor.
// fieldFunctionVisitor is used to walk the the field function expression
// of the `to` operation and to record all referenced columns. This visitor
// is only used when no tag columns are provided as input to the `to` func.
type fieldFunctionVisitor struct {
	visited  map[semantic.Node]bool
	captured map[string]bool
	rowParam string
}

func newFieldFunctionVisitor(rowParam string) *fieldFunctionVisitor {
	return &fieldFunctionVisitor{
		visited:  make(map[semantic.Node]bool),
		captured: make(map[string]bool),
		rowParam: rowParam,
	}
}

// A field function is of the form `(r) => { Function Body }`, and it returns an object
// mapping field keys to values for each row r of the input. Visit records every column
// that is referenced in `Function Body`. These columns are either directly or indirectly
// used as value columns and as such need to be recorded so as not to be used as tag columns.
func (v *fieldFunctionVisitor) Visit(node semantic.Node) semantic.Visitor {
	if v.visited[node] {
		return v
	}
	if member, ok := node.(*semantic.MemberExpression); ok {
		if obj, ok := member.Object.(*semantic.IdentifierExpression); ok {
			if obj.Name == v.rowParam {
				v.captured[member.Property] = true
			}
		}
	}
	v.visited[node] = true
	return v
}

func (v *fieldFunctionVisitor) Done(node semantic.Node) {}

// UpdateWatermark updates the watermark for the transformation for the `to` flux function.
func (t *ToTransformation) UpdateWatermark(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateWatermark(pt)
}

// UpdateProcessingTime updates the processing time for the transformation for the `to` flux function.
func (t *ToTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateProcessingTime(pt)
}

// Finish is called after the `to` flux function's transformation is done processing.
func (t *ToTransformation) Finish(id execute.DatasetID, err error) {
	if err == nil {
		err = t.buf.Flush()
	}
	t.d.Finish(err)
}

func defaultFieldMapping(er flux.ColReader, row int) (values.Object, error) {
	fieldColumnIdx := execute.ColIdx("_field", er.Cols())
	valueColumnIdx := execute.ColIdx(execute.DefaultValueColLabel, er.Cols())

	if fieldColumnIdx < 0 {
		return nil, &flux.Error{
			Code: codes.Invalid,
			Msg:  "table has no _field column",
		}
	}

	if valueColumnIdx < 0 {
		return nil, &flux.Error{
			Code: codes.Invalid,
			Msg:  "table has no _value column",
		}
	}

	value := execute.ValueForRow(er, row, valueColumnIdx)
	field := execute.ValueForRow(er, row, fieldColumnIdx)
	props := []semantic.PropertyType{
		{
			Key:   []byte(field.Str()),
			Value: value.Type(),
		},
	}
	fieldValueMapping := values.NewObject(semantic.NewObjectType(props))
	fieldValueMapping.Set(field.Str(), value)
	return fieldValueMapping, nil
}
