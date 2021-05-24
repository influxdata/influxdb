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
	"github.com/influxdata/flux/stdlib/kafka"
	"github.com/influxdata/flux/values"
	platform "github.com/influxdata/influxdb/v2"
	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/storage"
)

const (
	// ToKind is the kind for the `to` flux function
	ToKind = "influx2x/toKind"

	// TODO(jlapacik) remove this once we have execute.DefaultFieldColLabel
	defaultFieldColLabel       = "_field"
	DefaultMeasurementColLabel = "_measurement"
	DefaultBufferSize          = 1 << 14

	toOp = "influxdata/influxdb/to"
)

// ToOpSpec is the flux.OperationSpec for the `to` flux function.
type ToOpSpec struct {
	Bucket            string                       `json:"bucket"`
	BucketID          string                       `json:"bucketID"`
	Org               string                       `json:"org"`
	OrgID             string                       `json:"orgID"`
	Host              string                       `json:"host"`
	Token             string                       `json:"token"`
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

// argsReader is an interface for OperationSpec that have the same method to read args.
type argsReader interface {
	flux.OperationSpec
	ReadArgs(args flux.Arguments) error
}

// ReadArgs reads the args from flux.Arguments into the op spec
func (o *ToOpSpec) ReadArgs(args flux.Arguments) error {
	var err error
	var ok bool

	if o.Bucket, ok, _ = args.GetString("bucket"); !ok {
		if o.BucketID, err = args.GetRequiredString("bucketID"); err != nil {
			return err
		}
	} else if o.BucketID, ok, _ = args.GetString("bucketID"); ok {
		return &flux.Error{
			Code: codes.Invalid,
			Msg:  "cannot provide both `bucket` and `bucketID` parameters to the `to` function",
		}
	}

	if o.Org, ok, _ = args.GetString("org"); !ok {
		if o.OrgID, _, err = args.GetString("orgID"); err != nil {
			return err
		}
	} else if o.OrgID, ok, _ = args.GetString("orgID"); ok {
		return &flux.Error{
			Code: codes.Invalid,
			Msg:  "cannot provide both `org` and `orgID` parameters to the `to` function",
		}
	}

	if o.Host, ok, _ = args.GetString("host"); ok {
		if o.Token, err = args.GetRequiredString("token"); err != nil {
			return err
		}
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

	return err
}

func createToOpSpec(args flux.Arguments, a *flux.Administration) (flux.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}

	_, httpOK, err := args.GetString("url")
	if err != nil {
		return nil, err
	}

	_, kafkaOK, err := args.GetString("brokers")
	if err != nil {
		return nil, err
	}

	var s argsReader

	switch {
	case httpOK && kafkaOK:
		return nil, &flux.Error{
			Code: codes.Invalid,
			Msg:  "specify at most one of url, brokers in the same `to` function",
		}
	case kafkaOK:
		s = &kafka.ToKafkaOpSpec{}
	default:
		s = &ToOpSpec{}
	}
	if err := s.ReadArgs(args); err != nil {
		return nil, err
	}
	return s, nil
}

// Kind returns the kind for the ToOpSpec function.
func (ToOpSpec) Kind() flux.OperationKind {
	return ToKind
}

// BucketsAccessed returns the buckets accessed by the spec.
func (o *ToOpSpec) BucketsAccessed(orgID *platform2.ID) (readBuckets, writeBuckets []platform.BucketFilter) {
	bf := platform.BucketFilter{}
	if o.Bucket != "" {
		bf.Name = &o.Bucket
	}
	if o.BucketID != "" {
		id, err := platform2.IDFromString(o.BucketID)
		if err == nil {
			bf.ID = id
		}
	}
	if o.Org != "" {
		bf.Org = &o.Org
	}
	if o.OrgID != "" {
		id, err := platform2.IDFromString(o.OrgID)
		if err == nil {
			bf.OrganizationID = id
		}
	}
	writeBuckets = append(writeBuckets, bf)
	return readBuckets, writeBuckets
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
			BucketID:          s.BucketID,
			Org:               s.Org,
			OrgID:             s.OrgID,
			Host:              s.Host,
			Token:             s.Token,
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
	toDeps := deps.ToDeps
	t, err := NewToTransformation(a.Context(), d, cache, s, toDeps)
	if err != nil {
		return nil, nil, err
	}
	return t, d, nil
}

// ToTransformation is the transformation for the `to` flux function.
type ToTransformation struct {
	execute.ExecutionNode
	Ctx                context.Context
	OrgID              platform2.ID
	BucketID           platform2.ID
	d                  execute.Dataset
	fn                 *execute.RowMapFn
	cache              execute.TableBuilderCache
	spec               *ToProcedureSpec
	implicitTagColumns bool
	deps               ToDependencies
	buf                *storage.BufferedPointsWriter
}

// RetractTable retracts the table for the transformation for the `to` flux function.
func (t *ToTransformation) RetractTable(id execute.DatasetID, key flux.GroupKey) error {
	return t.d.RetractTable(key)
}

// NewToTransformation returns a new *ToTransformation with the appropriate fields set.
func NewToTransformation(ctx context.Context, d execute.Dataset, cache execute.TableBuilderCache, toSpec *ToProcedureSpec, deps ToDependencies) (x *ToTransformation, err error) {
	var fn *execute.RowMapFn
	spec := toSpec.Spec
	var bucketID, orgID *platform2.ID
	if spec.FieldFn.Fn != nil {
		fn = execute.NewRowMapFn(spec.FieldFn.Fn, compiler.ToScope(spec.FieldFn.Scope))
	}
	// Get organization ID
	if spec.Org != "" {
		oID, ok := deps.OrganizationLookup.Lookup(ctx, spec.Org)
		if !ok {
			return nil, &flux.Error{
				Code: codes.NotFound,
				Msg:  fmt.Sprintf("failed to look up organization %q", spec.Org),
			}
		}
		orgID = &oID
	} else if spec.OrgID != "" {
		if orgID, err = platform2.IDFromString(spec.OrgID); err != nil {
			return nil, err
		}
	} else {
		// No org or orgID provided as an arg, use the orgID from the context
		req := query.RequestFromContext(ctx)
		if req == nil {
			return nil, &errors.Error{
				Code: errors.EInternal,
				Msg:  "missing request on context",
				Op:   toOp,
			}
		}
		orgID = &req.OrganizationID
	}

	// Get bucket ID
	if spec.Bucket != "" {
		bID, ok := deps.BucketLookup.Lookup(ctx, *orgID, spec.Bucket)
		if !ok {
			return nil, &flux.Error{
				Code: codes.NotFound,
				Msg:  fmt.Sprintf("failed to look up bucket %q in org %q", spec.Bucket, spec.Org),
			}
		}
		bucketID = &bID
	} else if bucketID, err = platform2.IDFromString(spec.BucketID); err != nil {
		return nil, &flux.Error{
			Code: codes.Invalid,
			Msg:  "invalid bucket id",
			Err:  err,
		}
	}
	if orgID == nil || bucketID == nil {
		return nil, &flux.Error{
			Code: codes.Unknown,
			Msg:  "You must specify org and bucket",
		}
	}
	return &ToTransformation{
		Ctx:                ctx,
		OrgID:              *orgID,
		BucketID:           *bucketID,
		d:                  d,
		fn:                 fn,
		cache:              cache,
		spec:               toSpec,
		implicitTagColumns: spec.TagColumns == nil,
		deps:               deps,
		buf:                storage.NewBufferedPointsWriter(*orgID, *bucketID, DefaultBufferSize, deps.PointsWriter),
	}, nil
}

// Process does the actual work for the ToTransformation.
func (t *ToTransformation) Process(id execute.DatasetID, tbl flux.Table) error {
	if t.implicitTagColumns {

		// If no tag columns are specified, by default we exclude
		// _field, _value and _measurement from being tag columns.
		excludeColumns := map[string]bool{
			execute.DefaultValueColLabel: true,
			defaultFieldColLabel:         true,
			DefaultMeasurementColLabel:   true,
		}

		// If a field function is specified then we exclude any column that
		// is referenced in the function expression from being a tag column.
		if t.spec.Spec.FieldFn.Fn != nil {
			recordParam := t.spec.Spec.FieldFn.Fn.Parameters.List[0].Key.Name
			exprNode := t.spec.Spec.FieldFn.Fn
			colVisitor := newFieldFunctionVisitor(recordParam, tbl.Cols())

			// Walk the field function expression and record which columns
			// are referenced. None of these columns will be used as tag columns.
			semantic.Walk(colVisitor, exprNode)
			for k, v := range colVisitor.captured {
				excludeColumns[k] = v
			}
		}

		addTagsFromTable(t.spec.Spec, tbl, excludeColumns)
	}
	return writeTable(t.Ctx, t, tbl)
}

// fieldFunctionVisitor implements semantic.Visitor.
// fieldFunctionVisitor is used to walk the the field function expression
// of the `to` operation and to record all referenced columns. This visitor
// is only used when no tag columns are provided as input to the `to` func.
type fieldFunctionVisitor struct {
	columns  map[string]bool
	visited  map[semantic.Node]bool
	captured map[string]bool
	rowParam string
}

func newFieldFunctionVisitor(rowParam string, cols []flux.ColMeta) *fieldFunctionVisitor {
	columns := make(map[string]bool, len(cols))
	for _, col := range cols {
		columns[col.Label] = true
	}
	return &fieldFunctionVisitor{
		columns:  columns,
		visited:  make(map[semantic.Node]bool, len(cols)),
		captured: make(map[string]bool, len(cols)),
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
			if obj.Name == v.rowParam && v.columns[member.Property] {
				v.captured[member.Property] = true
			}
		}
	}
	v.visited[node] = true
	return v
}

func (v *fieldFunctionVisitor) Done(node semantic.Node) {}

func addTagsFromTable(spec *ToOpSpec, table flux.Table, exclude map[string]bool) {
	if cap(spec.TagColumns) < len(table.Cols()) {
		spec.TagColumns = make([]string, 0, len(table.Cols()))
	} else {
		spec.TagColumns = spec.TagColumns[:0]
	}
	for _, column := range table.Cols() {
		if column.Type == flux.TString && !exclude[column.Label] {
			spec.TagColumns = append(spec.TagColumns, column.Label)
		}
	}
	sort.Strings(spec.TagColumns)
}

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
		err = t.buf.Flush(t.Ctx)
	}
	t.d.Finish(err)
}

// ToDependencies contains the dependencies for executing the `to` function.
type ToDependencies struct {
	BucketLookup       BucketLookup
	OrganizationLookup OrganizationLookup
	PointsWriter       storage.PointsWriter
}

// Validate returns an error if any required field is unset.
func (d ToDependencies) Validate() error {
	if d.BucketLookup == nil {
		return &errors.Error{
			Code: errors.EInternal,
			Msg:  "missing bucket lookup dependency",
			Op:   toOp,
		}
	}
	if d.OrganizationLookup == nil {
		return &errors.Error{
			Code: errors.EInternal,
			Msg:  "missing organization lookup dependency",
			Op:   toOp,
		}
	}
	if d.PointsWriter == nil {
		return &errors.Error{
			Code: errors.EInternal,
			Msg:  "missing points writer dependency",
			Op:   toOp,
		}
	}
	return nil
}

type Stats struct {
	NRows    int
	Latest   time.Time
	Earliest time.Time
	NFields  int
	NTags    int
}

func (s Stats) Update(o Stats) {
	s.NRows += o.NRows
	if s.Latest.IsZero() || o.Latest.Unix() > s.Latest.Unix() {
		s.Latest = o.Latest
	}

	if s.Earliest.IsZero() || o.Earliest.Unix() < s.Earliest.Unix() {
		s.Earliest = o.Earliest
	}

	if o.NFields > s.NFields {
		s.NFields = o.NFields
	}

	if o.NTags > s.NTags {
		s.NTags = o.NTags
	}
}

func writeTable(ctx context.Context, t *ToTransformation, tbl flux.Table) (err error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	spec := t.spec.Spec

	// cache tag columns
	columns := tbl.Cols()
	isTag := make([]bool, len(columns))
	for i, col := range columns {
		tagIdx := sort.SearchStrings(spec.TagColumns, col.Label)
		isTag[i] = tagIdx < len(spec.TagColumns) && spec.TagColumns[tagIdx] == col.Label
	}
	// do time
	timeColLabel := spec.TimeColumn
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

	// prepare field function if applicable and record the number of values to write per row
	var fn *execute.RowMapPreparedFn
	if spec.FieldFn.Fn != nil {
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

	measurementStats := make(map[string]Stats)
	measurementName := ""
	return tbl.Do(func(er flux.ColReader) error {
		var pointTime time.Time
		var points models.Points
		var tags models.Tags
		kv := make([][]byte, 2, er.Len()*2+2) // +2 for field key, value
	outer:
		for i := 0; i < er.Len(); i++ {
			measurementName = ""
			fields := make(models.Fields)
			kv = kv[:0]
			// Gather the timestamp and the tags.
			for j, col := range er.Cols() {
				switch {
				case col.Label == spec.MeasurementColumn:
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
						return &errors.Error{
							Code: errors.EInvalid,
							Msg:  "invalid type for tag column",
							Op:   toOp,
						}
					}
					// TODO(docmerlin): instead of doing this sort of thing, it would be nice if we had a way that allocated a lot less.
					kv = append(kv, []byte(col.Label), er.Strings(j).Value(i))
				}
			}

			if pointTime.IsZero() {
				return &flux.Error{
					Code: codes.Invalid,
					Msg:  "timestamp missing from block",
				}
			}

			if measurementName == "" {
				return &flux.Error{
					Code: codes.Invalid,
					Msg:  fmt.Sprintf("no column with label %s exists", spec.MeasurementColumn),
				}
			}

			var fieldValues values.Object
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

			mstats := Stats{
				NRows:    1,
				Latest:   pointTime,
				Earliest: pointTime,
				NFields:  len(fields),
				NTags:    len(kv) / 2,
			}
			_, ok := measurementStats[measurementName]
			if !ok {
				measurementStats[measurementName] = mstats
			} else {
				measurementStats[measurementName].Update(mstats)
			}

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

		return t.buf.WritePoints(ctx, points)
	})
}

func defaultFieldMapping(er flux.ColReader, row int) (values.Object, error) {
	fieldColumnIdx := execute.ColIdx(defaultFieldColLabel, er.Cols())
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
