package outputs

import (
	"context"
	"errors"
	"fmt"
	"github.com/influxdata/platform/tsdb"
	"time"

	"sort"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/functions/outputs"
	"github.com/influxdata/flux/interpreter"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/models"
	istorage "github.com/influxdata/platform/query/functions/inputs/storage"
	"github.com/influxdata/platform/storage"
)

// ToKind is the kind for the `to` flux function
const ToKind = "to"

// TODO(jlapacik) remove this once we have execute.DefaultFieldColLabel
const defaultFieldColLabel = "_field"
const DefaultMeasurementColLabel = "_measurement"

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
	FieldFn           *semantic.FunctionExpression `json:"fieldFn"`
}

func init() {
	toSignature := flux.FunctionSignature(
		map[string]semantic.PolyType{
			"bucket":            semantic.String,
			"bucketID":          semantic.String,
			"org":               semantic.String,
			"orgID":             semantic.String,
			"host":              semantic.String,
			"token":             semantic.String,
			"timeColumn":        semantic.String,
			"measurementColumn": semantic.String,
			"tagColumns":        semantic.Array,
			"fieldFn": semantic.NewFunctionPolyType(semantic.FunctionPolySignature{
				Parameters: map[string]semantic.PolyType{
					"r": semantic.Tvar(1),
				},
				Required: semantic.LabelSet{"r"},
				Return:   semantic.Tvar(2),
			}),
		},
		[]string{},
	)

	flux.RegisterFunctionWithSideEffect(ToKind, createToOpSpec, toSignature)
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
		return errors.New("cannot provide both `bucket` and `bucketID` parameters to the `to` function")
	}

	if o.Org, ok, _ = args.GetString("org"); !ok {
		if o.OrgID, err = args.GetRequiredString("orgID"); err != nil {
			return err
		}
	} else if o.OrgID, ok, _ = args.GetString("orgID"); ok {
		return errors.New("cannot provide both `org` and `orgID` parameters to the `to` function")
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
		return nil, errors.New("specify at most one of url, brokers in the same `to` function")
	case httpOK:
		s = &outputs.ToHTTPOpSpec{}
	case kafkaOK:
		s = &outputs.ToKafkaOpSpec{}
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
func (o *ToOpSpec) BucketsAccessed() (readBuckets, writeBuckets []platform.BucketFilter) {
	bf := platform.BucketFilter{Name: &o.Bucket, Organization: &o.Org}
	writeBuckets = append(writeBuckets, bf)
	return readBuckets, writeBuckets
}

// ToProcedureSpec is the procedure spec for the `to` flux function.
type ToProcedureSpec struct {
	Spec *ToOpSpec
}

// Kind returns the kind for the procedure spec for the `to` flux function.
func (o *ToProcedureSpec) Kind() plan.ProcedureKind {
	return ToKind
}

// Copy clones the procedure spec for `to` flux function.
func (o *ToProcedureSpec) Copy() plan.ProcedureSpec {
	s := o.Spec
	var fn *semantic.FunctionExpression
	if s.FieldFn != nil {
		fn = s.FieldFn.Copy().(*semantic.FunctionExpression)
	}
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
			FieldFn:           fn,
		},
	}
	return res
}

func newToProcedure(qs flux.OperationSpec, a plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*ToOpSpec)
	if !ok && spec != nil {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}
	return &ToProcedureSpec{Spec: spec}, nil
}

func createToTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*ToProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	cache := execute.NewTableBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	deps := a.Dependencies()[ToKind].(ToDependencies)

	t, err := NewToTransformation(d, cache, s, deps)
	if err != nil {
		return nil, nil, err
	}
	return t, d, nil
}

// ToTransformation is the transformation for the `to` flux function.
type ToTransformation struct {
	d     execute.Dataset
	fn    *execute.RowMapFn
	cache execute.TableBuilderCache
	spec  *ToProcedureSpec
	deps  ToDependencies
}

// RetractTable retracts the table for the transformation for the `to` flux function.
func (t *ToTransformation) RetractTable(id execute.DatasetID, key flux.GroupKey) error {
	return t.d.RetractTable(key)
}

// NewToTransformation returns a new *ToTransformation with the appropriate fields set.
func NewToTransformation(d execute.Dataset, cache execute.TableBuilderCache, spec *ToProcedureSpec, deps ToDependencies) (*ToTransformation, error) {
	var fn *execute.RowMapFn
	var err error

	if spec.Spec.FieldFn != nil {
		if fn, err = execute.NewRowMapFn(spec.Spec.FieldFn); err != nil {
			return nil, err
		}
	}

	return &ToTransformation{
		d:     d,
		fn:    fn,
		cache: cache,
		spec:  spec,
		deps:  deps,
	}, nil
}

// Process does the actual work for the ToTransformation.
func (t *ToTransformation) Process(id execute.DatasetID, tbl flux.Table) error {
	if t.spec.Spec.TagColumns == nil {

		// If no tag columns are specified, by default we exclude
		// _field and _value from being tag columns.
		excludeColumns := map[string]bool{
			execute.DefaultValueColLabel: true,
			defaultFieldColLabel:         true,
		}

		// If a field function is specified then we exclude any column that
		// is referenced in the function expression from being a tag column.
		if t.spec.Spec.FieldFn != nil {
			recordParam := t.spec.Spec.FieldFn.Block.Parameters.List[0].Key.Name
			exprNode := t.spec.Spec.FieldFn
			colVisitor := newFieldFunctionVisitor(recordParam, tbl.Cols())

			// Walk the field function expression and record which columns
			// are referenced. None of these columns will be used as tag columns.
			semantic.Walk(colVisitor, exprNode)
			excludeColumns = colVisitor.captured
		}

		addTagsFromTable(t.spec.Spec, tbl, excludeColumns)
	}
	return writeTable(t, tbl)
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
	spec.TagColumns = make([]string, 0, len(table.Cols()))
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
	t.d.Finish(err)
}

// InjectToDependencies adds the To dependencies to the engine.
func InjectToDependencies(depsMap execute.Dependencies, deps ToDependencies) error {
	if err := deps.Validate(); err != nil {
		return err
	}
	depsMap[ToKind] = deps
	return nil
}

// ToDependencies contains the dependencies for executing the `to` function.
type ToDependencies struct {
	BucketLookup       istorage.BucketLookup
	OrganizationLookup istorage.OrganizationLookup
	PointsWriter       storage.PointsWriter
}

// Validate returns an error if any required field is unset.
func (d ToDependencies) Validate() error {
	if d.BucketLookup == nil {
		return errors.New("missing bucket lookup dependency")
	}
	if d.OrganizationLookup == nil {
		return errors.New("missing organization lookup dependency")
	}
	if d.PointsWriter == nil {
		return errors.New("missing points writer dependency")
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

func writeTable(t *ToTransformation, tbl flux.Table) error {
	var bucketID, orgID *platform.ID
	var err error

	d := t.deps
	spec := t.spec.Spec

	// Get organization ID
	if spec.Org != "" {
		oID, ok := d.OrganizationLookup.Lookup(context.TODO(), spec.Org)
		if !ok {
			return fmt.Errorf("failed to look up organization %q", spec.Org)
		}
		orgID = &oID
	} else if orgID, err = platform.IDFromString(spec.OrgID); err != nil {
		return err
	}

	// Get bucket ID
	if spec.Bucket != "" {
		bID, ok := d.BucketLookup.Lookup(*orgID, spec.Bucket)
		if !ok {
			return fmt.Errorf("failed to look up bucket %q in org %q", spec.Bucket, spec.Org)
		}
		bucketID = &bID
	} else if bucketID, err = platform.IDFromString(spec.BucketID); err != nil {
		return err
	}

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
		return errors.New("no time column detected")
	}
	if columns[timeColIdx].Type != flux.TTime {
		return fmt.Errorf("column %s of type %s is not of type %s", timeColLabel, columns[timeColIdx].Type, flux.TTime)
	}

	// prepare field function if applicable and record the number of values to write per row
	if spec.FieldFn != nil {
		if err = t.fn.Prepare(columns); err != nil {
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
		fields := make(models.Fields)
		var fieldValues values.Object
		for i := 0; i < er.Len(); i++ {
			tags = nil
			// Gather the timestamp and the tags.
			for j, col := range er.Cols() {
				switch {
				case col.Label == spec.MeasurementColumn:
					measurementName = er.Strings(j)[i]
				case col.Label == timeColLabel:
					pointTime = er.Times(j)[i].Time()
				case isTag[j]:
					if col.Type != flux.TString {
						return errors.New("invalid type for tag column")
					}
					// TODO(docmerlin): instead of doing this sort of thing, it would be nice if we had a way that allocated a lot less.
					// Note that tags are 2-tuples of key and then value.
					tags = append(tags, models.NewTag([]byte(col.Label), []byte(er.Strings(j)[i])))
				}
			}

			if pointTime.IsZero() {
				return errors.New("timestamp missing from block")
			}

			if measurementName == "" {
				return fmt.Errorf("no column with label %s exists", spec.MeasurementColumn)
			}

			if spec.FieldFn == nil {
				if fieldValues, err = defaultFieldMapping(er, i); err != nil {
					return err
				}
			} else if fieldValues, err = t.fn.Eval(i, er); err != nil {
				return err
			}

			fieldValues.Range(func(k string, v values.Value) {
				switch v.Type() {
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
				NTags:    len(tags),
			}
			_, ok := measurementStats[measurementName]
			if !ok {
				measurementStats[measurementName] = mstats
			} else {
				measurementStats[measurementName].Update(mstats)
			}

			pt, err := models.NewPoint(measurementName, tags, fields, pointTime)
			if err != nil {
				return err
			}
			points = append(points, pt)
			if err := execute.AppendRecord(i, er, builder); err != nil {
				return err
			}
		}
		points, err = tsdb.ExplodePoints(*orgID, *bucketID, points)
		return d.PointsWriter.WritePoints(points)
	})
}

func defaultFieldMapping(er flux.ColReader, row int) (values.Object, error) {
	fieldColumnIdx := execute.ColIdx(defaultFieldColLabel, er.Cols())
	valueColumnIdx := execute.ColIdx(execute.DefaultValueColLabel, er.Cols())

	if fieldColumnIdx < 0 {
		return nil, errors.New("table has no _field column")
	}

	if valueColumnIdx < 0 {
		return nil, errors.New("table has no _value column")
	}

	var value values.Value
	valueColumnType := er.Cols()[valueColumnIdx].Type

	switch valueColumnType {
	case flux.TFloat:
		value = values.NewFloat(er.Floats(valueColumnIdx)[row])
	case flux.TInt:
		value = values.NewInt(er.Ints(valueColumnIdx)[row])
	case flux.TUInt:
		value = values.NewUInt(er.UInts(valueColumnIdx)[row])
	case flux.TString:
		value = values.NewString(er.Strings(valueColumnIdx)[row])
	case flux.TTime:
		value = values.NewTime(er.Times(valueColumnIdx)[row])
	case flux.TBool:
		value = values.NewBool(er.Bools(valueColumnIdx)[row])
	default:
		return nil, fmt.Errorf("unsupported type %v for _value column", valueColumnType)
	}

	fieldValueMapping := values.NewObject()
	field := er.Strings(fieldColumnIdx)[row]
	fieldValueMapping.Set(field, value)

	return fieldValueMapping, nil
}
