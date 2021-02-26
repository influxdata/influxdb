package influxdb

import (
	"context"
	"fmt"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/runtime"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/stdlib/influxdata/influxdb"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxql"
	"time"
)

const (
	ToKind = "influx1x/toKind"
	DefaultBufferSize = 5000
)

type ToOpSpec struct {
	Bucket string `json:"bucket"`
}

func init() {
	toSignature := runtime.MustLookupBuiltinType("influxdata/influxdb", influxdb.ToKind)
	runtime.ReplacePackageValue("influxdata/influxdb", "to", flux.MustValue(flux.FunctionValueWithSideEffect(ToKind, createToOpSpec, toSignature)))
	flux.RegisterOpSpec(ToKind, func() flux.OperationSpec { return &ToOpSpec{}})
	plan.RegisterProcedureSpecWithSideEffect(ToKind, newToProcedure, ToKind)
	execute.RegisterTransformation(ToKind, createToTransformation)
}

var unsupportedToArgs = []string{"bucketID", "orgID", "host", "timeColumn", "measurementColumn", "tagColumns", "fieldFn"}

func (o *ToOpSpec) ReadArgs(args flux.Arguments) error {
	var err error
	var ok bool

	for _, arg := range unsupportedToArgs {
		if _, ok = args.Get(arg); ok {
			return &flux.Error{
				Code: codes.Invalid,
				Msg: fmt.Sprintf("argument %s for 'to' not supported in 1.x flux engine", arg),
			}
		}
	}

	if o.Bucket, err = args.GetRequiredString("bucket"); err != nil {
		return err
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
			Bucket: s.Bucket,
		},
	}
	return res
}

func newToProcedure(qs flux.OperationSpec, a plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*ToOpSpec)
	if !ok && spec != nil {
		return nil, &flux.Error{
			Code: codes.Internal,
			Msg: fmt.Sprintf("invalid spec type %T", qs),
		}
	}
	return &ToProcedureSpec{Spec: spec},nil
}

func createToTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*ToProcedureSpec)
	if !ok {
		return nil, nil, &flux.Error{
			Code: codes.Internal,
			Msg: fmt.Sprintf("invalid spec type %T", spec),
		}
	}
	cache := execute.NewTableBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	deps := GetStorageDependencies(a.Context())
	if deps == (StorageDependencies{}) {
		return nil, nil, &flux.Error{
			Code: codes.Unimplemented,
			Msg: "cannot return storage dependencies; storage dependencies are unimplemented",
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
	Ctx context.Context
	DB string
	RP string
	d execute.Dataset
	cache execute.TableBuilderCache
	deps StorageDependencies
	buf *coordinator.BufferedPointsWriter
}

func NewToTransformation(ctx context.Context, d execute.Dataset, cache execute.TableBuilderCache, toSpec *ToProcedureSpec, deps StorageDependencies) (*ToTransformation, error) {
	spec := toSpec.Spec
	var db, rp string
	db, rp, err := lookupDatabase(ctx, spec.Bucket, deps, influxql.WritePrivilege)
	if err != nil {
		return nil, err
	}
	return &ToTransformation{
		Ctx: ctx,
		DB: db,
		RP: rp,
		d: d,
		cache: cache,
		deps: deps,
		buf: coordinator.NewBufferedPointsWriter(deps.PointsWriter, db, rp, DefaultBufferSize),
	}, nil
}

func (t *ToTransformation) RetractTable(id execute.DatasetID, key flux.GroupKey) error {
	return t.d.RetractTable(key)
}

// Process does the actual work for the ToTransformation.
func (t *ToTransformation) Process(id execute.DatasetID, tbl flux.Table) error {
	//TODO(lesam): this is where 2.x overrides with explicit tag columns
	measurementColumn := "_measurement"
	fieldColumn := "_field"
	excludeColumns := map[string]bool{
		execute.DefaultValueColLabel: true,
		fieldColumn:         true,
		measurementColumn:   true,
	}

	isTagColumn := func (column flux.ColMeta) bool {
		return column.Type == flux.TString && !excludeColumns[column.Label]
	}

	columns := tbl.Cols()
	isTag := make([]bool, len(columns))
	numTags := 0
	for i, col := range columns {
		isTag[i] = isTagColumn(col)
		numTags++
	}

	// TODO(lesam): this is where 2.x overrides the default time column label
	timeColLabel := execute.DefaultTimeColLabel
	timeColIdx := execute.ColIdx(timeColLabel, columns)
	if timeColIdx < 0 {
		return &flux.Error{
			Code: codes.Invalid,
			Msg: "no time column detected",
		}
	}
	if columns[timeColIdx].Type != flux.TTime {
		return &flux.Error{
			Code: codes.Invalid,
			Msg: fmt.Sprintf("column %s of type %s is not of type %s", timeColLabel, columns[timeColIdx].Type, flux.TTime),
		}
	}

	builder, new := t.cache.TableBuilder(tbl.Key())
	if new {
		if err := execute.AddTableCols(tbl, builder); err != nil {
			return err
		}
	}

	return tbl.Do(func(er flux.ColReader) error {
		kv := make([][]byte, 0, numTags * 2)
		var tags models.Tags
		var points models.Points
		outer:
		for i := 0; i < er.Len(); i++ {
			measurementName := ""
			fields := make(models.Fields)
			var pointTime time.Time
			kv = kv[0:]

			// get the non-field values
			for j, col := range er.Cols() {
				switch {
				case col.Label == measurementColumn:
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
							Msg: "Invalid type for tag column",
						}
					}
					kv = append(kv, []byte(col.Label), er.Strings(j).Value(i))
				}
			}

			// validate the point
			if pointTime.IsZero() {
				return &flux.Error{
					Code: codes.Invalid,
					Msg: "timestamp missing from block",
				}
			}
			if measurementName == "" {
				return &flux.Error{
					Code: codes.Invalid,
					Msg: fmt.Sprintf("no column with label %s exists", measurementColumn),
				}
			}

			var fieldValues values.Object
			var err error
			// TODO(lesam): this is where we would support the `fn` argument to `to`
			if fieldValues, err = defaultFieldMapping(er, i); err != nil {
				return err
			}

			fieldValues.Range(func(k string, v values.Value){
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
