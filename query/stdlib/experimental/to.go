package experimental

import (
	"context"
	"errors"
	"fmt"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/runtime"
	"github.com/influxdata/flux/semantic"
	_ "github.com/influxdata/flux/stdlib/experimental"
	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/query/stdlib/influxdata/influxdb"
	"github.com/influxdata/influxdb/v2/storage"
)

// ToKind is the kind for the `to` flux function
const ExperimentalToKind = "influxdb-experimental-to"

// ToOpSpec is the flux.OperationSpec for the `to` flux function.
type ToOpSpec struct {
	Bucket   string `json:"bucket"`
	BucketID string `json:"bucketID"`
	Org      string `json:"org"`
	OrgID    string `json:"orgID"`
	Host     string `json:"host"`
	Token    string `json:"token"`
}

func init() {
	toSignature := runtime.MustLookupBuiltinType("experimental", "to")
	runtime.ReplacePackageValue("experimental", "to", flux.MustValue(flux.FunctionValueWithSideEffect("to", createToOpSpec, toSignature)))
	plan.RegisterProcedureSpecWithSideEffect(ExperimentalToKind, newToProcedure, ExperimentalToKind)
	execute.RegisterTransformation(ExperimentalToKind, createToTransformation)
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

	return err
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

// Kind returns the kind for the ToOpSpec function.
func (ToOpSpec) Kind() flux.OperationKind {
	return ExperimentalToKind
}

// BucketsAccessed returns the buckets accessed by the spec.
func (o *ToOpSpec) BucketsAccessed(orgID *platform.ID) (readBuckets, writeBuckets []platform.BucketFilter) {
	bf := platform.BucketFilter{}
	if o.Bucket != "" {
		bf.Name = &o.Bucket
	}
	if o.BucketID != "" {
		id, err := platform.IDFromString(o.BucketID)
		if err == nil {
			bf.ID = id
		}
	}
	if o.Org != "" {
		bf.Org = &o.Org
	}
	if o.OrgID != "" {
		id, err := platform.IDFromString(o.OrgID)
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
	return ExperimentalToKind
}

// Copy clones the procedure spec for `to` flux function.
func (o *ToProcedureSpec) Copy() plan.ProcedureSpec {
	s := o.Spec
	res := &ToProcedureSpec{
		Spec: &ToOpSpec{
			Bucket:   s.Bucket,
			BucketID: s.BucketID,
			Org:      s.Org,
			OrgID:    s.OrgID,
			Host:     s.Host,
			Token:    s.Token,
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
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	cache := execute.NewTableBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	deps := influxdb.GetStorageDependencies(a.Context()).ToDeps

	t, err := NewToTransformation(a.Context(), d, cache, s, deps)
	if err != nil {
		return nil, nil, err
	}
	return t, d, nil
}

// ToTransformation is the transformation for the `to` flux function.
type ToTransformation struct {
	execute.ExecutionNode
	ctx      context.Context
	bucketID platform.ID
	orgID    platform.ID
	d        execute.Dataset
	cache    execute.TableBuilderCache
	spec     *ToOpSpec
	deps     influxdb.ToDependencies
	buf      *storage.BufferedPointsWriter
}

// RetractTable retracts the table for the transformation for the `to` flux function.
func (t *ToTransformation) RetractTable(id execute.DatasetID, key flux.GroupKey) error {
	return t.d.RetractTable(key)
}

// NewToTransformation returns a new *ToTransformation with the appropriate fields set.
func NewToTransformation(ctx context.Context, d execute.Dataset, cache execute.TableBuilderCache, spec *ToProcedureSpec, deps influxdb.ToDependencies) (*ToTransformation, error) {
	var err error

	var orgID platform.ID
	// Get organization name and ID
	if spec.Spec.Org != "" {
		oID, ok := deps.OrganizationLookup.Lookup(ctx, spec.Spec.Org)
		if !ok {
			return nil, fmt.Errorf("failed to look up organization %q", spec.Spec.Org)
		}
		orgID = oID
	} else if spec.Spec.OrgID != "" {
		if oid, err := platform.IDFromString(spec.Spec.OrgID); err != nil {
			return nil, err
		} else {
			orgID = *oid
		}
	} else {
		// No org or orgID provided as an arg, use the orgID from the context
		req := query.RequestFromContext(ctx)
		if req == nil {
			return nil, errors.New("missing request on context")
		}
		orgID = req.OrganizationID
	}

	var bucketID *platform.ID
	// Get bucket name and ID
	// User will have specified exactly one in the ToOpSpec.
	if spec.Spec.Bucket != "" {
		bID, ok := deps.BucketLookup.Lookup(ctx, orgID, spec.Spec.Bucket)
		if !ok {
			return nil, fmt.Errorf("failed to look up bucket %q in org %q", spec.Spec.Bucket, spec.Spec.Org)
		}
		bucketID = &bID
	} else {
		if bucketID, err = platform.IDFromString(spec.Spec.BucketID); err != nil {
			return nil, err
		}
	}
	return &ToTransformation{
		ctx:      ctx,
		bucketID: *bucketID,
		orgID:    orgID,
		d:        d,
		cache:    cache,
		spec:     spec.Spec,
		deps:     deps,
		buf:      storage.NewBufferedPointsWriter(orgID, *bucketID, influxdb.DefaultBufferSize, deps.PointsWriter),
	}, nil
}

// Process does the actual work for the ToTransformation.
func (t *ToTransformation) Process(id execute.DatasetID, tbl flux.Table) error {
	return t.writeTable(t.ctx, tbl)
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
	if err != nil {
		t.d.Finish(err)
		return
	}
	err = t.buf.Flush(t.ctx)
	t.d.Finish(err)
}

const (
	defaultFieldColLabel       = "_field"
	defaultMeasurementColLabel = influxdb.DefaultMeasurementColLabel
	defaultTimeColLabel        = execute.DefaultTimeColLabel
	defaultStartColLabel       = execute.DefaultStartColLabel
	defaultStopColLabel        = execute.DefaultStopColLabel
)

type LabelAndOffset struct {
	Label  string
	Offset int
}

// TablePointsMetadata stores state needed to write the points from one table.
type TablePointsMetadata struct {
	MeasurementName string
	// The tags in the table (final element is left as nil, to be replaced by field name)
	Tags [][]byte
	// The offset in tags where to store the field name
	FieldKeyTagValueOffset int
	// The column offset in the input table where the _time column is stored
	TimestampOffset int
	// The labels and offsets of all the fields in the table
	Fields []LabelAndOffset
}

func GetTablePointsMetadata(tbl flux.Table) (*TablePointsMetadata, error) {
	// Find measurement, tags
	var measurement string
	tagmap := make(map[string]string, len(tbl.Key().Cols())+2)
	isTag := make(map[string]bool)
	for j, col := range tbl.Key().Cols() {
		switch col.Label {
		case defaultStartColLabel:
			continue
		case defaultStopColLabel:
			continue
		case defaultFieldColLabel:
			return nil, fmt.Errorf("found column %q in the group key; experimental.to() expects pivoted data", col.Label)
		case defaultMeasurementColLabel:
			if col.Type != flux.TString {
				return nil, fmt.Errorf("group key column %q has type %v; type %v is required", col.Label, col.Type, flux.TString)
			}
			measurement = tbl.Key().ValueString(j)
		default:
			if col.Type != flux.TString {
				return nil, fmt.Errorf("group key column %q has type %v; type %v is required", col.Label, col.Type, flux.TString)
			}
			isTag[col.Label] = true
			tagmap[col.Label] = tbl.Key().ValueString(j)
		}
	}
	if len(measurement) == 0 {
		return nil, fmt.Errorf("required column %q not in group key", defaultMeasurementColLabel)
	}
	t := models.NewTags(tagmap)

	tags := make([][]byte, 0, len(t)*2)
	for i := range t {
		tags = append(tags, t[i].Key, t[i].Value)
	}

	// Loop over all columns to find fields and _time
	fields := make([]LabelAndOffset, 0, len(tbl.Cols())-len(tbl.Key().Cols()))
	timestampOffset := -1
	for j, col := range tbl.Cols() {
		switch col.Label {
		case defaultStartColLabel:
			continue
		case defaultStopColLabel:
			continue
		case defaultMeasurementColLabel:
			continue
		case defaultTimeColLabel:
			if col.Type != flux.TTime {
				return nil, fmt.Errorf("column %q has type string; type %s is required", defaultTimeColLabel, flux.TTime)
			}
			timestampOffset = j
			continue
		default:
			if !isTag[col.Label] {
				fields = append(fields, LabelAndOffset{Label: col.Label, Offset: j})
			}
		}
	}
	if timestampOffset == -1 {
		return nil, fmt.Errorf("input table is missing required column %q", defaultTimeColLabel)
	}

	tmd := &TablePointsMetadata{
		MeasurementName: measurement,
		Tags:            tags,
		TimestampOffset: timestampOffset,
		Fields:          fields,
	}

	return tmd, nil
}

func (t *ToTransformation) writeTable(ctx context.Context, tbl flux.Table) error {
	builder, new := t.cache.TableBuilder(tbl.Key())
	if new {
		if err := execute.AddTableCols(tbl, builder); err != nil {
			return err
		}
	}

	tmd, err := GetTablePointsMetadata(tbl)
	if err != nil {
		return err
	}

	pointName := tmd.MeasurementName
	return tbl.Do(func(cr flux.ColReader) error {
		if cr.Len() == 0 {
			// Nothing to do
			return nil
		}

		var (
			points models.Points
			tags   models.Tags
		)

		for i := 0; i < cr.Len(); i++ {
			fields := make(models.Fields, len(tmd.Fields))
			for _, lao := range tmd.Fields {
				fieldVal := execute.ValueForRow(cr, i, lao.Offset)

				// Skip this iteration if field value is null
				if fieldVal.IsNull() {
					continue
				}

				switch fieldVal.Type() {
				case semantic.BasicFloat:
					fields[lao.Label] = fieldVal.Float()
				case semantic.BasicInt:
					fields[lao.Label] = fieldVal.Int()
				case semantic.BasicUint:
					fields[lao.Label] = fieldVal.UInt()
				case semantic.BasicString:
					fields[lao.Label] = fieldVal.Str()
				case semantic.BasicBool:
					fields[lao.Label] = fieldVal.Bool()
				default:
					return fmt.Errorf("unsupported field type %v", fieldVal.Type())
				}
			}

			timestamp := execute.ValueForRow(cr, i, tmd.TimestampOffset).Time().Time()
			tags, err := models.NewTagsKeyValues(tags, tmd.Tags...)
			if err != nil {
				return err
			}
			pt, err := models.NewPoint(pointName, tags, fields, timestamp)
			if err != nil {
				return err
			}
			points = append(points, pt)

			if err := execute.AppendRecord(i, cr, builder); err != nil {
				return err
			}
		}

		return t.buf.WritePoints(ctx, points)
	})
}
