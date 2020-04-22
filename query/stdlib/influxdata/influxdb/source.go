package influxdb

import (
	"context"
	"errors"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/semantic"
	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
)

func init() {
	execute.RegisterSource(ReadRangePhysKind, createReadFilterSource)
	execute.RegisterSource(ReadGroupPhysKind, createReadGroupSource)
	execute.RegisterSource(ReadWindowAggregatePhysKind, createReadWindowAggregateSource)
	execute.RegisterSource(ReadTagKeysPhysKind, createReadTagKeysSource)
	execute.RegisterSource(ReadTagValuesPhysKind, createReadTagValuesSource)
}

type runner interface {
	run(ctx context.Context) error
}

type Source struct {
	id execute.DatasetID
	ts []execute.Transformation

	alloc *memory.Allocator
	stats cursors.CursorStats

	runner runner

	m     *metrics
	orgID platform.ID
	op    string
}

func (s *Source) Run(ctx context.Context) {
	labelValues := s.m.getLabelValues(ctx, s.orgID, s.op)
	start := time.Now()
	var err error
	if flux.IsExperimentalTracingEnabled() {
		span, ctxWithSpan := tracing.StartSpanFromContextWithOperationName(ctx, "source-"+s.op)
		err = s.runner.run(ctxWithSpan)
		span.Finish()
	} else {
		err = s.runner.run(ctx)
	}
	s.m.recordMetrics(labelValues, start)
	for _, t := range s.ts {
		t.Finish(s.id, err)
	}
}

func (s *Source) AddTransformation(t execute.Transformation) {
	s.ts = append(s.ts, t)
}

func (s *Source) Metadata() flux.Metadata {
	return flux.Metadata{
		"influxdb/scanned-bytes":  []interface{}{s.stats.ScannedBytes},
		"influxdb/scanned-values": []interface{}{s.stats.ScannedValues},
	}
}

func (s *Source) processTables(ctx context.Context, tables TableIterator, watermark execute.Time) error {
	err := tables.Do(func(tbl flux.Table) error {
		return s.processTable(ctx, tbl)
	})
	if err != nil {
		return err
	}

	// Track the number of bytes and values scanned.
	stats := tables.Statistics()
	s.stats.ScannedValues += stats.ScannedValues
	s.stats.ScannedBytes += stats.ScannedBytes

	for _, t := range s.ts {
		if err := t.UpdateWatermark(s.id, watermark); err != nil {
			return err
		}
	}
	return nil
}

func (s *Source) processTable(ctx context.Context, tbl flux.Table) error {
	if len(s.ts) == 0 {
		tbl.Done()
		return nil
	} else if len(s.ts) == 1 {
		return s.ts[0].Process(s.id, tbl)
	}

	// There is more than one transformation so we need to
	// copy the table for each transformation.
	bufTable, err := execute.CopyTable(tbl)
	if err != nil {
		return err
	}
	defer bufTable.Done()

	for _, t := range s.ts {
		if err := t.Process(s.id, bufTable.Copy()); err != nil {
			return err
		}
	}
	return nil
}

type readFilterSource struct {
	Source
	reader   Reader
	readSpec ReadFilterSpec
}

func ReadFilterSource(id execute.DatasetID, r Reader, readSpec ReadFilterSpec, a execute.Administration) execute.Source {
	src := new(readFilterSource)

	src.id = id
	src.alloc = a.Allocator()

	src.reader = r
	src.readSpec = readSpec

	src.m = GetStorageDependencies(a.Context()).FromDeps.Metrics
	src.orgID = readSpec.OrganizationID
	src.op = "readFilter"

	src.runner = src
	return src
}

func (s *readFilterSource) run(ctx context.Context) error {
	stop := s.readSpec.Bounds.Stop
	tables, err := s.reader.ReadFilter(
		ctx,
		s.readSpec,
		s.alloc,
	)
	if err != nil {
		return err
	}
	return s.processTables(ctx, tables, stop)
}

func createReadFilterSource(s plan.ProcedureSpec, id execute.DatasetID, a execute.Administration) (execute.Source, error) {
	span, ctx := tracing.StartSpanFromContext(a.Context())
	defer span.Finish()

	spec := s.(*ReadRangePhysSpec)

	bounds := a.StreamContext().Bounds()
	if bounds == nil {
		return nil, &flux.Error{
			Code: codes.Internal,
			Msg:  "nil bounds passed to from",
		}
	}

	deps := GetStorageDependencies(a.Context()).FromDeps

	req := query.RequestFromContext(a.Context())
	if req == nil {
		return nil, &flux.Error{
			Code: codes.Internal,
			Msg:  "missing request on context",
		}
	}

	orgID := req.OrganizationID
	bucketID, err := spec.LookupBucketID(ctx, orgID, deps.BucketLookup)
	if err != nil {
		return nil, err
	}

	var filter *semantic.FunctionExpression
	if spec.FilterSet {
		filter = spec.Filter
	}
	return ReadFilterSource(
		id,
		deps.Reader,
		ReadFilterSpec{
			OrganizationID: orgID,
			BucketID:       bucketID,
			Bounds:         *bounds,
			Predicate:      filter,
		},
		a,
	), nil
}

type readGroupSource struct {
	Source
	reader   Reader
	readSpec ReadGroupSpec
}

func ReadGroupSource(id execute.DatasetID, r Reader, readSpec ReadGroupSpec, a execute.Administration) execute.Source {
	src := new(readGroupSource)

	src.id = id
	src.alloc = a.Allocator()

	src.reader = r
	src.readSpec = readSpec

	src.m = GetStorageDependencies(a.Context()).FromDeps.Metrics
	src.orgID = readSpec.OrganizationID
	src.op = "readGroup"

	src.runner = src
	return src
}

func (s *readGroupSource) run(ctx context.Context) error {
	stop := s.readSpec.Bounds.Stop
	tables, err := s.reader.ReadGroup(
		ctx,
		s.readSpec,
		s.alloc,
	)
	if err != nil {
		return err
	}
	return s.processTables(ctx, tables, stop)
}

func createReadGroupSource(s plan.ProcedureSpec, id execute.DatasetID, a execute.Administration) (execute.Source, error) {
	span, ctx := tracing.StartSpanFromContext(a.Context())
	defer span.Finish()

	spec := s.(*ReadGroupPhysSpec)

	bounds := a.StreamContext().Bounds()
	if bounds == nil {
		return nil, errors.New("nil bounds passed to from")
	}

	deps := GetStorageDependencies(a.Context()).FromDeps

	req := query.RequestFromContext(a.Context())
	if req == nil {
		return nil, errors.New("missing request on context")
	}

	orgID := req.OrganizationID
	bucketID, err := spec.LookupBucketID(ctx, orgID, deps.BucketLookup)
	if err != nil {
		return nil, err
	}

	var filter *semantic.FunctionExpression
	if spec.FilterSet {
		filter = spec.Filter
	}
	return ReadGroupSource(
		id,
		deps.Reader,
		ReadGroupSpec{
			ReadFilterSpec: ReadFilterSpec{
				OrganizationID: orgID,
				BucketID:       bucketID,
				Bounds:         *bounds,
				Predicate:      filter,
			},
			GroupMode:       ToGroupMode(spec.GroupMode),
			GroupKeys:       spec.GroupKeys,
			AggregateMethod: spec.AggregateMethod,
		},
		a,
	), nil
}

type readWindowAggregateSource struct {
	Source
	reader   WindowAggregateReader
	readSpec ReadWindowAggregateSpec
}

func ReadWindowAggregateSource(id execute.DatasetID, r WindowAggregateReader, readSpec ReadWindowAggregateSpec, a execute.Administration) execute.Source {
	src := new(readWindowAggregateSource)

	src.id = id
	src.alloc = a.Allocator()

	src.reader = r
	src.readSpec = readSpec

	src.m = GetStorageDependencies(a.Context()).FromDeps.Metrics
	src.orgID = readSpec.OrganizationID
	src.op = "readWindowAggregate"

	src.runner = src
	return src
}

func (s *readWindowAggregateSource) run(ctx context.Context) error {
	stop := s.readSpec.Bounds.Stop
	tables, err := s.reader.ReadWindowAggregate(
		ctx,
		s.readSpec,
		s.alloc,
	)
	if err != nil {
		return err
	}
	return s.processTables(ctx, tables, stop)
}

func createReadWindowAggregateSource(s plan.ProcedureSpec, id execute.DatasetID, a execute.Administration) (execute.Source, error) {
	span, ctx := tracing.StartSpanFromContext(a.Context())
	defer span.Finish()

	spec := s.(*ReadWindowAggregatePhysSpec)

	bounds := a.StreamContext().Bounds()
	if bounds == nil {
		return nil, &flux.Error{
			Code: codes.Internal,
			Msg:  "nil bounds passed to from",
		}
	}

	deps := GetStorageDependencies(a.Context()).FromDeps
	reader := deps.Reader.(WindowAggregateReader)

	req := query.RequestFromContext(a.Context())
	if req == nil {
		return nil, &flux.Error{
			Code: codes.Internal,
			Msg:  "missing request on context",
		}
	}

	orgID := req.OrganizationID
	bucketID, err := spec.LookupBucketID(ctx, orgID, deps.BucketLookup)
	if err != nil {
		return nil, err
	}

	var filter *semantic.FunctionExpression
	if spec.FilterSet {
		filter = spec.Filter
	}
	return ReadWindowAggregateSource(
		id,
		reader,
		ReadWindowAggregateSpec{
			ReadFilterSpec: ReadFilterSpec{
				OrganizationID: orgID,
				BucketID:       bucketID,
				Bounds:         *bounds,
				Predicate:      filter,
			},
			WindowEvery: spec.WindowEvery,
			Aggregates:  spec.Aggregates,
		},
		a,
	), nil
}

func createReadTagKeysSource(prSpec plan.ProcedureSpec, dsid execute.DatasetID, a execute.Administration) (execute.Source, error) {
	span, ctx := tracing.StartSpanFromContext(a.Context())
	defer span.Finish()

	spec := prSpec.(*ReadTagKeysPhysSpec)
	deps := GetStorageDependencies(a.Context()).FromDeps
	req := query.RequestFromContext(a.Context())
	if req == nil {
		return nil, errors.New("missing request on context")
	}
	orgID := req.OrganizationID

	bucketID, err := spec.LookupBucketID(ctx, orgID, deps.BucketLookup)
	if err != nil {
		return nil, err
	}

	var filter *semantic.FunctionExpression
	if spec.FilterSet {
		filter = spec.Filter
	}

	bounds := a.StreamContext().Bounds()
	return ReadTagKeysSource(
		dsid,
		deps.Reader,
		ReadTagKeysSpec{
			ReadFilterSpec: ReadFilterSpec{
				OrganizationID: orgID,
				BucketID:       bucketID,
				Bounds:         *bounds,
				Predicate:      filter,
			},
		},
		a,
	), nil
}

type readTagKeysSource struct {
	Source

	reader   Reader
	readSpec ReadTagKeysSpec
}

func ReadTagKeysSource(id execute.DatasetID, r Reader, readSpec ReadTagKeysSpec, a execute.Administration) execute.Source {
	src := &readTagKeysSource{
		reader:   r,
		readSpec: readSpec,
	}
	src.id = id
	src.alloc = a.Allocator()

	src.m = GetStorageDependencies(a.Context()).FromDeps.Metrics
	src.orgID = readSpec.OrganizationID
	src.op = "readTagKeys"

	src.runner = src
	return src
}

func (s *readTagKeysSource) run(ctx context.Context) error {
	ti, err := s.reader.ReadTagKeys(ctx, s.readSpec, s.alloc)
	if err != nil {
		return err
	}
	return s.processTables(ctx, ti, execute.Now())
}

func createReadTagValuesSource(prSpec plan.ProcedureSpec, dsid execute.DatasetID, a execute.Administration) (execute.Source, error) {
	span, ctx := tracing.StartSpanFromContext(a.Context())
	defer span.Finish()

	spec := prSpec.(*ReadTagValuesPhysSpec)
	deps := GetStorageDependencies(a.Context()).FromDeps
	req := query.RequestFromContext(a.Context())
	if req == nil {
		return nil, errors.New("missing request on context")
	}
	orgID := req.OrganizationID

	bucketID, err := spec.LookupBucketID(ctx, orgID, deps.BucketLookup)
	if err != nil {
		return nil, err
	}

	var filter *semantic.FunctionExpression
	if spec.FilterSet {
		filter = spec.Filter
	}

	bounds := a.StreamContext().Bounds()
	return ReadTagValuesSource(
		dsid,
		deps.Reader,
		ReadTagValuesSpec{
			ReadFilterSpec: ReadFilterSpec{
				OrganizationID: orgID,
				BucketID:       bucketID,
				Bounds:         *bounds,
				Predicate:      filter,
			},
			TagKey: spec.TagKey,
		},
		a,
	), nil
}

type readTagValuesSource struct {
	Source

	reader   Reader
	readSpec ReadTagValuesSpec
}

func ReadTagValuesSource(id execute.DatasetID, r Reader, readSpec ReadTagValuesSpec, a execute.Administration) execute.Source {
	src := &readTagValuesSource{
		reader:   r,
		readSpec: readSpec,
	}
	src.id = id
	src.alloc = a.Allocator()

	src.m = GetStorageDependencies(a.Context()).FromDeps.Metrics
	src.orgID = readSpec.OrganizationID
	src.op = "readTagValues"

	src.runner = src
	return src
}

func (s *readTagValuesSource) run(ctx context.Context) error {
	ti, err := s.reader.ReadTagValues(ctx, s.readSpec, s.alloc)
	if err != nil {
		return err
	}
	return s.processTables(ctx, ti, execute.Now())
}
