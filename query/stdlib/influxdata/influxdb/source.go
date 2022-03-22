package influxdb

import (
	"context"
	"errors"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/metadata"
	"github.com/influxdata/flux/plan"
	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
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
	execute.ExecutionNode
	id execute.DatasetID
	ts []execute.Transformation

	alloc memory.Allocator
	stats cursors.CursorStats

	runner runner

	m     *metrics
	orgID platform2.ID
	op    string
}

func (s *Source) Run(ctx context.Context) {
	labelValues := s.m.getLabelValues(ctx, s.orgID, s.op)
	start := time.Now()
	err := s.runner.run(ctx)
	s.m.recordMetrics(labelValues, start)
	for _, t := range s.ts {
		t.Finish(s.id, err)
	}
}

func (s *Source) AddTransformation(t execute.Transformation) {
	s.ts = append(s.ts, t)
}

func (s *Source) Metadata() metadata.Metadata {
	return metadata.Metadata{
		"influxdb/scanned-bytes":  []interface{}{s.stats.ScannedBytes},
		"influxdb/scanned-values": []interface{}{s.stats.ScannedValues},
	}
}

func (s *Source) processTables(ctx context.Context, tables query.TableIterator, watermark execute.Time) error {
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
	reader   query.StorageReader
	readSpec query.ReadFilterSpec
}

func ReadFilterSource(id execute.DatasetID, r query.StorageReader, readSpec query.ReadFilterSpec, a execute.Administration) execute.Source {
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

	return ReadFilterSource(
		id,
		deps.Reader,
		query.ReadFilterSpec{
			OrganizationID: orgID,
			BucketID:       bucketID,
			Bounds:         *bounds,
			Predicate:      spec.Filter,
		},
		a,
	), nil
}

type readGroupSource struct {
	Source
	reader   query.StorageReader
	readSpec query.ReadGroupSpec
}

func ReadGroupSource(id execute.DatasetID, r query.StorageReader, readSpec query.ReadGroupSpec, a execute.Administration) execute.Source {
	src := new(readGroupSource)

	src.id = id
	src.alloc = a.Allocator()

	src.reader = r
	src.readSpec = readSpec

	src.m = GetStorageDependencies(a.Context()).FromDeps.Metrics
	src.orgID = readSpec.OrganizationID
	src.op = readSpec.Name()

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

	return ReadGroupSource(
		id,
		deps.Reader,
		query.ReadGroupSpec{
			ReadFilterSpec: query.ReadFilterSpec{
				OrganizationID: orgID,
				BucketID:       bucketID,
				Bounds:         *bounds,
				Predicate:      spec.Filter,
			},
			GroupMode:       query.ToGroupMode(spec.GroupMode),
			GroupKeys:       spec.GroupKeys,
			AggregateMethod: spec.AggregateMethod,
		},
		a,
	), nil
}

type readWindowAggregateSource struct {
	Source
	reader   query.StorageReader
	readSpec query.ReadWindowAggregateSpec
}

func ReadWindowAggregateSource(id execute.DatasetID, r query.StorageReader, readSpec query.ReadWindowAggregateSpec, a execute.Administration) execute.Source {
	src := new(readWindowAggregateSource)

	src.id = id
	src.alloc = a.Allocator()

	src.reader = r
	src.readSpec = readSpec

	src.m = GetStorageDependencies(a.Context()).FromDeps.Metrics
	src.orgID = readSpec.OrganizationID
	src.op = readSpec.Name()

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

	return ReadWindowAggregateSource(
		id,
		deps.Reader,
		query.ReadWindowAggregateSpec{
			ReadFilterSpec: query.ReadFilterSpec{
				OrganizationID: orgID,
				BucketID:       bucketID,
				Bounds:         *bounds,
				Predicate:      spec.Filter,
			},
			Window: execute.Window{
				Every:  spec.WindowEvery,
				Period: spec.WindowEvery,
				Offset: spec.Offset,
			},
			Aggregates:     spec.Aggregates,
			CreateEmpty:    spec.CreateEmpty,
			TimeColumn:     spec.TimeColumn,
			ForceAggregate: spec.ForceAggregate,
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

	bounds := a.StreamContext().Bounds()
	return ReadTagKeysSource(
		dsid,
		deps.Reader,
		query.ReadTagKeysSpec{
			ReadFilterSpec: query.ReadFilterSpec{
				OrganizationID: orgID,
				BucketID:       bucketID,
				Bounds:         *bounds,
				Predicate:      spec.Filter,
			},
		},
		a,
	), nil
}

type readTagKeysSource struct {
	Source

	reader   query.StorageReader
	readSpec query.ReadTagKeysSpec
}

func ReadTagKeysSource(id execute.DatasetID, r query.StorageReader, readSpec query.ReadTagKeysSpec, a execute.Administration) execute.Source {
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

	bounds := a.StreamContext().Bounds()
	return ReadTagValuesSource(
		dsid,
		deps.Reader,
		query.ReadTagValuesSpec{
			ReadFilterSpec: query.ReadFilterSpec{
				OrganizationID: orgID,
				BucketID:       bucketID,
				Bounds:         *bounds,
				Predicate:      spec.Filter,
			},
			TagKey: spec.TagKey,
		},
		a,
	), nil
}

type readTagValuesSource struct {
	Source

	reader   query.StorageReader
	readSpec query.ReadTagValuesSpec
}

func ReadTagValuesSource(id execute.DatasetID, r query.StorageReader, readSpec query.ReadTagValuesSpec, a execute.Administration) execute.Source {
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
