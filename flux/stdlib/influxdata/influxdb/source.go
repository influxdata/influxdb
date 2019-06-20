package influxdb

import (
	"context"
	"errors"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/influxdb/tsdb/cursors"
)

func init() {
	execute.RegisterSource(ReadRangePhysKind, createReadFilterSource)
	execute.RegisterSource(ReadGroupPhysKind, createReadGroupSource)
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
}

func (s *Source) Run(ctx context.Context) {
	err := s.runner.run(ctx)
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

func ReadFilterSource(id execute.DatasetID, r Reader, readSpec ReadFilterSpec, alloc *memory.Allocator) execute.Source {
	src := new(readFilterSource)

	src.id = id
	src.alloc = alloc

	src.reader = r
	src.readSpec = readSpec

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
	ctx := a.Context()

	spec := s.(*ReadRangePhysSpec)

	bounds := a.StreamContext().Bounds()
	if bounds == nil {
		return nil, errors.New("nil bounds passed to from")
	}

	deps := a.Dependencies()[FromKind].(Dependencies)

	db, rp, err := spec.LookupDatabase(ctx, deps, a)
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
			Database:        db,
			RetentionPolicy: rp,
			Bounds:          *bounds,
			Predicate:       filter,
		},
		a.Allocator(),
	), nil
}

type readGroupSource struct {
	Source
	reader   Reader
	readSpec ReadGroupSpec
}

func ReadGroupSource(id execute.DatasetID, r Reader, readSpec ReadGroupSpec, alloc *memory.Allocator) execute.Source {
	src := new(readGroupSource)

	src.id = id
	src.alloc = alloc

	src.reader = r
	src.readSpec = readSpec

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
	ctx := a.Context()

	spec := s.(*ReadGroupPhysSpec)

	bounds := a.StreamContext().Bounds()
	if bounds == nil {
		return nil, errors.New("nil bounds passed to from")
	}

	deps := a.Dependencies()[FromKind].(Dependencies)

	db, rp, err := spec.LookupDatabase(ctx, deps, a)
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
				Database:        db,
				RetentionPolicy: rp,
				Bounds:          *bounds,
				Predicate:       filter,
			},
			GroupMode:       ToGroupMode(spec.GroupMode),
			GroupKeys:       spec.GroupKeys,
			AggregateMethod: spec.AggregateMethod,
		},
		a.Allocator(),
	), nil
}

func createReadTagKeysSource(prSpec plan.ProcedureSpec, dsid execute.DatasetID, a execute.Administration) (execute.Source, error) {
	ctx := a.Context()

	spec := prSpec.(*ReadTagKeysPhysSpec)
	deps := a.Dependencies()[FromKind].(Dependencies)

	db, rp, err := spec.LookupDatabase(ctx, deps, a)
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
				Database:        db,
				RetentionPolicy: rp,
				Bounds:          *bounds,
				Predicate:       filter,
			},
		},
		a.Allocator(),
	), nil
}

type readTagKeysSource struct {
	Source

	reader   Reader
	readSpec ReadTagKeysSpec
}

func ReadTagKeysSource(id execute.DatasetID, r Reader, readSpec ReadTagKeysSpec, alloc *memory.Allocator) execute.Source {
	src := &readTagKeysSource{
		reader:   r,
		readSpec: readSpec,
	}
	src.id = id
	src.alloc = alloc
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
	ctx := a.Context()

	spec := prSpec.(*ReadTagValuesPhysSpec)
	deps := a.Dependencies()[FromKind].(Dependencies)

	db, rp, err := spec.LookupDatabase(ctx, deps, a)
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
				Database:        db,
				RetentionPolicy: rp,
				Bounds:          *bounds,
				Predicate:       filter,
			},
			TagKey: spec.TagKey,
		},
		a.Allocator(),
	), nil
}

type readTagValuesSource struct {
	Source

	reader   Reader
	readSpec ReadTagValuesSpec
}

func ReadTagValuesSource(id execute.DatasetID, r Reader, readSpec ReadTagValuesSpec, alloc *memory.Allocator) execute.Source {
	src := &readTagValuesSource{
		reader:   r,
		readSpec: readSpec,
	}
	src.id = id
	src.alloc = alloc
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
