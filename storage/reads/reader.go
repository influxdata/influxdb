package reads

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/gogo/protobuf/types"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/platform/models"
	fstorage "github.com/influxdata/platform/query/functions/inputs/storage"
	"github.com/influxdata/platform/storage/reads/datatypes"
	"github.com/influxdata/platform/tsdb/cursors"
)

type storageTable interface {
	flux.Table
	Close()
	Cancel()
}

type storeReader struct {
	s Store
}

func NewReader(s Store) fstorage.Reader {
	return &storeReader{s: s}
}

func (r *storeReader) Read(ctx context.Context, rs fstorage.ReadSpec, start, stop execute.Time) (flux.TableIterator, error) {
	var predicate *datatypes.Predicate
	if rs.Predicate != nil {
		p, err := toStoragePredicate(rs.Predicate)
		if err != nil {
			return nil, err
		}
		predicate = p
	}

	return &tableIterator{
		ctx:       ctx,
		bounds:    execute.Bounds{Start: start, Stop: stop},
		s:         r.s,
		readSpec:  rs,
		predicate: predicate,
	}, nil
}

func (r *storeReader) Close() {}

type tableIterator struct {
	ctx       context.Context
	bounds    execute.Bounds
	s         Store
	readSpec  fstorage.ReadSpec
	predicate *datatypes.Predicate
}

func (bi *tableIterator) Do(f func(flux.Table) error) error {
	src, err := bi.s.GetSource(bi.readSpec)
	if err != nil {
		return err
	}

	// Setup read request
	var req datatypes.ReadRequest
	if any, err := types.MarshalAny(src); err != nil {
		return err
	} else {
		req.ReadSource = any
	}
	req.Predicate = bi.predicate
	req.Descending = bi.readSpec.Descending
	req.TimestampRange.Start = int64(bi.bounds.Start)
	req.TimestampRange.End = int64(bi.bounds.Stop)
	req.Group = convertGroupMode(bi.readSpec.GroupMode)
	req.GroupKeys = bi.readSpec.GroupKeys
	req.SeriesLimit = bi.readSpec.SeriesLimit
	req.PointsLimit = bi.readSpec.PointsLimit
	req.SeriesOffset = bi.readSpec.SeriesOffset

	if req.PointsLimit == -1 {
		req.Hints.SetNoPoints()
	}

	if agg, err := determineAggregateMethod(bi.readSpec.AggregateMethod); err != nil {
		return err
	} else if agg != datatypes.AggregateTypeNone {
		req.Aggregate = &datatypes.Aggregate{Type: agg}
	}

	switch {
	case req.Group != datatypes.GroupAll:
		rs, err := bi.s.GroupRead(bi.ctx, &req)
		if err != nil {
			return err
		}

		if rs == nil {
			return nil
		}

		if req.Hints.NoPoints() {
			return bi.handleGroupReadNoPoints(f, rs)
		}
		return bi.handleGroupRead(f, rs)

	default:
		rs, err := bi.s.Read(bi.ctx, &req)
		if err != nil {
			return err
		}

		if rs == nil {
			return nil
		}

		if req.Hints.NoPoints() {
			return bi.handleReadNoPoints(f, rs)
		}
		return bi.handleRead(f, rs)
	}
}

func (bi *tableIterator) handleRead(f func(flux.Table) error, rs ResultSet) error {
	// these resources must be closed if not nil on return
	var (
		cur   cursors.Cursor
		table storageTable
	)

	defer func() {
		if table != nil {
			table.Close()
		}
		if cur != nil {
			cur.Close()
		}
		rs.Close()
	}()

READ:
	for rs.Next() {
		cur = rs.Cursor()
		if cur == nil {
			// no data for series key + field combination
			continue
		}

		key := groupKeyForSeries(rs.Tags(), &bi.readSpec, bi.bounds)
		done := make(chan struct{})
		switch typedCur := cur.(type) {
		case cursors.IntegerArrayCursor:
			cols, defs := determineTableColsForSeries(rs.Tags(), flux.TInt)
			table = newIntegerTable(done, typedCur, bi.bounds, key, cols, rs.Tags(), defs)
		case cursors.FloatArrayCursor:
			cols, defs := determineTableColsForSeries(rs.Tags(), flux.TFloat)
			table = newFloatTable(done, typedCur, bi.bounds, key, cols, rs.Tags(), defs)
		case cursors.UnsignedArrayCursor:
			cols, defs := determineTableColsForSeries(rs.Tags(), flux.TUInt)
			table = newUnsignedTable(done, typedCur, bi.bounds, key, cols, rs.Tags(), defs)
		case cursors.BooleanArrayCursor:
			cols, defs := determineTableColsForSeries(rs.Tags(), flux.TBool)
			table = newBooleanTable(done, typedCur, bi.bounds, key, cols, rs.Tags(), defs)
		case cursors.StringArrayCursor:
			cols, defs := determineTableColsForSeries(rs.Tags(), flux.TString)
			table = newStringTable(done, typedCur, bi.bounds, key, cols, rs.Tags(), defs)
		default:
			panic(fmt.Sprintf("unreachable: %T", typedCur))
		}

		cur = nil

		if !table.Empty() {
			if err := f(table); err != nil {
				table.Close()
				table = nil
				return err
			}
			select {
			case <-done:
			case <-bi.ctx.Done():
				table.Cancel()
				break READ
			}
		}

		table.Close()
		table = nil
	}
	return nil
}

func (bi *tableIterator) handleReadNoPoints(f func(flux.Table) error, rs ResultSet) error {
	// these resources must be closed if not nil on return
	var table storageTable

	defer func() {
		if table != nil {
			table.Close()
		}
		rs.Close()
	}()

READ:
	for rs.Next() {
		cur := rs.Cursor()
		if !hasPoints(cur) {
			// no data for series key + field combination
			continue
		}

		key := groupKeyForSeries(rs.Tags(), &bi.readSpec, bi.bounds)
		done := make(chan struct{})
		cols, defs := determineTableColsForSeries(rs.Tags(), flux.TString)
		table = newTableNoPoints(done, bi.bounds, key, cols, rs.Tags(), defs)

		if err := f(table); err != nil {
			table.Close()
			table = nil
			return err
		}
		select {
		case <-done:
		case <-bi.ctx.Done():
			table.Cancel()
			break READ
		}

		table.Close()
		table = nil
	}
	return nil
}

func (bi *tableIterator) handleGroupRead(f func(flux.Table) error, rs GroupResultSet) error {
	// these resources must be closed if not nil on return
	var (
		gc    GroupCursor
		cur   cursors.Cursor
		table storageTable
	)

	defer func() {
		if table != nil {
			table.Close()
		}
		if cur != nil {
			cur.Close()
		}
		if gc != nil {
			gc.Close()
		}
		rs.Close()
	}()

	gc = rs.Next()
READ:
	for gc != nil {
		for gc.Next() {
			cur = gc.Cursor()
			if cur != nil {
				break
			}
		}

		if cur == nil {
			gc.Close()
			gc = rs.Next()
			continue
		}

		key := groupKeyForGroup(gc.PartitionKeyVals(), &bi.readSpec, bi.bounds)
		done := make(chan struct{})
		switch typedCur := cur.(type) {
		case cursors.IntegerArrayCursor:
			cols, defs := determineTableColsForGroup(gc.Keys(), flux.TInt)
			table = newIntegerGroupTable(done, gc, typedCur, bi.bounds, key, cols, gc.Tags(), defs)
		case cursors.FloatArrayCursor:
			cols, defs := determineTableColsForGroup(gc.Keys(), flux.TFloat)
			table = newFloatGroupTable(done, gc, typedCur, bi.bounds, key, cols, gc.Tags(), defs)
		case cursors.UnsignedArrayCursor:
			cols, defs := determineTableColsForGroup(gc.Keys(), flux.TUInt)
			table = newUnsignedGroupTable(done, gc, typedCur, bi.bounds, key, cols, gc.Tags(), defs)
		case cursors.BooleanArrayCursor:
			cols, defs := determineTableColsForGroup(gc.Keys(), flux.TBool)
			table = newBooleanGroupTable(done, gc, typedCur, bi.bounds, key, cols, gc.Tags(), defs)
		case cursors.StringArrayCursor:
			cols, defs := determineTableColsForGroup(gc.Keys(), flux.TString)
			table = newStringGroupTable(done, gc, typedCur, bi.bounds, key, cols, gc.Tags(), defs)
		default:
			panic(fmt.Sprintf("unreachable: %T", typedCur))
		}

		// table owns these resources and is responsible for closing them
		cur = nil
		gc = nil

		if err := f(table); err != nil {
			table.Close()
			table = nil
			return err
		}
		select {
		case <-done:
		case <-bi.ctx.Done():
			table.Cancel()
			break READ
		}

		table.Close()
		table = nil

		gc = rs.Next()
	}
	return nil
}

func (bi *tableIterator) handleGroupReadNoPoints(f func(flux.Table) error, rs GroupResultSet) error {
	// these resources must be closed if not nil on return
	var (
		gc    GroupCursor
		table storageTable
	)

	defer func() {
		if table != nil {
			table.Close()
		}
		if gc != nil {
			gc.Close()
		}
		rs.Close()
	}()

	gc = rs.Next()
READ:
	for gc != nil {
		key := groupKeyForGroup(gc.PartitionKeyVals(), &bi.readSpec, bi.bounds)
		done := make(chan struct{})
		cols, defs := determineTableColsForGroup(gc.Keys(), flux.TString)
		table = newGroupTableNoPoints(done, bi.bounds, key, cols, defs)
		gc.Close()
		gc = nil

		if err := f(table); err != nil {
			table.Close()
			table = nil
			return err
		}
		select {
		case <-done:
		case <-bi.ctx.Done():
			table.Cancel()
			break READ
		}

		table.Close()
		table = nil

		gc = rs.Next()
	}
	return nil
}

func determineAggregateMethod(agg string) (datatypes.Aggregate_AggregateType, error) {
	if agg == "" {
		return datatypes.AggregateTypeNone, nil
	}

	if t, ok := datatypes.Aggregate_AggregateType_value[strings.ToUpper(agg)]; ok {
		return datatypes.Aggregate_AggregateType(t), nil
	}
	return 0, fmt.Errorf("unknown aggregate type %q", agg)
}

func convertGroupMode(m fstorage.GroupMode) datatypes.ReadRequest_Group {
	switch m {
	case fstorage.GroupModeNone:
		return datatypes.GroupNone
	case fstorage.GroupModeBy:
		return datatypes.GroupBy
	case fstorage.GroupModeExcept:
		return datatypes.GroupExcept

	case fstorage.GroupModeDefault, fstorage.GroupModeAll:
		fallthrough
	default:
		return datatypes.GroupAll
	}
}

const (
	startColIdx = 0
	stopColIdx  = 1
	timeColIdx  = 2
	valueColIdx = 3
)

func determineTableColsForSeries(tags models.Tags, typ flux.ColType) ([]flux.ColMeta, [][]byte) {
	cols := make([]flux.ColMeta, 4+len(tags))
	defs := make([][]byte, 4+len(tags))
	cols[startColIdx] = flux.ColMeta{
		Label: execute.DefaultStartColLabel,
		Type:  flux.TTime,
	}
	cols[stopColIdx] = flux.ColMeta{
		Label: execute.DefaultStopColLabel,
		Type:  flux.TTime,
	}
	cols[timeColIdx] = flux.ColMeta{
		Label: execute.DefaultTimeColLabel,
		Type:  flux.TTime,
	}
	cols[valueColIdx] = flux.ColMeta{
		Label: execute.DefaultValueColLabel,
		Type:  typ,
	}
	for j, tag := range tags {
		cols[4+j] = flux.ColMeta{
			Label: string(tag.Key),
			Type:  flux.TString,
		}
		defs[4+j] = []byte("")
	}
	return cols, defs
}

func groupKeyForSeries(tags models.Tags, readSpec *fstorage.ReadSpec, bnds execute.Bounds) flux.GroupKey {
	cols := make([]flux.ColMeta, 2, len(tags))
	vs := make([]values.Value, 2, len(tags))
	cols[0] = flux.ColMeta{
		Label: execute.DefaultStartColLabel,
		Type:  flux.TTime,
	}
	vs[0] = values.NewTime(bnds.Start)
	cols[1] = flux.ColMeta{
		Label: execute.DefaultStopColLabel,
		Type:  flux.TTime,
	}
	vs[1] = values.NewTime(bnds.Stop)
	switch readSpec.GroupMode {
	case fstorage.GroupModeBy:
		// group key in GroupKeys order, including tags in the GroupKeys slice
		for _, k := range readSpec.GroupKeys {
			bk := []byte(k)
			for _, t := range tags {
				if bytes.Equal(t.Key, bk) && len(t.Value) > 0 {
					cols = append(cols, flux.ColMeta{
						Label: k,
						Type:  flux.TString,
					})
					vs = append(vs, values.NewString(string(t.Value)))
				}
			}
		}
	case fstorage.GroupModeExcept:
		// group key in GroupKeys order, skipping tags in the GroupKeys slice
		panic("not implemented")
	case fstorage.GroupModeDefault, fstorage.GroupModeAll:
		for i := range tags {
			cols = append(cols, flux.ColMeta{
				Label: string(tags[i].Key),
				Type:  flux.TString,
			})
			vs = append(vs, values.NewString(string(tags[i].Value)))
		}
	}
	return execute.NewGroupKey(cols, vs)
}

func determineTableColsForGroup(tagKeys [][]byte, typ flux.ColType) ([]flux.ColMeta, [][]byte) {
	cols := make([]flux.ColMeta, 4+len(tagKeys))
	defs := make([][]byte, 4+len(tagKeys))
	cols[startColIdx] = flux.ColMeta{
		Label: execute.DefaultStartColLabel,
		Type:  flux.TTime,
	}
	cols[stopColIdx] = flux.ColMeta{
		Label: execute.DefaultStopColLabel,
		Type:  flux.TTime,
	}
	cols[timeColIdx] = flux.ColMeta{
		Label: execute.DefaultTimeColLabel,
		Type:  flux.TTime,
	}
	cols[valueColIdx] = flux.ColMeta{
		Label: execute.DefaultValueColLabel,
		Type:  typ,
	}
	for j, tag := range tagKeys {
		cols[4+j] = flux.ColMeta{
			Label: string(tag),
			Type:  flux.TString,
		}
		defs[4+j] = []byte("")

	}
	return cols, defs
}

func groupKeyForGroup(kv [][]byte, readSpec *fstorage.ReadSpec, bnds execute.Bounds) flux.GroupKey {
	cols := make([]flux.ColMeta, 2, len(readSpec.GroupKeys)+2)
	vs := make([]values.Value, 2, len(readSpec.GroupKeys)+2)
	cols[0] = flux.ColMeta{
		Label: execute.DefaultStartColLabel,
		Type:  flux.TTime,
	}
	vs[0] = values.NewTime(bnds.Start)
	cols[1] = flux.ColMeta{
		Label: execute.DefaultStopColLabel,
		Type:  flux.TTime,
	}
	vs[1] = values.NewTime(bnds.Stop)
	for i := range readSpec.GroupKeys {
		cols = append(cols, flux.ColMeta{
			Label: readSpec.GroupKeys[i],
			Type:  flux.TString,
		})
		vs = append(vs, values.NewString(string(kv[i])))
	}
	return execute.NewGroupKey(cols, vs)
}
