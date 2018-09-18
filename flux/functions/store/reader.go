package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/gogo/protobuf/types"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	fstorage "github.com/influxdata/flux/functions/storage"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/storage"
	ostorage "github.com/influxdata/influxdb/services/storage"
	"github.com/influxdata/influxdb/tsdb"
)

type storageTable interface {
	flux.Table
	Close()
	Done() chan struct{}
}

type storeReader struct {
	s storage.Store
}

func NewReader(s storage.Store) fstorage.Reader {
	return &storeReader{s: s}
}

func (r *storeReader) Read(ctx context.Context, rs fstorage.ReadSpec, start, stop execute.Time) (flux.TableIterator, error) {
	var predicate *ostorage.Predicate
	if rs.Predicate != nil {
		p, err := ToStoragePredicate(rs.Predicate)
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
	s         storage.Store
	readSpec  fstorage.ReadSpec
	predicate *ostorage.Predicate
}

func (bi *tableIterator) Do(f func(flux.Table) error) error {
	src := ostorage.ReadSource{Database: string(bi.readSpec.BucketID)}
	if i := strings.IndexByte(src.Database, '/'); i > -1 {
		src.RetentionPolicy = src.Database[i+1:]
		src.Database = src.Database[:i]
	}

	// Setup read request
	var req ostorage.ReadRequest
	if any, err := types.MarshalAny(&src); err != nil {
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
	} else if agg != ostorage.AggregateTypeNone {
		req.Aggregate = &ostorage.Aggregate{Type: agg}
	}

	switch {
	case req.Group != ostorage.GroupAll:
		rs, err := bi.s.GroupRead(bi.ctx, &req)
		if err != nil {
			return err
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

		if req.Hints.NoPoints() {
			return bi.handleReadNoPoints(f, rs)
		}
		return bi.handleRead(f, rs)
	}
}

func (bi *tableIterator) handleRead(f func(flux.Table) error, rs ostorage.ResultSet) error {
	defer func() {
		rs.Close()
		fmt.Println("handleRead: DONE")
	}()

READ:
	for rs.Next() {
		cur := rs.Cursor()
		if cur == nil {
			// no data for series key + field combination
			continue
		}

		key := groupKeyForSeries(rs.Tags(), &bi.readSpec, bi.bounds)
		var table storageTable

		switch cur := cur.(type) {
		case tsdb.IntegerArrayCursor:
			cols, defs := determineTableColsForSeries(rs.Tags(), flux.TInt)
			table = newIntegerTable(cur, bi.bounds, key, cols, rs.Tags(), defs)
		case tsdb.FloatArrayCursor:
			cols, defs := determineTableColsForSeries(rs.Tags(), flux.TFloat)
			table = newFloatTable(cur, bi.bounds, key, cols, rs.Tags(), defs)
		case tsdb.UnsignedArrayCursor:
			cols, defs := determineTableColsForSeries(rs.Tags(), flux.TUInt)
			table = newUnsignedTable(cur, bi.bounds, key, cols, rs.Tags(), defs)
		case tsdb.BooleanArrayCursor:
			cols, defs := determineTableColsForSeries(rs.Tags(), flux.TBool)
			table = newBooleanTable(cur, bi.bounds, key, cols, rs.Tags(), defs)
		case tsdb.StringArrayCursor:
			cols, defs := determineTableColsForSeries(rs.Tags(), flux.TString)
			table = newStringTable(cur, bi.bounds, key, cols, rs.Tags(), defs)
		default:
			panic(fmt.Sprintf("unreachable: %T", cur))
		}

		if table.Empty() {
			table.Close()
			continue
		}

		if err := f(table); err != nil {
			table.Close()
			return err
		}
		select {
		case <-table.Done():
		case <-bi.ctx.Done():
			fmt.Println("CANCELED")
			break READ
		}
	}
	return nil
}

func (bi *tableIterator) handleReadNoPoints(f func(flux.Table) error, rs ostorage.ResultSet) error {
	defer func() {
		rs.Close()
		fmt.Println("handleReadNoPoints: DONE")
	}()

READ:
	for rs.Next() {
		cur := rs.Cursor()
		if !hasPoints(cur) {
			// no data for series key + field combination
			continue
		}

		key := groupKeyForSeries(rs.Tags(), &bi.readSpec, bi.bounds)
		cols, defs := determineTableColsForSeries(rs.Tags(), flux.TString)
		table := newTableNoPoints(bi.bounds, key, cols, rs.Tags(), defs)

		if err := f(table); err != nil {
			table.Close()
			return err
		}
		select {
		case <-table.Done():
		case <-bi.ctx.Done():
			fmt.Println("CANCELED")
			break READ
		}
	}
	return nil
}

func (bi *tableIterator) handleGroupRead(f func(flux.Table) error, rs ostorage.GroupResultSet) error {
	defer func() {
		rs.Close()
		fmt.Println("handleGroupRead: DONE")
	}()
	gc := rs.Next()
READ:
	for gc != nil {
		var cur tsdb.Cursor
		for gc.Next() {
			cur = gc.Cursor()
			if cur != nil {
				break
			}
		}

		if cur == nil {
			gc = rs.Next()
			continue
		}

		key := groupKeyForGroup(gc.PartitionKeyVals(), &bi.readSpec, bi.bounds)
		var table storageTable

		switch cur := cur.(type) {
		case tsdb.IntegerArrayCursor:
			cols, defs := determineTableColsForGroup(gc.Keys(), flux.TInt)
			table = newIntegerGroupTable(gc, cur, bi.bounds, key, cols, gc.Tags(), defs)
		case tsdb.FloatArrayCursor:
			cols, defs := determineTableColsForGroup(gc.Keys(), flux.TFloat)
			table = newFloatGroupTable(gc, cur, bi.bounds, key, cols, gc.Tags(), defs)
		case tsdb.UnsignedArrayCursor:
			cols, defs := determineTableColsForGroup(gc.Keys(), flux.TUInt)
			table = newUnsignedGroupTable(gc, cur, bi.bounds, key, cols, gc.Tags(), defs)
		case tsdb.BooleanArrayCursor:
			cols, defs := determineTableColsForGroup(gc.Keys(), flux.TBool)
			table = newBooleanGroupTable(gc, cur, bi.bounds, key, cols, gc.Tags(), defs)
		case tsdb.StringArrayCursor:
			cols, defs := determineTableColsForGroup(gc.Keys(), flux.TString)
			table = newStringGroupTable(gc, cur, bi.bounds, key, cols, gc.Tags(), defs)
		default:
			panic(fmt.Sprintf("unreachable: %T", cur))
		}

		if err := f(table); err != nil {
			table.Close()
			return err
		}
		// Wait until the table has been read.
		select {
		case <-table.Done():
		case <-bi.ctx.Done():
			fmt.Println("CANCELED")
			break READ
		}

		gc = rs.Next()
	}
	return nil
}

func (bi *tableIterator) handleGroupReadNoPoints(f func(flux.Table) error, rs ostorage.GroupResultSet) error {
	defer func() {
		rs.Close()
		fmt.Println("handleGroupReadNoPoints: DONE")
	}()
	gc := rs.Next()
READ:
	for gc != nil {
		key := groupKeyForGroup(gc.PartitionKeyVals(), &bi.readSpec, bi.bounds)
		cols, defs := determineTableColsForGroup(gc.Keys(), flux.TString)
		table := newGroupTableNoPoints(gc, bi.bounds, key, cols, gc.Tags(), defs)

		if err := f(table); err != nil {
			table.Close()
			return err
		}
		// Wait until the table has been read.
		select {
		case <-table.Done():
		case <-bi.ctx.Done():
			fmt.Println("CANCELED")
			break READ
		}

		gc = rs.Next()
	}
	return nil
}

func determineAggregateMethod(agg string) (ostorage.Aggregate_AggregateType, error) {
	if agg == "" {
		return ostorage.AggregateTypeNone, nil
	}

	if t, ok := ostorage.Aggregate_AggregateType_value[strings.ToUpper(agg)]; ok {
		return ostorage.Aggregate_AggregateType(t), nil
	}
	return 0, fmt.Errorf("unknown aggregate type %q", agg)
}

func convertGroupMode(m fstorage.GroupMode) ostorage.ReadRequest_Group {
	switch m {
	case fstorage.GroupModeNone:
		return ostorage.GroupNone
	case fstorage.GroupModeBy:
		return ostorage.GroupBy
	case fstorage.GroupModeExcept:
		return ostorage.GroupExcept

	case fstorage.GroupModeDefault, fstorage.GroupModeAll:
		fallthrough
	default:
		return ostorage.GroupAll
	}
}

const (
	startColIdx = 0
	stopColIdx  = 1
	timeColIdx  = 2
	valueColIdx = 3
)

func determineTableColsForSeries(tags models.Tags, typ flux.DataType) ([]flux.ColMeta, [][]byte) {
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
	vs[0] = values.NewTimeValue(bnds.Start)
	cols[1] = flux.ColMeta{
		Label: execute.DefaultStopColLabel,
		Type:  flux.TTime,
	}
	vs[1] = values.NewTimeValue(bnds.Stop)
	switch readSpec.GroupMode {
	case fstorage.GroupModeBy:
		// group key in GroupKeys order, including tags in the GroupKeys slice
		for _, k := range readSpec.GroupKeys {
			if v := tags.Get([]byte(k)); len(v) > 0 {
				cols = append(cols, flux.ColMeta{
					Label: k,
					Type:  flux.TString,
				})
				vs = append(vs, values.NewStringValue(string(v)))
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
			vs = append(vs, values.NewStringValue(string(tags[i].Value)))
		}
	}
	return execute.NewGroupKey(cols, vs)
}

func determineTableColsForGroup(tagKeys [][]byte, typ flux.DataType) ([]flux.ColMeta, [][]byte) {
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
	vs[0] = values.NewTimeValue(bnds.Start)
	cols[1] = flux.ColMeta{
		Label: execute.DefaultStopColLabel,
		Type:  flux.TTime,
	}
	vs[1] = values.NewTimeValue(bnds.Stop)
	for i := range readSpec.GroupKeys {
		cols = append(cols, flux.ColMeta{
			Label: readSpec.GroupKeys[i],
			Type:  flux.TString,
		})
		vs = append(vs, values.NewStringValue(string(kv[i])))
	}
	return execute.NewGroupKey(cols, vs)
}
