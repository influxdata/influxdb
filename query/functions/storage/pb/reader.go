package pb

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/influxdata/ifql/functions/storage"
	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/yarpc"
	"github.com/pkg/errors"
)

func NewReader(hl storage.HostLookup) (*reader, error) {
	// TODO(nathanielc): Watch for host changes
	hosts := hl.Hosts()
	conns := make([]connection, len(hosts))
	for i, h := range hosts {
		conn, err := yarpc.Dial(h)
		if err != nil {
			return nil, err
		}
		conns[i] = connection{
			host:   h,
			conn:   conn,
			client: NewStorageClient(conn),
		}
	}
	return &reader{
		conns: conns,
	}, nil
}

type reader struct {
	conns []connection
}

type connection struct {
	host   string
	conn   *yarpc.ClientConn
	client StorageClient
}

func (sr *reader) Read(ctx context.Context, trace map[string]string, readSpec storage.ReadSpec, start, stop execute.Time) (execute.BlockIterator, error) {
	var predicate *Predicate
	if readSpec.Predicate != nil {
		p, err := ToStoragePredicate(readSpec.Predicate)
		if err != nil {
			return nil, err
		}
		predicate = p
	}

	bi := &bockIterator{
		ctx:   ctx,
		trace: trace,
		bounds: execute.Bounds{
			Start: start,
			Stop:  stop,
		},
		conns:     sr.conns,
		readSpec:  readSpec,
		predicate: predicate,
	}
	return bi, nil
}

func (sr *reader) Close() {
	for _, conn := range sr.conns {
		_ = conn.conn.Close()
	}
}

type bockIterator struct {
	ctx       context.Context
	trace     map[string]string
	bounds    execute.Bounds
	conns     []connection
	readSpec  storage.ReadSpec
	predicate *Predicate
}

func (bi *bockIterator) Do(f func(execute.Block) error) error {
	// Setup read request
	var req ReadRequest
	req.Database = string(bi.readSpec.BucketID)
	req.Predicate = bi.predicate
	req.Descending = bi.readSpec.Descending
	req.TimestampRange.Start = int64(bi.bounds.Start)
	req.TimestampRange.End = int64(bi.bounds.Stop)
	req.Grouping = bi.readSpec.GroupKeys

	req.SeriesLimit = bi.readSpec.SeriesLimit
	req.PointsLimit = bi.readSpec.PointsLimit
	req.SeriesOffset = bi.readSpec.SeriesOffset
	req.Trace = bi.trace

	if agg, err := determineAggregateMethod(bi.readSpec.AggregateMethod); err != nil {
		return err
	} else if agg != AggregateTypeNone {
		req.Aggregate = &Aggregate{Type: agg}
	}

	streams := make([]*streamState, 0, len(bi.conns))
	for _, c := range bi.conns {
		if len(bi.readSpec.Hosts) > 0 {
			// Filter down to only hosts provided
			found := false
			for _, h := range bi.readSpec.Hosts {
				if c.host == h {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		stream, err := c.client.Read(bi.ctx, &req)
		if err != nil {
			return err
		}
		streams = append(streams, &streamState{
			stream:   stream,
			readSpec: &bi.readSpec,
		})
	}
	ms := &mergedStreams{
		streams: streams,
	}

	for ms.more() {
		if p := ms.peek(); readFrameType(p) != seriesType {
			//This means the consumer didn't read all the data off the block
			return errors.New("internal error: short read")
		}
		frame := ms.next()
		s := frame.GetSeries()
		typ := convertDataType(s.DataType)
		key := partitionKeyForSeries(s, &bi.readSpec)
		cols := bi.determineBlockCols(s, typ)
		block := newBlock(bi.bounds, key, cols, ms, &bi.readSpec, s.Tags)

		if err := f(block); err != nil {
			// TODO(nathanielc): Close streams since we have abandoned the request
			return err
		}
		// Wait until the block has been read.
		block.wait()
	}
	return nil
}

func determineAggregateMethod(agg string) (Aggregate_AggregateType, error) {
	if agg == "" {
		return AggregateTypeNone, nil
	}

	if t, ok := Aggregate_AggregateType_value[strings.ToUpper(agg)]; ok {
		return Aggregate_AggregateType(t), nil
	}
	return 0, fmt.Errorf("unknown aggregate type %q", agg)
}

func convertDataType(t ReadResponse_DataType) execute.DataType {
	switch t {
	case DataTypeFloat:
		return execute.TFloat
	case DataTypeInteger:
		return execute.TInt
	case DataTypeUnsigned:
		return execute.TUInt
	case DataTypeBoolean:
		return execute.TBool
	case DataTypeString:
		return execute.TString
	default:
		return execute.TInvalid
	}
}

const (
	startColIdx = 0
	stopColIdx  = 1
	timeColIdx  = 2
	valueColIdx = 3
)

func (bi *bockIterator) determineBlockCols(s *ReadResponse_SeriesFrame, typ execute.DataType) []execute.ColMeta {
	cols := make([]execute.ColMeta, 4+len(s.Tags))
	cols[startColIdx] = execute.ColMeta{
		Label: execute.DefaultStartColLabel,
		Type:  execute.TTime,
	}
	cols[stopColIdx] = execute.ColMeta{
		Label: execute.DefaultStopColLabel,
		Type:  execute.TTime,
	}
	cols[timeColIdx] = execute.ColMeta{
		Label: execute.DefaultTimeColLabel,
		Type:  execute.TTime,
	}
	cols[valueColIdx] = execute.ColMeta{
		Label: execute.DefaultValueColLabel,
		Type:  typ,
	}
	for j, tag := range s.Tags {
		cols[4+j] = execute.ColMeta{
			Label: string(tag.Key),
			Type:  execute.TString,
		}
	}
	return cols
}

func partitionKeyForSeries(s *ReadResponse_SeriesFrame, readSpec *storage.ReadSpec) execute.PartitionKey {
	cols := make([]execute.ColMeta, 0, len(s.Tags))
	values := make([]interface{}, 0, len(s.Tags))
	if len(readSpec.GroupKeys) > 0 {
		for _, tag := range s.Tags {
			if !execute.ContainsStr(readSpec.GroupKeys, string(tag.Key)) {
				continue
			}
			cols = append(cols, execute.ColMeta{
				Label: string(tag.Key),
				Type:  execute.TString,
			})
			values = append(values, string(tag.Value))
		}
	} else if len(readSpec.GroupExcept) > 0 {
		for _, tag := range s.Tags {
			if !execute.ContainsStr(readSpec.GroupExcept, string(tag.Key)) {
				continue
			}
			cols = append(cols, execute.ColMeta{
				Label: string(tag.Key),
				Type:  execute.TString,
			})
			values = append(values, string(tag.Value))
		}
	} else if !readSpec.MergeAll {
		for _, tag := range s.Tags {
			cols = append(cols, execute.ColMeta{
				Label: string(tag.Key),
				Type:  execute.TString,
			})
			values = append(values, string(tag.Value))
		}

	}
	return execute.NewPartitionKey(cols, values)
}

// block implement OneTimeBlock as it can only be read once.
// Since it can only be read once it is also a ValueIterator for itself.
type block struct {
	bounds execute.Bounds
	key    execute.PartitionKey
	cols   []execute.ColMeta

	// cache of the tags on the current series.
	// len(tags) == len(colMeta)
	tags [][]byte

	readSpec *storage.ReadSpec

	done chan struct{}

	ms *mergedStreams

	// The current number of records in memory
	l int
	// colBufs are the buffers for the given columns.
	colBufs []interface{}

	// resuable buffer for the time column
	timeBuf []execute.Time

	// resuable buffers for the different types of values
	boolBuf   []bool
	intBuf    []int64
	uintBuf   []uint64
	floatBuf  []float64
	stringBuf []string

	err error
}

func newBlock(
	bounds execute.Bounds,
	key execute.PartitionKey,
	cols []execute.ColMeta,
	ms *mergedStreams,
	readSpec *storage.ReadSpec,
	tags []Tag,
) *block {
	b := &block{
		bounds:   bounds,
		key:      key,
		tags:     make([][]byte, len(cols)),
		colBufs:  make([]interface{}, len(cols)),
		cols:     cols,
		readSpec: readSpec,
		ms:       ms,
		done:     make(chan struct{}),
	}
	b.readTags(tags)
	return b
}

func (b *block) RefCount(n int) {
	//TODO(nathanielc): Have the storageBlock consume the Allocator,
	// once we have zero-copy serialization over the network
}

func (b *block) Err() error { return b.err }

func (b *block) wait() {
	<-b.done
}

func (b *block) Key() execute.PartitionKey {
	return b.key
}
func (b *block) Cols() []execute.ColMeta {
	return b.cols
}

// onetime satisfies the OneTimeBlock interface since this block may only be read once.
func (b *block) onetime() {}
func (b *block) Do(f func(execute.ColReader) error) error {
	defer close(b.done)
	for b.advance() {
		if err := f(b); err != nil {
			return err
		}
	}
	return b.err
}

func (b *block) Len() int {
	return b.l
}

func (b *block) Bools(j int) []bool {
	execute.CheckColType(b.cols[j], execute.TBool)
	return b.colBufs[j].([]bool)
}
func (b *block) Ints(j int) []int64 {
	execute.CheckColType(b.cols[j], execute.TInt)
	return b.colBufs[j].([]int64)
}
func (b *block) UInts(j int) []uint64 {
	execute.CheckColType(b.cols[j], execute.TUInt)
	return b.colBufs[j].([]uint64)
}
func (b *block) Floats(j int) []float64 {
	execute.CheckColType(b.cols[j], execute.TFloat)
	return b.colBufs[j].([]float64)
}
func (b *block) Strings(j int) []string {
	execute.CheckColType(b.cols[j], execute.TString)
	return b.colBufs[j].([]string)
}
func (b *block) Times(j int) []execute.Time {
	execute.CheckColType(b.cols[j], execute.TTime)
	return b.colBufs[j].([]execute.Time)
}

// readTags populates b.tags with the provided tags
func (b *block) readTags(tags []Tag) {
	for j := range b.tags {
		b.tags[j] = nil
	}
	for _, t := range tags {
		k := string(t.Key)
		j := execute.ColIdx(k, b.cols)
		b.tags[j] = t.Value
	}
}

func (b *block) advance() bool {
	for b.ms.more() {
		//reset buffers
		b.timeBuf = b.timeBuf[0:0]
		b.boolBuf = b.boolBuf[0:0]
		b.intBuf = b.intBuf[0:0]
		b.uintBuf = b.uintBuf[0:0]
		b.stringBuf = b.stringBuf[0:0]
		b.floatBuf = b.floatBuf[0:0]

		switch p := b.ms.peek(); readFrameType(p) {
		case seriesType:
			if !b.ms.key().Equal(b.key) {
				// We have reached the end of data for this block
				return false
			}
			s := p.GetSeries()
			b.readTags(s.Tags)

			// Advance to next frame
			b.ms.next()

			if b.readSpec.PointsLimit == -1 {
				// do not expect points frames
				b.l = 0
				return true
			}
		case boolPointsType:
			if b.cols[valueColIdx].Type != execute.TBool {
				b.err = fmt.Errorf("value type changed from %s -> %s", b.cols[valueColIdx].Type, execute.TBool)
				// TODO: Add error handling
				// Type changed,
				return false
			}
			// read next frame
			frame := b.ms.next()
			p := frame.GetBooleanPoints()
			l := len(p.Timestamps)
			b.l = l
			if l > cap(b.timeBuf) {
				b.timeBuf = make([]execute.Time, l)
			} else {
				b.timeBuf = b.timeBuf[:l]
			}
			if l > cap(b.boolBuf) {
				b.boolBuf = make([]bool, l)
			} else {
				b.boolBuf = b.boolBuf[:l]
			}

			for i, c := range p.Timestamps {
				b.timeBuf[i] = execute.Time(c)
				b.boolBuf[i] = p.Values[i]
			}
			b.colBufs[timeColIdx] = b.timeBuf
			b.colBufs[valueColIdx] = b.boolBuf
			b.appendTags()
			b.appendBounds()
			return true
		case intPointsType:
			if b.cols[valueColIdx].Type != execute.TInt {
				b.err = fmt.Errorf("value type changed from %s -> %s", b.cols[valueColIdx].Type, execute.TInt)
				// TODO: Add error handling
				// Type changed,
				return false
			}
			// read next frame
			frame := b.ms.next()
			p := frame.GetIntegerPoints()
			l := len(p.Timestamps)
			b.l = l
			if l > cap(b.timeBuf) {
				b.timeBuf = make([]execute.Time, l)
			} else {
				b.timeBuf = b.timeBuf[:l]
			}
			if l > cap(b.uintBuf) {
				b.intBuf = make([]int64, l)
			} else {
				b.intBuf = b.intBuf[:l]
			}

			for i, c := range p.Timestamps {
				b.timeBuf[i] = execute.Time(c)
				b.intBuf[i] = p.Values[i]
			}
			b.colBufs[timeColIdx] = b.timeBuf
			b.colBufs[valueColIdx] = b.intBuf
			b.appendTags()
			b.appendBounds()
			return true
		case uintPointsType:
			if b.cols[valueColIdx].Type != execute.TUInt {
				b.err = fmt.Errorf("value type changed from %s -> %s", b.cols[valueColIdx].Type, execute.TUInt)
				// TODO: Add error handling
				// Type changed,
				return false
			}
			// read next frame
			frame := b.ms.next()
			p := frame.GetUnsignedPoints()
			l := len(p.Timestamps)
			b.l = l
			if l > cap(b.timeBuf) {
				b.timeBuf = make([]execute.Time, l)
			} else {
				b.timeBuf = b.timeBuf[:l]
			}
			if l > cap(b.intBuf) {
				b.uintBuf = make([]uint64, l)
			} else {
				b.uintBuf = b.uintBuf[:l]
			}

			for i, c := range p.Timestamps {
				b.timeBuf[i] = execute.Time(c)
				b.uintBuf[i] = p.Values[i]
			}
			b.colBufs[timeColIdx] = b.timeBuf
			b.colBufs[valueColIdx] = b.uintBuf
			b.appendTags()
			b.appendBounds()
			return true
		case floatPointsType:
			if b.cols[valueColIdx].Type != execute.TFloat {
				b.err = fmt.Errorf("value type changed from %s -> %s", b.cols[valueColIdx].Type, execute.TFloat)
				// TODO: Add error handling
				// Type changed,
				return false
			}
			// read next frame
			frame := b.ms.next()
			p := frame.GetFloatPoints()

			l := len(p.Timestamps)
			b.l = l
			if l > cap(b.timeBuf) {
				b.timeBuf = make([]execute.Time, l)
			} else {
				b.timeBuf = b.timeBuf[:l]
			}
			if l > cap(b.floatBuf) {
				b.floatBuf = make([]float64, l)
			} else {
				b.floatBuf = b.floatBuf[:l]
			}

			for i, c := range p.Timestamps {
				b.timeBuf[i] = execute.Time(c)
				b.floatBuf[i] = p.Values[i]
			}
			b.colBufs[timeColIdx] = b.timeBuf
			b.colBufs[valueColIdx] = b.floatBuf
			b.appendTags()
			b.appendBounds()
			return true
		case stringPointsType:
			if b.cols[valueColIdx].Type != execute.TString {
				b.err = fmt.Errorf("value type changed from %s -> %s", b.cols[valueColIdx].Type, execute.TString)
				// TODO: Add error handling
				// Type changed,
				return false
			}
			// read next frame
			frame := b.ms.next()
			p := frame.GetStringPoints()

			l := len(p.Timestamps)
			b.l = l
			if l > cap(b.timeBuf) {
				b.timeBuf = make([]execute.Time, l)
			} else {
				b.timeBuf = b.timeBuf[:l]
			}
			if l > cap(b.stringBuf) {
				b.stringBuf = make([]string, l)
			} else {
				b.stringBuf = b.stringBuf[:l]
			}

			for i, c := range p.Timestamps {
				b.timeBuf[i] = execute.Time(c)
				b.stringBuf[i] = p.Values[i]
			}
			b.colBufs[timeColIdx] = b.timeBuf
			b.colBufs[valueColIdx] = b.stringBuf
			b.appendTags()
			b.appendBounds()
			return true
		}
	}
	return false
}

// appendTags fills the colBufs for the tag columns with the tag value.
func (b *block) appendTags() {
	for j := range b.cols {
		v := b.tags[j]
		if v != nil {
			if b.colBufs[j] == nil {
				b.colBufs[j] = make([]string, b.l)
			}
			colBuf := b.colBufs[j].([]string)
			if cap(colBuf) < b.l {
				colBuf = make([]string, b.l)
			} else {
				colBuf = colBuf[:b.l]
			}
			vStr := string(v)
			for i := range colBuf {
				colBuf[i] = vStr
			}
			b.colBufs[j] = colBuf
		}
	}
}

// appendBounds fills the colBufs for the time bounds
func (b *block) appendBounds() {
	bounds := []execute.Time{b.bounds.Start, b.bounds.Stop}
	for j := range []int{startColIdx, stopColIdx} {
		if b.colBufs[j] == nil {
			b.colBufs[j] = make([]execute.Time, b.l)
		}
		colBuf := b.colBufs[j].([]execute.Time)
		if cap(colBuf) < b.l {
			colBuf = make([]execute.Time, b.l)
		} else {
			colBuf = colBuf[:b.l]
		}
		for i := range colBuf {
			colBuf[i] = bounds[j]
		}
		b.colBufs[j] = colBuf
	}
}

type streamState struct {
	stream     Storage_ReadClient
	rep        ReadResponse
	currentKey execute.PartitionKey
	readSpec   *storage.ReadSpec
	finished   bool
}

func (s *streamState) peek() ReadResponse_Frame {
	return s.rep.Frames[0]
}

func (s *streamState) more() bool {
	if s.finished {
		return false
	}
	if len(s.rep.Frames) > 0 {
		return true
	}
	if err := s.stream.RecvMsg(&s.rep); err != nil {
		s.finished = true
		if err == io.EOF {
			// We are done
			return false
		}
		//TODO add proper error handling
		return false
	}
	if len(s.rep.Frames) == 0 {
		return false
	}
	s.computeKey()
	return true
}

func (s *streamState) key() execute.PartitionKey {
	return s.currentKey
}

func (s *streamState) computeKey() {
	// Determine new currentKey
	if p := s.peek(); readFrameType(p) == seriesType {
		series := p.GetSeries()
		s.currentKey = partitionKeyForSeries(series, s.readSpec)
	}
}
func (s *streamState) next() ReadResponse_Frame {
	frame := s.rep.Frames[0]
	s.rep.Frames = s.rep.Frames[1:]
	if len(s.rep.Frames) > 0 {
		s.computeKey()
	}
	return frame
}

type mergedStreams struct {
	streams    []*streamState
	currentKey execute.PartitionKey
	i          int
}

func (s *mergedStreams) key() execute.PartitionKey {
	if len(s.streams) == 1 {
		return s.streams[0].key()
	}
	return s.currentKey
}
func (s *mergedStreams) peek() ReadResponse_Frame {
	return s.streams[s.i].peek()
}

func (s *mergedStreams) next() ReadResponse_Frame {
	return s.streams[s.i].next()
}

func (s *mergedStreams) more() bool {
	// Optimze for the case of just one stream
	if len(s.streams) == 1 {
		return s.streams[0].more()
	}
	if s.i < 0 {
		return false
	}
	if s.currentKey == nil {
		return s.determineNewKey()
	}
	if s.streams[s.i].more() {
		if s.streams[s.i].key().Equal(s.currentKey) {
			return true
		}
		return s.advance()
	}
	return s.advance()
}

func (s *mergedStreams) advance() bool {
	s.i++
	if s.i == len(s.streams) {
		if !s.determineNewKey() {
			// no new data on any stream
			return false
		}
	}
	return s.more()
}

func (s *mergedStreams) determineNewKey() bool {
	minIdx := -1
	var minKey execute.PartitionKey
	for i, stream := range s.streams {
		if !stream.more() {
			continue
		}
		k := stream.key()
		if k.Less(minKey) {
			minIdx = i
			minKey = k
		}
	}
	s.currentKey = minKey
	s.i = minIdx
	return s.i >= 0
}

type frameType int

const (
	seriesType frameType = iota
	boolPointsType
	intPointsType
	uintPointsType
	floatPointsType
	stringPointsType
)

func readFrameType(frame ReadResponse_Frame) frameType {
	switch frame.Data.(type) {
	case *ReadResponse_Frame_Series:
		return seriesType
	case *ReadResponse_Frame_BooleanPoints:
		return boolPointsType
	case *ReadResponse_Frame_IntegerPoints:
		return intPointsType
	case *ReadResponse_Frame_UnsignedPoints:
		return uintPointsType
	case *ReadResponse_Frame_FloatPoints:
		return floatPointsType
	case *ReadResponse_Frame_StringPoints:
		return stringPointsType
	default:
		panic(fmt.Errorf("unknown read response frame type: %T", frame.Data))
	}
}
