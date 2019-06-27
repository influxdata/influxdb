package reads

import (
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb/cursors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var (
	// ErrPartitionKeyOrder means the partition keys for a
	// GroupResultSetStreamReader were incorrectly ordered.
	ErrPartitionKeyOrder = errors.New("invalid partition key order")

	// ErrStreamNoData means the StreamReader repeatedly returned no data
	// when calling Recv
	ErrStreamNoData = errors.New("peekFrame: no data")
)

// peekFrameRetries specifies the number of times peekFrame will
// retry before returning ErrStreamNoData when StreamReader.Recv
// returns an empty result.
const peekFrameRetries = 2

type StreamReader interface {
	Recv() (*datatypes.ReadResponse, error)
}

// statistics is the interface which wraps the Stats method.
type statistics interface {
	Stats() cursors.CursorStats
}

var zeroStatistics statistics = &emptyStatistics{}

type emptyStatistics struct{}

func (*emptyStatistics) Stats() cursors.CursorStats {
	return cursors.CursorStats{}
}

type StreamClient interface {
	StreamReader
	grpc.ClientStream
}

// StorageReadClient adapts a grpc client to implement the cursors.Statistics
// interface and read the statistics from the gRPC trailer.
type StorageReadClient struct {
	client  StreamClient
	trailer metadata.MD
}

// NewStorageReadClient returns a new StorageReadClient which implements
// StreamReader and reads the gRPC trailer to return CursorStats.
func NewStorageReadClient(client StreamClient) *StorageReadClient {
	return &StorageReadClient{client: client}
}

func (rc *StorageReadClient) Recv() (res *datatypes.ReadResponse, err error) {
	res, err = rc.client.Recv()
	if err != nil {
		rc.trailer = rc.client.Trailer()
	}
	return res, err
}

func (rc *StorageReadClient) Stats() (stats cursors.CursorStats) {
	for _, s := range rc.trailer.Get("scanned-bytes") {
		v, err := strconv.Atoi(s)
		if err != nil {
			continue
		}
		stats.ScannedBytes += v
	}
	for _, s := range rc.trailer.Get("scanned-values") {
		v, err := strconv.Atoi(s)
		if err != nil {
			continue
		}
		stats.ScannedValues += v
	}
	return stats
}

type ResultSetStreamReader struct {
	fr  frameReader
	cur cursorReaders

	tags models.Tags
	prev models.Tags
}

func NewResultSetStreamReader(stream StreamReader) *ResultSetStreamReader {
	r := &ResultSetStreamReader{fr: frameReader{s: stream, state: stateReadSeries}}
	r.fr.init()
	r.cur.setFrameReader(&r.fr)
	return r
}

func (r *ResultSetStreamReader) Err() error             { return r.fr.err }
func (r *ResultSetStreamReader) Close()                 { r.fr.state = stateDone }
func (r *ResultSetStreamReader) Cursor() cursors.Cursor { return r.cur.cursor() }
func (r *ResultSetStreamReader) Stats() cursors.CursorStats {
	return r.fr.stats.Stats()
}

func (r *ResultSetStreamReader) Next() bool {
	if r.fr.state == stateReadSeries {
		return r.readSeriesFrame()
	}

	if r.fr.state == stateDone || r.fr.state == stateReadErr {
		return false
	}

	r.fr.setErr(fmt.Errorf("expected reader in state %v, was in state %v", stateReadSeries, r.fr.state))

	return false
}

func (r *ResultSetStreamReader) readSeriesFrame() bool {
	f := r.fr.peekFrame()
	if f == nil {
		return false
	}
	r.fr.nextFrame()

	if sf, ok := f.Data.(*datatypes.ReadResponse_Frame_Series); ok {
		r.fr.state = stateReadPoints

		r.prev, r.tags = r.tags, r.prev

		if cap(r.tags) < len(sf.Series.Tags) {
			r.tags = make(models.Tags, len(sf.Series.Tags))
		} else {
			r.tags = r.tags[:len(sf.Series.Tags)]
		}

		for i := range sf.Series.Tags {
			r.tags[i].Key = sf.Series.Tags[i].Key
			r.tags[i].Value = sf.Series.Tags[i].Value
		}

		r.cur.nextType = sf.Series.DataType

		return true
	} else {
		r.fr.setErr(fmt.Errorf("expected series frame, got %T", f.Data))
	}

	return false
}

func (r *ResultSetStreamReader) Tags() models.Tags {
	return r.tags
}

type GroupResultSetStreamReader struct {
	fr frameReader
	gc groupCursorStreamReader
}

func NewGroupResultSetStreamReader(stream StreamReader) *GroupResultSetStreamReader {
	r := &GroupResultSetStreamReader{fr: frameReader{s: stream, state: stateReadGroup}}
	r.fr.init()
	r.gc.fr = &r.fr
	r.gc.cur.setFrameReader(&r.fr)
	return r
}

func (r *GroupResultSetStreamReader) Err() error { return r.fr.err }

func (r *GroupResultSetStreamReader) Next() GroupCursor {
	if r.fr.state == stateReadGroup {
		return r.readGroupFrame()
	}

	if r.fr.state == stateDone || r.fr.state == stateReadErr {
		return nil
	}

	r.fr.setErr(fmt.Errorf("expected reader in state %v, was in state %v", stateReadGroup, r.fr.state))

	return nil
}

func (r *GroupResultSetStreamReader) readGroupFrame() GroupCursor {
	f := r.fr.peekFrame()
	if f == nil {
		return nil
	}
	r.fr.nextFrame()

	if sf, ok := f.Data.(*datatypes.ReadResponse_Frame_Group); ok {
		r.fr.state = stateReadSeries

		if cap(r.gc.tagKeys) < len(sf.Group.TagKeys) {
			r.gc.tagKeys = make([][]byte, len(sf.Group.TagKeys))
		} else {
			r.gc.tagKeys = r.gc.tagKeys[:len(sf.Group.TagKeys)]
		}
		copy(r.gc.tagKeys, sf.Group.TagKeys)

		r.gc.partitionKeyVals, r.gc.prevKey = r.gc.prevKey, r.gc.partitionKeyVals

		if cap(r.gc.partitionKeyVals) < len(sf.Group.PartitionKeyVals) {
			r.gc.partitionKeyVals = make([][]byte, len(sf.Group.PartitionKeyVals))
		} else {
			r.gc.partitionKeyVals = r.gc.partitionKeyVals[:len(sf.Group.PartitionKeyVals)]
		}

		copy(r.gc.partitionKeyVals, sf.Group.PartitionKeyVals)

		if comparePartitionKey(r.gc.partitionKeyVals, r.gc.prevKey, nilSortHi) == 1 || r.gc.prevKey == nil {
			return &r.gc
		}

		r.fr.setErr(ErrPartitionKeyOrder)
	} else {
		r.fr.setErr(fmt.Errorf("expected group frame, got %T", f.Data))
	}

	return nil
}

func (r *GroupResultSetStreamReader) Close() {
	r.fr.state = stateDone
}

type groupCursorStreamReader struct {
	fr  *frameReader
	cur cursorReaders

	tagKeys          [][]byte
	partitionKeyVals [][]byte
	prevKey          [][]byte
	tags             models.Tags
}

func (gc *groupCursorStreamReader) Err() error                 { return gc.fr.err }
func (gc *groupCursorStreamReader) Tags() models.Tags          { return gc.tags }
func (gc *groupCursorStreamReader) Keys() [][]byte             { return gc.tagKeys }
func (gc *groupCursorStreamReader) PartitionKeyVals() [][]byte { return gc.partitionKeyVals }
func (gc *groupCursorStreamReader) Cursor() cursors.Cursor     { return gc.cur.cursor() }
func (gc *groupCursorStreamReader) Stats() cursors.CursorStats {
	return gc.fr.stats.Stats()
}

func (gc *groupCursorStreamReader) Next() bool {
	if gc.fr.state == stateReadSeries {
		return gc.readSeriesFrame()
	}

	if gc.fr.state == stateDone || gc.fr.state == stateReadErr || gc.fr.state == stateReadGroup {
		return false
	}

	gc.fr.setErr(fmt.Errorf("expected reader in state %v, was in state %v", stateReadSeries, gc.fr.state))

	return false
}

func (gc *groupCursorStreamReader) readSeriesFrame() bool {
	f := gc.fr.peekFrame()
	if f == nil {
		return false
	}

	if sf, ok := f.Data.(*datatypes.ReadResponse_Frame_Series); ok {
		gc.fr.nextFrame()
		gc.fr.state = stateReadPoints

		if cap(gc.tags) < len(sf.Series.Tags) {
			gc.tags = make(models.Tags, len(sf.Series.Tags))
		} else {
			gc.tags = gc.tags[:len(sf.Series.Tags)]
		}

		for i := range sf.Series.Tags {
			gc.tags[i].Key = sf.Series.Tags[i].Key
			gc.tags[i].Value = sf.Series.Tags[i].Value
		}

		gc.cur.nextType = sf.Series.DataType

		return true
	} else if _, ok := f.Data.(*datatypes.ReadResponse_Frame_Group); ok {
		gc.fr.state = stateReadGroup
		return false
	}

	gc.fr.setErr(fmt.Errorf("expected series frame, got %T", f.Data))

	return false
}

func (gc *groupCursorStreamReader) Close() {
RETRY:
	if gc.fr.state == stateReadPoints {
		cur := gc.Cursor()
		if cur != nil {
			cur.Close()
		}
	}

	if gc.fr.state == stateReadSeries {
		gc.readSeriesFrame()
		goto RETRY
	}
}

type readState byte

const (
	stateReadGroup readState = iota
	stateReadSeries
	stateReadPoints
	stateReadFloatPoints
	stateReadIntegerPoints
	stateReadUnsignedPoints
	stateReadBooleanPoints
	stateReadStringPoints
	stateReadErr
	stateDone
)

type frameReader struct {
	s     StreamReader
	stats statistics
	state readState
	buf   []datatypes.ReadResponse_Frame
	p     int
	err   error
}

func (r *frameReader) init() {
	if stats, ok := r.s.(statistics); ok {
		r.stats = stats
	} else {
		r.stats = zeroStatistics
	}
}

func (r *frameReader) peekFrame() *datatypes.ReadResponse_Frame {
	retries := peekFrameRetries

RETRY:
	if r.p < len(r.buf) {
		f := &r.buf[r.p]
		return f
	}

	r.p = 0
	r.buf = nil
	res, err := r.s.Recv()
	if err == nil {
		if res != nil {
			r.buf = res.Frames
		}
		if retries > 0 {
			retries--
			goto RETRY
		}

		r.setErr(ErrStreamNoData)
	} else if err == io.EOF {
		r.state = stateDone
	} else {
		r.setErr(err)
	}
	return nil
}

func (r *frameReader) nextFrame() { r.p++ }

func (r *frameReader) setErr(err error) {
	r.err = err
	r.state = stateReadErr
}

type cursorReaders struct {
	fr       *frameReader
	nextType datatypes.ReadResponse_DataType

	cc cursors.Cursor

	f floatCursorStreamReader
	i integerCursorStreamReader
	u unsignedCursorStreamReader
	b booleanCursorStreamReader
	s stringCursorStreamReader
}

func (cur *cursorReaders) setFrameReader(fr *frameReader) {
	cur.fr = fr
	cur.f.fr = fr
	cur.i.fr = fr
	cur.u.fr = fr
	cur.b.fr = fr
	cur.s.fr = fr
}

func (cur *cursorReaders) cursor() cursors.Cursor {
	cur.cc = nil
	if cur.fr.state != stateReadPoints {
		cur.fr.setErr(fmt.Errorf("expected reader in state %v, was in state %v", stateReadPoints, cur.fr.state))
		return cur.cc
	}

	switch cur.nextType {
	case datatypes.DataTypeFloat:
		cur.fr.state = stateReadFloatPoints
		cur.cc = &cur.f

	case datatypes.DataTypeInteger:
		cur.fr.state = stateReadIntegerPoints
		cur.cc = &cur.i

	case datatypes.DataTypeUnsigned:
		cur.fr.state = stateReadUnsignedPoints
		cur.cc = &cur.u

	case datatypes.DataTypeBoolean:
		cur.fr.state = stateReadBooleanPoints
		cur.cc = &cur.b

	case datatypes.DataTypeString:
		cur.fr.state = stateReadStringPoints
		cur.cc = &cur.s

	default:
		cur.fr.setErr(fmt.Errorf("unexpected data type, %d", cur.nextType))
	}

	return cur.cc
}
