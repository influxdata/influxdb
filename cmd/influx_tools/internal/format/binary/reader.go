package binary

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/influxdata/influxdb/cmd/influx_tools/internal/tlv"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

type Reader struct {
	r     io.Reader
	pr    *PointsReader
	state *readerState
	stats *readerStats
}

type readerStats struct {
	series int
	counts [8]struct {
		series, values int
	}
}
type readerState byte

const (
	readHeader readerState = iota + 1
	readBucket
	readSeries
	readPoints
	done
)

func NewReader(reader io.Reader) *Reader {
	state := readHeader
	var stats readerStats
	r := &Reader{r: reader, state: &state, stats: &stats,
		pr: &PointsReader{r: reader, values: make(tsm1.Values, tsdb.DefaultMaxPointsPerBlock), state: &state, stats: &stats}}
	return r
}

func (r *Reader) ReadHeader() (*Header, error) {
	if *r.state != readHeader {
		return nil, fmt.Errorf("expected reader in state %v, was in state %v\n", readHeader, *r.state)
	}

	var magic [len(Magic)]byte
	n, err := r.r.Read(magic[:])
	if err != nil {
		return nil, err
	}

	if n < len(Magic) || !bytes.Equal(magic[:], Magic[:]) {
		return nil, errors.New("IFLXDUMP header not present")
	}

	t, lv, err := tlv.ReadTLV(r.r)
	if err != nil {
		return nil, err
	}
	if t != byte(HeaderType) {
		return nil, fmt.Errorf("expected header type, got %v", t)
	}
	h := &Header{}
	err = h.Unmarshal(lv)
	*r.state = readBucket

	return h, err
}

func (r *Reader) Close() error {
	return nil
}

func (r *Reader) NextBucket() (*BucketHeader, error) {
	if *r.state != readBucket {
		return nil, fmt.Errorf("expected reader in state %v, was in state %v", readBucket, *r.state)
	}

	t, lv, err := tlv.ReadTLV(r.r)
	if err != nil {
		if err == io.EOF {
			*r.state = done
			return nil, nil
		}
		return nil, err
	}
	if t != byte(BucketHeaderType) {
		return nil, fmt.Errorf("expected bucket header type, got %v", t)
	}

	bh := &BucketHeader{}
	err = bh.Unmarshal(lv)
	if err != nil {
		return nil, err
	}
	*r.state = readSeries

	return bh, nil
}

func (r *Reader) NextSeries() (*SeriesHeader, error) {
	if *r.state != readSeries {
		return nil, fmt.Errorf("expected reader in state %v, was in state %v", readSeries, *r.state)
	}

	t, lv, err := tlv.ReadTLV(r.r)
	if err != nil {
		return nil, err
	}
	if t == byte(BucketFooterType) {
		*r.state = readBucket
		return nil, nil
	}
	if t != byte(SeriesHeaderType) {
		return nil, fmt.Errorf("expected series header type, got %v", t)
	}
	sh := &SeriesHeader{}
	err = sh.Unmarshal(lv)
	if err != nil {
		return nil, err
	}
	r.stats.series++
	r.stats.counts[sh.FieldType&7].series++

	var pointsType MessageType
	switch sh.FieldType {
	case FloatFieldType:
		pointsType = FloatPointsType
	case IntegerFieldType:
		pointsType = IntegerPointsType
	case UnsignedFieldType:
		pointsType = UnsignedPointsType
	case BooleanFieldType:
		pointsType = BooleanPointsType
	case StringFieldType:
		pointsType = StringPointsType
	default:
		return nil, fmt.Errorf("unsupported series field type %v", sh.FieldType)
	}

	*r.state = readPoints
	r.pr.Reset(pointsType)
	return sh, nil
}

func (r *Reader) Points() *PointsReader {
	return r.pr
}

type PointsReader struct {
	pointsType MessageType
	r          io.Reader
	values     tsm1.Values
	n          int
	state      *readerState
	stats      *readerStats
}

func (pr *PointsReader) Reset(pointsType MessageType) {
	pr.pointsType = pointsType
	pr.n = 0
}

func (pr *PointsReader) Next() (bool, error) {
	if *pr.state != readPoints {
		return false, fmt.Errorf("expected reader in state %v, was in state %v", readPoints, *pr.state)
	}

	t, lv, err := tlv.ReadTLV(pr.r)
	if err != nil {
		return false, err
	}
	if t == byte(SeriesFooterType) {
		*pr.state = readSeries
		return false, nil
	}
	if t != byte(pr.pointsType) {
		return false, fmt.Errorf("expected message type %v, got %v", pr.pointsType, t)
	}
	err = pr.marshalValues(lv)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (pr *PointsReader) Values() tsm1.Values {
	return pr.values[:pr.n]
}

func (pr *PointsReader) marshalValues(lv []byte) error {
	switch pr.pointsType {
	case FloatPointsType:
		return pr.marshalFloats(lv)
	case IntegerPointsType:
		return pr.marshalIntegers(lv)
	case UnsignedPointsType:
		return pr.marshalUnsigned(lv)
	case BooleanPointsType:
		return pr.marshalBooleans(lv)
	case StringPointsType:
		return pr.marshalStrings(lv)
	default:
		return fmt.Errorf("unsupported points type %v", pr.pointsType)
	}
}

func (pr *PointsReader) marshalFloats(lv []byte) error {
	fp := &FloatPoints{}
	err := fp.Unmarshal(lv)
	if err != nil {
		return err
	}
	for i, t := range fp.Timestamps {
		pr.values[i] = tsm1.NewFloatValue(t, fp.Values[i])
	}
	pr.stats.counts[0].values += len(fp.Timestamps)
	pr.n = len(fp.Timestamps)
	return nil
}

func (pr *PointsReader) marshalIntegers(lv []byte) error {
	ip := &IntegerPoints{}
	err := ip.Unmarshal(lv)
	if err != nil {
		return err
	}
	for i, t := range ip.Timestamps {
		pr.values[i] = tsm1.NewIntegerValue(t, ip.Values[i])
	}
	pr.stats.counts[1].values += len(ip.Timestamps)
	pr.n = len(ip.Timestamps)
	return nil
}

func (pr *PointsReader) marshalUnsigned(lv []byte) error {
	up := &UnsignedPoints{}
	err := up.Unmarshal(lv)
	if err != nil {
		return err
	}
	for i, t := range up.Timestamps {
		pr.values[i] = tsm1.NewUnsignedValue(t, up.Values[i])
	}
	pr.stats.counts[2].values += len(up.Timestamps)
	pr.n = len(up.Timestamps)
	return nil
}

func (pr *PointsReader) marshalBooleans(lv []byte) error {
	bp := &BooleanPoints{}
	err := bp.Unmarshal(lv)
	if err != nil {
		return err
	}
	for i, t := range bp.Timestamps {
		pr.values[i] = tsm1.NewBooleanValue(t, bp.Values[i])
	}
	pr.stats.counts[3].values += len(bp.Timestamps)
	pr.n = len(bp.Timestamps)
	return nil
}

func (pr *PointsReader) marshalStrings(lv []byte) error {
	sp := &StringPoints{}
	err := sp.Unmarshal(lv)
	if err != nil {
		return err
	}
	for i, t := range sp.Timestamps {
		pr.values[i] = tsm1.NewStringValue(t, sp.Values[i])
	}
	pr.stats.counts[4].values += len(sp.Timestamps)
	pr.n = len(sp.Timestamps)
	return nil
}
