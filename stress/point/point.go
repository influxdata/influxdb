package point

import (
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/stress/lineprotocol"
)

type point struct {
	seriesKey []byte
	Ints      []*lineprotocol.Int
	Floats    []*lineprotocol.Float
	time      *lineprotocol.Timestamp
	fields    []lineprotocol.Field
}

func New(sk []byte, ints, floats []string, p lineprotocol.Precision) *point {
	fields := []lineprotocol.Field{}
	e := &point{
		seriesKey: sk,
		time:      lineprotocol.NewTimestamp(p),
		fields:    fields,
	}

	for _, i := range ints {
		n := &lineprotocol.Int{Key: []byte(i)}
		e.Ints = append(e.Ints, n)
		e.fields = append(e.fields, n)
	}

	for _, f := range floats {
		n := &lineprotocol.Float{Key: []byte(f)}
		e.Floats = append(e.Floats, n)
		e.fields = append(e.fields, n)
	}

	return e
}

func (p *point) Series() []byte {
	return p.seriesKey
}

func (p *point) Fields() []lineprotocol.Field {
	return p.fields
}

func (p *point) Time() *lineprotocol.Timestamp {
	return p.time
}

func (p *point) SetTime(t time.Time) {
	p.time.SetTime(&t)
}

func (p *point) Update() {
	for _, i := range p.Ints {
		atomic.AddInt64(&i.Value, int64(1)) // maybe remove the go's
	}

	for _, f := range p.Floats {
		// Need to do something else here
		// There will be a race here
		f.Value += 1.0
	}
}

func NewPoints(seriesKey, fields string, seriesN int, pc lineprotocol.Precision) []lineprotocol.Point {
	pts := []lineprotocol.Point{}
	series := generateSeriesKeys(seriesKey, seriesN)
	ints, floats := generateFieldSet(fields)
	for _, sk := range series {
		p := New(sk, ints, floats, pc)
		pts = append(pts, p)
	}

	return pts
}
