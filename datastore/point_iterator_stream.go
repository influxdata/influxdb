package datastore

import "github.com/influxdb/influxdb/protocol"

// PointIteratorStream is a struct that implements the StreamQuery
// interface and is used by the shard with the Merger to merge the
// data points locally to form a monotic stream of points (increasing
// or decreasing timestamps)
type PointIteratorStream struct {
	pi     *PointIterator
	name   string
	fields []string
}

// Returns true if the point iterator is still valid
func (pis PointIteratorStream) HasPoint() bool {
	return pis.pi.Valid()
}

// Returns the next point from the point iterator
func (pis PointIteratorStream) Next() *protocol.Series {
	p := pis.pi.Point()
	s := &protocol.Series{
		Name:   &pis.name,
		Fields: pis.fields,
		Points: []*protocol.Point{p},
	}
	pis.pi.Next()
	return s
}

// Returns true if the point iterator is not valid
func (pis PointIteratorStream) Closed() bool {
	return !pis.pi.Valid()
}
