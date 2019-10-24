package reads

import (
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb/cursors"
)

type StringValuesStreamReader interface {
	Recv() (*datatypes.StringValuesResponse, error)
}

type StringIteratorStreamReader struct {
	stream   StringValuesStreamReader
	response *datatypes.StringValuesResponse
	i        int

	err error
}

// API compatibility
var _ cursors.StringIterator = (*StringIteratorStreamReader)(nil)

func NewStringIteratorStreamReader(stream StringValuesStreamReader) *StringIteratorStreamReader {
	return &StringIteratorStreamReader{
		stream: stream,
	}
}

func (r *StringIteratorStreamReader) Err() error {
	return r.err
}

func (r *StringIteratorStreamReader) Next() bool {
	if r.err != nil {
		return false
	}

	if r.response == nil || len(r.response.Values)-1 <= r.i {
		r.response, r.err = r.stream.Recv()
		if r.err != nil {
			return false
		}
		r.i = 0

	} else {
		r.i++
	}

	return len(r.response.Values) > r.i
}

func (r *StringIteratorStreamReader) Value() string {
	if len(r.response.Values) > r.i {
		return string(r.response.Values[r.i])
	}

	// Better than panic.
	return ""
}

func (r *StringIteratorStreamReader) Stats() cursors.CursorStats {
	return cursors.CursorStats{}
}
