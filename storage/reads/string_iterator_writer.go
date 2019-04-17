package reads

import (
	"github.com/influxdata/influxdb/storage"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
)

type StringIteratorStream interface {
	Send(*datatypes.StringValuesResponse) error
}

type StringIteratorWriter struct {
	stream StringIteratorStream
	res    *datatypes.StringValuesResponse
	err    error

	sz int // estimated size in bytes for pending write
}

func NewStringIteratorWriter(stream StringIteratorStream) *StringIteratorWriter {
	siw := &StringIteratorWriter{
		stream: stream,
		res: &datatypes.StringValuesResponse{
			Values: nil,
		},
	}

	return siw
}

func (w *StringIteratorWriter) Err() error {
	return w.err
}

func (w *StringIteratorWriter) WriteStringIterator(si storage.StringIterator) error {
	for si.Next() {
		v := si.Value()
		if v == "" {
			// no value, no biggie
			continue
		}

		w.res.Values = append(w.res.Values, []byte(v))
		w.sz += len(v)
	}

	return nil
}

func (w *StringIteratorWriter) Flush() {
	if w.err != nil || w.sz == 0 {
		return
	}

	w.sz = 0

	if w.err = w.stream.Send(w.res); w.err != nil {
		return
	}

	for i := range w.res.Values {
		w.res.Values[i] = nil
	}
	w.res.Values = w.res.Values[:0]
}
