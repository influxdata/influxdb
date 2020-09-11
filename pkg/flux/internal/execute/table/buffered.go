package table

import (
	"sync/atomic"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/influxdb/v2/pkg/flux/internal/errors"
)

// BufferedTable represents a table of buffered column readers.
type BufferedTable struct {
	used     int32
	empty    bool
	GroupKey flux.GroupKey
	Columns  []flux.ColMeta
	Buffers  []flux.ColReader
}

// FromBuffer constructs a flux.Table from a single flux.ColReader.
func FromBuffer(cr flux.ColReader) flux.Table {
	return &BufferedTable{
		GroupKey: cr.Key(),
		Columns:  cr.Cols(),
		Buffers:  []flux.ColReader{cr},
	}
}

func (b *BufferedTable) Key() flux.GroupKey {
	return b.GroupKey
}

func (b *BufferedTable) Cols() []flux.ColMeta {
	return b.Columns
}

func (b *BufferedTable) Do(f func(flux.ColReader) error) error {
	if !atomic.CompareAndSwapInt32(&b.used, 0, 1) {
		return errors.New(codes.Internal, "table already read")
	}

	i := 0
	defer func() {
		for ; i < len(b.Buffers); i++ {
			b.Buffers[i].Release()
		}
	}()

	b.empty = true
	for ; i < len(b.Buffers); i++ {
		cr := b.Buffers[i]
		if cr.Len() > 0 {
			b.empty = false
		}
		if err := f(cr); err != nil {
			return err
		}
		cr.Release()
	}
	return nil
}

func (b *BufferedTable) Done() {
	if atomic.CompareAndSwapInt32(&b.used, 0, 1) {
		b.empty = b.isEmpty()
		for _, buf := range b.Buffers {
			buf.Release()
		}
		b.Buffers = nil
	}
}

func (b *BufferedTable) Empty() bool {
	if atomic.LoadInt32(&b.used) != 0 {
		return b.empty
	}
	return b.isEmpty()
}

func (b *BufferedTable) isEmpty() bool {
	for _, buf := range b.Buffers {
		if buf.Len() > 0 {
			return false
		}
	}
	return true
}
