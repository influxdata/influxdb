package estimator

import (
	"fmt"
	"io"

	"github.com/cespare/xxhash"
	"github.com/clarkduvall/hyperloglog"
)

// Sketch is the interface representing a sketch for estimating cardinality.
type Sketch interface {
	// Add adds a single value to the sketch.
	Add(v []byte) error

	// Count returns a cardinality estimate for the sketch.
	Count() (count uint64, err error)

	// ReadFrom implements the io.ReaderAt interface.
	//
	// Implementations of the ReadFrom method should ensure that values are
	// streamed from the provided reader into the Sketch.
	ReadFrom(r io.Reader) (n int64, err error)

	// Merge merges another sketch into this one.
	Merge(s Sketch) error
}

type HyperLogLogPlus struct {
	hll *hyperloglog.HyperLogLogPlus
}

func NewHyperLogLogPlus(precision uint8) (*HyperLogLogPlus, error) {
	hll, err := hyperloglog.NewPlus(precision)
	if err != nil {
		return nil, err
	}

	return &HyperLogLogPlus{hll: hll}, nil
}

// hash64 implements the hyperloglog.Hash64 interface.
// See: https://godoc.org/github.com/clarkduvall/hyperloglog#Hash64
type hash64 uint64

func (h hash64) Sum64() uint64 {
	return uint64(h)
}

func (s *HyperLogLogPlus) Add(v []byte) error {
	s.hll.Add(hash64(xxhash.Sum64(v)))
	return nil
}

func (s *HyperLogLogPlus) Count() (count uint64, err error) { return s.hll.Count(), nil }

func (s *HyperLogLogPlus) ReadFrom(r io.Reader) (n int64, err error) {
	var (
		m   int
		buf [4]byte
	)

	for err == nil {
		if m, err = r.Read(buf[:]); err == nil {
			if m < len(buf) {
				err = fmt.Errorf("short read. Only read %d bytes", m)
			} else {
				n += int64(m)
				err = s.Add(buf[:])
			}
		}
	}

	if err != io.EOF {
		return 0, err
	}
	return n, nil
}

func (s *HyperLogLogPlus) Merge(sketch Sketch) error {
	other, ok := sketch.(*HyperLogLogPlus)
	if !ok {
		return fmt.Errorf("sketch is of type %T", sketch)
	}

	return s.hll.Merge(other.hll)
}
