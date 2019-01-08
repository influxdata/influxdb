package tsi1

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sort"

	"github.com/influxdata/influxdb/pkg/binaryutil"
)

const (
	// MeasurementCardinalityStatsMagicNumber is written as the first 4 bytes
	// of a data file to identify the file as a tsi1 cardinality file.
	MeasurementCardinalityStatsMagicNumber string = "TSIS"

	// MeasurementCardinalityVersion indicates the version of the TSIC file format.
	MeasurementCardinalityStatsVersion byte = 1
)

// MeasurementCardinalityStats represents a set of measurement sizes.
type MeasurementCardinalityStats map[string]int

// NewMeasurementCardinality returns a new instance of MeasurementCardinality.
func NewMeasurementCardinalityStats() MeasurementCardinalityStats {
	return make(MeasurementCardinalityStats)
}

// MeasurementNames returns a list of sorted measurement names.
func (s MeasurementCardinalityStats) MeasurementNames() []string {
	a := make([]string, 0, len(s))
	for name := range s {
		a = append(a, name)
	}
	sort.Strings(a)
	return a
}

// Inc increments a measurement count by 1.
func (s MeasurementCardinalityStats) Inc(name []byte) {
	s[string(name)]++
}

// Dec decrements a measurement count by 1. Deleted if zero.
func (s MeasurementCardinalityStats) Dec(name []byte) {
	v := s[string(name)]
	if v == 1 {
		delete(s, string(name))
	} else {
		s[string(name)] = v - 1
	}
}

// Add adds the values of all measurements in other to s.
func (s MeasurementCardinalityStats) Add(other MeasurementCardinalityStats) {
	for name, v := range other {
		s[name] += v
	}
}

// Sub subtracts the values of all measurements in other from s.
func (s MeasurementCardinalityStats) Sub(other MeasurementCardinalityStats) {
	for name, v := range other {
		s[name] -= v
	}
}

// Clone returns a copy of s.
func (s MeasurementCardinalityStats) Clone() MeasurementCardinalityStats {
	other := make(MeasurementCardinalityStats, len(s))
	for k, v := range s {
		other[k] = v
	}
	return other
}

// ReadFrom reads stats from r in a binary format. Reader must also be an io.ByteReader.
func (s MeasurementCardinalityStats) ReadFrom(r io.Reader) (n int64, err error) {
	br, ok := r.(io.ByteReader)
	if !ok {
		return 0, fmt.Errorf("tsm1.MeasurementCardinalityStats.ReadFrom: ByteReader required")
	}

	// Read & verify magic.
	magic := make([]byte, 4)
	nn, err := io.ReadFull(r, magic)
	if n += int64(nn); err != nil {
		return n, fmt.Errorf("tsm1.MeasurementCardinalityStats.ReadFrom: cannot read stats magic: %s", err)
	} else if string(magic) != MeasurementCardinalityStatsMagicNumber {
		return n, fmt.Errorf("tsm1.MeasurementCardinalityStats.ReadFrom: invalid tsm1 stats file")
	}

	// Read & verify version.
	version := make([]byte, 1)
	nn, err = io.ReadFull(r, version)
	if n += int64(nn); err != nil {
		return n, fmt.Errorf("tsm1.MeasurementCardinalityStats.ReadFrom: cannot read stats version: %s", err)
	} else if version[0] != MeasurementCardinalityStatsVersion {
		return n, fmt.Errorf("tsm1.MeasurementCardinalityStats.ReadFrom: incompatible tsm1 stats version: %d", version[0])
	}

	// Read checksum.
	checksum := make([]byte, 4)
	nn, err = io.ReadFull(r, checksum)
	if n += int64(nn); err != nil {
		return n, fmt.Errorf("tsm1.MeasurementCardinalityStats.ReadFrom: cannot read checksum: %s", err)
	}

	// Read measurement count.
	measurementN, err := binary.ReadVarint(br)
	if err != nil {
		return n, fmt.Errorf("tsm1.MeasurementCardinalityStats.ReadFrom: cannot read stats measurement count: %s", err)
	}
	n += int64(binaryutil.VarintSize(measurementN))

	// Read measurements.
	for i := int64(0); i < measurementN; i++ {
		nn64, err := s.readMeasurementFrom(r)
		if n += nn64; err != nil {
			return n, err
		}
	}

	// Expect end-of-file.
	buf := make([]byte, 1)
	if _, err := r.Read(buf); err != io.EOF {
		return n, fmt.Errorf("tsm1.MeasurementCardinalityStats.ReadFrom: file too large, expected EOF")
	}

	return n, nil
}

// readMeasurementFrom reads a measurement stat from r in a binary format.
func (s MeasurementCardinalityStats) readMeasurementFrom(r io.Reader) (n int64, err error) {
	br, ok := r.(io.ByteReader)
	if !ok {
		return 0, fmt.Errorf("tsm1.MeasurementCardinalityStats.readMeasurementFrom: ByteReader required")
	}

	// Read measurement name length.
	nameLen, err := binary.ReadVarint(br)
	if err != nil {
		return n, fmt.Errorf("tsm1.MeasurementCardinalityStats.readMeasurementFrom: cannot read stats measurement name length: %s", err)
	}
	n += int64(binaryutil.VarintSize(nameLen))

	// Read measurement name. Use large capacity so it can usually be stack allocated.
	// Go allocates unescaped variables smaller than 64KB on the stack.
	name := make([]byte, nameLen)
	nn, err := io.ReadFull(r, name)
	if n += int64(nn); err != nil {
		return n, fmt.Errorf("tsm1.MeasurementCardinalityStats.readMeasurementFrom: cannot read stats measurement name: %s", err)
	}

	// Read size.
	sz, err := binary.ReadVarint(br)
	if err != nil {
		return n, fmt.Errorf("tsm1.MeasurementCardinalityStats.readMeasurementFrom: cannot read stats measurement size: %s", err)
	}
	n += int64(binaryutil.VarintSize(sz))

	// Insert into map.
	s[string(name)] = int(sz)

	return n, nil
}

// WriteTo writes stats to w in a binary format.
func (s MeasurementCardinalityStats) WriteTo(w io.Writer) (n int64, err error) {
	// Write magic & version.
	nn, err := io.WriteString(w, MeasurementCardinalityStatsMagicNumber)
	if n += int64(nn); err != nil {
		return n, err
	}
	nn, err = w.Write([]byte{MeasurementCardinalityStatsVersion})
	if n += int64(nn); err != nil {
		return n, err
	}

	// Write measurement count.
	var buf bytes.Buffer
	b := make([]byte, binary.MaxVarintLen64)
	if _, err = buf.Write(b[:binary.PutVarint(b, int64(len(s)))]); err != nil {
		return n, err
	}

	// Write all measurements in sorted order.
	for _, name := range s.MeasurementNames() {
		if _, err := s.writeMeasurementTo(&buf, name, s[name]); err != nil {
			return n, err
		}
	}
	data := buf.Bytes()

	// Compute & write checksum.
	if err := binary.Write(w, binary.BigEndian, crc32.ChecksumIEEE(data)); err != nil {
		return n, err
	}
	n += 4

	// Write buffer.
	nn, err = w.Write(data)
	if n += int64(nn); err != nil {
		return n, err
	}

	return n, err
}

func (s MeasurementCardinalityStats) writeMeasurementTo(w io.Writer, name string, sz int) (n int64, err error) {
	// Write measurement name length.
	buf := make([]byte, binary.MaxVarintLen64)
	nn, err := w.Write(buf[:binary.PutVarint(buf, int64(len(name)))])
	if n += int64(nn); err != nil {
		return n, err
	}

	// Write measurement name.
	nn, err = io.WriteString(w, name)
	if n += int64(nn); err != nil {
		return n, err
	}

	// Write size.
	nn, err = w.Write(buf[:binary.PutVarint(buf, int64(sz))])
	if n += int64(nn); err != nil {
		return n, err
	}

	return n, err
}
