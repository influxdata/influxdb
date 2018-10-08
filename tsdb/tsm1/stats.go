package tsm1

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sort"
	"strings"

	"github.com/influxdata/platform/pkg/binaryutil"
)

const (
	// MeasurementMagicNumber is written as the first 4 bytes of a data file to
	// identify the file as a tsm1 stats file.
	MeasurementStatsMagicNumber string = "TSS1"

	// MeasurementStatsVersion indicates the version of the TSS1 file format.
	MeasurementStatsVersion byte = 1
)

// MeasurementStats represents a set of measurement sizes.
type MeasurementStats map[string]int

// NewStats returns a new instance of Stats.
func NewMeasurementStats() MeasurementStats {
	return make(MeasurementStats)
}

// MeasurementNames returns a list of sorted measurement names.
func (s MeasurementStats) MeasurementNames() []string {
	a := make([]string, 0, len(s))
	for name := range s {
		a = append(a, name)
	}
	sort.Strings(a)
	return a
}

// Add adds the values of all measurements in other to s.
func (s MeasurementStats) Add(other MeasurementStats) {
	for name, v := range other {
		s[name] += v
	}
}

// Sub subtracts the values of all measurements in other from s.
func (s MeasurementStats) Sub(other MeasurementStats) {
	for name, v := range other {
		s[name] -= v
	}
}

// ReadFrom reads stats from r in a binary format. Reader must also be an io.ByteReader.
func (s MeasurementStats) ReadFrom(r io.Reader) (n int64, err error) {
	br, ok := r.(io.ByteReader)
	if !ok {
		return 0, fmt.Errorf("tsm1.MeasurementStats.ReadFrom: ByteReader required")
	}

	// Read & verify magic.
	magic := make([]byte, 4)
	nn, err := io.ReadFull(r, magic)
	if n += int64(nn); err != nil {
		return n, fmt.Errorf("tsm1.MeasurementStats.ReadFrom: cannot read stats magic: %s", err)
	} else if string(magic) != MeasurementStatsMagicNumber {
		return n, fmt.Errorf("tsm1.MeasurementStats.ReadFrom: invalid tsm1 stats file")
	}

	// Read & verify version.
	version := make([]byte, 1)
	nn, err = io.ReadFull(r, version)
	if n += int64(nn); err != nil {
		return n, fmt.Errorf("tsm1.MeasurementStats.ReadFrom: cannot read stats version: %s", err)
	} else if version[0] != MeasurementStatsVersion {
		return n, fmt.Errorf("tsm1.MeasurementStats.ReadFrom: incompatible tsm1 stats version: %d", version[0])
	}

	// Read checksum.
	checksum := make([]byte, 4)
	nn, err = io.ReadFull(r, checksum)
	if n += int64(nn); err != nil {
		return n, fmt.Errorf("tsm1.MeasurementStats.ReadFrom: cannot read checksum: %s", err)
	}

	// Read measurement count.
	measurementN, err := binary.ReadVarint(br)
	if err != nil {
		return n, fmt.Errorf("tsm1.MeasurementStats.ReadFrom: cannot read stats measurement count: %s", err)
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
		return n, fmt.Errorf("tsm1.MeasurementStats.ReadFrom: file too large, expected EOF")
	}

	return n, nil
}

// readMeasurementFrom reads a measurement stat from r in a binary format.
func (s MeasurementStats) readMeasurementFrom(r io.Reader) (n int64, err error) {
	br, ok := r.(io.ByteReader)
	if !ok {
		return 0, fmt.Errorf("tsm1.MeasurementStats.readMeasurementFrom: ByteReader required")
	}

	// Read measurement name length.
	nameLen, err := binary.ReadVarint(br)
	if err != nil {
		return n, fmt.Errorf("tsm1.MeasurementStats.readMeasurementFrom: cannot read stats measurement name length: %s", err)
	}
	n += int64(binaryutil.VarintSize(nameLen))

	// Read measurement name. Use large capacity so it can usually be stack allocated.
	// Go allocates unescaped variables smaller than 64KB on the stack.
	name := make([]byte, nameLen)
	nn, err := io.ReadFull(r, name)
	if n += int64(nn); err != nil {
		return n, fmt.Errorf("tsm1.MeasurementStats.readMeasurementFrom: cannot read stats measurement name: %s", err)
	}

	// Read size.
	sz, err := binary.ReadVarint(br)
	if err != nil {
		return n, fmt.Errorf("tsm1.MeasurementStats.readMeasurementFrom: cannot read stats measurement size: %s", err)
	}
	n += int64(binaryutil.VarintSize(sz))

	// Insert into map.
	s[string(name)] = int(sz)

	return n, nil
}

// WriteTo writes stats to w in a binary format.
func (s MeasurementStats) WriteTo(w io.Writer) (n int64, err error) {
	// Write magic & version.
	nn, err := io.WriteString(w, MeasurementStatsMagicNumber)
	if n += int64(nn); err != nil {
		return n, err
	}
	nn, err = w.Write([]byte{MeasurementStatsVersion})
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

func (s MeasurementStats) writeMeasurementTo(w io.Writer, name string, sz int) (n int64, err error) {
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

// StatsFilename returns the path to the stats file for a given TSM file path.
func StatsFilename(tsmPath string) string {
	if strings.HasSuffix(tsmPath, "."+TmpTSMFileExtension) {
		tsmPath = strings.TrimSuffix(tsmPath, "."+TmpTSMFileExtension)
	}
	if strings.HasSuffix(tsmPath, "."+TSMFileExtension) {
		tsmPath = strings.TrimSuffix(tsmPath, "."+TSMFileExtension)
	}
	return tsmPath + "." + TSSFileExtension
}
