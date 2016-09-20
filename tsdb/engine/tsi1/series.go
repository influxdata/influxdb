package tsi1

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"

	"github.com/influxdata/influxdb/models"
)

// ErrSeriesOverflow is returned when too many series are added to a series writer.
var ErrSeriesOverflow = errors.New("series overflow")

// Series list field size constants.
const (
	SeriesListTrailerSize = 8

	TermCountSize   = 4
	SeriesCountSize = 4
	SeriesIDSize    = 4
)

// Series flag constants.
const (
	SeriesTombstoneFlag = 0x01
)

// SeriesList represents the section of the index which holds the term
// dictionary and a sorted list of series keys.
type SeriesList struct {
	termData   []byte
	seriesData []byte
}

// SeriesOffset returns offset of the encoded series key.
// Returns 0 if the key does not exist in the series list.
func (l *SeriesList) SeriesOffset(key []byte) (offset uint32, deleted bool) {
	offset = uint32(len(l.termData) + SeriesCountSize)
	data := l.seriesData[SeriesCountSize:]

	for i, n := uint32(0), l.SeriesCount(); i < n; i++ {
		// Read series flag.
		flag := data[0]
		data = data[1:]

		// Read series length.
		ln, sz := binary.Uvarint(data)
		data = data[sz:]

		// Return offset if the series key matches.
		if bytes.Equal(key, data[:ln]) {
			deleted = (flag & SeriesTombstoneFlag) != 0
			return offset, deleted
		}

		// Update offset & move data forward.
		data = data[ln:]
		offset += uint32(ln) + uint32(sz)
	}

	return 0, false
}

// EncodeSeries returns a dictionary-encoded series key.
func (l *SeriesList) EncodeSeries(name string, tags models.Tags) []byte {
	// Build a buffer with the minimum space for the name, tag count, and tags.
	buf := make([]byte, 2+len(tags))
	return l.AppendEncodeSeries(buf[:0], name, tags)
}

// AppendEncodeSeries appends an encoded series value to dst and returns the new slice.
func (l *SeriesList) AppendEncodeSeries(dst []byte, name string, tags models.Tags) []byte {
	var buf [binary.MaxVarintLen32]byte

	// Append encoded name.
	n := binary.PutUvarint(buf[:], uint64(l.EncodeTerm([]byte(name))))
	dst = append(dst, buf[:n]...)

	// Append encoded tag count.
	n = binary.PutUvarint(buf[:], uint64(len(tags)))
	dst = append(dst, buf[:n]...)

	// Append tags.
	for _, t := range tags {
		n := binary.PutUvarint(buf[:], uint64(l.EncodeTerm(t.Key)))
		dst = append(dst, buf[:n]...)

		n = binary.PutUvarint(buf[:], uint64(l.EncodeTerm(t.Value)))
		dst = append(dst, buf[:n]...)
	}

	return dst
}

// DecodeSeries decodes a dictionary encoded series into a name and tagset.
func (l *SeriesList) DecodeSeries(v []byte) (name string, tags models.Tags) {
	// Read name.
	offset, n := binary.Uvarint(v)
	name, v = string(l.DecodeTerm(uint32(offset))), v[n:]

	// Read tag count.
	tagN, n := binary.Uvarint(v)
	v = v[n:]

	// Loop over tag key/values.
	for i := 0; i < int(tagN); i++ {
		// Read key.
		offset, n := binary.Uvarint(v)
		key, v := l.DecodeTerm(uint32(offset)), v[n:]

		// Read value.
		offset, n = binary.Uvarint(v)
		value, v := l.DecodeTerm(uint32(offset)), v[n:]

		// Add to tagset.
		tags.Set(key, value)
	}

	return name, tags
}

// DecodeTerm returns the term at the given offset.
func (l *SeriesList) DecodeTerm(offset uint32) []byte {
	buf := l.termData[offset:]

	// Read length at offset.
	i, n := binary.Uvarint(buf)
	buf = buf[n:]

	// Return term data.
	return buf[:i]
}

// EncodeTerm returns the offset of v within data.
func (l *SeriesList) EncodeTerm(v []byte) uint32 {
	offset := uint32(TermCountSize)
	data := l.termData[offset:]

	for i, n := uint32(0), l.TermCount(); i < n; i++ {
		// Read term length.
		ln, sz := binary.Uvarint(data)
		data = data[sz:]

		// Return offset if the term matches.
		if bytes.Equal(v, data[:ln]) {
			return offset
		}

		// Update offset & move data forward.
		data = data[ln:]
		offset += uint32(ln) + uint32(sz)
	}

	return 0
}

// TermCount returns the number of terms within the dictionary.
func (l *SeriesList) TermCount() uint32 {
	return binary.BigEndian.Uint32(l.termData[:TermCountSize])
}

// SeriesCount returns the number of series.
func (l *SeriesList) SeriesCount() uint32 {
	return binary.BigEndian.Uint32(l.seriesData[:SeriesCountSize])
}

// UnmarshalBinary unpacks data into the series list.
//
// If data is an mmap then it should stay open until the series list is no
// longer used because data access is performed directly from the byte slice.
func (l *SeriesList) UnmarshalBinary(data []byte) error {
	// Ensure data is at least long enough to contain a trailer.
	if len(data) < SeriesListTrailerSize {
		return io.ErrShortBuffer
	}

	// Read trailer offsets.
	termDataOffset := binary.BigEndian.Uint32(data[len(data)-8:])
	seriesDataOffset := binary.BigEndian.Uint32(data[len(data)-4:])

	// Save reference to term list data.
	termDataSize := seriesDataOffset - termDataOffset
	l.termData = data[termDataOffset:]
	l.termData = l.termData[:termDataSize]

	// Save reference to series data.
	seriesDataSize := uint32(len(data)) - seriesDataOffset - SeriesListTrailerSize
	l.seriesData = data[seriesDataOffset:]
	l.seriesData = l.seriesData[:seriesDataSize]

	return nil
}

// SeriesListWriter writes a SeriesDictionary block.
type SeriesListWriter struct {
	terms  map[string]int // term frequency
	series []serie        // series list
}

// NewSeriesListWriter returns a new instance of SeriesListWriter.
func NewSeriesListWriter() *SeriesListWriter {
	return &SeriesListWriter{
		terms: make(map[string]int),
	}
}

// Add adds a series to the writer's set.
// Returns an ErrSeriesOverflow if no more series can be held in the writer.
func (sw *SeriesListWriter) Add(name string, tags models.Tags) error {
	return sw.append(name, tags, false)
}

// Delete marks a series as tombstoned.
func (sw *SeriesListWriter) Delete(name string, tags models.Tags) error {
	return sw.append(name, tags, true)
}

func (sw *SeriesListWriter) append(name string, tags models.Tags, deleted bool) error {
	// Ensure writer doesn't add too many series.
	if len(sw.series) == math.MaxUint32 {
		return ErrSeriesOverflow
	}

	// Increment term counts.
	sw.terms[name]++
	for _, t := range tags {
		sw.terms[string(t.Key)]++
		sw.terms[string(t.Value)]++
	}

	// Append series to list.
	sw.series = append(sw.series, serie{name: name, tags: tags, deleted: deleted})

	return nil
}

// WriteTo computes the dictionary encoding of the series and writes to w.
func (sw *SeriesListWriter) WriteTo(w io.Writer) (n int64, err error) {
	terms := NewTermList(sw.terms)

	// Write term dictionary.
	termDataOffset := n
	nn, err := sw.writeTermListTo(w, terms)
	n += nn
	if err != nil {
		return n, err
	}

	// Write dictionary-encoded series list.
	seriesDataOffset := n
	nn, err = sw.writeSeriesTo(w, terms, uint32(n))
	n += nn
	if err != nil {
		return n, err
	}

	// Write trailer.
	nn, err = sw.writeTrailerTo(w, uint32(termDataOffset), uint32(seriesDataOffset))
	n += nn
	if err != nil {
		return n, err
	}

	return n, nil
}

// writeTermListTo writes the terms to w.
func (sw *SeriesListWriter) writeTermListTo(w io.Writer, terms *TermList) (n int64, err error) {
	buf := make([]byte, binary.MaxVarintLen32)

	// Write term count.
	binary.BigEndian.PutUint32(buf[:4], uint32(terms.Len()))
	nn, err := w.Write(buf[:4])
	n += int64(nn)
	if err != nil {
		return n, err
	}

	// Write terms.
	for i := range terms.a {
		e := &terms.a[i]

		// Ensure that we can reference the offset using a uint32.
		if n > math.MaxUint32 {
			return n, errors.New("series dictionary exceeded max size")
		}

		// Track starting offset of the term.
		e.offset = uint32(n)

		// Join varint(length) & term in buffer.
		sz := binary.PutUvarint(buf, uint64(len(e.term)))
		buf = append(buf[:sz], e.term...)

		// Write buffer to writer.
		nn, err := w.Write(buf)
		n += int64(nn)
		if err != nil {
			return n, err
		}
	}

	return n, nil
}

// writeSeriesTo writes dictionary-encoded series to w in sorted order.
func (sw *SeriesListWriter) writeSeriesTo(w io.Writer, terms *TermList, offset uint32) (n int64, err error) {
	buf := make([]byte, binary.MaxVarintLen32+1)

	// Write series count.
	binary.BigEndian.PutUint32(buf[:4], uint32(len(sw.series)))
	nn, err := w.Write(buf[:4])
	n += int64(nn)
	if err != nil {
		return n, err
	}

	// Write series.
	var seriesBuf []byte
	for i := range sw.series {
		s := &sw.series[i]

		// Ensure that we can reference the series using a uint32.
		if int64(offset)+n > math.MaxUint32 {
			return n, errors.New("series list exceeded max size")
		}

		// Track offset of the series.
		s.offset = uint32(offset + uint32(n))

		// Write encoded series to a separate buffer.
		seriesBuf = terms.AppendEncodedSeries(seriesBuf[:0], s.name, s.tags)

		// Join flag, varint(length), & dictionary-encoded series in buffer.
		buf[0] = 0 // TODO(benbjohnson): series tombstone
		sz := binary.PutUvarint(buf[1:], uint64(len(seriesBuf)))
		buf = append(buf[:1+sz], seriesBuf...)

		// Write buffer to writer.
		nn, err := w.Write(buf)
		n += int64(nn)
		if err != nil {
			return n, err
		}
	}

	return n, nil
}

// writeTrailerTo writes offsets to the end of the series list.
func (sw *SeriesListWriter) writeTrailerTo(w io.Writer, termDataOffset, seriesDataOffset uint32) (n int64, err error) {
	// Write offset of term list.
	if err := binary.Write(w, binary.BigEndian, termDataOffset); err != nil {
		return n, err
	}
	n += 4

	// Write offset of series data.
	if err := binary.Write(w, binary.BigEndian, seriesDataOffset); err != nil {
		return n, err
	}
	n += 4

	return n, nil
}

type serie struct {
	name    string
	tags    models.Tags
	deleted bool
	offset  uint32
}

type series []serie

func (a series) Len() int      { return len(a) }
func (a series) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a series) Less(i, j int) bool {
	if a[i].name != a[j].name {
		return a[i].name < a[i].name
	}
	panic("TODO: CompareTags(a[i].tags, a[j].tags)")
}
