package tsi1

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sort"

	"github.com/influxdata/influxdb/models"
)

// ErrSeriesOverflow is returned when too many series are added to a series writer.
var ErrSeriesOverflow = errors.New("series overflow")

// Series list field size constants.
const (
	// Series list trailer field sizes.
	TermListOffsetSize   = 8
	TermListSizeSize     = 8
	SeriesDataOffsetSize = 8
	SeriesDataSizeSize   = 8

	SeriesListTrailerSize = TermListOffsetSize +
		TermListSizeSize +
		SeriesDataOffsetSize +
		SeriesDataSizeSize

	// Other field sizes
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
	t := ReadSeriesListTrailer(data)

	// Slice term list data.
	l.termData = data[t.TermList.Offset:]
	l.termData = l.termData[:t.TermList.Size]

	// Slice series data data.
	l.seriesData = data[t.SeriesData.Offset:]
	l.seriesData = l.seriesData[:t.SeriesData.Size]

	return nil
}

// SeriesListWriter writes a SeriesDictionary block.
type SeriesListWriter struct {
	terms  map[string]int // term frequency
	series []serie        // series list

	// Term list is available after writer has been written.
	termList *TermList
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
	if len(sw.series) == math.MaxInt32 {
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
	var t SeriesListTrailer

	terms := NewTermList(sw.terms)

	// Write term dictionary.
	t.TermList.Offset = n
	nn, err := sw.writeTermListTo(w, terms)
	n += nn
	if err != nil {
		return n, err
	}
	t.TermList.Size = n - t.TermList.Offset

	// Write dictionary-encoded series list.
	t.SeriesData.Offset = n
	nn, err = sw.writeSeriesTo(w, terms, uint32(n))
	n += nn
	if err != nil {
		return n, err
	}
	t.SeriesData.Size = n - t.SeriesData.Offset

	// Write trailer.
	if err := sw.writeTrailerTo(w, t, &n); err != nil {
		return n, err
	}

	// Save term list for future encoding.
	sw.termList = terms

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

	// Ensure series are sorted.
	sort.Sort(series(sw.series))

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
func (sw *SeriesListWriter) writeTrailerTo(w io.Writer, t SeriesListTrailer, n *int64) error {
	if err := writeUint64To(w, uint64(t.TermList.Offset), n); err != nil {
		return err
	} else if err := writeUint64To(w, uint64(t.TermList.Size), n); err != nil {
		return err
	}

	if err := writeUint64To(w, uint64(t.SeriesData.Offset), n); err != nil {
		return err
	} else if err := writeUint64To(w, uint64(t.SeriesData.Size), n); err != nil {
		return err
	}

	return nil
}

// Offset returns the series offset from the writer.
// Only valid after the series list has been written to a writer.
func (sw *SeriesListWriter) Offset(name string, tags models.Tags) uint32 {
	// Find position of series.
	i := sort.Search(len(sw.series), func(i int) bool {
		s := &sw.series[i]
		if s.name != name {
			return s.name >= name
		}
		return models.CompareTags(s.tags, tags) != -1
	})

	// Ignore if it's not an exact match.
	if i >= len(sw.series) {
		return 0
	} else if s := &sw.series[i]; s.name != name || !s.tags.Equal(tags) {
		return 0
	}

	// Return offset & deleted flag of series.
	return sw.series[i].offset
}

// ReadSeriesListTrailer returns the series list trailer from data.
func ReadSeriesListTrailer(data []byte) SeriesListTrailer {
	var t SeriesListTrailer

	// Slice trailer data.
	buf := data[len(data)-SeriesListTrailerSize:]

	// Read term list info.
	t.TermList.Offset = int64(binary.BigEndian.Uint64(buf[0:TermListOffsetSize]))
	buf = buf[TermListOffsetSize:]
	t.TermList.Size = int64(binary.BigEndian.Uint64(buf[0:TermListSizeSize]))
	buf = buf[TermListSizeSize:]

	// Read series data info.
	t.SeriesData.Offset = int64(binary.BigEndian.Uint64(buf[0:SeriesDataOffsetSize]))
	buf = buf[SeriesDataOffsetSize:]
	t.SeriesData.Size = int64(binary.BigEndian.Uint64(buf[0:SeriesDataSizeSize]))
	buf = buf[SeriesDataSizeSize:]

	return t
}

// SeriesListTrailer represents meta data written to the end of the series list.
type SeriesListTrailer struct {
	TermList struct {
		Offset int64
		Size   int64
	}
	SeriesData struct {
		Offset int64
		Size   int64
	}
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
		return a[i].name < a[j].name
	}
	return models.CompareTags(a[i].tags, a[j].tags) == -1
}
