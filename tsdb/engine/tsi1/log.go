package tsi1

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"sort"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/mmap"
)

// Log entry flag constants.
const (
	LogEntrySeriesTombstoneFlag      = 0x01
	LogEntryMeasurementTombstoneFlag = 0x02
	LogEntryTagKeyTombstoneFlag      = 0x04
	LogEntryTagValueTombstoneFlag    = 0x08
)

// LogFile represents an on-disk write-ahead log file.
type LogFile struct {
	data    []byte     // mmap
	file    *os.File   // writer
	buf     []byte     // marshaling buffer
	entries []LogEntry // parsed entries

	// In-memory index.
	mms map[string]logMeasurement

	// Filepath to the log file.
	Path string
}

// NewLogFile returns a new instance of LogFile.
func NewLogFile() *LogFile {
	return &LogFile{
		mms: make(map[string]logMeasurement),
	}
}

// Open reads the log from a file and validates all the checksums.
func (f *LogFile) Open() error {
	if err := f.open(); err != nil {
		f.Close()
		return err
	}
	return nil
}

func (f *LogFile) open() error {
	// Open file for appending.
	file, err := os.OpenFile(f.Path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	f.file = file

	// Open a read-only memory map of the existing data.
	data, err := mmap.Map(f.Path)
	if err != nil {
		return err
	}
	f.data = data

	// Read log entries from mmap.
	f.entries = nil
	for buf := f.data; len(buf) > 0; {
		// Read next entry.
		var e LogEntry
		if err := e.UnmarshalBinary(buf); err != nil {
			return err
		}
		f.entries = append(f.entries, e)

		// Move buffer forward.
		buf = buf[e.Size:]
	}

	return nil
}

// Close shuts down the file handle and mmap.
func (f *LogFile) Close() error {
	if f.file != nil {
		f.file.Close()
	}
	if f.data != nil {
		mmap.Unmap(f.data)
	}
	f.entries = nil
	return nil
}

// DeleteMeasurement adds a tombstone for a measurement to the log file.
func (f *LogFile) DeleteMeasurement(name []byte) error {
	// Append log entry.
	if err := f.append(LogEntry{Flag: LogEntryMeasurementTombstoneFlag, Name: name}); err != nil {
		return err
	}

	// Delete measurement from index.
	mm := f.measurement(name)
	mm.deleted = true
	f.mms[string(name)] = mm

	return nil
}

// DeleteTagKey adds a tombstone for a tag key to the log file.
func (f *LogFile) DeleteTagKey(name, key []byte) error {
	return f.append(LogEntry{Flag: LogEntryTagKeyTombstoneFlag, Name: name, Tags: models.Tags{{Key: key}}})
}

// DeleteTagValue adds a tombstone for a tag value to the log file.
func (f *LogFile) DeleteTagValue(name, key, value []byte) error {
	return f.append(LogEntry{Flag: LogEntryTagValueTombstoneFlag, Name: name, Tags: models.Tags{{Key: key, Value: value}}})
}

// AddSeries adds a series to the log file.
func (f *LogFile) AddSeries(name []byte, tags models.Tags) error {
	return f.insertSeries(LogEntry{Name: name, Tags: tags})
}

// DeleteSeries adds a tombstone for a series to the log file.
func (f *LogFile) DeleteSeries(name []byte, tags models.Tags) error {
	return f.insertSeries(LogEntry{Flag: LogEntrySeriesTombstoneFlag, Name: name, Tags: tags})
}

// insertSeries inserts a series entry.
func (f *LogFile) insertSeries(e LogEntry) error {
	// Append log entry.
	if err := f.append(e); err != nil {
		return err
	}

	// Check if series is deleted.
	deleted := (e.Flag & LogEntrySeriesTombstoneFlag) != 0

	// Fetch measurement.
	mm := f.measurement(e.Name)
	if !deleted {
		mm.deleted = false
	}

	// Save tags.
	for _, t := range e.Tags {
		// Fetch key.
		ts, ok := mm.tagSet[string(t.Key)]
		if !ok {
			ts = logTagSet{name: t.Key, tagValues: make(map[string]logTagValue)}
		}
		if !deleted {
			ts.deleted = false
		}

		// Fetch value.
		tv, ok := ts.tagValues[string(t.Value)]
		if !ok {
			tv.name = t.Value
		}
		if !deleted {
			tv.deleted = false
		}
		tv.insertEntry(e)
		ts.tagValues[string(t.Value)] = tv

		// Save key.
		mm.tagSet[string(t.Key)] = ts
	}

	// Save measurement.
	f.mms[string(e.Name)] = mm

	return nil
}

// append adds a generic entry to the end of the file.
func (f *LogFile) append(e LogEntry) error {
	// Marshal entry to the local buffer.
	f.buf = appendLogEntry(f.buf[0:], &e)

	// Append checksum.
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], e.Checksum)
	f.buf = append(f.buf, buf[:]...)

	// Save the size of the record.
	e.Size = len(f.buf)

	// Write record to file.
	if n, err := f.file.Write(f.buf); err != nil {
		// Move position backwards over partial entry.
		// Log should be reopened if seeking cannot be completed.
		if _, err := f.file.Seek(int64(-n), os.SEEK_CUR); err != nil {
			f.Close()
		}
		return err
	}

	// Save entry to in-memory list.
	f.entries = append(f.entries, e)

	return nil
}

// measurement returns a measurement by name.
func (f *LogFile) measurement(name []byte) logMeasurement {
	mm, ok := f.mms[string(name)]
	if !ok {
		mm = logMeasurement{name: name, tagSet: make(map[string]logTagSet)}
	}
	return mm
}

// MeasurementIterator returns an iterator over all the measurements in the file.
func (f *LogFile) MeasurementIterator() MeasurementIterator {
	var itr measurementIterator
	for _, mm := range f.mms {
		itr.mms = append(itr.mms, MeasurementElem{
			Name:    mm.name,
			Deleted: mm.deleted,
		})
	}
	sort.Sort(MeasurementElems(itr.mms))
	return &itr
}

// LogEntry represents a single log entry in the write-ahead log.
type LogEntry struct {
	Flag     byte        // flag
	Name     []byte      // measurement name
	Tags     models.Tags // tagset
	Checksum uint32      // checksum of flag/name/tags.
	Size     int         // total size of record, in bytes.
}

// UnmarshalBinary unmarshals data into e.
func (e *LogEntry) UnmarshalBinary(data []byte) error {
	start := len(data)

	// Parse flag data.
	e.Flag, data = data[0], data[1:]

	// Parse name.
	sz, n := binary.Uvarint(data)
	e.Name, data = data[n:n+int(sz)], data[n+int(sz):]

	// Parse tag count.
	tagN, n := binary.Uvarint(data)
	data = data[n:]

	// Parse tags.
	tags := make(models.Tags, tagN)
	for i := range tags {
		tag := &tags[i]

		// Parse key.
		sz, n := binary.Uvarint(data)
		tag.Key, data = data[n:n+int(sz)], data[n+int(sz):]

		// Parse value.
		sz, n = binary.Uvarint(data)
		tag.Value, data = data[n:n+int(sz)], data[n+int(sz):]
	}

	// Parse checksum.
	e.Checksum, data = binary.BigEndian.Uint32(data[:4]), data[4:]

	// Save length of elem.
	e.Size = start - len(data)

	return nil
}

// appendLogEntry appends to dst and returns the new buffer.
// This updates the checksum on the entry.
func appendLogEntry(dst []byte, e *LogEntry) []byte {
	var buf [binary.MaxVarintLen64]byte
	start := len(dst)

	// Append flag.
	dst = append(dst, e.Flag)

	// Append name.
	n := binary.PutUvarint(buf[:], uint64(len(e.Name)))
	dst = append(dst, buf[:n]...)
	dst = append(dst, e.Name...)

	// Append tag count.
	n = binary.PutUvarint(buf[:], uint64(len(e.Tags)))
	dst = append(dst, buf[:n]...)

	// Append key/value pairs.
	for i := range e.Tags {
		t := &e.Tags[i]

		// Append key.
		n := binary.PutUvarint(buf[:], uint64(len(t.Key)))
		dst = append(dst, buf[:n]...)
		dst = append(dst, t.Key...)

		// Append value.
		n = binary.PutUvarint(buf[:], uint64(len(t.Value)))
		dst = append(dst, buf[:n]...)
		dst = append(dst, t.Value...)
	}

	// Calculate checksum.
	e.Checksum = crc32.ChecksumIEEE(dst[start:])

	// Append checksum.
	binary.BigEndian.PutUint32(buf[:4], e.Checksum)
	dst = append(dst, buf[:4]...)

	return dst
}

type logMeasurement struct {
	name    []byte
	tagSet  map[string]logTagSet
	deleted bool
}

type logTagSet struct {
	name      []byte
	tagValues map[string]logTagValue
	deleted   bool
}

type logTagValue struct {
	name    []byte
	deleted bool
	entries []LogEntry
}

// insertEntry inserts an entry into the tag value in sorted order.
// If another entry matches the name/tags then it is overrwritten.
func (tv *logTagValue) insertEntry(e LogEntry) {
	i := sort.Search(len(tv.entries), func(i int) bool {
		if cmp := bytes.Compare(tv.entries[i].Name, e.Name); cmp != 0 {
			return cmp != -1
		}
		return models.CompareTags(tv.entries[i].Tags, e.Tags) != -1
	})

	// Update entry if it already exists.
	if i < len(tv.entries) && bytes.Equal(tv.entries[i].Name, e.Name) && tv.entries[i].Tags.Equal(e.Tags) {
		tv.entries[i] = e
		return
	}

	// Otherwise insert new entry.
	tv.entries = append(tv.entries, LogEntry{})
	copy(tv.entries[i+1:], tv.entries[i:])
	tv.entries[i] = e
}
