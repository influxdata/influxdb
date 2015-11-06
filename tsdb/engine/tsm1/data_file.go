package tsm1

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/golang/snappy"
)

type DataFiles struct {
	path string

	files         dataFiles
	mu            sync.RWMutex
	currentFileID int
}

// DataFile provides access to a compressed TSM file
type DataFile interface {
	// Name returns the name of the underlying file.
	Name() string

	// MinTime returns the minimum time stored in the file.
	MinTime() int64

	// MaxTime returns the maximum time stored in the file.
	MaxTime() int64

	// ModTime returns the last time the file was modified.
	ModTime() time.Time

	// Size returns the size of the file in bytes.
	Size() uint32

	// Unreference decrements a reference counter indicating the file
	// is no longer in used by the caller.
	Unreference()
	// Reference increments a reference counter indicating the file
	// is in use by the caller.
	Reference()

	// Delete removes the file from underlying filesystem.  It will close
	// the file if it is not already closed.
	Delete() error

	// Deleted returns true if the the underlying files is deleted.
	Deleted() bool

	// Close closes the file.
	Close() error

	// IndexPoistion returns the offset in the file where the index blocks begins.
	IndexPosition() uint32

	// Block returns the key, compressed block and the offset for the next block given
	// a position in the file.
	Block(pos uint32) (key string, encodedBlock []byte, nextBlockStart uint32)

	// Bytes returns the raw bytes specified by the range.
	Bytes(from, to uint32) []byte

	// CompressedBlockMinTime returns the minimum time stored in the given compressed block.
	CompressedBlockMinTime(block []byte) int64

	// IndexMinMaxTimes returns the minimum and maximum times for all series stored in the index.
	IndexMinMaxTimes() map[uint64]TimeRange

	// StartingPositionForID returns the offset within the file of the first block for a given series ID.
	StartingPositionForID(id uint64) uint32
}

func (d *DataFiles) Open() error {
	files, err := filepath.Glob(filepath.Join(d.path, fmt.Sprintf("*.%s", Format)))
	if err != nil {
		return err
	}
	for _, fn := range files {
		// if the file has a checkpoint it's not valid, so remove it
		if removed := d.removeFileIfCheckpointExists(fn); removed {
			continue
		}

		id, err := idFromFileName(fn)
		if err != nil {
			return err
		}
		if id >= d.currentFileID {
			d.currentFileID = id + 1
		}
		f, err := os.OpenFile(fn, os.O_RDONLY, 0666)
		if err != nil {
			return fmt.Errorf("error opening file %s: %s", fn, err.Error())
		}
		df := NewDataFile(f)
		d.files = append(d.files, df)
	}
	sort.Sort(d.files)
	return nil
}

func (d *DataFiles) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, df := range d.files {
		_ = df.Close()
	}

	d.files = nil
	d.currentFileID = 0
	return nil
}

func (d *DataFiles) NextFileName() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.currentFileID++
	return filepath.Join(d.path, fmt.Sprintf("%07d.%s", d.currentFileID, Format))
}

// DataFileCount returns the number of data files in the database
func (d *DataFiles) Count() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.files)
}

func (d *DataFiles) Add(files ...DataFile) {
	// update the engine to point at the new dataFiles
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, fr := range files {
		exists := false

		for _, df := range d.files {

			if df == fr {
				exists = true
				break
			}
		}
		if !exists {
			d.files = append(d.files, fr)
		}
	}

	sort.Sort(d.files)
}

func (d *DataFiles) Remove(files ...DataFile) {
	d.mu.Lock()
	defer d.mu.Unlock()

	var newFiles dataFiles
	for _, df := range d.files {
		removeFile := false

		for _, fr := range files {
			if df == fr {
				removeFile = true
				break
			}
		}

		if !removeFile {
			newFiles = append(newFiles, df)
		}
	}

	sort.Sort(newFiles)
	d.files = newFiles
}

func (d *DataFiles) Copy() []DataFile {
	d.mu.RLock()
	defer d.mu.RUnlock()
	a := make([]DataFile, len(d.files))
	copy(a, d.files)
	return a
}

func (d *DataFiles) Replace(files dataFiles) {
	// update the delete map and files
	d.mu.Lock()
	defer d.mu.Unlock()

	d.files = files
}

func (d *DataFiles) Overlapping(min, max int64) []DataFile {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var a []DataFile
	for _, f := range d.files {
		fmin, fmax := f.MinTime(), f.MaxTime()
		if min < fmax && fmin < max {
			a = append(a, f)
		} else if max >= fmin && max < fmax {
			a = append(a, f)
		}
	}
	return a
}

func (d *DataFiles) Compactable(age time.Duration, size uint32) dataFiles {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var a dataFiles
	for _, df := range d.files {
		if time.Since(df.ModTime()) > age && df.Size() < size {
			a = append(a, df)
		} else if len(a) > 0 {
			// only compact contiguous ranges. If we hit the negative case and
			// there are files to compact, stop here
			break
		}
	}
	return a
}

// removeFileIfCheckpointExists will remove the file if its associated checkpoint fil is there.
// It returns true if the file was removed. This is for recovery of data files on startup
func (d *DataFiles) removeFileIfCheckpointExists(fileName string) bool {
	checkpointName := fmt.Sprintf("%s.%s", fileName, CheckpointExtension)
	_, err := os.Stat(checkpointName)

	// if there's no checkpoint, move on
	if err != nil {
		return false
	}

	// there's a checkpoint so we know this file isn't safe so we should remove it
	err = os.RemoveAll(fileName)
	if err != nil {
		panic(fmt.Sprintf("error removing file %s", err.Error()))
	}

	err = os.RemoveAll(checkpointName)
	if err != nil {
		panic(fmt.Sprintf("error removing file %s", err.Error()))
	}

	return true
}

type dataFile struct {
	f       *os.File
	mu      sync.RWMutex
	size    uint32
	modTime time.Time
	mmap    []byte

	// refs tracks lives references to this dataFile
	refs sync.RWMutex
}

// byte size constants for the data file
const (
	fileHeaderSize  = 4
	seriesCountSize = 4
	timeSize        = 8
	seriesIDSize    = 8
	positionSize    = 4
	blockLengthSize = 4

	minTimeOffset = 20
	maxTimeOffset = 12

	// keyLengthSize is the number of bytes used to specify the length of a key in a block
	keyLengthSize = 2

	// indexEntry consists of an ID, starting position, and the min and max time
	indexEntrySize = seriesIDSize + positionSize + 2*timeSize

	// fileFooterSize is the size of the footer that's written after the index
	fileFooterSize = 2*timeSize + seriesCountSize
)

func NewDataFile(f *os.File) DataFile {
	// seek back to the beginning to hand off to the mmap
	if _, err := f.Seek(0, 0); err != nil {
		panic(fmt.Sprintf("error seeking to beginning of file: %s", err.Error()))
	}

	fInfo, err := f.Stat()
	if err != nil {
		panic(fmt.Sprintf("error getting stats of file: %s", err.Error()))
	}
	mmap, err := syscall.Mmap(int(f.Fd()), 0, int(fInfo.Size()), syscall.PROT_READ, syscall.MAP_SHARED|MAP_POPULATE)
	if err != nil {
		panic(fmt.Sprintf("error opening mmap: %s", err.Error()))
	}

	return &dataFile{
		f:       f,
		mmap:    mmap,
		size:    uint32(fInfo.Size()),
		modTime: fInfo.ModTime(),
	}
}

// Reference records a live usage of this dataFile.  When there are
// live references, the dataFile will not be able to be closed or deleted
// until it is unreferences.
func (d *dataFile) Reference() {
	d.refs.RLock()
}

// Ureference removes a live usage of this dataFile
func (d *dataFile) Unreference() {
	d.refs.RUnlock()
}

func (d *dataFile) ModTime() time.Time {
	return d.modTime
}

func (d *dataFile) Size() uint32 {
	return d.size
}

func (d *dataFile) Name() string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.Deleted() {
		return ""
	}
	return d.f.Name()
}

func (d *dataFile) Deleted() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.f == nil
}

func (d *dataFile) Close() error {
	// Allow the current references to expire
	d.refs.Lock()
	defer d.refs.Unlock()

	d.mu.Lock()
	defer d.mu.Unlock()
	return d.close()
}

func (d *dataFile) Delete() error {
	// Allow the current references to expire
	d.refs.Lock()
	defer d.refs.Unlock()

	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.close(); err != nil {
		return err
	}

	if d.f == nil {
		return nil
	}

	err := os.RemoveAll(d.f.Name())
	if err != nil {
		return err
	}
	d.f = nil
	return nil
}

func (d *dataFile) close() error {
	if d.mmap == nil {
		return nil
	}
	err := syscall.Munmap(d.mmap)
	if err != nil {
		return err
	}

	d.mmap = nil
	return d.f.Close()
}

func (d *dataFile) MinTime() int64 {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if len(d.mmap) == 0 {
		return 0
	}
	minTimePosition := d.size - minTimeOffset
	timeBytes := d.mmap[minTimePosition : minTimePosition+timeSize]
	return int64(btou64(timeBytes))
}

func (d *dataFile) MaxTime() int64 {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if len(d.mmap) == 0 {
		return 0
	}

	maxTimePosition := d.size - maxTimeOffset
	timeBytes := d.mmap[maxTimePosition : maxTimePosition+timeSize]
	return int64(btou64(timeBytes))
}

func (d *dataFile) SeriesCount() uint32 {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if len(d.mmap) == 0 {
		return 0
	}

	return btou32(d.mmap[d.size-seriesCountSize:])
}

func (d *dataFile) IndexPosition() uint32 {
	return d.size - uint32(d.SeriesCount()*indexEntrySize+20)
}

// keyAndBlockStart will read the key at the beginning of a block or return an empty string
// if the key isn't present and give the position of the compressed block
func (d *dataFile) keyAndBlockStart(pos uint32) (string, uint32) {
	keyStart := pos + keyLengthSize
	keyLen := uint32(btou16(d.mmap[pos:keyStart]))
	if keyLen == 0 {
		return "", keyStart
	}
	key, err := snappy.Decode(nil, d.mmap[keyStart:keyStart+keyLen])
	if err != nil {
		panic(fmt.Sprintf("error decoding key: %s", err.Error()))
	}
	return string(key), keyStart + keyLen
}

func (d *dataFile) blockLengthAndEnd(blockStart uint32) (uint32, uint32) {
	length := btou32(d.mmap[blockStart : blockStart+blockLengthSize])
	return length, blockStart + length + blockLengthSize
}

// compressedBlockMinTime will return the starting time for a compressed block given
// its position
func (d *dataFile) CompressedBlockMinTime(block []byte) int64 {
	return int64(btou64(block[seriesIDSize+blockLengthSize : seriesIDSize+blockLengthSize+timeSize]))
}

// StartingPositionForID returns the position in the file of the
// first block for the given ID. If zero is returned the ID doesn't
// have any data in this file.
func (d *dataFile) StartingPositionForID(id uint64) uint32 {
	pos, _, _ := d.positionMinMaxTime(id)
	return pos
}

// positionMinMaxTime will return the position of the first block for the given ID
// and the min and max time of the values for the given ID in the data file
func (d *dataFile) positionMinMaxTime(id uint64) (uint32, int64, int64) {
	seriesCount := d.SeriesCount()
	indexStart := d.IndexPosition()

	min := uint32(0)
	max := uint32(seriesCount)

	for min < max {
		mid := (max-min)/2 + min

		offset := mid*indexEntrySize + indexStart
		checkID := btou64(d.mmap[offset : offset+seriesIDSize])

		if checkID == id {
			positionStart := offset + seriesIDSize
			positionEnd := positionStart + positionSize
			minTimeEnd := positionEnd + timeSize

			return btou32(d.mmap[positionStart:positionEnd]), int64(btou64(d.mmap[positionEnd:minTimeEnd])), int64(btou64(d.mmap[minTimeEnd : minTimeEnd+timeSize]))
		} else if checkID < id {
			min = mid + 1
		} else {
			max = mid
		}
	}

	return uint32(0), int64(0), int64(0)
}

// block will return the key, the encoded byte slice and the position of next block
func (d *dataFile) Block(pos uint32) (key string, encodedBlock []byte, nextBlockStart uint32) {
	key, start := d.keyAndBlockStart(pos)
	_, end := d.blockLengthAndEnd(start)
	return key, d.mmap[start+blockLengthSize : end], end
}

func (d *dataFile) Bytes(from, to uint32) []byte {
	return d.mmap[from:to]
}

// blockLength will return the length of the compressed block that starts at the given position
func (d *dataFile) blockLength(pos uint32) uint32 {
	return btou32(d.mmap[pos : pos+positionSize])
}

// indexMinMaxTimes will return a map of the IDs in the file along with their min and max times from the index
func (d *dataFile) IndexMinMaxTimes() map[uint64]TimeRange {
	pos := d.IndexPosition()
	stop := d.size - fileFooterSize

	m := make(map[uint64]TimeRange)
	for pos < stop {
		idEndPos := pos + seriesIDSize
		minEndPos := idEndPos + timeSize
		end := minEndPos + timeSize
		id := btou64(d.mmap[pos:idEndPos])
		min := int64(btou64(d.mmap[idEndPos:minEndPos]))
		max := int64(btou64(d.mmap[minEndPos:end]))
		pos = end

		m[id] = TimeRange{Min: min, Max: max}
	}

	return m
}

type TimeRange struct {
	Min int64
	Max int64
}

type dataFiles []DataFile

func (a dataFiles) Len() int           { return len(a) }
func (a dataFiles) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a dataFiles) Less(i, j int) bool { return a[i].MinTime() < a[j].MinTime() }

func dataFilesEquals(a, b []DataFile) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v.MinTime() != b[i].MinTime() && v.MaxTime() != b[i].MaxTime() {
			return false
		}
	}
	return true
}
