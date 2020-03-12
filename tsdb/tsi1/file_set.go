package tsi1

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"unsafe"

	"github.com/influxdata/influxdb/pkg/lifecycle"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/seriesfile"
	"github.com/influxdata/influxql"
)

// FileSet represents a collection of files.
type FileSet struct {
	sfile        *seriesfile.SeriesFile
	sfileref     *lifecycle.Reference
	files        []File
	filesref     lifecycle.References
	manifestSize int64 // Size of the manifest file in bytes.
}

// NewFileSet returns a new instance of FileSet.
func NewFileSet(sfile *seriesfile.SeriesFile, files []File) (*FileSet, error) {
	// First try to acquire a reference to the series file.
	sfileref, err := sfile.Acquire()
	if err != nil {
		return nil, err
	}

	// Next, acquire references to all of the passed in files.
	filesref := make(lifecycle.References, 0, len(files))
	for _, f := range files {
		ref, err := f.Acquire()
		if err != nil {
			filesref.Release()
			sfileref.Release()
			return nil, err
		}
		filesref = append(filesref, ref)
	}

	return &FileSet{
		sfile:    sfile,
		sfileref: sfileref,
		files:    files,
		filesref: filesref,
	}, nil
}

// bytes estimates the memory footprint of this FileSet, in bytes.
func (fs *FileSet) bytes() int {
	var b int
	// Do not count SeriesFile because it belongs to the code that constructed this FileSet.
	for _, file := range fs.files {
		b += file.bytes()
	}
	b += int(unsafe.Sizeof(*fs))
	return b
}

func (fs *FileSet) SeriesFile() *seriesfile.SeriesFile { return fs.sfile }

// Release releases all resources on the file set.
func (fs *FileSet) Release() {
	fs.filesref.Release()
	fs.sfileref.Release()
}

// Duplicate returns a copy of the FileSet, acquiring another resource to the
// files and series file for the file set.
func (fs *FileSet) Duplicate() (*FileSet, error) {
	return NewFileSet(fs.sfile, fs.files)
}

// PrependLogFile returns a new file set with f added at the beginning.
// Filters do not need to be rebuilt because log files have no bloom filter.
func (fs *FileSet) PrependLogFile(f *LogFile) (*FileSet, error) {
	return NewFileSet(fs.sfile, append([]File{f}, fs.files...))
}

// Size returns the on-disk size of the FileSet.
func (fs *FileSet) Size() int64 {
	var total int64
	for _, f := range fs.files {
		total += f.Size()
	}
	return total + int64(fs.manifestSize)
}

// MustReplace swaps a list of files for a single file and returns a new file set.
// The caller should always guarantee that the files exist and are contiguous.
func (fs *FileSet) MustReplace(oldFiles []File, newFile File) (*FileSet, error) {
	assert(len(oldFiles) > 0, "cannot replace empty files")

	// Find index of first old file.
	var i int
	for ; i < len(fs.files); i++ {
		if fs.files[i] == oldFiles[0] {
			break
		} else if i == len(fs.files)-1 {
			return nil, errors.New("first replacement file not found")
		}
	}

	// Ensure all old files are contiguous.
	for j := range oldFiles {
		if fs.files[i+j] != oldFiles[j] {
			return nil, fmt.Errorf("cannot replace non-contiguous files: subset=%+v, fileset=%+v", Files(oldFiles).IDs(), Files(fs.files).IDs())
		}
	}

	// Copy to new fileset.
	other := make([]File, len(fs.files)-len(oldFiles)+1)
	copy(other[:i], fs.files[:i])
	other[i] = newFile
	copy(other[i+1:], fs.files[i+len(oldFiles):])

	// Build new fileset and rebuild changed filters.
	return NewFileSet(fs.sfile, other)
}

// MaxID returns the highest file identifier.
func (fs *FileSet) MaxID() int {
	var max int
	for _, f := range fs.files {
		if i := f.ID(); i > max {
			max = i
		}
	}
	return max
}

// Files returns all files in the set.
func (fs *FileSet) Files() []File {
	return fs.files
}

// LogFiles returns all log files from the file set.
func (fs *FileSet) LogFiles() []*LogFile {
	var a []*LogFile
	for _, f := range fs.files {
		if f, ok := f.(*LogFile); ok {
			a = append(a, f)
		}
	}
	return a
}

// IndexFiles returns all index files from the file set.
func (fs *FileSet) IndexFiles() []*IndexFile {
	var a []*IndexFile
	for _, f := range fs.files {
		if f, ok := f.(*IndexFile); ok {
			a = append(a, f)
		}
	}
	return a
}

// LastContiguousIndexFilesByLevel returns the last contiguous files by level.
// These can be used by the compaction scheduler.
func (fs *FileSet) LastContiguousIndexFilesByLevel(level int) []*IndexFile {
	if level == 0 {
		return nil
	}

	var a []*IndexFile
	for i := len(fs.files) - 1; i >= 0; i-- {
		f := fs.files[i]

		// Ignore files above level, stop on files below level.
		if level < f.Level() {
			continue
		} else if level > f.Level() {
			break
		}

		a = append([]*IndexFile{f.(*IndexFile)}, a...)
	}
	return a
}

// Measurement returns a measurement by name.
func (fs *FileSet) Measurement(name []byte) MeasurementElem {
	for _, f := range fs.files {
		if e := f.Measurement(name); e == nil {
			continue
		} else if e.Deleted() {
			return nil
		} else {
			return e
		}
	}
	return nil
}

// MeasurementIterator returns an iterator over all measurements in the index.
func (fs *FileSet) MeasurementIterator() MeasurementIterator {
	a := make([]MeasurementIterator, 0, len(fs.files))
	for _, f := range fs.files {
		itr := f.MeasurementIterator()
		if itr != nil {
			a = append(a, itr)
		}
	}
	return MergeMeasurementIterators(a...)
}

// TagKeyIterator returns an iterator over all tag keys for a measurement.
func (fs *FileSet) TagKeyIterator(name []byte) TagKeyIterator {
	a := make([]TagKeyIterator, 0, len(fs.files))
	for _, f := range fs.files {
		itr := f.TagKeyIterator(name)
		if itr != nil {
			a = append(a, itr)
		}
	}
	return MergeTagKeyIterators(a...)
}

// MeasurementSeriesIDIterator returns a series iterator for a measurement.
func (fs *FileSet) MeasurementSeriesIDIterator(name []byte) tsdb.SeriesIDIterator {
	a := make([]tsdb.SeriesIDIterator, 0, len(fs.files))
	for _, f := range fs.files {
		itr := f.MeasurementSeriesIDIterator(name)
		if itr != nil {
			a = append(a, itr)
		}
	}
	return tsdb.MergeSeriesIDIterators(a...)
}

// MeasurementTagKeysByExpr extracts the tag keys wanted by the expression.
func (fs *FileSet) MeasurementTagKeysByExpr(name []byte, expr influxql.Expr) (map[string]struct{}, error) {
	// Return all keys if no condition was passed in.
	if expr == nil {
		m := make(map[string]struct{})
		if itr := fs.TagKeyIterator(name); itr != nil {
			for e := itr.Next(); e != nil; e = itr.Next() {
				m[string(e.Key())] = struct{}{}
			}
		}
		return m, nil
	}

	switch e := expr.(type) {
	case *influxql.BinaryExpr:
		switch e.Op {
		case influxql.EQ, influxql.NEQ, influxql.EQREGEX, influxql.NEQREGEX:
			tag, ok := e.LHS.(*influxql.VarRef)
			if !ok {
				return nil, fmt.Errorf("left side of '%s' must be a tag key", e.Op.String())
			} else if tag.Val != "_tagKey" {
				return nil, nil
			}

			if influxql.IsRegexOp(e.Op) {
				re, ok := e.RHS.(*influxql.RegexLiteral)
				if !ok {
					return nil, fmt.Errorf("right side of '%s' must be a regular expression", e.Op.String())
				}
				return fs.tagKeysByFilter(name, e.Op, nil, re.Val), nil
			}

			s, ok := e.RHS.(*influxql.StringLiteral)
			if !ok {
				return nil, fmt.Errorf("right side of '%s' must be a tag value string", e.Op.String())
			}
			return fs.tagKeysByFilter(name, e.Op, []byte(s.Val), nil), nil

		case influxql.AND, influxql.OR:
			lhs, err := fs.MeasurementTagKeysByExpr(name, e.LHS)
			if err != nil {
				return nil, err
			}

			rhs, err := fs.MeasurementTagKeysByExpr(name, e.RHS)
			if err != nil {
				return nil, err
			}

			if lhs != nil && rhs != nil {
				if e.Op == influxql.OR {
					return unionStringSets(lhs, rhs), nil
				}
				return intersectStringSets(lhs, rhs), nil
			} else if lhs != nil {
				return lhs, nil
			} else if rhs != nil {
				return rhs, nil
			}
			return nil, nil
		default:
			return nil, fmt.Errorf("invalid operator")
		}

	case *influxql.ParenExpr:
		return fs.MeasurementTagKeysByExpr(name, e.Expr)
	}

	return nil, fmt.Errorf("%#v", expr)
}

// tagKeysByFilter will filter the tag keys for the measurement.
func (fs *FileSet) tagKeysByFilter(name []byte, op influxql.Token, val []byte, regex *regexp.Regexp) map[string]struct{} {
	ss := make(map[string]struct{})
	itr := fs.TagKeyIterator(name)
	if itr != nil {
		for e := itr.Next(); e != nil; e = itr.Next() {
			var matched bool
			switch op {
			case influxql.EQ:
				matched = bytes.Equal(e.Key(), val)
			case influxql.NEQ:
				matched = !bytes.Equal(e.Key(), val)
			case influxql.EQREGEX:
				matched = regex.Match(e.Key())
			case influxql.NEQREGEX:
				matched = !regex.Match(e.Key())
			}

			if !matched {
				continue
			}
			ss[string(e.Key())] = struct{}{}
		}
	}
	return ss
}

// TagKeySeriesIDIterator returns a series iterator for all values across a single key.
func (fs *FileSet) TagKeySeriesIDIterator(name, key []byte) (tsdb.SeriesIDIterator, error) {
	a := make([]tsdb.SeriesIDIterator, 0, len(fs.files))
	for _, f := range fs.files {
		itr, err := f.TagKeySeriesIDIterator(name, key)
		if err != nil {
			return nil, err
		} else if itr != nil {
			a = append(a, itr)
		}
	}
	return tsdb.MergeSeriesIDIterators(a...), nil
}

// HasTagKey returns true if the tag key exists.
func (fs *FileSet) HasTagKey(name, key []byte) bool {
	for _, f := range fs.files {
		if e := f.TagKey(name, key); e != nil {
			return !e.Deleted()
		}
	}
	return false
}

// HasTagValue returns true if the tag value exists.
func (fs *FileSet) HasTagValue(name, key, value []byte) bool {
	for _, f := range fs.files {
		if e := f.TagValue(name, key, value); e != nil {
			return !e.Deleted()
		}
	}
	return false
}

// TagValueIterator returns a value iterator for a tag key.
func (fs *FileSet) TagValueIterator(name, key []byte) TagValueIterator {
	a := make([]TagValueIterator, 0, len(fs.files))
	for _, f := range fs.files {
		itr := f.TagValueIterator(name, key)
		if itr != nil {
			a = append(a, itr)
		}
	}
	return MergeTagValueIterators(a...)
}

// TagValueSeriesIDIterator returns a series iterator for a single tag value.
func (fs *FileSet) TagValueSeriesIDIterator(name, key, value []byte) (tsdb.SeriesIDIterator, error) {
	ss := tsdb.NewSeriesIDSet()

	var ftss *tsdb.SeriesIDSet
	for i := len(fs.files) - 1; i >= 0; i-- {
		f := fs.files[i]

		// Remove tombstones set in previous file.
		if ftss != nil && ftss.Cardinality() > 0 {
			ss.RemoveSet(ftss)
		}

		// Fetch tag value series set for this file and merge into overall set.
		fss, err := f.TagValueSeriesIDSet(name, key, value)
		if err != nil {
			return nil, err
		} else if fss != nil {
			ss.Merge(fss)
		}

		// Fetch tombstone set to be processed on next file.
		if ftss, err = f.TombstoneSeriesIDSet(); err != nil {
			return nil, err
		}
	}
	return tsdb.NewSeriesIDSetIterator(ss), nil
}

// File represents a log or index file.
type File interface {
	Close() error
	Path() string

	ID() int
	Level() int

	Measurement(name []byte) MeasurementElem
	MeasurementIterator() MeasurementIterator
	MeasurementHasSeries(ss *tsdb.SeriesIDSet, name []byte) bool

	TagKey(name, key []byte) TagKeyElem
	TagKeyIterator(name []byte) TagKeyIterator

	TagValue(name, key, value []byte) TagValueElem
	TagValueIterator(name, key []byte) TagValueIterator

	// Series iteration.
	MeasurementSeriesIDIterator(name []byte) tsdb.SeriesIDIterator
	TagKeySeriesIDIterator(name, key []byte) (tsdb.SeriesIDIterator, error)
	TagValueSeriesIDSet(name, key, value []byte) (*tsdb.SeriesIDSet, error)

	// Bitmap series existence.
	SeriesIDSet() (*tsdb.SeriesIDSet, error)
	TombstoneSeriesIDSet() (*tsdb.SeriesIDSet, error)

	// Reference counting.
	Acquire() (*lifecycle.Reference, error)

	// Size of file on disk
	Size() int64

	// Estimated memory footprint
	bytes() int
}

type Files []File

func (a Files) IDs() []int {
	ids := make([]int, len(a))
	for i := range a {
		ids[i] = a[i].ID()
	}
	return ids
}

// fileSetSeriesIDIterator attaches a fileset to an iterator that is released on close.
type fileSetSeriesIDIterator struct {
	fs  *FileSet
	itr tsdb.SeriesIDIterator
}

func newFileSetSeriesIDIterator(fs *FileSet, itr tsdb.SeriesIDIterator) tsdb.SeriesIDIterator {
	if itr == nil {
		fs.Release()
		return nil
	}
	if itr, ok := itr.(tsdb.SeriesIDSetIterator); ok {
		return &fileSetSeriesIDSetIterator{fs: fs, itr: itr}
	}
	return &fileSetSeriesIDIterator{fs: fs, itr: itr}
}

func (itr *fileSetSeriesIDIterator) Next() (tsdb.SeriesIDElem, error) {
	return itr.itr.Next()
}

func (itr *fileSetSeriesIDIterator) Close() error {
	itr.fs.Release()
	return itr.itr.Close()
}

// fileSetSeriesIDSetIterator attaches a fileset to an iterator that is released on close.
type fileSetSeriesIDSetIterator struct {
	fs  *FileSet
	itr tsdb.SeriesIDSetIterator
}

func (itr *fileSetSeriesIDSetIterator) Next() (tsdb.SeriesIDElem, error) {
	return itr.itr.Next()
}

func (itr *fileSetSeriesIDSetIterator) Close() error {
	itr.fs.Release()
	return itr.itr.Close()
}

func (itr *fileSetSeriesIDSetIterator) SeriesIDSet() *tsdb.SeriesIDSet {
	return itr.itr.SeriesIDSet()
}

// fileSetMeasurementIterator attaches a fileset to an iterator that is released on close.
type fileSetMeasurementIterator struct {
	fs  *FileSet
	itr tsdb.MeasurementIterator
}

func newFileSetMeasurementIterator(fs *FileSet, itr tsdb.MeasurementIterator) tsdb.MeasurementIterator {
	if itr == nil {
		fs.Release()
		return nil
	}
	return &fileSetMeasurementIterator{fs: fs, itr: itr}
}

func (itr *fileSetMeasurementIterator) Next() ([]byte, error) {
	return itr.itr.Next()
}

func (itr *fileSetMeasurementIterator) Close() error {
	itr.fs.Release()
	return itr.itr.Close()
}

// fileSetTagKeyIterator attaches a fileset to an iterator that is released on close.
type fileSetTagKeyIterator struct {
	fs  *FileSet
	itr tsdb.TagKeyIterator
}

func newFileSetTagKeyIterator(fs *FileSet, itr tsdb.TagKeyIterator) tsdb.TagKeyIterator {
	if itr == nil {
		fs.Release()
		return nil
	}
	return &fileSetTagKeyIterator{fs: fs, itr: itr}
}

func (itr *fileSetTagKeyIterator) Next() ([]byte, error) {
	return itr.itr.Next()
}

func (itr *fileSetTagKeyIterator) Close() error {
	itr.fs.Release()
	return itr.itr.Close()
}

// fileSetTagValueIterator attaches a fileset to an iterator that is released on close.
type fileSetTagValueIterator struct {
	fs  *FileSet
	itr tsdb.TagValueIterator
}

func newFileSetTagValueIterator(fs *FileSet, itr tsdb.TagValueIterator) tsdb.TagValueIterator {
	if itr == nil {
		fs.Release()
		return nil
	}
	return &fileSetTagValueIterator{fs: fs, itr: itr}
}

func (itr *fileSetTagValueIterator) Next() ([]byte, error) {
	return itr.itr.Next()
}

func (itr *fileSetTagValueIterator) Close() error {
	itr.fs.Release()
	return itr.itr.Close()
}
