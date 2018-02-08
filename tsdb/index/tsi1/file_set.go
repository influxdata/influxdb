package tsi1

import (
	"bytes"
	"fmt"
	"regexp"
	"sync"

	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/pkg/estimator/hll"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

// FileSet represents a collection of files.
type FileSet struct {
	levels       []CompactionLevel
	sfile        *tsdb.SeriesFile
	files        []File
	database     string
	manifestSize int64 // Size of the manifest file in bytes.
}

// NewFileSet returns a new instance of FileSet.
func NewFileSet(database string, levels []CompactionLevel, sfile *tsdb.SeriesFile, files []File) (*FileSet, error) {
	return &FileSet{
		levels:   levels,
		sfile:    sfile,
		files:    files,
		database: database,
	}, nil
}

// Close closes all the files in the file set.
func (fs FileSet) Close() error {
	var err error
	for _, f := range fs.files {
		if e := f.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

// Retain adds a reference count to all files.
func (fs *FileSet) Retain() {
	for _, f := range fs.files {
		f.Retain()
	}
}

// Release removes a reference count from all files.
func (fs *FileSet) Release() {
	for _, f := range fs.files {
		f.Release()
	}
}

// SeriesFile returns the attached series file.
func (fs *FileSet) SeriesFile() *tsdb.SeriesFile { return fs.sfile }

// PrependLogFile returns a new file set with f added at the beginning.
// Filters do not need to be rebuilt because log files have no bloom filter.
func (fs *FileSet) PrependLogFile(f *LogFile) *FileSet {
	return &FileSet{
		database: fs.database,
		levels:   fs.levels,
		sfile:    fs.sfile,
		files:    append([]File{f}, fs.files...),
	}
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
func (fs *FileSet) MustReplace(oldFiles []File, newFile File) *FileSet {
	assert(len(oldFiles) > 0, "cannot replace empty files")

	// Find index of first old file.
	var i int
	for ; i < len(fs.files); i++ {
		if fs.files[i] == oldFiles[0] {
			break
		} else if i == len(fs.files)-1 {
			panic("first replacement file not found")
		}
	}

	// Ensure all old files are contiguous.
	for j := range oldFiles {
		if fs.files[i+j] != oldFiles[j] {
			panic(fmt.Sprintf("cannot replace non-contiguous files: subset=%+v, fileset=%+v", Files(oldFiles).IDs(), Files(fs.files).IDs()))
		}
	}

	// Copy to new fileset.
	other := make([]File, len(fs.files)-len(oldFiles)+1)
	copy(other[:i], fs.files[:i])
	other[i] = newFile
	copy(other[i+1:], fs.files[i+len(oldFiles):])

	// Build new fileset and rebuild changed filters.
	return &FileSet{
		levels:   fs.levels,
		files:    other,
		database: fs.database,
	}
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

/*
// SeriesIDIterator returns an iterator over all series in the index.
func (fs *FileSet) SeriesIDIterator() tsdb.SeriesIDIterator {
	a := make([]tsdb.SeriesIDIterator, 0, len(fs.files))
	for _, f := range fs.files {
		itr := f.SeriesIDIterator()
		if itr == nil {
			continue
		}
		a = append(a, itr)
	}
	return FilterUndeletedSeriesIterator(MergeSeriesIterators(a...))
}
*/

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
func (fs *FileSet) TagKeySeriesIDIterator(name, key []byte) tsdb.SeriesIDIterator {
	a := make([]tsdb.SeriesIDIterator, 0, len(fs.files))
	for _, f := range fs.files {
		itr := f.TagKeySeriesIDIterator(name, key)
		if itr != nil {
			a = append(a, itr)
		}
	}
	return tsdb.MergeSeriesIDIterators(a...)
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
func (fs *FileSet) TagValueSeriesIDIterator(name, key, value []byte) tsdb.SeriesIDIterator {
	a := make([]tsdb.SeriesIDIterator, 0, len(fs.files))
	for _, f := range fs.files {
		itr := f.TagValueSeriesIDIterator(name, key, value)
		if itr != nil {
			a = append(a, itr)
		}
	}
	return tsdb.MergeSeriesIDIterators(a...)
}

// MeasurementsSketches returns the merged measurement sketches for the FileSet.
func (fs *FileSet) MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error) {
	sketch, tsketch := hll.NewDefaultPlus(), hll.NewDefaultPlus()

	// Iterate over all the files and merge the sketches into the result.
	for _, f := range fs.files {
		if err := f.MergeMeasurementsSketches(sketch, tsketch); err != nil {
			return nil, nil, err
		}
	}
	return sketch, tsketch, nil
}

// SeriesSketches returns the merged measurement sketches for the FileSet.
func (fs *FileSet) SeriesSketches() (estimator.Sketch, estimator.Sketch, error) {
	sketch, tsketch := hll.NewDefaultPlus(), hll.NewDefaultPlus()

	// Iterate over all the files and merge the sketches into the result.
	for _, f := range fs.files {
		if err := f.MergeSeriesSketches(sketch, tsketch); err != nil {
			return nil, nil, err
		}
	}
	return sketch, tsketch, nil
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
	TagKeySeriesIDIterator(name, key []byte) tsdb.SeriesIDIterator
	TagValueSeriesIDIterator(name, key, value []byte) tsdb.SeriesIDIterator

	// Sketches for cardinality estimation
	MergeMeasurementsSketches(s, t estimator.Sketch) error
	MergeSeriesSketches(s, t estimator.Sketch) error

	// Bitmap series existance.
	SeriesIDSet() (*tsdb.SeriesIDSet, error)
	TombstoneSeriesIDSet() (*tsdb.SeriesIDSet, error)

	// Reference counting.
	Retain()
	Release()

	// Size of file on disk
	Size() int64
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
	once sync.Once
	fs   *FileSet
	itr  tsdb.SeriesIDIterator
}

func newFileSetSeriesIDIterator(fs *FileSet, itr tsdb.SeriesIDIterator) tsdb.SeriesIDIterator {
	if itr == nil {
		fs.Release()
		return nil
	}
	return &fileSetSeriesIDIterator{fs: fs, itr: itr}
}

func (itr *fileSetSeriesIDIterator) Next() (tsdb.SeriesIDElem, error) {
	return itr.itr.Next()
}

func (itr *fileSetSeriesIDIterator) Close() error {
	itr.once.Do(func() { itr.fs.Release() })
	return itr.itr.Close()
}

// fileSetMeasurementIterator attaches a fileset to an iterator that is released on close.
type fileSetMeasurementIterator struct {
	once sync.Once
	fs   *FileSet
	itr  tsdb.MeasurementIterator
}

func newFileSetMeasurementIterator(fs *FileSet, itr tsdb.MeasurementIterator) *fileSetMeasurementIterator {
	return &fileSetMeasurementIterator{fs: fs, itr: itr}
}

func (itr *fileSetMeasurementIterator) Next() ([]byte, error) {
	return itr.itr.Next()
}

func (itr *fileSetMeasurementIterator) Close() error {
	itr.once.Do(func() { itr.fs.Release() })
	return itr.itr.Close()
}

// fileSetTagKeyIterator attaches a fileset to an iterator that is released on close.
type fileSetTagKeyIterator struct {
	once sync.Once
	fs   *FileSet
	itr  tsdb.TagKeyIterator
}

func newFileSetTagKeyIterator(fs *FileSet, itr tsdb.TagKeyIterator) *fileSetTagKeyIterator {
	return &fileSetTagKeyIterator{fs: fs, itr: itr}
}

func (itr *fileSetTagKeyIterator) Next() ([]byte, error) {
	return itr.itr.Next()
}

func (itr *fileSetTagKeyIterator) Close() error {
	itr.once.Do(func() { itr.fs.Release() })
	return itr.itr.Close()
}

// fileSetTagValueIterator attaches a fileset to an iterator that is released on close.
type fileSetTagValueIterator struct {
	once sync.Once
	fs   *FileSet
	itr  tsdb.TagValueIterator
}

func newFileSetTagValueIterator(fs *FileSet, itr tsdb.TagValueIterator) *fileSetTagValueIterator {
	return &fileSetTagValueIterator{fs: fs, itr: itr}
}

func (itr *fileSetTagValueIterator) Next() ([]byte, error) {
	return itr.itr.Next()
}

func (itr *fileSetTagValueIterator) Close() error {
	itr.once.Do(func() { itr.fs.Release() })
	return itr.itr.Close()
}
