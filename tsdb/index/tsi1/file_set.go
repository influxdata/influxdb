package tsi1

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/bloom"
	"github.com/influxdata/influxdb/pkg/bytesutil"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/pkg/estimator/hll"
	"github.com/influxdata/influxdb/tsdb"
)

// FileSet represents a collection of files.
type FileSet struct {
	levels  []CompactionLevel
	files   []File
	filters []*bloom.Filter // per-level filters
}

// NewFileSet returns a new instance of FileSet.
func NewFileSet(levels []CompactionLevel, files []File) (*FileSet, error) {
	fs := &FileSet{levels: levels, files: files}
	if err := fs.buildFilters(); err != nil {
		return nil, err
	}
	return fs, nil
}

// Close closes all the files in the file set.
func (p FileSet) Close() error {
	var err error
	for _, f := range p.files {
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

// Prepend returns a new file set with f added at the beginning.
func (fs *FileSet) Prepend(f File) (*FileSet, error) {
	return NewFileSet(fs.levels, append([]File{f}, fs.files...))
}

// MustReplace swaps a list of files for a single file and returns a new file set.
// The caller should always guarentee that the files exist and are contiguous.
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

	fs, err := NewFileSet(fs.levels, other)
	if err != nil {
		panic("cannot build file set: " + err.Error())
	}
	return fs
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

// SeriesIterator returns an iterator over all series in the index.
func (fs *FileSet) SeriesIterator() SeriesIterator {
	a := make([]SeriesIterator, 0, len(fs.files))
	for _, f := range fs.files {
		itr := f.SeriesIterator()
		if itr == nil {
			continue
		}
		a = append(a, itr)
	}
	return FilterUndeletedSeriesIterator(MergeSeriesIterators(a...))
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
	return FilterUndeletedMeasurementIterator(MergeMeasurementIterators(a...))
}

// MeasurementSeriesIterator returns an iterator over all non-tombstoned series
// in the index for the provided measurement.
func (fs *FileSet) MeasurementSeriesIterator(name []byte) SeriesIterator {
	a := make([]SeriesIterator, 0, len(fs.files))
	for _, f := range fs.files {
		itr := f.MeasurementSeriesIterator(name)
		if itr != nil {
			a = append(a, itr)
		}
	}
	return FilterUndeletedSeriesIterator(MergeSeriesIterators(a...))
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

// MeasurementTagKeysByExpr extracts the tag keys wanted by the expression.
func (fs *FileSet) MeasurementTagKeysByExpr(name []byte, expr influxql.Expr) (map[string]struct{}, error) {
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

// tagValuesByKeyAndExpr retrieves tag values for the provided tag keys.
//
// tagValuesByKeyAndExpr returns sets of values for each key, indexable by the
// position of the tag key in the keys argument.
//
// N.B tagValuesByKeyAndExpr relies on keys being sorted in ascending
// lexicographic order.
func (fs *FileSet) tagValuesByKeyAndExpr(name []byte, keys []string, expr influxql.Expr, fieldset *tsdb.MeasurementFieldSet) ([]map[string]struct{}, error) {
	itr, err := fs.seriesByExprIterator(name, expr, fieldset.Fields(string(name)))
	if err != nil {
		return nil, err
	} else if itr == nil {
		return nil, nil
	}

	keyIdxs := make(map[string]int, len(keys))
	for ki, key := range keys {
		keyIdxs[key] = ki

		// Check that keys are in order.
		if ki > 0 && key < keys[ki-1] {
			return nil, fmt.Errorf("keys %v are not in ascending order", keys)
		}
	}

	resultSet := make([]map[string]struct{}, len(keys))
	for i := 0; i < len(resultSet); i++ {
		resultSet[i] = make(map[string]struct{})
	}

	// Iterate all series to collect tag values.
	for e := itr.Next(); e != nil; e = itr.Next() {
		for _, t := range e.Tags() {
			if idx, ok := keyIdxs[string(t.Key)]; ok {
				resultSet[idx][string(t.Value)] = struct{}{}
			} else if string(t.Key) > keys[len(keys)-1] {
				// The tag key is > the largest key we're interested in.
				break
			}
		}
	}
	return resultSet, nil
}

// tagKeysByFilter will filter the tag keys for the measurement.
func (fs *FileSet) tagKeysByFilter(name []byte, op influxql.Token, val []byte, regex *regexp.Regexp) map[string]struct{} {
	ss := make(map[string]struct{})
	itr := fs.TagKeyIterator(name)
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
	return ss
}

// TagKeySeriesIterator returns a series iterator for all values across a single key.
func (fs *FileSet) TagKeySeriesIterator(name, key []byte) SeriesIterator {
	a := make([]SeriesIterator, 0, len(fs.files))
	for _, f := range fs.files {
		itr := f.TagKeySeriesIterator(name, key)
		if itr != nil {
			a = append(a, itr)
		}
	}
	return FilterUndeletedSeriesIterator(MergeSeriesIterators(a...))
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

// TagValueSeriesIterator returns a series iterator for a single tag value.
func (fs *FileSet) TagValueSeriesIterator(name, key, value []byte) SeriesIterator {
	a := make([]SeriesIterator, 0, len(fs.files))
	for _, f := range fs.files {
		itr := f.TagValueSeriesIterator(name, key, value)
		if itr != nil {
			a = append(a, itr)
		}
	}
	return FilterUndeletedSeriesIterator(MergeSeriesIterators(a...))
}

// MatchTagValueSeriesIterator returns a series iterator for tags which match value.
// If matches is false, returns iterators which do not match value.
func (fs *FileSet) MatchTagValueSeriesIterator(name, key []byte, value *regexp.Regexp, matches bool) SeriesIterator {
	matchEmpty := value.MatchString("")

	if matches {
		if matchEmpty {
			return FilterUndeletedSeriesIterator(fs.matchTagValueEqualEmptySeriesIterator(name, key, value))
		}
		return FilterUndeletedSeriesIterator(fs.matchTagValueEqualNotEmptySeriesIterator(name, key, value))
	}

	if matchEmpty {
		return FilterUndeletedSeriesIterator(fs.matchTagValueNotEqualEmptySeriesIterator(name, key, value))
	}
	return FilterUndeletedSeriesIterator(fs.matchTagValueNotEqualNotEmptySeriesIterator(name, key, value))
}

func (fs *FileSet) matchTagValueEqualEmptySeriesIterator(name, key []byte, value *regexp.Regexp) SeriesIterator {
	vitr := fs.TagValueIterator(name, key)
	if vitr == nil {
		return fs.MeasurementSeriesIterator(name)
	}

	var itrs []SeriesIterator
	for e := vitr.Next(); e != nil; e = vitr.Next() {
		if !value.Match(e.Value()) {
			itrs = append(itrs, fs.TagValueSeriesIterator(name, key, e.Value()))
		}
	}

	return DifferenceSeriesIterators(
		fs.MeasurementSeriesIterator(name),
		MergeSeriesIterators(itrs...),
	)
}

func (fs *FileSet) matchTagValueEqualNotEmptySeriesIterator(name, key []byte, value *regexp.Regexp) SeriesIterator {
	vitr := fs.TagValueIterator(name, key)
	if vitr == nil {
		return nil
	}

	var itrs []SeriesIterator
	for e := vitr.Next(); e != nil; e = vitr.Next() {
		if value.Match(e.Value()) {
			itrs = append(itrs, fs.TagValueSeriesIterator(name, key, e.Value()))
		}
	}
	return MergeSeriesIterators(itrs...)
}

func (fs *FileSet) matchTagValueNotEqualEmptySeriesIterator(name, key []byte, value *regexp.Regexp) SeriesIterator {
	vitr := fs.TagValueIterator(name, key)
	if vitr == nil {
		return nil
	}

	var itrs []SeriesIterator
	for e := vitr.Next(); e != nil; e = vitr.Next() {
		if !value.Match(e.Value()) {
			itrs = append(itrs, fs.TagValueSeriesIterator(name, key, e.Value()))
		}
	}
	return MergeSeriesIterators(itrs...)
}

func (fs *FileSet) matchTagValueNotEqualNotEmptySeriesIterator(name, key []byte, value *regexp.Regexp) SeriesIterator {
	vitr := fs.TagValueIterator(name, key)
	if vitr == nil {
		return fs.MeasurementSeriesIterator(name)
	}

	var itrs []SeriesIterator
	for e := vitr.Next(); e != nil; e = vitr.Next() {
		if value.Match(e.Value()) {
			itrs = append(itrs, fs.TagValueSeriesIterator(name, key, e.Value()))
		}
	}

	return DifferenceSeriesIterators(
		fs.MeasurementSeriesIterator(name),
		MergeSeriesIterators(itrs...),
	)
}

func (fs *FileSet) MeasurementNamesByExpr(expr influxql.Expr) ([][]byte, error) {
	// Return filtered list if expression exists.
	if expr != nil {
		return fs.measurementNamesByExpr(expr)
	}

	// Iterate over all measurements if no condition exists.
	var names [][]byte
	itr := fs.MeasurementIterator()
	for e := itr.Next(); e != nil; e = itr.Next() {
		names = append(names, e.Name())
	}
	return names, nil
}

func (fs *FileSet) measurementNamesByExpr(expr influxql.Expr) ([][]byte, error) {
	if expr == nil {
		return nil, nil
	}

	switch e := expr.(type) {
	case *influxql.BinaryExpr:
		switch e.Op {
		case influxql.EQ, influxql.NEQ, influxql.EQREGEX, influxql.NEQREGEX:
			tag, ok := e.LHS.(*influxql.VarRef)
			if !ok {
				return nil, fmt.Errorf("left side of '%s' must be a tag key", e.Op.String())
			}

			// Retrieve value or regex expression from RHS.
			var value string
			var regex *regexp.Regexp
			if influxql.IsRegexOp(e.Op) {
				re, ok := e.RHS.(*influxql.RegexLiteral)
				if !ok {
					return nil, fmt.Errorf("right side of '%s' must be a regular expression", e.Op.String())
				}
				regex = re.Val
			} else {
				s, ok := e.RHS.(*influxql.StringLiteral)
				if !ok {
					return nil, fmt.Errorf("right side of '%s' must be a tag value string", e.Op.String())
				}
				value = s.Val
			}

			// Match on name, if specified.
			if tag.Val == "_name" {
				return fs.measurementNamesByNameFilter(e.Op, value, regex), nil
			} else if influxql.IsSystemName(tag.Val) {
				return nil, nil
			}
			return fs.measurementNamesByTagFilter(e.Op, tag.Val, value, regex), nil

		case influxql.OR, influxql.AND:
			lhs, err := fs.measurementNamesByExpr(e.LHS)
			if err != nil {
				return nil, err
			}

			rhs, err := fs.measurementNamesByExpr(e.RHS)
			if err != nil {
				return nil, err
			}

			if e.Op == influxql.OR {
				return bytesutil.Union(lhs, rhs), nil
			}
			return bytesutil.Intersect(lhs, rhs), nil

		default:
			return nil, fmt.Errorf("invalid tag comparison operator")
		}

	case *influxql.ParenExpr:
		return fs.measurementNamesByExpr(e.Expr)
	default:
		return nil, fmt.Errorf("%#v", expr)
	}
}

// measurementNamesByNameFilter returns matching measurement names in sorted order.
func (fs *FileSet) measurementNamesByNameFilter(op influxql.Token, val string, regex *regexp.Regexp) [][]byte {
	var names [][]byte
	itr := fs.MeasurementIterator()
	for e := itr.Next(); e != nil; e = itr.Next() {
		var matched bool
		switch op {
		case influxql.EQ:
			matched = string(e.Name()) == val
		case influxql.NEQ:
			matched = string(e.Name()) != val
		case influxql.EQREGEX:
			matched = regex.Match(e.Name())
		case influxql.NEQREGEX:
			matched = !regex.Match(e.Name())
		}

		if matched {
			names = append(names, e.Name())
		}
	}
	bytesutil.Sort(names)
	return names
}

func (fs *FileSet) measurementNamesByTagFilter(op influxql.Token, key, val string, regex *regexp.Regexp) [][]byte {
	var names [][]byte

	mitr := fs.MeasurementIterator()
	for me := mitr.Next(); me != nil; me = mitr.Next() {
		// If the operator is non-regex, only check the specified value.
		var tagMatch bool
		if op == influxql.EQ || op == influxql.NEQ {
			if fs.HasTagValue(me.Name(), []byte(key), []byte(val)) {
				tagMatch = true
			}
		} else {
			// Else, the operator is a regex and we have to check all tag
			// values against the regular expression.
			vitr := fs.TagValueIterator(me.Name(), []byte(key))
			if vitr != nil {
				for ve := vitr.Next(); ve != nil; ve = vitr.Next() {
					if regex.Match(ve.Value()) {
						tagMatch = true
						break
					}
				}
			}
		}

		//
		// XNOR gate
		//
		// tags match | operation is EQ | measurement matches
		// --------------------------------------------------
		//     True   |       True      |      True
		//     True   |       False     |      False
		//     False  |       True      |      False
		//     False  |       False     |      True
		if tagMatch == (op == influxql.EQ || op == influxql.EQREGEX) {
			names = append(names, me.Name())
			continue
		}
	}

	bytesutil.Sort(names)
	return names
}

// HasSeries returns true if the series exists and is not tombstoned.
func (fs *FileSet) HasSeries(name []byte, tags models.Tags, buf []byte) bool {
	for _, f := range fs.files {
		if exists, tombstoned := f.HasSeries(name, tags, buf); exists {
			return !tombstoned
		}
	}
	return false
}

// FilterNamesTags filters out any series which already exist. It modifies the
// provided slices of names and tags.
func (fs *FileSet) FilterNamesTags(names [][]byte, tagsSlice []models.Tags) ([][]byte, []models.Tags) {
	buf := make([]byte, 4096)

	// Filter across all log files.
	// Log files obtain a read lock and should be done in bulk for performance.
	for _, f := range fs.LogFiles() {
		names, tagsSlice = f.FilterNamesTags(names, tagsSlice)
	}

	// Filter across remaining index files.
	indexFiles := fs.IndexFiles()
	newNames, newTagsSlice := names[:0], tagsSlice[:0]
	for i := range names {
		name, tags := names[i], tagsSlice[i]
		currentLevel, skipLevel := -1, false

		var exists, tombstoned bool
		for j := 0; j < len(indexFiles); j++ {
			f := indexFiles[j]

			// Check for existence on the level when it changes.
			if level := f.Level(); currentLevel != level {
				currentLevel, skipLevel = level, false

				if filter := fs.filters[level]; filter != nil {
					if !filter.Contains(AppendSeriesKey(buf[:0], name, tags)) {
						skipLevel = true
					}
				}
			}

			// Skip file if in level where it doesn't exist.
			if skipLevel {
				continue
			}

			// Stop once we find the series in a file.
			if exists, tombstoned = f.HasSeries(name, tags, buf); exists {
				break
			}
		}

		// If the series doesn't exist or it has been tombstoned then add it.
		if !exists || tombstoned {
			newNames = append(newNames, name)
			newTagsSlice = append(newTagsSlice, tags)
		}
	}
	return newNames, newTagsSlice
}

// SeriesSketches returns the merged series sketches for the FileSet.
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

// MeasurementSeriesByExprIterator returns a series iterator for a measurement
// that is filtered by expr. If expr only contains time expressions then this
// call is equivalent to MeasurementSeriesIterator().
func (fs *FileSet) MeasurementSeriesByExprIterator(name []byte, expr influxql.Expr, fieldset *tsdb.MeasurementFieldSet) (SeriesIterator, error) {
	// Return all series for the measurement if there are no tag expressions.
	if expr == nil {
		return fs.MeasurementSeriesIterator(name), nil
	}
	return fs.seriesByExprIterator(name, expr, fieldset.CreateFieldsIfNotExists(name))
}

// MeasurementSeriesKeysByExpr returns a list of series keys matching expr.
func (fs *FileSet) MeasurementSeriesKeysByExpr(name []byte, expr influxql.Expr, fieldset *tsdb.MeasurementFieldSet) ([][]byte, error) {
	// Create iterator for all matching series.
	itr, err := fs.MeasurementSeriesByExprIterator(name, expr, fieldset)
	if err != nil {
		return nil, err
	} else if itr == nil {
		return nil, nil
	}

	// Iterate over all series and generate keys.
	var keys [][]byte
	for e := itr.Next(); e != nil; e = itr.Next() {
		// Check for unsupported field filters.
		// Any remaining filters means there were fields (e.g., `WHERE value = 1.2`).
		if e.Expr() != nil {
			return nil, errors.New("fields not supported in WHERE clause during deletion")
		}

		keys = append(keys, models.MakeKey(e.Name(), e.Tags()))
	}
	return keys, nil
}

func (fs *FileSet) seriesByExprIterator(name []byte, expr influxql.Expr, mf *tsdb.MeasurementFields) (SeriesIterator, error) {
	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		switch expr.Op {
		case influxql.AND, influxql.OR:
			// Get the series IDs and filter expressions for the LHS.
			litr, err := fs.seriesByExprIterator(name, expr.LHS, mf)
			if err != nil {
				return nil, err
			}

			// Get the series IDs and filter expressions for the RHS.
			ritr, err := fs.seriesByExprIterator(name, expr.RHS, mf)
			if err != nil {
				return nil, err
			}

			// Intersect iterators if expression is "AND".
			if expr.Op == influxql.AND {
				return IntersectSeriesIterators(litr, ritr), nil
			}

			// Union iterators if expression is "OR".
			return UnionSeriesIterators(litr, ritr), nil

		default:
			return fs.seriesByBinaryExprIterator(name, expr, mf)
		}

	case *influxql.ParenExpr:
		return fs.seriesByExprIterator(name, expr.Expr, mf)

	default:
		return nil, nil
	}
}

// seriesByBinaryExprIterator returns a series iterator and a filtering expression.
func (fs *FileSet) seriesByBinaryExprIterator(name []byte, n *influxql.BinaryExpr, mf *tsdb.MeasurementFields) (SeriesIterator, error) {
	// If this binary expression has another binary expression, then this
	// is some expression math and we should just pass it to the underlying query.
	if _, ok := n.LHS.(*influxql.BinaryExpr); ok {
		return newSeriesExprIterator(fs.MeasurementSeriesIterator(name), n), nil
	} else if _, ok := n.RHS.(*influxql.BinaryExpr); ok {
		return newSeriesExprIterator(fs.MeasurementSeriesIterator(name), n), nil
	}

	// Retrieve the variable reference from the correct side of the expression.
	key, ok := n.LHS.(*influxql.VarRef)
	value := n.RHS
	if !ok {
		key, ok = n.RHS.(*influxql.VarRef)
		if !ok {
			return nil, fmt.Errorf("invalid expression: %s", n.String())
		}
		value = n.LHS
	}

	// For fields, return all series from this measurement.
	if key.Val != "_name" && ((key.Type == influxql.Unknown && mf.HasField(key.Val)) || key.Type == influxql.AnyField || (key.Type != influxql.Tag && key.Type != influxql.Unknown)) {
		return newSeriesExprIterator(fs.MeasurementSeriesIterator(name), n), nil
	} else if value, ok := value.(*influxql.VarRef); ok {
		// Check if the RHS is a variable and if it is a field.
		if value.Val != "_name" && ((value.Type == influxql.Unknown && mf.HasField(value.Val)) || key.Type == influxql.AnyField || (value.Type != influxql.Tag && value.Type != influxql.Unknown)) {
			return newSeriesExprIterator(fs.MeasurementSeriesIterator(name), n), nil
		}
	}

	// Create iterator based on value type.
	switch value := value.(type) {
	case *influxql.StringLiteral:
		return fs.seriesByBinaryExprStringIterator(name, []byte(key.Val), []byte(value.Val), n.Op)
	case *influxql.RegexLiteral:
		return fs.seriesByBinaryExprRegexIterator(name, []byte(key.Val), value.Val, n.Op)
	case *influxql.VarRef:
		return fs.seriesByBinaryExprVarRefIterator(name, []byte(key.Val), value, n.Op)
	default:
		if n.Op == influxql.NEQ || n.Op == influxql.NEQREGEX {
			return fs.MeasurementSeriesIterator(name), nil
		}
		return nil, nil
	}
}

func (fs *FileSet) seriesByBinaryExprStringIterator(name, key, value []byte, op influxql.Token) (SeriesIterator, error) {
	// Special handling for "_name" to match measurement name.
	if bytes.Equal(key, []byte("_name")) {
		if (op == influxql.EQ && bytes.Equal(value, name)) || (op == influxql.NEQ && !bytes.Equal(value, name)) {
			return fs.MeasurementSeriesIterator(name), nil
		}
		return nil, nil
	}

	if op == influxql.EQ {
		// Match a specific value.
		if len(value) != 0 {
			return fs.TagValueSeriesIterator(name, key, value), nil
		}

		// Return all measurement series that have no values from this tag key.
		return DifferenceSeriesIterators(
			fs.MeasurementSeriesIterator(name),
			fs.TagKeySeriesIterator(name, key),
		), nil
	}

	// Return all measurement series without this tag value.
	if len(value) != 0 {
		return DifferenceSeriesIterators(
			fs.MeasurementSeriesIterator(name),
			fs.TagValueSeriesIterator(name, key, value),
		), nil
	}

	// Return all series across all values of this tag key.
	return fs.TagKeySeriesIterator(name, key), nil
}

func (fs *FileSet) seriesByBinaryExprRegexIterator(name, key []byte, value *regexp.Regexp, op influxql.Token) (SeriesIterator, error) {
	// Special handling for "_name" to match measurement name.
	if bytes.Equal(key, []byte("_name")) {
		match := value.Match(name)
		if (op == influxql.EQREGEX && match) || (op == influxql.NEQREGEX && !match) {
			return newSeriesExprIterator(fs.MeasurementSeriesIterator(name), &influxql.BooleanLiteral{Val: true}), nil
		}
		return nil, nil
	}
	return fs.MatchTagValueSeriesIterator(name, key, value, op == influxql.EQREGEX), nil
}

func (fs *FileSet) seriesByBinaryExprVarRefIterator(name, key []byte, value *influxql.VarRef, op influxql.Token) (SeriesIterator, error) {
	if op == influxql.EQ {
		return IntersectSeriesIterators(
			fs.TagKeySeriesIterator(name, key),
			fs.TagKeySeriesIterator(name, []byte(value.Val)),
		), nil
	}

	return DifferenceSeriesIterators(
		fs.TagKeySeriesIterator(name, key),
		fs.TagKeySeriesIterator(name, []byte(value.Val)),
	), nil
}

// buildFilters builds a series existence filter for each compaction level.
func (fs *FileSet) buildFilters() error {
	if len(fs.levels) == 0 {
		fs.filters = nil
		return nil
	}

	// Generate filters for each level.
	fs.filters = make([]*bloom.Filter, len(fs.levels))

	// Merge filters at each level.
	for _, f := range fs.files {
		level := f.Level()

		// Skip if file has no bloom filter.
		if f.Filter() == nil {
			continue
		}

		// Initialize a filter if it doesn't exist.
		if fs.filters[level] == nil {
			lvl := fs.levels[level]
			fs.filters[level] = bloom.NewFilter(lvl.M, lvl.K)
		}

		// Merge filter.
		if err := fs.filters[level].Merge(f.Filter()); err != nil {
			return err
		}
	}

	return nil
}

// File represents a log or index file.
type File interface {
	Close() error
	Path() string

	ID() int
	Level() int

	Measurement(name []byte) MeasurementElem
	MeasurementIterator() MeasurementIterator
	HasSeries(name []byte, tags models.Tags, buf []byte) (exists, tombstoned bool)
	Series(name []byte, tags models.Tags) SeriesElem
	SeriesN() uint64

	TagKey(name, key []byte) TagKeyElem
	TagKeyIterator(name []byte) TagKeyIterator

	TagValue(name, key, value []byte) TagValueElem
	TagValueIterator(name, key []byte) TagValueIterator

	// Series iteration.
	SeriesIterator() SeriesIterator
	MeasurementSeriesIterator(name []byte) SeriesIterator
	TagKeySeriesIterator(name, key []byte) SeriesIterator
	TagValueSeriesIterator(name, key, value []byte) SeriesIterator

	// Sketches for cardinality estimation
	MergeSeriesSketches(s, t estimator.Sketch) error
	MergeMeasurementsSketches(s, t estimator.Sketch) error

	// Series existence bloom filter.
	Filter() *bloom.Filter

	// Reference counting.
	Retain()
	Release()
}

type Files []File

func (a Files) IDs() []int {
	ids := make([]int, len(a))
	for i := range a {
		ids[i] = a[i].ID()
	}
	return ids
}
