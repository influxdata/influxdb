package tsi1

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/bytesutil"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/pkg/estimator/hll"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

// FileSet represents a collection of files.
type FileSet struct {
	levels   []CompactionLevel
	sfile    *SeriesFile
	files    []File
	database string
}

// NewFileSet returns a new instance of FileSet.
func NewFileSet(database string, levels []CompactionLevel, sfile *SeriesFile, files []File) (*FileSet, error) {
	return &FileSet{
		levels:   levels,
		sfile:    sfile,
		files:    files,
		database: database,
	}, nil
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

// SeriesFile returns the attached series file.
func (fs *FileSet) SeriesFile() *SeriesFile { return fs.sfile }

// PrependLogFile returns a new file set with f added at the beginning.
// Filters do not need to be rebuilt because log files have no bloom filter.
func (fs *FileSet) PrependLogFile(f *LogFile) *FileSet {
	return &FileSet{
		levels: fs.levels,
		sfile:  fs.sfile,
		files:  append([]File{f}, fs.files...),
	}
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

	// Build new fileset and rebuild changed filters.
	return &FileSet{
		levels: fs.levels,
		files:  other,
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

// MeasurementSeriesIDIterator returns an iterator over all non-tombstoned series
// in the index for the provided measurement.
func (fs *FileSet) MeasurementSeriesIDIterator(name []byte) SeriesIDIterator {
	a := make([]SeriesIDIterator, 0, len(fs.files))
	for _, f := range fs.files {
		itr := f.MeasurementSeriesIDIterator(name)
		if itr != nil {
			a = append(a, itr)
		}
	}
	return FilterUndeletedSeriesIDIterator(MergeSeriesIDIterators(a...))
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

// tagValuesByKeyAndExpr retrieves tag values for the provided tag keys.
//
// tagValuesByKeyAndExpr returns sets of values for each key, indexable by the
// position of the tag key in the keys argument.
//
// N.B tagValuesByKeyAndExpr relies on keys being sorted in ascending
// lexicographic order.
func (fs *FileSet) tagValuesByKeyAndExpr(auth query.Authorizer, name []byte, keys []string, expr influxql.Expr, fieldset *tsdb.MeasurementFieldSet) ([]map[string]struct{}, error) {
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
	for e := itr.Next(); e.SeriesID != 0; e = itr.Next() {
		buf := fs.sfile.SeriesKey(e.SeriesID)
		if buf == nil {
			continue
		}

		if auth != nil {
			name, tags := ParseSeriesKey(buf)
			if !auth.AuthorizeSeriesRead(fs.database, name, tags) {
				continue
			}
		}

		_, buf = ReadSeriesKeyLen(buf)
		_, buf = ReadSeriesKeyMeasurement(buf)
		tagN, buf := ReadSeriesKeyTagN(buf)
		for i := 0; i < tagN; i++ {
			var key, value []byte
			key, value, buf = ReadSeriesKeyTag(buf)

			if idx, ok := keyIdxs[string(key)]; ok {
				resultSet[idx][string(value)] = struct{}{}
			} else if string(key) > keys[len(keys)-1] {
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

// TagKeySeriesIDIterator returns a series iterator for all values across a single key.
func (fs *FileSet) TagKeySeriesIDIterator(name, key []byte) SeriesIDIterator {
	a := make([]SeriesIDIterator, 0, len(fs.files))
	for _, f := range fs.files {
		itr := f.TagKeySeriesIDIterator(name, key)
		if itr != nil {
			a = append(a, itr)
		}
	}
	return FilterUndeletedSeriesIDIterator(MergeSeriesIDIterators(a...))
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
func (fs *FileSet) TagValueSeriesIDIterator(name, key, value []byte) SeriesIDIterator {
	a := make([]SeriesIDIterator, 0, len(fs.files))
	for _, f := range fs.files {
		itr := f.TagValueSeriesIDIterator(name, key, value)
		if itr != nil {
			a = append(a, itr)
		}
	}
	return FilterUndeletedSeriesIDIterator(MergeSeriesIDIterators(a...))
}

// MatchTagValueSeriesIDIterator returns a series iterator for tags which match value.
// If matches is false, returns iterators which do not match value.
func (fs *FileSet) MatchTagValueSeriesIDIterator(name, key []byte, value *regexp.Regexp, matches bool) SeriesIDIterator {
	matchEmpty := value.MatchString("")

	if matches {
		if matchEmpty {
			return FilterUndeletedSeriesIDIterator(fs.matchTagValueEqualEmptySeriesIDIterator(name, key, value))
		}
		return FilterUndeletedSeriesIDIterator(fs.matchTagValueEqualNotEmptySeriesIDIterator(name, key, value))
	}

	if matchEmpty {
		return FilterUndeletedSeriesIDIterator(fs.matchTagValueNotEqualEmptySeriesIDIterator(name, key, value))
	}
	return FilterUndeletedSeriesIDIterator(fs.matchTagValueNotEqualNotEmptySeriesIDIterator(name, key, value))
}

func (fs *FileSet) matchTagValueEqualEmptySeriesIDIterator(name, key []byte, value *regexp.Regexp) SeriesIDIterator {
	vitr := fs.TagValueIterator(name, key)
	if vitr == nil {
		return fs.MeasurementSeriesIDIterator(name)
	}

	var itrs []SeriesIDIterator
	for e := vitr.Next(); e != nil; e = vitr.Next() {
		if !value.Match(e.Value()) {
			itrs = append(itrs, fs.TagValueSeriesIDIterator(name, key, e.Value()))
		}
	}

	return DifferenceSeriesIDIterators(
		fs.MeasurementSeriesIDIterator(name),
		MergeSeriesIDIterators(itrs...),
	)
}

func (fs *FileSet) matchTagValueEqualNotEmptySeriesIDIterator(name, key []byte, value *regexp.Regexp) SeriesIDIterator {
	vitr := fs.TagValueIterator(name, key)
	if vitr == nil {
		return nil
	}

	var itrs []SeriesIDIterator
	for e := vitr.Next(); e != nil; e = vitr.Next() {
		if value.Match(e.Value()) {
			itrs = append(itrs, fs.TagValueSeriesIDIterator(name, key, e.Value()))
		}
	}
	return MergeSeriesIDIterators(itrs...)
}

func (fs *FileSet) matchTagValueNotEqualEmptySeriesIDIterator(name, key []byte, value *regexp.Regexp) SeriesIDIterator {
	vitr := fs.TagValueIterator(name, key)
	if vitr == nil {
		return nil
	}

	var itrs []SeriesIDIterator
	for e := vitr.Next(); e != nil; e = vitr.Next() {
		if !value.Match(e.Value()) {
			itrs = append(itrs, fs.TagValueSeriesIDIterator(name, key, e.Value()))
		}
	}
	return MergeSeriesIDIterators(itrs...)
}

func (fs *FileSet) matchTagValueNotEqualNotEmptySeriesIDIterator(name, key []byte, value *regexp.Regexp) SeriesIDIterator {
	vitr := fs.TagValueIterator(name, key)
	if vitr == nil {
		return fs.MeasurementSeriesIDIterator(name)
	}

	var itrs []SeriesIDIterator
	for e := vitr.Next(); e != nil; e = vitr.Next() {
		if value.Match(e.Value()) {
			itrs = append(itrs, fs.TagValueSeriesIDIterator(name, key, e.Value()))
		}
	}

	return DifferenceSeriesIDIterators(
		fs.MeasurementSeriesIDIterator(name),
		MergeSeriesIDIterators(itrs...),
	)
}

func (fs *FileSet) MeasurementNamesByExpr(expr influxql.Expr) ([][]byte, error) {
	// Return filtered list if expression exists.
	if expr != nil {
		return fs.measurementNamesByExpr(expr)
	}

	itr := fs.MeasurementIterator()
	if itr == nil {
		return nil, nil
	}

	// Iterate over all measurements if no condition exists.
	var names [][]byte
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
	itr := fs.MeasurementIterator()
	if itr == nil {
		return nil
	}

	var names [][]byte
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
	if mitr == nil {
		return nil
	}

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
// call is equivalent to MeasurementSeriesIDIterator().
func (fs *FileSet) MeasurementSeriesByExprIterator(name []byte, expr influxql.Expr, fieldset *tsdb.MeasurementFieldSet) (SeriesIDIterator, error) {
	// Return all series for the measurement if there are no tag expressions.
	if expr == nil {
		return fs.MeasurementSeriesIDIterator(name), nil
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
	for e := itr.Next(); e.SeriesID != 0; e = itr.Next() {
		// Check for unsupported field filters.
		// Any remaining filters means there were fields (e.g., `WHERE value = 1.2`).
		if e.Expr != nil {
			if v, ok := e.Expr.(*influxql.BooleanLiteral); !ok || !v.Val {
				return nil, errors.New("fields not supported in WHERE clause during deletion")
			}
		}

		seriesKey := fs.sfile.SeriesKey(e.SeriesID)
		assert(seriesKey != nil, "series key not found")

		name, tags := ParseSeriesKey(seriesKey)
		keys = append(keys, models.MakeKey(name, tags))
	}
	return keys, nil
}

func (fs *FileSet) seriesByExprIterator(name []byte, expr influxql.Expr, mf *tsdb.MeasurementFields) (SeriesIDIterator, error) {
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
				return IntersectSeriesIDIterators(litr, ritr), nil
			}

			// Union iterators if expression is "OR".
			return UnionSeriesIDIterators(litr, ritr), nil

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
func (fs *FileSet) seriesByBinaryExprIterator(name []byte, n *influxql.BinaryExpr, mf *tsdb.MeasurementFields) (SeriesIDIterator, error) {
	// If this binary expression has another binary expression, then this
	// is some expression math and we should just pass it to the underlying query.
	if _, ok := n.LHS.(*influxql.BinaryExpr); ok {
		return newSeriesIDExprIterator(fs.MeasurementSeriesIDIterator(name), n), nil
	} else if _, ok := n.RHS.(*influxql.BinaryExpr); ok {
		return newSeriesIDExprIterator(fs.MeasurementSeriesIDIterator(name), n), nil
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
		return newSeriesIDExprIterator(fs.MeasurementSeriesIDIterator(name), n), nil
	} else if value, ok := value.(*influxql.VarRef); ok {
		// Check if the RHS is a variable and if it is a field.
		if value.Val != "_name" && ((value.Type == influxql.Unknown && mf.HasField(value.Val)) || key.Type == influxql.AnyField || (value.Type != influxql.Tag && value.Type != influxql.Unknown)) {
			return newSeriesIDExprIterator(fs.MeasurementSeriesIDIterator(name), n), nil
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
			return fs.MeasurementSeriesIDIterator(name), nil
		}
		return nil, nil
	}
}

func (fs *FileSet) seriesByBinaryExprStringIterator(name, key, value []byte, op influxql.Token) (SeriesIDIterator, error) {
	// Special handling for "_name" to match measurement name.
	if bytes.Equal(key, []byte("_name")) {
		if (op == influxql.EQ && bytes.Equal(value, name)) || (op == influxql.NEQ && !bytes.Equal(value, name)) {
			return fs.MeasurementSeriesIDIterator(name), nil
		}
		return nil, nil
	}

	if op == influxql.EQ {
		// Match a specific value.
		if len(value) != 0 {
			return fs.TagValueSeriesIDIterator(name, key, value), nil
		}

		// Return all measurement series that have no values from this tag key.
		return DifferenceSeriesIDIterators(
			fs.MeasurementSeriesIDIterator(name),
			fs.TagKeySeriesIDIterator(name, key),
		), nil
	}

	// Return all measurement series without this tag value.
	if len(value) != 0 {
		return DifferenceSeriesIDIterators(
			fs.MeasurementSeriesIDIterator(name),
			fs.TagValueSeriesIDIterator(name, key, value),
		), nil
	}

	// Return all series across all values of this tag key.
	return fs.TagKeySeriesIDIterator(name, key), nil
}

func (fs *FileSet) seriesByBinaryExprRegexIterator(name, key []byte, value *regexp.Regexp, op influxql.Token) (SeriesIDIterator, error) {
	// Special handling for "_name" to match measurement name.
	if bytes.Equal(key, []byte("_name")) {
		match := value.Match(name)
		if (op == influxql.EQREGEX && match) || (op == influxql.NEQREGEX && !match) {
			return newSeriesIDExprIterator(fs.MeasurementSeriesIDIterator(name), &influxql.BooleanLiteral{Val: true}), nil
		}
		return nil, nil
	}
	return fs.MatchTagValueSeriesIDIterator(name, key, value, op == influxql.EQREGEX), nil
}

func (fs *FileSet) seriesByBinaryExprVarRefIterator(name, key []byte, value *influxql.VarRef, op influxql.Token) (SeriesIDIterator, error) {
	if op == influxql.EQ {
		return IntersectSeriesIDIterators(
			fs.TagKeySeriesIDIterator(name, key),
			fs.TagKeySeriesIDIterator(name, []byte(value.Val)),
		), nil
	}

	return DifferenceSeriesIDIterators(
		fs.TagKeySeriesIDIterator(name, key),
		fs.TagKeySeriesIDIterator(name, []byte(value.Val)),
	), nil
}

// File represents a log or index file.
type File interface {
	Close() error
	Path() string

	ID() int
	Level() int

	Measurement(name []byte) MeasurementElem
	MeasurementIterator() MeasurementIterator
	// HasSeries(name []byte, tags models.Tags, buf []byte) (exists, tombstoned bool)
	// Series(name []byte, tags models.Tags) SeriesIDElem
	// SeriesN() uint64

	TagKey(name, key []byte) TagKeyElem
	TagKeyIterator(name []byte) TagKeyIterator

	TagValue(name, key, value []byte) TagValueElem
	TagValueIterator(name, key []byte) TagValueIterator

	// Series iteration.
	// SeriesIDIterator() SeriesIDIterator
	MeasurementSeriesIDIterator(name []byte) SeriesIDIterator
	TagKeySeriesIDIterator(name, key []byte) SeriesIDIterator
	TagValueSeriesIDIterator(name, key, value []byte) SeriesIDIterator

	// Sketches for cardinality estimation
	MergeMeasurementsSketches(s, t estimator.Sketch) error

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
