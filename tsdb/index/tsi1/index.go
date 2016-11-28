package tsi1

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/influxdata/influxdb/pkg/estimator/hll"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/bytesutil"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/tsdb"
)

// Default compaction thresholds.
const (
	DefaultMaxLogFileSize = 1 * 1024 * 1024 // 10MB

	DefaultCompactionMonitorInterval = 30 * time.Second
)

func init() {
	tsdb.RegisterIndex("tsi1", func(id uint64, path string, opt tsdb.EngineOptions) tsdb.Index {
		idx := NewIndex()
		idx.ShardID = id
		idx.Path = path
		return idx
	})
}

// File extensions.
const (
	LogFileExt   = ".tsl"
	IndexFileExt = ".tsi"

	CompactingExt = ".compacting"
)

// ManifestFileName is the name of the index manifest file.
const ManifestFileName = "MANIFEST"

// Ensure index implements the interface.
var _ tsdb.Index = &Index{}

// Index represents a collection of layered index files and WAL.
type Index struct {
	mu         sync.RWMutex
	opened     bool
	logFiles   []*LogFile
	indexFiles IndexFiles

	// Compaction management.
	manualCompactNotify chan compactNotify
	fastCompactNotify   chan struct{}

	// Close management.
	closing chan struct{}
	wg      sync.WaitGroup

	// Fieldset shared with engine.
	fieldset *tsdb.MeasurementFieldSet

	// Associated shard info.
	ShardID uint64

	// Root directory of the index files.
	Path string

	// Log file compaction thresholds.
	MaxLogFileSize int64

	// Frequency of compaction checks.
	CompactionMonitorInterval time.Duration
}

// NewIndex returns a new instance of Index.
func NewIndex() *Index {
	return &Index{
		manualCompactNotify: make(chan compactNotify),
		fastCompactNotify:   make(chan struct{}),

		closing: make(chan struct{}),

		// Default compaction thresholds.
		MaxLogFileSize: DefaultMaxLogFileSize,

		CompactionMonitorInterval: DefaultCompactionMonitorInterval,
	}
}

// Open opens the index.
func (i *Index) Open() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.opened {
		return errors.New("index already open")
	}

	// Create directory if it doesn't exist.
	if err := os.MkdirAll(i.Path, 0777); err != nil {
		return err
	}

	// Read manifest file.
	m, err := ReadManifestFile(filepath.Join(i.Path, ManifestFileName))
	if os.IsNotExist(err) {
		m = &Manifest{}
	} else if err != nil {
		return err
	}

	// Ensure at least one log file exists.
	if len(m.LogFiles) == 0 {
		m.LogFiles = []string{FormatLogFileName(1)}

		if err := i.writeManifestFile(); err != nil {
			return err
		}
	}

	// Open each log file in the manifest.
	for _, filename := range m.LogFiles {
		f, err := i.openLogFile(filepath.Join(i.Path, filename))
		if err != nil {
			return err
		}
		i.logFiles = append(i.logFiles, f)
	}

	// Open each index file in the manifest.
	for _, filename := range m.IndexFiles {
		f, err := i.openIndexFile(filepath.Join(i.Path, filename))
		if err != nil {
			return err
		}
		i.indexFiles = append(i.indexFiles, f)
	}

	// Delete any files not in the manifest.
	if err := i.deleteNonManifestFiles(m); err != nil {
		return err
	}

	// Start compaction monitor.
	i.wg.Add(1)
	go func() { defer i.wg.Done(); i.monitorCompaction() }()

	// Mark opened.
	i.opened = true

	return nil
}

// openLogFile opens a log file and appends it to the index.
func (i *Index) openLogFile(path string) (*LogFile, error) {
	f := NewLogFile()
	f.Path = path
	if err := f.Open(); err != nil {
		return nil, err
	}
	return f, nil
}

// openIndexFile opens a log file and appends it to the index.
func (i *Index) openIndexFile(path string) (*IndexFile, error) {
	f := NewIndexFile()
	f.Path = path
	if err := f.Open(); err != nil {
		return nil, err
	}
	return f, nil
}

// deleteNonManifestFiles removes all files not in the manifest.
func (i *Index) deleteNonManifestFiles(m *Manifest) error {
	dir, err := os.Open(i.Path)
	if err != nil {
		return err
	}
	defer dir.Close()

	fis, err := dir.Readdir(-1)
	if err != nil {
		return err
	}

	// Loop over all files and remove any not in the manifest.
	for _, fi := range fis {
		filename := filepath.Base(fi.Name())
		if filename == ManifestFileName || m.HasFile(filename) {
			continue
		}

		if err := os.RemoveAll(filename); err != nil {
			return err
		}
	}

	return nil
}

// Close closes the index.
func (i *Index) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Close log files.
	for _, f := range i.logFiles {
		f.Close()
	}
	i.logFiles = nil

	// Close index files.
	for _, f := range i.indexFiles {
		f.Close()
	}
	i.indexFiles = nil

	return nil
}

// ManifestPath returns the path to the index's manifest file.
func (i *Index) ManifestPath() string {
	return filepath.Join(i.Path, ManifestFileName)
}

// Manifest returns a manifest for the index.
func (i *Index) Manifest() *Manifest {
	m := &Manifest{
		LogFiles:   make([]string, len(i.logFiles)),
		IndexFiles: make([]string, len(i.indexFiles)),
	}

	for j, f := range i.logFiles {
		m.LogFiles[j] = filepath.Base(f.Path)
	}
	for j, f := range i.indexFiles {
		m.IndexFiles[j] = filepath.Base(f.Path)
	}

	return m
}

// writeManifestFile writes the manifest to the appropriate file path.
func (i *Index) writeManifestFile() error {
	return WriteManifestFile(i.ManifestPath(), i.Manifest())
}

// maxFileID returns the highest file id from the index.
func (i *Index) maxFileID() int {
	var max int
	for _, f := range i.logFiles {
		if i := ParseFileID(f.Path); i > max {
			max = i
		}
	}
	for _, f := range i.indexFiles {
		if i := ParseFileID(f.Path); i > max {
			max = i
		}
	}
	return max
}

// SetFieldSet sets a shared field set from the engine.
func (i *Index) SetFieldSet(fs *tsdb.MeasurementFieldSet) {
	i.mu.Lock()
	i.fieldset = fs
	i.mu.Unlock()
}

// FileN returns the number of log and index files within the index.
func (i *Index) FileN() int { return len(i.logFiles) + len(i.indexFiles) }

// files returns a list of all log & index files.
//
// OPTIMIZE(benbjohnson): Convert to an iterator to remove allocation.
func (i *Index) files() []File {
	a := make([]File, 0, len(i.logFiles)+len(i.indexFiles))
	for _, f := range i.logFiles {
		a = append(a, f)
	}
	for _, f := range i.indexFiles {
		a = append(a, f)
	}
	return a
}

// SeriesIterator returns an iterator over all series in the index.
func (i *Index) SeriesIterator() SeriesIterator {
	a := make([]SeriesIterator, 0, i.FileN())
	for _, f := range i.files() {
		itr := f.SeriesIterator()
		if itr == nil {
			continue
		}
		a = append(a, itr)
	}
	return FilterUndeletedSeriesIterator(MergeSeriesIterators(a...))
}

// ForEachMeasurementName iterates over all measurement names in the index.
func (i *Index) ForEachMeasurementName(fn func(name []byte) error) error {
	itr := i.MeasurementIterator()
	if itr == nil {
		return nil
	}

	for e := itr.Next(); e != nil; e = itr.Next() {
		if err := fn(e.Name()); err != nil {
			return err
		}
	}

	return nil
}

// MeasurementSeriesIterator returns an iterator over all series in the index.
func (i *Index) MeasurementSeriesIterator(name []byte) SeriesIterator {
	a := make([]SeriesIterator, 0, i.FileN())
	for _, f := range i.files() {
		itr := f.MeasurementSeriesIterator(name)
		if itr != nil {
			a = append(a, itr)
		}
	}
	return FilterUndeletedSeriesIterator(MergeSeriesIterators(a...))
}

// TagKeyIterator returns an iterator over all tag keys for a measurement.
func (i *Index) TagKeyIterator(name []byte) TagKeyIterator {
	a := make([]TagKeyIterator, 0, i.FileN())
	for _, f := range i.files() {
		itr := f.TagKeyIterator(name)
		if itr != nil {
			a = append(a, itr)
		}
	}
	return MergeTagKeyIterators(a...)
}

// MeasurementIterator returns an iterator over all measurements in the index.
func (i *Index) MeasurementIterator() MeasurementIterator {
	a := make([]MeasurementIterator, 0, i.FileN())
	for _, f := range i.logFiles {
		a = append(a, f.MeasurementIterator())
	}
	for _, f := range i.indexFiles {
		a = append(a, f.MeasurementIterator())
	}
	return FilterUndeletedMeasurementIterator(MergeMeasurementIterators(a...))
}

func (i *Index) MeasurementNamesByExpr(expr influxql.Expr) ([][]byte, error) {
	// Return filtered list if expression exists.
	if expr != nil {
		return i.measurementNamesByExpr(expr)
	}

	// Iterate over all measurements if no condition exists.
	var names [][]byte
	itr := i.MeasurementIterator()
	for e := itr.Next(); e != nil; e = itr.Next() {
		names = append(names, e.Name())
	}
	return names, nil
}

func (i *Index) measurementNamesByExpr(expr influxql.Expr) ([][]byte, error) {
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
				return i.measurementNamesByNameFilter(e.Op, value, regex), nil
			} else if influxql.IsSystemName(tag.Val) {
				return nil, nil
			}
			return i.measurementNamesByTagFilter(e.Op, tag.Val, value, regex), nil

		case influxql.OR, influxql.AND:
			lhs, err := i.measurementNamesByExpr(e.LHS)
			if err != nil {
				return nil, err
			}

			rhs, err := i.measurementNamesByExpr(e.RHS)
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
		return i.measurementNamesByExpr(e.Expr)
	default:
		return nil, fmt.Errorf("%#v", expr)
	}
}

// measurementNamesByNameFilter returns matching measurement names in sorted order.
func (i *Index) measurementNamesByNameFilter(op influxql.Token, val string, regex *regexp.Regexp) [][]byte {
	var names [][]byte
	itr := i.MeasurementIterator()
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

func (i *Index) measurementNamesByTagFilter(op influxql.Token, key, val string, regex *regexp.Regexp) [][]byte {
	var names [][]byte

	mitr := i.MeasurementIterator()
	for me := mitr.Next(); me != nil; me = mitr.Next() {
		// If the operator is non-regex, only check the specified value.
		var tagMatch bool
		if op == influxql.EQ || op == influxql.NEQ {
			if i.HasTagValue(me.Name(), []byte(key), []byte(val)) {
				tagMatch = true
			}
		} else {
			// Else, the operator is a regex and we have to check all tag
			// values against the regular expression.
			vitr := i.TagValueIterator(me.Name(), []byte(key))
			for ve := vitr.Next(); ve != nil; ve = vitr.Next() {
				if regex.Match(ve.Value()) {
					tagMatch = true
					break
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

func (i *Index) MeasurementNamesByRegex(re *regexp.Regexp) ([][]byte, error) {
	itr := i.MeasurementIterator()
	var a [][]byte
	for e := itr.Next(); e != nil; e = itr.Next() {
		if re.Match(e.Name()) {
			a = append(a, e.Name())
		}
	}
	return a, nil
}

// DropMeasurement deletes a measurement from the index.
func (i *Index) DropMeasurement(name []byte) error {
	// Delete all keys and values.
	if kitr := i.TagKeyIterator(name); kitr != nil {
		for k := kitr.Next(); k != nil; k = kitr.Next() {
			// Delete key if not already deleted.
			if !k.Deleted() {
				if err := i.logFiles[0].DeleteTagKey(name, k.Key()); err != nil {
					return err
				}
			}

			// Delete each value in key.
			if vitr := k.TagValueIterator(); vitr != nil {
				for v := vitr.Next(); v != nil; v = vitr.Next() {
					if !v.Deleted() {
						if err := i.logFiles[0].DeleteTagValue(name, k.Key(), v.Value()); err != nil {
							return err
						}
					}
				}
			}
		}
	}

	// Delete all series in measurement.
	if sitr := i.MeasurementSeriesIterator(name); sitr != nil {
		for s := sitr.Next(); s != nil; s = sitr.Next() {
			if !s.Deleted() {
				if err := i.logFiles[0].DeleteSeries(s.Name(), s.Tags()); err != nil {
					return err
				}
			}
		}
	}

	// Mark measurement as deleted.
	if err := i.logFiles[0].DeleteMeasurement(name); err != nil {
		return err
	}

	i.CheckFastCompaction()
	return nil
}

// CreateSeriesIfNotExists creates a series if it doesn't exist or is deleted.
func (i *Index) CreateSeriesIfNotExists(key, name []byte, tags models.Tags) error {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if e := i.series(name, tags); e != nil {
		return nil
	}

	if err := i.logFiles[0].AddSeries(name, tags); err != nil {
		return err
	}

	i.checkFastCompaction()
	return nil
}

// series returns a series by name/tags.
func (i *Index) series(name []byte, tags models.Tags) SeriesElem {
	for _, f := range i.files() {
		if e := f.Series(name, tags); e != nil && !e.Deleted() {
			return e
		}
	}
	return nil
}

func (i *Index) DropSeries(keys [][]byte) error {
	for _, key := range keys {
		name, tags, err := models.ParseKey(key)
		if err != nil {
			return err
		}

		if err := i.logFiles[0].DeleteSeries([]byte(name), tags); err != nil {
			return err
		}
	}

	i.CheckFastCompaction()
	return nil
}

func (i *Index) SeriesN() (n uint64, err error) {
	// FIXME(edd): Use sketches.

	// HACK(benbjohnson): Use first log file until edd adds sketches.
	return i.logFiles[0].SeriesN(), nil
}

func (i *Index) sketches(nextSketches func(*IndexFile) (estimator.Sketch, estimator.Sketch)) (estimator.Sketch, estimator.Sketch, error) {
	sketch, tsketch := hll.NewDefaultPlus(), hll.NewDefaultPlus()

	// Iterate over all the index files and merge all the sketches.
	for _, f := range i.indexFiles {
		s, t := nextSketches(f)
		if err := sketch.Merge(s); err != nil {
			return nil, nil, err
		}

		if err := tsketch.Merge(t); err != nil {
			return nil, nil, err
		}
	}
	return sketch, tsketch, nil
}

// SeriesSketches returns the two sketches for the index by merging all
// instances of the type sketch types in all the indexes files.
func (i *Index) SeriesSketches() (estimator.Sketch, estimator.Sketch, error) {
	sketch, tsketch, err := i.sketches(func(i *IndexFile) (estimator.Sketch, estimator.Sketch) {
		return i.sblk.sketch, i.sblk.tsketch
	})

	if err != nil {
		return nil, nil, err
	}

	// Merge in any current log file sketches.
	for _, f := range i.logFiles {
		if err := sketch.Merge(f.sSketch); err != nil {
			return nil, nil, err
		}
		if err := tsketch.Merge(f.sTSketch); err != nil {
			return nil, nil, err
		}
	}
	return sketch, tsketch, err
}

// MeasurementsSketches returns the two sketches for the index by merging all
// instances of the type sketch types in all the indexes files.
func (i *Index) MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error) {
	sketch, tsketch, err := i.sketches(func(i *IndexFile) (estimator.Sketch, estimator.Sketch) {
		return i.mblk.sketch, i.mblk.tsketch
	})

	if err != nil {
		return nil, nil, err
	}

	// Merge in any current log file sketches.
	for _, f := range i.logFiles {
		if err := sketch.Merge(f.mSketch); err != nil {
			return nil, nil, err
		}
		if err := tsketch.Merge(f.mTSketch); err != nil {
			return nil, nil, err
		}
	}
	return sketch, tsketch, err
}

// Dereference is a nop.
func (i *Index) Dereference([]byte) {}

// MeasurementTagKeysByExpr extracts the tag keys wanted by the expression.
func (i *Index) MeasurementTagKeysByExpr(name []byte, expr influxql.Expr) (map[string]struct{}, error) {
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
				return i.tagKeysByFilter(name, e.Op, nil, re.Val), nil
			}

			s, ok := e.RHS.(*influxql.StringLiteral)
			if !ok {
				return nil, fmt.Errorf("right side of '%s' must be a tag value string", e.Op.String())
			}
			return i.tagKeysByFilter(name, e.Op, []byte(s.Val), nil), nil

		case influxql.AND, influxql.OR:
			lhs, err := i.MeasurementTagKeysByExpr(name, e.LHS)
			if err != nil {
				return nil, err
			}

			rhs, err := i.MeasurementTagKeysByExpr(name, e.RHS)
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
		return i.MeasurementTagKeysByExpr(name, e.Expr)
	}

	return nil, fmt.Errorf("%#v", expr)
}

// tagKeysByFilter will filter the tag keys for the measurement.
func (i *Index) tagKeysByFilter(name []byte, op influxql.Token, val []byte, regex *regexp.Regexp) map[string]struct{} {
	ss := make(map[string]struct{})
	itr := i.TagKeyIterator(name)
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
func (i *Index) TagKeySeriesIterator(name, key []byte) SeriesIterator {
	a := make([]SeriesIterator, 0, i.FileN())
	for _, f := range i.files() {
		itr := f.TagKeySeriesIterator(name, key)
		if itr != nil {
			a = append(a, itr)
		}
	}
	return FilterUndeletedSeriesIterator(MergeSeriesIterators(a...))
}

// HasTagValue returns true if the tag value exists.
func (i *Index) HasTagValue(name, key, value []byte) bool {
	for _, f := range i.files() {
		if e := f.TagValue(name, key, value); e != nil {
			return !e.Deleted()
		}
	}
	return false
}

// TagValueIterator returns a value iterator for a tag key.
func (i *Index) TagValueIterator(name, key []byte) TagValueIterator {
	a := make([]TagValueIterator, 0, i.FileN())
	for _, f := range i.files() {
		itr := f.TagValueIterator(name, key)
		if itr != nil {
			a = append(a, itr)
		}
	}
	return MergeTagValueIterators(a...)
}

// TagValueSeriesIterator returns a series iterator for a single tag value.
func (i *Index) TagValueSeriesIterator(name, key, value []byte) SeriesIterator {
	a := make([]SeriesIterator, 0, i.FileN())
	for _, f := range i.files() {
		itr := f.TagValueSeriesIterator(name, key, value)
		if itr != nil {
			a = append(a, itr)
		}
	}
	return FilterUndeletedSeriesIterator(MergeSeriesIterators(a...))
}

// MatchTagValueSeriesIterator returns a series iterator for tags which match value.
// If matches is false, returns iterators which do not match value.
func (i *Index) MatchTagValueSeriesIterator(name, key []byte, value *regexp.Regexp, matches bool) SeriesIterator {
	matchEmpty := value.MatchString("")

	if matches {
		if matchEmpty {
			return FilterUndeletedSeriesIterator(i.matchTagValueEqualEmptySeriesIterator(name, key, value))
		}
		return FilterUndeletedSeriesIterator(i.matchTagValueEqualNotEmptySeriesIterator(name, key, value))
	}

	if matchEmpty {
		return FilterUndeletedSeriesIterator(i.matchTagValueNotEqualEmptySeriesIterator(name, key, value))
	}
	return FilterUndeletedSeriesIterator(i.matchTagValueNotEqualNotEmptySeriesIterator(name, key, value))
}

func (i *Index) matchTagValueEqualEmptySeriesIterator(name, key []byte, value *regexp.Regexp) SeriesIterator {
	vitr := i.TagValueIterator(name, key)
	if vitr == nil {
		return i.MeasurementSeriesIterator(name)
	}

	var itrs []SeriesIterator
	for e := vitr.Next(); e != nil; e = vitr.Next() {
		if !value.Match(e.Value()) {
			itrs = append(itrs, i.TagValueSeriesIterator(name, key, e.Value()))
		}
	}

	return DifferenceSeriesIterators(
		i.MeasurementSeriesIterator(name),
		MergeSeriesIterators(itrs...),
	)
}

func (i *Index) matchTagValueEqualNotEmptySeriesIterator(name, key []byte, value *regexp.Regexp) SeriesIterator {
	vitr := i.TagValueIterator(name, key)
	if vitr == nil {
		return nil
	}

	var itrs []SeriesIterator
	for e := vitr.Next(); e != nil; e = vitr.Next() {
		if value.Match(e.Value()) {
			itrs = append(itrs, i.TagValueSeriesIterator(name, key, e.Value()))
		}
	}
	return MergeSeriesIterators(itrs...)
}

func (i *Index) matchTagValueNotEqualEmptySeriesIterator(name, key []byte, value *regexp.Regexp) SeriesIterator {
	vitr := i.TagValueIterator(name, key)
	if vitr == nil {
		return nil
	}

	var itrs []SeriesIterator
	for e := vitr.Next(); e != nil; e = vitr.Next() {
		if !value.Match(e.Value()) {
			itrs = append(itrs, i.TagValueSeriesIterator(name, key, e.Value()))
		}
	}
	return MergeSeriesIterators(itrs...)
}

func (i *Index) matchTagValueNotEqualNotEmptySeriesIterator(name, key []byte, value *regexp.Regexp) SeriesIterator {
	vitr := i.TagValueIterator(name, key)
	if vitr == nil {
		return i.MeasurementSeriesIterator(name)
	}

	var itrs []SeriesIterator
	for e := vitr.Next(); e != nil; e = vitr.Next() {
		if value.Match(e.Value()) {
			itrs = append(itrs, i.TagValueSeriesIterator(name, key, e.Value()))
		}
	}

	return DifferenceSeriesIterators(
		i.MeasurementSeriesIterator(name),
		MergeSeriesIterators(itrs...),
	)
}

// ForEachMeasurementSeriesByExpr iterates over all series in a measurement filtered by an expression.
func (i *Index) ForEachMeasurementSeriesByExpr(name []byte, condition influxql.Expr, fn func(tags models.Tags) error) error {
	itr, err := i.MeasurementSeriesByExprIterator(name, condition)
	if err != nil {
		return err
	} else if itr == nil {
		return nil
	}

	for e := itr.Next(); e != nil; e = itr.Next() {
		if err := fn(e.Tags()); err != nil {
			return err
		}
	}

	return nil
}

// ForEachMeasurementTagKey iterates over all tag keys in a measurement.
func (i *Index) ForEachMeasurementTagKey(name []byte, fn func(key []byte) error) error {
	itr := i.TagKeyIterator(name)
	if itr == nil {
		return nil
	}

	for e := itr.Next(); e != nil; e = itr.Next() {
		if err := fn(e.Key()); err != nil {
			return err
		}
	}

	return nil
}

// TagSets returns an ordered list of tag sets for a measurement by dimension
// and filtered by an optional conditional expression.
func (i *Index) TagSets(name []byte, dimensions []string, condition influxql.Expr) ([]*influxql.TagSet, error) {
	itr, err := i.MeasurementSeriesByExprIterator(name, condition)
	if err != nil {
		return nil, err
	} else if itr == nil {
		return nil, nil
	}

	// For every series, get the tag values for the requested tag keys i.e.
	// dimensions. This is the TagSet for that series. Series with the same
	// TagSet are then grouped together, because for the purpose of GROUP BY
	// they are part of the same composite series.
	tagSets := make(map[string]*influxql.TagSet, 64)

	for e := itr.Next(); e != nil; e = itr.Next() {
		tags := make(map[string]string, len(dimensions))

		// Build the TagSet for this series.
		for _, dim := range dimensions {
			tags[dim] = e.Tags().GetString(dim)
		}

		// Convert the TagSet to a string, so it can be added to a map
		// allowing TagSets to be handled as a set.
		tagsAsKey := tsdb.MarshalTags(tags)
		tagSet, ok := tagSets[string(tagsAsKey)]
		if !ok {
			// This TagSet is new, create a new entry for it.
			tagSet = &influxql.TagSet{
				Tags: tags,
				Key:  tagsAsKey,
			}
		}
		// Associate the series and filter with the Tagset.
		tagSet.AddFilter(string(SeriesElemKey(e)), e.Expr())

		// Ensure it's back in the map.
		tagSets[string(tagsAsKey)] = tagSet
	}

	// Sort the series in each tag set.
	for _, t := range tagSets {
		sort.Sort(t)
	}

	// The TagSets have been created, as a map of TagSets. Just send
	// the values back as a slice, sorting for consistency.
	sortedTagsSets := make([]*influxql.TagSet, 0, len(tagSets))
	for _, v := range tagSets {
		sortedTagsSets = append(sortedTagsSets, v)
	}
	sort.Sort(byTagKey(sortedTagsSets))

	return sortedTagsSets, nil
}

// MeasurementSeriesByExprIterator returns a series iterator for a measurement
// that is filtered by expr. If expr only contains time expressions then this
// call is equivalent to MeasurementSeriesIterator().
func (i *Index) MeasurementSeriesByExprIterator(name []byte, expr influxql.Expr) (SeriesIterator, error) {
	// Return all series for the measurement if there are no tag expressions.
	if expr == nil || influxql.OnlyTimeExpr(expr) {
		return i.MeasurementSeriesIterator(name), nil
	}
	return i.seriesByExprIterator(name, expr, i.fieldset.CreateFieldsIfNotExists(string(name)))
}

// MeasurementSeriesKeysByExpr returns a list of series keys matching expr.
func (i *Index) MeasurementSeriesKeysByExpr(name []byte, expr influxql.Expr) ([][]byte, error) {
	// Create iterator for all matching series.
	itr, err := i.MeasurementSeriesByExprIterator(name, expr)
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

func (i *Index) seriesByExprIterator(name []byte, expr influxql.Expr, mf *tsdb.MeasurementFields) (SeriesIterator, error) {
	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		switch expr.Op {
		case influxql.AND, influxql.OR:
			// Get the series IDs and filter expressions for the LHS.
			litr, err := i.seriesByExprIterator(name, expr.LHS, mf)
			if err != nil {
				return nil, err
			}

			// Get the series IDs and filter expressions for the RHS.
			ritr, err := i.seriesByExprIterator(name, expr.RHS, mf)
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
			return i.seriesByBinaryExprIterator(name, expr, mf)
		}

	case *influxql.ParenExpr:
		return i.seriesByExprIterator(name, expr.Expr, mf)

	default:
		return nil, nil
	}
}

// seriesByBinaryExprIterator returns a series iterator and a filtering expression.
func (i *Index) seriesByBinaryExprIterator(name []byte, n *influxql.BinaryExpr, mf *tsdb.MeasurementFields) (SeriesIterator, error) {
	// If this binary expression has another binary expression, then this
	// is some expression math and we should just pass it to the underlying query.
	if _, ok := n.LHS.(*influxql.BinaryExpr); ok {
		return newSeriesExprIterator(i.MeasurementSeriesIterator(name), n), nil
	} else if _, ok := n.RHS.(*influxql.BinaryExpr); ok {
		return newSeriesExprIterator(i.MeasurementSeriesIterator(name), n), nil
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

	// For time literals, return all series and "true" as the filter.
	if _, ok := value.(*influxql.TimeLiteral); ok || key.Val == "time" {
		return newSeriesExprIterator(i.MeasurementSeriesIterator(name), &influxql.BooleanLiteral{Val: true}), nil
	}

	// For fields, return all series from this measurement.
	if key.Val != "_name" && ((key.Type == influxql.Unknown && mf.HasField(key.Val)) || key.Type == influxql.AnyField || (key.Type != influxql.Tag && key.Type != influxql.Unknown)) {
		return newSeriesExprIterator(i.MeasurementSeriesIterator(name), n), nil
	} else if value, ok := value.(*influxql.VarRef); ok {
		// Check if the RHS is a variable and if it is a field.
		if value.Val != "_name" && ((value.Type == influxql.Unknown && mf.HasField(value.Val)) || key.Type == influxql.AnyField || (value.Type != influxql.Tag && value.Type != influxql.Unknown)) {
			return newSeriesExprIterator(i.MeasurementSeriesIterator(name), n), nil
		}
	}

	// Create iterator based on value type.
	switch value := value.(type) {
	case *influxql.StringLiteral:
		return i.seriesByBinaryExprStringIterator(name, []byte(key.Val), []byte(value.Val), n.Op)
	case *influxql.RegexLiteral:
		return i.seriesByBinaryExprRegexIterator(name, []byte(key.Val), value.Val, n.Op)
	case *influxql.VarRef:
		return i.seriesByBinaryExprVarRefIterator(name, []byte(key.Val), value, n.Op)
	default:
		if n.Op == influxql.NEQ || n.Op == influxql.NEQREGEX {
			return i.MeasurementSeriesIterator(name), nil
		}
		return nil, nil
	}
}

func (i *Index) seriesByBinaryExprStringIterator(name, key, value []byte, op influxql.Token) (SeriesIterator, error) {
	// Special handling for "_name" to match measurement name.
	if bytes.Equal(key, []byte("_name")) {
		if (op == influxql.EQ && bytes.Equal(value, name)) || (op == influxql.NEQ && !bytes.Equal(value, name)) {
			return i.MeasurementSeriesIterator(name), nil
		}
		return nil, nil
	}

	if op == influxql.EQ {
		// Match a specific value.
		if len(value) != 0 {
			return i.TagValueSeriesIterator(name, key, value), nil
		}

		// Return all measurement series that have no values from this tag key.
		return DifferenceSeriesIterators(
			i.MeasurementSeriesIterator(name),
			i.TagKeySeriesIterator(name, key),
		), nil
	}

	// Return all measurement series without this tag value.
	if len(value) != 0 {
		return DifferenceSeriesIterators(
			i.MeasurementSeriesIterator(name),
			i.TagValueSeriesIterator(name, key, value),
		), nil
	}

	// Return all series across all values of this tag key.
	return i.TagKeySeriesIterator(name, key), nil
}

func (i *Index) seriesByBinaryExprRegexIterator(name, key []byte, value *regexp.Regexp, op influxql.Token) (SeriesIterator, error) {
	// Special handling for "_name" to match measurement name.
	if bytes.Equal(key, []byte("_name")) {
		match := value.Match(name)
		if (op == influxql.EQREGEX && match) || (op == influxql.NEQREGEX && !match) {
			return newSeriesExprIterator(i.MeasurementSeriesIterator(name), &influxql.BooleanLiteral{Val: true}), nil
		}
		return nil, nil
	}
	return i.MatchTagValueSeriesIterator(name, key, value, op == influxql.EQREGEX), nil
}

func (i *Index) seriesByBinaryExprVarRefIterator(name, key []byte, value *influxql.VarRef, op influxql.Token) (SeriesIterator, error) {
	if op == influxql.EQ {
		return IntersectSeriesIterators(
			i.TagKeySeriesIterator(name, key),
			i.TagKeySeriesIterator(name, []byte(value.Val)),
		), nil
	}

	return DifferenceSeriesIterators(
		i.TagKeySeriesIterator(name, key),
		i.TagKeySeriesIterator(name, []byte(value.Val)),
	), nil
}

func (i *Index) SetFieldName(measurement, name string)  {}
func (i *Index) RemoveShard(shardID uint64)             {}
func (i *Index) AssignShard(k string, shardID uint64)   {}
func (i *Index) UnassignShard(k string, shardID uint64) {}

// SeriesPointIterator returns an influxql iterator over all series.
func (i *Index) SeriesPointIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	return newSeriesPointIterator(i, opt), nil
}

// Compact runs a compaction check. Returns once the check is complete.
// If force is true then all files are compacted into a single index file regardless of size.
func (i *Index) Compact(force bool) error {
	info := compactNotify{force: force, ch: make(chan error)}
	i.manualCompactNotify <- info

	select {
	case err := <-info.ch:
		return err
	case <-i.closing:
		return nil
	}
}

// monitorCompaction periodically checks for files that need to be compacted.
func (i *Index) monitorCompaction() {
	// Ignore full compaction if interval is unset.
	var c <-chan time.Time
	if i.CompactionMonitorInterval > 0 {
		ticker := time.NewTicker(i.CompactionMonitorInterval)
		c = ticker.C
		defer ticker.Stop()
	}

	// Wait for compaction checks or for the index to close.
	for {
		select {
		case <-i.closing:
			return
		case <-i.fastCompactNotify:
			if err := i.compactLogFile(); err != nil {
				log.Printf("fast compaction error: %s", err)
			}
		case <-c:
			if err := i.checkFullCompaction(false); err != nil {
				log.Printf("full compaction error: %s", err)
			}
		case info := <-i.manualCompactNotify:
			if err := i.compactLogFile(); err != nil {
				info.ch <- err
				continue
			} else if err := i.checkFullCompaction(info.force); err != nil {
				info.ch <- err
				continue
			}
			info.ch <- nil
		}
	}
}

// compactLogFile starts a new log file and compacts the previous one.
func (i *Index) compactLogFile() error {
	if err := i.prependNewLogFile(); err != nil {
		return err
	}
	if err := i.compactSecondaryLogFile(); err != nil {
		return err
	}
	return nil
}

// prependNewLogFile adds a new log file so that the current log file can be compacted.
// This function is a no-op if there is currently more than one log file.
func (i *Index) prependNewLogFile() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Ignore if there is already a secondary log file that needs compacting.
	if len(i.logFiles) == 2 {
		return nil
	} else if len(i.logFiles) > 2 {
		panic("should not have more than two log files at a time")
	}

	// Generate new file identifier.
	id := i.maxFileID() + 1

	// Open file and insert it into the first position.
	f, err := i.openLogFile(filepath.Join(i.Path, FormatLogFileName(id)))
	if err != nil {
		return err
	}
	i.logFiles = append([]*LogFile{f}, i.logFiles...)

	// Write new manifest.
	if err := i.writeManifestFile(); err != nil {
		// TODO: Close index if write fails.
		return err
	}

	return nil
}

// compactSecondaryLogFile compacts the secondary log file into an index file.
func (i *Index) compactSecondaryLogFile() error {
	id, logFile := func() (int, *LogFile) {
		i.mu.Lock()
		defer i.mu.Unlock()

		if len(i.logFiles) < 2 {
			return 0, nil
		}
		return i.maxFileID() + 1, i.logFiles[1]
	}()

	// Exit if there is no secondary log file.
	if logFile == nil {
		return nil
	}

	// Create new index file.
	path := filepath.Join(i.Path, FormatIndexFileName(id))
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Compact log file to new index file.
	if _, err := logFile.WriteTo(f); err != nil {
		return err
	}

	// Close file.
	if err := f.Close(); err != nil {
		return err
	}

	// Reopen as an index file.
	file := NewIndexFile()
	file.Path = path
	if err := file.Open(); err != nil {
		return err
	}

	// Obtain lock to swap in index file and write manifest.
	i.mu.Lock()
	defer i.mu.Unlock()

	// Remove old log file and prepend new index file.
	i.logFiles = []*LogFile{i.logFiles[0]}
	i.indexFiles = append(IndexFiles{file}, i.indexFiles...)

	// TODO: Close old log file.

	// Write new manifest.
	if err := i.writeManifestFile(); err != nil {
		// TODO: Close index if write fails.
		return err
	}

	return nil
}

// checkFullCompaction compacts all index files if the total size of index files
// is double the size of the largest index file. If force is true then all files
// are compacted regardless of size.
func (i *Index) checkFullCompaction(force bool) error {
	// Only perform size check if compaction check is not forced.
	if !force {
		// Calculate total & max file sizes.
		maxN, totalN, err := i.indexFileStats()
		if err != nil {
			return err
		}

		// Ignore if total is not twice the size of the largest index file.
		if maxN*2 < totalN {
			return nil
		}
	}

	// Retrieve list of index files under lock.
	i.mu.Lock()
	indexFiles := i.indexFiles
	id := i.maxFileID() + 1
	i.mu.Unlock()

	// Ignore if there are not at least two index files.
	if len(indexFiles) < 2 {
		return nil
	}

	// Create new index file.
	path := filepath.Join(i.Path, FormatIndexFileName(id))
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Compact all index files to new index file.
	if _, err := indexFiles.WriteTo(f); err != nil {
		return err
	}

	// Close file.
	if err := f.Close(); err != nil {
		return err
	}

	// Reopen as an index file.
	file := NewIndexFile()
	file.Path = path
	if err := file.Open(); err != nil {
		return err
	}

	// Obtain lock to swap in index file and write manifest.
	i.mu.Lock()
	defer i.mu.Unlock()

	// Replace index files with new index file.
	i.indexFiles = IndexFiles{file}

	// TODO: Close old index files.

	// Write new manifest.
	if err := i.writeManifestFile(); err != nil {
		// TODO: Close index if write fails.
		return err
	}

	return nil
}

// indexFileStats returns the max index file size and the total file size for all index files.
func (i *Index) indexFileStats() (maxN, totalN int64, err error) {
	// Retrieve index file list under lock.
	i.mu.Lock()
	indexFiles := i.indexFiles
	i.mu.Unlock()

	// Iterate over each file and determine size.
	for _, f := range indexFiles {
		fi, err := os.Stat(f.Path)
		if os.IsNotExist(err) {
			continue
		} else if err != nil {
			return 0, 0, err
		} else if fi.Size() > maxN {
			maxN = fi.Size()
		}
		totalN += fi.Size()
	}
	return maxN, totalN, nil
}

// CheckFastCompaction notifies the index to begin compacting log file if the
// log file is above the max log file size.
func (i *Index) CheckFastCompaction() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.checkFastCompaction()
}

func (i *Index) checkFastCompaction() {
	if i.logFiles[0].Size() < i.MaxLogFileSize {
		return
	}

	// Send signal to begin compaction of current log file.
	select {
	case i.fastCompactNotify <- struct{}{}:
	default:
	}
}

// compactNotify represents a manual compaction notification.
type compactNotify struct {
	force bool
	ch    chan error
}

// File represents a log or index file.
type File interface {
	Measurement(name []byte) MeasurementElem
	Series(name []byte, tags models.Tags) SeriesElem

	TagKeyIterator(name []byte) TagKeyIterator

	TagValue(name, key, value []byte) TagValueElem
	TagValueIterator(name, key []byte) TagValueIterator

	// Series iteration.
	SeriesIterator() SeriesIterator
	MeasurementSeriesIterator(name []byte) SeriesIterator
	TagKeySeriesIterator(name, key []byte) SeriesIterator
	TagValueSeriesIterator(name, key, value []byte) SeriesIterator
}

// FilterExprs represents a map of series IDs to filter expressions.
type FilterExprs map[uint64]influxql.Expr

// DeleteBoolLiteralTrues deletes all elements whose filter expression is a boolean literal true.
func (fe FilterExprs) DeleteBoolLiteralTrues() {
	for id, expr := range fe {
		if e, ok := expr.(*influxql.BooleanLiteral); ok && e.Val == true {
			delete(fe, id)
		}
	}
}

// Len returns the number of elements.
func (fe FilterExprs) Len() int {
	if fe == nil {
		return 0
	}
	return len(fe)
}

// seriesPointIterator adapts SeriesIterator to an influxql.Iterator.
type seriesPointIterator struct {
	index *Index
	mitr  MeasurementIterator
	sitr  SeriesIterator
	opt   influxql.IteratorOptions

	point influxql.FloatPoint // reusable point
}

// newSeriesPointIterator returns a new instance of seriesPointIterator.
func newSeriesPointIterator(index *Index, opt influxql.IteratorOptions) *seriesPointIterator {
	return &seriesPointIterator{
		index: index,
		mitr:  index.MeasurementIterator(),
		point: influxql.FloatPoint{
			Aux: make([]interface{}, len(opt.Aux)),
		},
		opt: opt,
	}
}

// Stats returns stats about the points processed.
func (itr *seriesPointIterator) Stats() influxql.IteratorStats { return influxql.IteratorStats{} }

// Close closes the iterator.
func (itr *seriesPointIterator) Close() error { return nil }

// Next emits the next point in the iterator.
func (itr *seriesPointIterator) Next() (*influxql.FloatPoint, error) {
	for {
		// Create new series iterator, if necessary.
		// Exit if there are no measurements remaining.
		if itr.sitr == nil {
			m := itr.mitr.Next()
			if m == nil {
				return nil, nil
			}

			sitr, err := itr.index.MeasurementSeriesByExprIterator(m.Name(), itr.opt.Condition)
			if err != nil {
				return nil, err
			} else if sitr == nil {
				continue
			}
			itr.sitr = sitr
		}

		// Read next series element.
		e := itr.sitr.Next()
		if e == nil {
			itr.sitr = nil
			continue
		}

		// Convert to a key.
		key := string(models.MakeKey(e.Name(), e.Tags()))

		// Write auxiliary fields.
		for i, f := range itr.opt.Aux {
			switch f.Val {
			case "key":
				itr.point.Aux[i] = key
			}
		}
		return &itr.point, nil
	}
}

// unionStringSets returns the union of two sets
func unionStringSets(a, b map[string]struct{}) map[string]struct{} {
	other := make(map[string]struct{})
	for k := range a {
		other[k] = struct{}{}
	}
	for k := range b {
		other[k] = struct{}{}
	}
	return other
}

// intersectStringSets returns the intersection of two sets.
func intersectStringSets(a, b map[string]struct{}) map[string]struct{} {
	if len(a) < len(b) {
		a, b = b, a
	}

	other := make(map[string]struct{})
	for k := range a {
		if _, ok := b[k]; ok {
			other[k] = struct{}{}
		}
	}
	return other
}

var fileIDRegex = regexp.MustCompile(`^(\d+)\..+$`)

// ParseFileID extracts the numeric id from a log or index file path.
// Returns 0 if it cannot be parsed.
func ParseFileID(name string) int {
	a := fileIDRegex.FindStringSubmatch(filepath.Base(name))
	if a == nil {
		return 0
	}

	i, _ := strconv.Atoi(a[1])
	return i
}

// Manifest represents the list of log & index files that make up the index.
// The files are listed in time order, not necessarily ID order.
type Manifest struct {
	LogFiles   []string `json:"logs,omitempty"`
	IndexFiles []string `json:"indexes,omitempty"`
}

// HasFile returns true if name is listed in the log files or index files.
func (m *Manifest) HasFile(name string) bool {
	for _, filename := range m.LogFiles {
		if filename == name {
			return true
		}
	}
	for _, filename := range m.IndexFiles {
		if filename == name {
			return true
		}
	}
	return false
}

// ReadManifestFile reads a manifest from a file path.
func ReadManifestFile(path string) (*Manifest, error) {
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// Decode manifest.
	var m Manifest
	if err := json.Unmarshal(buf, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// WriteManifestFile writes a manifest to a file path.
func WriteManifestFile(path string, m *Manifest) error {
	buf, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	buf = append(buf, '\n')

	if err := ioutil.WriteFile(path, buf, 0666); err != nil {
		return err
	}

	return nil
}
