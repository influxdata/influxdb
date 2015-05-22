package tsdb

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/influxdb/influxdb/influxql"

	"github.com/boltdb/bolt"
)

// Shard represents a self-contained time series database. An inverted index of the measurement and tag data is
// kept along with the raw time series data. Data can be split across many shards. The query engine in TSDB
// is responsible for combining the output of many shards into a single query result.
type Shard struct {
	db *bolt.DB // underlying data store

	// in memory metadata index, built on load and updated when new series come in
	mu           sync.RWMutex
	measurements map[string]*Measurement // measurement name to object and index
	series       map[string]*Series      // map series key to the Series object
	names        []string                // sorted list of the measurement names
	lastID       uint64                  // last used series ID. They're in memory only for this shard
}

// NewShard returns a new initialized Shard
func NewShard() *Shard {
	return &Shard{
		measurements: make(map[string]*Measurement),
		series:       make(map[string]*Series),
		names:        make([]string, 0),
	}
}

// open initializes and opens the shard's store.
func (s *Shard) Open(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Return an error if the shard is already open.
	if s.db != nil {
		return errors.New("shard already open")
	}

	// Open store on shard.
	store, err := bolt.Open(path, 0666, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	s.db = store

	// Initialize store.
	if err := s.db.Update(func(tx *bolt.Tx) error {
		_, _ = tx.CreateBucketIfNotExists([]byte("series"))
		_, _ = tx.CreateBucketIfNotExists([]byte("measurements"))
		_, _ = tx.CreateBucketIfNotExists([]byte("values"))

		return nil
	}); err != nil {
		_ = s.Close()
		return fmt.Errorf("init: %s", err)
	}

	return s.loadMetadataIndex()
}

// close shuts down the shard's store.
func (s *Shard) Close() error {
	if s.db != nil {
		_ = s.db.Close()
	}
	return nil
}

// struct to hold information for a field to create on a measurement
type fieldCreate struct {
	measurement string
	field       *Field
}

// struct to hold information for a series to create
type seriesCreate struct {
	measurement string
	series      *Series
}

// WritePoints will write the raw data points and any new metadata to the index in the shard
func (s *Shard) WritePoints(points []Point) error {
	var seriesToCreate []*seriesCreate
	var fieldsToCreate []*fieldCreate

	// validate fields, check which series are new, whose metadata should be saved and indexed
	s.mu.RLock()
	for _, p := range points {
		var measurement *Measurement

		if ss := s.series[p.Key()]; ss == nil {
			series := &Series{Key: p.Key(), Tags: p.Tags()}
			seriesToCreate = append(seriesToCreate, &seriesCreate{p.Name(), series})

			// if the measurement doesn't exist, all fields need to be created
			if m := s.measurements[p.Name()]; m == nil {
				for name, value := range p.Fields() {
					fieldsToCreate = append(fieldsToCreate, &fieldCreate{p.Name(), &Field{Name: name, Type: influxql.InspectDataType(value)}})
				}
				continue // no need to validate since they're all new fields
			} else {
				measurement = m
			}
		} else {
			measurement = ss.measurement
		}

		// validate field types
		for name, value := range p.Fields() {
			if f := measurement.FieldByName(name); f != nil {
				// Field present in Metastore, make sure there is no type conflict.
				if f.Type != influxql.InspectDataType(value) {
					return fmt.Errorf("input field \"%s\" is type %T, already exists as type %s", name, value, f.Type)
				}

				data, err := measurement.fieldCodec.EncodeFields(p.Fields())
				if err != nil {
					return err
				}
				p.SetData(data)
				continue // Field is present, and it's of the same type. Nothing more to do.
			}

			fieldsToCreate = append(fieldsToCreate, &fieldCreate{p.Name(), &Field{Name: name, Type: influxql.InspectDataType(value)}})
		}
	}
	s.mu.RUnlock()

	// if there are any metadata updates get the write lock and do them
	var measurementsToCreate []*Measurement
	if len(seriesToCreate) > 0 || len(fieldsToCreate) > 0 {
		s.mu.Lock()
		defer s.mu.Unlock()

		// add any new series to the in-memory index
		for _, ss := range seriesToCreate {
			s.createSeriesIndexIfNotExists(ss.measurement, ss.series)
		}

		// add fields
		for _, f := range fieldsToCreate {
			measurementsToSave := make(map[string]*Measurement)

			m := s.measurements[f.measurement]
			if m == nil {
				m = s.createMeasurementIndexIfNotExists(f.measurement)
			}
			measurementsToSave[f.measurement] = m

			// add the field to the in memory index
			if err := m.createFieldIfNotExists(f.field.Name, f.field.Type); err != nil {
				return err
			}

			// now put the measurements in the list to save into Bolt
			for _, m := range measurementsToSave {
				measurementsToCreate = append(measurementsToCreate, m)
			}
		}
	}

	// make sure all data is encoded before attempting to save to bolt
	for _, p := range points {
		// marshal the raw data if it hasn't been marshaled already
		if p.Data() == nil {
			// this was populated earlier, don't need to validate that it's there.
			data, err := s.series[p.Key()].measurement.fieldCodec.EncodeFields(p.Fields())
			if err != nil {
				return err
			}
			p.SetData(data)
		}
	}

	// save to the underlying bolt instance
	if err := s.db.Update(func(tx *bolt.Tx) error {
		// save any new metadata
		if len(seriesToCreate) > 0 {
			b := tx.Bucket([]byte("series"))
			for _, sc := range seriesToCreate {
				if err := b.Put([]byte(sc.series.Key), mustMarshalJSON(sc.series)); err != nil {
					return err
				}
			}
		}
		if len(measurementsToCreate) > 0 {
			b := tx.Bucket([]byte("measurements"))
			for _, m := range measurementsToCreate {
				if err := b.Put([]byte(m.Name), mustMarshalJSON(m)); err != nil {
					return err
				}
			}
		}

		// save the raw point data
		b := tx.Bucket([]byte("values"))
		for _, p := range points {
			bp, err := b.CreateBucketIfNotExists([]byte(p.Key()))
			if err != nil {
				return err
			}
			if err := bp.Put([]byte(p.Key()), p.Data()); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		_ = s.Close()
		return err
	}

	return nil
}

// loadsMetadataIndex loads the shard metadata into memory. This should only be called by Open
func (s *Shard) loadMetadataIndex() error {
	return s.db.View(func(tx *bolt.Tx) error {
		// load measurement metadata
		meta := tx.Bucket([]byte("measurements"))
		c := meta.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			m := s.createMeasurementIndexIfNotExists(string(k))
			mustUnmarshalJSON(v, &m)
			m.fieldCodec = NewFieldCodec(m)
		}

		// load series metadata
		meta = tx.Bucket([]byte("series"))
		c = meta.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var series *Series
			mustUnmarshalJSON(v, &series)
			s.createSeriesIndexIfNotExists(measurementFromSeriesKey(string(k)), series)
		}
		return nil
	})
}

// createSeriesIndexIfNotExists adds the series for the given measurement to the index and sets its ID or returns the existing series object
func (s *Shard) createSeriesIndexIfNotExists(measurementName string, series *Series) *Series {
	// if there is a measurement for this id, it's already been added
	ss := s.series[series.Key]
	if ss != nil {
		return ss
	}

	// get or create the measurement index
	m := s.createMeasurementIndexIfNotExists(measurementName)

	// set the in memory ID for query processing on this shard
	series.id = s.lastID + 1
	s.lastID += 1

	series.measurement = m
	s.series[series.Key] = series

	m.addSeries(series)

	return series
}

// addMeasurementToIndexIfNotExists creates or retrieves an in memory index object for the measurement
func (s *Shard) createMeasurementIndexIfNotExists(name string) *Measurement {
	m := s.measurements[name]
	if m == nil {
		m = NewMeasurement(name)
		s.measurements[name] = m
		s.names = append(s.names, name)
		sort.Strings(s.names)
	}
	return m
}

// measurementsByExpr takes and expression containing only tags and returns
// a list of matching *Measurement.
func (db *Shard) measurementsByExpr(expr influxql.Expr) (Measurements, error) {
	switch e := expr.(type) {
	case *influxql.BinaryExpr:
		switch e.Op {
		case influxql.EQ, influxql.NEQ, influxql.EQREGEX, influxql.NEQREGEX:
			tag, ok := e.LHS.(*influxql.VarRef)
			if !ok {
				return nil, fmt.Errorf("left side of '%s' must be a tag name", e.Op.String())
			}

			tf := &TagFilter{
				Op:  e.Op,
				Key: tag.Val,
			}

			if influxql.IsRegexOp(e.Op) {
				re, ok := e.RHS.(*influxql.RegexLiteral)
				if !ok {
					return nil, fmt.Errorf("right side of '%s' must be a regular expression", e.Op.String())
				}
				tf.Regex = re.Val
			} else {
				s, ok := e.RHS.(*influxql.StringLiteral)
				if !ok {
					return nil, fmt.Errorf("right side of '%s' must be a tag value string", e.Op.String())
				}
				tf.Value = s.Val
			}

			return db.measurementsByTagFilters([]*TagFilter{tf}), nil
		case influxql.OR, influxql.AND:
			lhsIDs, err := db.measurementsByExpr(e.LHS)
			if err != nil {
				return nil, err
			}

			rhsIDs, err := db.measurementsByExpr(e.RHS)
			if err != nil {
				return nil, err
			}

			if e.Op == influxql.OR {
				return lhsIDs.union(rhsIDs), nil
			}

			return lhsIDs.intersect(rhsIDs), nil
		default:
			return nil, fmt.Errorf("invalid operator")
		}
	case *influxql.ParenExpr:
		return db.measurementsByExpr(e.Expr)
	}
	return nil, fmt.Errorf("%#v", expr)
}

func (db *Shard) measurementsByTagFilters(filters []*TagFilter) Measurements {
	// If no filters, then return all measurements.
	if len(filters) == 0 {
		measurements := make(Measurements, 0, len(db.measurements))
		for _, m := range db.measurements {
			measurements = append(measurements, m)
		}
		return measurements
	}

	// Build a list of measurements matching the filters.
	var measurements Measurements
	var tagMatch bool

	// Iterate through all measurements in the database.
	for _, m := range db.measurements {
		// Iterate filters seeing if the measurement has a matching tag.
		for _, f := range filters {
			tagVals, ok := m.seriesByTagKeyValue[f.Key]
			if !ok {
				continue
			}

			tagMatch = false

			// If the operator is non-regex, only check the specified value.
			if f.Op == influxql.EQ || f.Op == influxql.NEQ {
				if _, ok := tagVals[f.Value]; ok {
					tagMatch = true
				}
			} else {
				// Else, the operator is regex and we have to check all tag
				// values against the regular expression.
				for tagVal := range tagVals {
					if f.Regex.MatchString(tagVal) {
						tagMatch = true
						break
					}
				}
			}

			isEQ := (f.Op == influxql.EQ || f.Op == influxql.EQREGEX)

			// tags match | operation is EQ | measurement matches
			// --------------------------------------------------
			//     True   |       True      |      True
			//     True   |       False     |      False
			//     False  |       True      |      False
			//     False  |       False     |      True

			if tagMatch == isEQ {
				measurements = append(measurements, m)
				break
			}
		}
	}

	return measurements
}

// measurementsByRegex returns the measurements that match the regex.
func (db *Shard) measurementsByRegex(re *regexp.Regexp) Measurements {
	var matches Measurements
	for _, m := range db.measurements {
		if re.MatchString(m.Name) {
			matches = append(matches, m)
		}
	}
	return matches
}

// Measurements returns a list of all measurements.
func (db *Shard) Measurements() Measurements {
	measurements := make(Measurements, 0, len(db.measurements))
	for _, m := range db.measurements {
		measurements = append(measurements, m)
	}
	return measurements
}

// mustMarshal encodes a value to JSON.
// This will panic if an error occurs. This should only be used internally when
// an invalid marshal will cause corruption and a panic is appropriate.
func mustMarshalJSON(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic("marshal: " + err.Error())
	}
	return b
}

// mustUnmarshalJSON decodes a value from JSON.
// This will panic if an error occurs. This should only be used internally when
// an invalid unmarshal will cause corruption and a panic is appropriate.
func mustUnmarshalJSON(b []byte, v interface{}) {
	if err := json.Unmarshal(b, v); err != nil {
		panic("unmarshal: " + err.Error())
	}
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }

// u64tob converts a uint64 into an 8-byte slice.
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

var (
	// ErrFieldOverflow is returned when too many fields are created on a measurement.
	ErrFieldOverflow = errors.New("field overflow")

	// ErrFieldTypeConflict is returned when a new field already exists with a different type.
	ErrFieldTypeConflict = errors.New("field type conflict")

	// ErrFieldNotFound is returned when a field cannot be found.
	ErrFieldNotFound = errors.New("field not found")

	// ErrFieldUnmappedID is returned when the system is presented, during decode, with a field ID
	// there is no mapping for.
	ErrFieldUnmappedID = errors.New("field ID not mapped")
)
