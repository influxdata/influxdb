package gen

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sort"
	"unicode/utf8"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/models"
)

type Spec struct {
	SeriesLimit  *int64
	Measurements []MeasurementSpec
}

func NewSeriesGeneratorFromSpec(s *Spec, tr TimeRange) SeriesGenerator {
	sg := make([]SeriesGenerator, len(s.Measurements))
	for i := range s.Measurements {
		sg[i] = newSeriesGeneratorFromMeasurementSpec(&s.Measurements[i], tr)
	}
	if s.SeriesLimit == nil {
		return NewMergedSeriesGenerator(sg)
	}
	return NewMergedSeriesGeneratorLimit(sg, *s.SeriesLimit)
}

type MeasurementSpec struct {
	Name            string
	SeriesLimit     *SeriesLimit
	TagsSpec        *TagsSpec
	FieldValuesSpec *FieldValuesSpec
}

func newSeriesGeneratorFromMeasurementSpec(ms *MeasurementSpec, tr TimeRange) SeriesGenerator {
	if ms.SeriesLimit == nil {
		return NewSeriesGenerator(
			[]byte(ms.Name),
			[]byte(ms.FieldValuesSpec.Name),
			newTimeValuesSequenceFromFieldValuesSpec(ms.FieldValuesSpec, tr),
			newTagsSequenceFromTagsSpec(ms.TagsSpec))
	}
	return NewSeriesGeneratorLimit(
		[]byte(ms.Name),
		[]byte(ms.FieldValuesSpec.Name),
		newTimeValuesSequenceFromFieldValuesSpec(ms.FieldValuesSpec, tr),
		newTagsSequenceFromTagsSpec(ms.TagsSpec),
		int64(*ms.SeriesLimit))
}

// NewTimeValuesSequenceFn returns a TimeValuesSequence that will generate a
// sequence of values based on the spec.
type NewTimeValuesSequenceFn func(spec TimeSequenceSpec) TimeValuesSequence

type NewTagsValuesSequenceFn func() TagsSequence

type NewCountableSequenceFn func() CountableSequence

type TagsSpec struct {
	Tags   []*TagValuesSpec
	Sample *sample
}

func newTagsSequenceFromTagsSpec(ts *TagsSpec) TagsSequence {
	var keys []string
	var vals []CountableSequence
	for _, spec := range ts.Tags {
		keys = append(keys, spec.TagKey)
		vals = append(vals, spec.Values())
	}

	var opts []tagsValuesOption
	if ts.Sample != nil && *ts.Sample != 1.0 {
		opts = append(opts, TagValuesSampleOption(float64(*ts.Sample)))
	}

	return NewTagsValuesSequenceKeysValues(keys, vals, opts...)
}

type TagValuesSpec struct {
	TagKey string
	Values NewCountableSequenceFn
}

type FieldValuesSpec struct {
	TimeSequenceSpec
	Name     string
	DataType models.FieldType
	Values   NewTimeValuesSequenceFn
}

func newTimeValuesSequenceFromFieldValuesSpec(fs *FieldValuesSpec, tr TimeRange) TimeValuesSequence {
	return fs.Values(fs.TimeSequenceSpec.ForTimeRange(tr))
}

func NewSpecFromToml(s string) (*Spec, error) {
	var out Schema
	if _, err := toml.Decode(s, &out); err != nil {
		return nil, err
	}
	return NewSpecFromSchema(&out)
}

func NewSpecFromPath(p string) (*Spec, error) {
	var err error
	p, err = filepath.Abs(p)
	if err != nil {
		return nil, err
	}

	var out Schema
	if _, err := toml.DecodeFile(p, &out); err != nil {
		return nil, err
	}
	return newSpecFromSchema(&out, schemaDir(path.Dir(p)))
}

func NewSchemaFromPath(path string) (*Schema, error) {
	var out Schema
	if _, err := toml.DecodeFile(path, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

type schemaToSpecState int

const (
	stateOk schemaToSpecState = iota
	stateErr
)

type schemaToSpec struct {
	schemaDir string
	stack     []interface{}
	state     schemaToSpecState
	spec      *Spec
	err       error
}

func (s *schemaToSpec) push(v interface{}) {
	s.stack = append(s.stack, v)
}

func (s *schemaToSpec) pop() interface{} {
	tail := len(s.stack) - 1
	v := s.stack[tail]
	s.stack[tail] = nil
	s.stack = s.stack[:tail]
	return v
}

func (s *schemaToSpec) peek() interface{} {
	if len(s.stack) == 0 {
		return nil
	}
	return s.stack[len(s.stack)-1]
}

func (s *schemaToSpec) Visit(node SchemaNode) (w Visitor) {
	switch s.state {
	case stateOk:
		if s.visit(node) {
			return s
		}
		s.state = stateErr

	case stateErr:
		s.visitErr(node)
	}

	return nil
}

func (s *schemaToSpec) visit(node SchemaNode) bool {
	switch n := node.(type) {
	case *Schema:
		s.spec.Measurements = s.pop().([]MeasurementSpec)
		if n.SeriesLimit != nil {
			sl := int64(*n.SeriesLimit)
			s.spec.SeriesLimit = &sl
		}

	case Measurements:
		// flatten measurements
		var mss []MeasurementSpec
		for {
			if specs, ok := s.peek().([]MeasurementSpec); ok {
				s.pop()
				mss = append(mss, specs...)
				continue
			}
			break
		}
		sort.Slice(mss, func(i, j int) bool {
			return mss[i].Name < mss[j].Name
		})

		// validate field types are homogeneous for a single measurement
		mg := make(map[string]models.FieldType)
		for i := range mss {
			spec := &mss[i]
			key := spec.Name + "." + spec.FieldValuesSpec.Name
			ft := spec.FieldValuesSpec.DataType
			if dt, ok := mg[key]; !ok {
				mg[key] = ft
			} else if dt != ft {
				s.err = fmt.Errorf("field %q data-type conflict, found %s and %s",
					key,
					dt,
					ft)
				return false
			}
		}

		s.push(mss)

	case *Measurement:
		var ms []MeasurementSpec

		fields := s.pop().([]*FieldValuesSpec)
		tagsSpec := s.pop().(*TagsSpec)

		tagsSpec.Sample = n.Sample

		// default: sample 50%
		if n.Sample == nil {
			s := sample(0.5)
			tagsSpec.Sample = &s
		}

		for _, spec := range fields {
			ms = append(ms, MeasurementSpec{
				Name:            n.Name,
				SeriesLimit:     n.SeriesLimit,
				TagsSpec:        tagsSpec,
				FieldValuesSpec: spec,
			})
		}

		// NOTE: sort each measurement name + field name to ensure series are produced
		//  in correct order
		sort.Slice(ms, func(i, j int) bool {
			return ms[i].FieldValuesSpec.Name < ms[j].FieldValuesSpec.Name
		})
		s.push(ms)

	case Tags:
		var ts TagsSpec
		for {
			if spec, ok := s.peek().(*TagValuesSpec); ok {
				s.pop()
				ts.Tags = append(ts.Tags, spec)
				continue
			}
			break
		}
		// Tag keys must be sorted to produce a valid series key sequence
		sort.Slice(ts.Tags, func(i, j int) bool {
			return ts.Tags[i].TagKey < ts.Tags[j].TagKey
		})

		for i := 1; i < len(ts.Tags); i++ {
			if ts.Tags[i-1].TagKey == ts.Tags[i].TagKey {
				s.err = fmt.Errorf("duplicate tag keys %q", ts.Tags[i].TagKey)
				return false
			}
		}

		s.push(&ts)

	case Fields:
		// combine fields
		var fs []*FieldValuesSpec
		for {
			if spec, ok := s.peek().(*FieldValuesSpec); ok {
				s.pop()
				fs = append(fs, spec)
				continue
			}
			break
		}

		sort.Slice(fs, func(i, j int) bool {
			return fs[i].Name < fs[j].Name
		})

		for i := 1; i < len(fs); i++ {
			if fs[i-1].Name == fs[i].Name {
				s.err = fmt.Errorf("duplicate field names %q", fs[i].Name)
				return false
			}
		}

		s.push(fs)

	case *Field:
		fs, ok := s.peek().(*FieldValuesSpec)
		if !ok {
			panic(fmt.Sprintf("unexpected type %T", fs))
		}

		fs.TimeSequenceSpec = n.TimeSequenceSpec()
		fs.Name = n.Name

	case *FieldConstantValue:
		var fs FieldValuesSpec
		switch v := n.Value.(type) {
		case float64:
			fs.DataType = models.Float
			fs.Values = func(spec TimeSequenceSpec) TimeValuesSequence {
				return NewTimeFloatValuesSequence(
					spec.Count,
					NewTimestampSequenceFromSpec(spec),
					NewFloatConstantValuesSequence(v),
				)
			}
		case int64:
			fs.DataType = models.Integer
			fs.Values = func(spec TimeSequenceSpec) TimeValuesSequence {
				return NewTimeIntegerValuesSequence(
					spec.Count,
					NewTimestampSequenceFromSpec(spec),
					NewIntegerConstantValuesSequence(v),
				)
			}
		case string:
			fs.DataType = models.String
			fs.Values = func(spec TimeSequenceSpec) TimeValuesSequence {
				return NewTimeStringValuesSequence(
					spec.Count,
					NewTimestampSequenceFromSpec(spec),
					NewStringConstantValuesSequence(v),
				)
			}
		case bool:
			fs.DataType = models.Boolean
			fs.Values = func(spec TimeSequenceSpec) TimeValuesSequence {
				return NewTimeBooleanValuesSequence(
					spec.Count,
					NewTimestampSequenceFromSpec(spec),
					NewBooleanConstantValuesSequence(v),
				)
			}
		default:
			panic(fmt.Sprintf("unexpected type %T", v))
		}

		s.push(&fs)

	case *FieldArraySource:
		var fs FieldValuesSpec
		switch v := n.Value.(type) {
		case []float64:
			fs.DataType = models.Float
			fs.Values = func(spec TimeSequenceSpec) TimeValuesSequence {
				return NewTimeFloatValuesSequence(
					spec.Count,
					NewTimestampSequenceFromSpec(spec),
					NewFloatArrayValuesSequence(v),
				)
			}
		case []int64:
			fs.DataType = models.Integer
			fs.Values = func(spec TimeSequenceSpec) TimeValuesSequence {
				return NewTimeIntegerValuesSequence(
					spec.Count,
					NewTimestampSequenceFromSpec(spec),
					NewIntegerArrayValuesSequence(v),
				)
			}
		case []string:
			fs.DataType = models.String
			fs.Values = func(spec TimeSequenceSpec) TimeValuesSequence {
				return NewTimeStringValuesSequence(
					spec.Count,
					NewTimestampSequenceFromSpec(spec),
					NewStringArrayValuesSequence(v),
				)
			}
		case []bool:
			fs.DataType = models.Boolean
			fs.Values = func(spec TimeSequenceSpec) TimeValuesSequence {
				return NewTimeBooleanValuesSequence(
					spec.Count,
					NewTimestampSequenceFromSpec(spec),
					NewBooleanArrayValuesSequence(v),
				)
			}
		default:
			panic(fmt.Sprintf("unexpected type %T", v))
		}

		s.push(&fs)

	case *FieldFloatRandomSource:
		var fs FieldValuesSpec
		fs.DataType = models.Float
		fs.Values = NewTimeValuesSequenceFn(func(spec TimeSequenceSpec) TimeValuesSequence {
			return NewTimeFloatValuesSequence(
				spec.Count,
				NewTimestampSequenceFromSpec(spec),
				NewFloatRandomValuesSequence(n.Min, n.Max, rand.New(rand.NewSource(n.Seed))),
			)
		})
		s.push(&fs)

	case *FieldIntegerZipfSource:
		var fs FieldValuesSpec
		fs.DataType = models.Integer
		fs.Values = NewTimeValuesSequenceFn(func(spec TimeSequenceSpec) TimeValuesSequence {
			return NewTimeIntegerValuesSequence(
				spec.Count,
				NewTimestampSequenceFromSpec(spec),
				NewIntegerZipfValuesSequence(n),
			)
		})
		s.push(&fs)

	case *Tag:
		s.push(&TagValuesSpec{
			TagKey: n.Name,
			Values: s.pop().(NewCountableSequenceFn),
		})

	case *TagSequenceSource:
		s.push(NewCountableSequenceFn(func() CountableSequence {
			return NewCounterByteSequence(n.Format, int(n.Start), int(n.Start+n.Count))
		}))

	case *TagFileSource:
		p, err := s.resolvePath(n.Path)
		if err != nil {
			s.err = err
			return false
		}

		lines, err := s.readLines(p)
		if err != nil {
			s.err = err
			return false
		}

		s.push(NewCountableSequenceFn(func() CountableSequence {
			return NewStringArraySequence(lines)
		}))

	case *TagArraySource:
		s.push(NewCountableSequenceFn(func() CountableSequence {
			return NewStringArraySequence(n.Values)
		}))

	case nil:

	default:
		panic(fmt.Sprintf("unexpected type %T", node))
	}

	return true
}

func (s *schemaToSpec) visitErr(node SchemaNode) {
	switch n := node.(type) {
	case *Schema:
		s.err = fmt.Errorf("error processing schema: %v", s.err)
	case *Measurement:
		s.err = fmt.Errorf("measurement %q: %v", n.Name, s.err)
	case *Tag:
		s.err = fmt.Errorf("tag %q: %v", n.Name, s.err)
	case *Field:
		s.err = fmt.Errorf("field %q: %v", n.Name, s.err)
	}
}

func (s *schemaToSpec) resolvePath(p string) (string, error) {
	fullPath := os.ExpandEnv(p)
	if !filepath.IsAbs(fullPath) {
		fullPath = filepath.Join(s.schemaDir, fullPath)
	}

	fi, err := os.Stat(fullPath)
	if err != nil {
		return "", fmt.Errorf("error resolving path %q: %v", p, err)
	}

	if fi.IsDir() {
		return "", fmt.Errorf("path %q is not a file: resolved to %s", p, fullPath)
	}

	return fullPath, nil
}

func (s *schemaToSpec) readLines(p string) ([]string, error) {
	fp, err := s.resolvePath(p)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(fp)
	if err != nil {
		return nil, fmt.Errorf("path error: %v", err)
	}
	defer f.Close()
	scan := bufio.NewScanner(f)
	scan.Split(bufio.ScanLines)

	n := 0
	var lines []string

	for scan.Scan() {
		if len(scan.Bytes()) == 0 {
			// skip empty lines
			continue
		}

		if !utf8.Valid(scan.Bytes()) {
			return nil, fmt.Errorf("path %q, invalid UTF-8 on line %d", p, n)
		}
		lines = append(lines, scan.Text())
	}

	if scan.Err() != nil {
		return nil, scan.Err()
	}

	return lines, nil
}

type option func(s *schemaToSpec)

func schemaDir(p string) option {
	return func(s *schemaToSpec) {
		s.schemaDir = p
	}
}

func NewSpecFromSchema(root *Schema) (*Spec, error) {
	return newSpecFromSchema(root)
}

func newSpecFromSchema(root *Schema, opts ...option) (*Spec, error) {
	var spec Spec

	vis := &schemaToSpec{spec: &spec}
	for _, o := range opts {
		o(vis)
	}

	WalkUp(vis, root)
	if vis.err != nil {
		return nil, vis.err
	}

	return &spec, nil
}
