package query

import (
	"io"
	"regexp"

	"github.com/influxdata/influxdb/influxql"
)

type ShardGroup interface {
	MeasurementsByRegex(re *regexp.Regexp) []string
	FieldDimensions(measurements []string) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error)
	MapType(measurement, field string) influxql.DataType
	CreateIterator(measurement string, opt influxql.IteratorOptions) (influxql.Iterator, error)
}

type ShardMapper interface {
	MapShards(m *influxql.Measurement, opt *influxql.SelectOptions) (Database, error)
}

// compiledSource is a source that links together a variable reference with a source.
type compiledSource interface {
	link(m ShardMapper) (storage, error)
}

type Database interface {
	CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error)
	FieldDimensions() (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error)
	MapType(field string) influxql.DataType
	io.Closer
}

// storage is an abstraction over the storage layer.
type storage interface {
	FieldDimensions() (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error)
	MapType(field string) influxql.DataType
	io.Closer
	resolve(ref *influxql.VarRef, out *WriteEdge)
}

type measurement struct {
	stmt   *compiledStatement
	source *influxql.Measurement
}

func (m *measurement) link(shardMapper ShardMapper) (storage, error) {
	opt := influxql.SelectOptions{
		MinTime: m.stmt.TimeRange.Min,
		MaxTime: m.stmt.TimeRange.Max,
	}
	shard, err := shardMapper.MapShards(m.source, &opt)
	if err != nil {
		return nil, err
	}
	return &storageEngine{
		stmt:  m.stmt,
		shard: shard,
	}, nil
}

type storageEngine struct {
	stmt  *compiledStatement
	shard Database
}

func (s *storageEngine) FieldDimensions() (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	return s.shard.FieldDimensions()
}

func (s *storageEngine) MapType(field string) influxql.DataType {
	return s.shard.MapType(field)
}

func (s *storageEngine) resolve(ref *influxql.VarRef, out *WriteEdge) {
	ic := &IteratorCreator{
		Expr:            ref,
		Condition:       s.stmt.Condition,
		AuxiliaryFields: s.stmt.AuxiliaryFields,
		Database:        s.shard,
		Dimensions:      s.stmt.Dimensions.Get(),
		Tags:            s.stmt.Tags.Map(),
		Interval:        s.stmt.Interval,
		TimeRange:       s.stmt.TimeRange,
		Ascending:       s.stmt.Ascending,
		SLimit:          s.stmt.SLimit,
		SOffset:         s.stmt.SOffset,
		MaxSeriesN:      s.stmt.Options.MaxSelectSeriesN,
		Output:          out,
	}
	out.Node = ic
}

func (s *storageEngine) Close() error {
	return s.shard.Close()
}

type subquery struct {
	stmt       *compiledStatement
	condition  influxql.Expr // the outer condition applied to the result of this subquery
	dimensions *Dimensions   // the outer dimensions applied to the result of this subquery
	auxFields  *AuxiliaryFields
}

func (s *subquery) link(m ShardMapper) (storage, error) {
	// Link the subquery statement and store the linked fields.
	fields, names, err := s.stmt.link(m)
	if err != nil {
		return nil, err
	}
	dimensions := s.stmt.Tags.Map()
	columns := make([]influxql.VarRef, len(names))

	// Zip together the outputs from the linker into a single stream of auxiliary fields.
	zip := &Zip{Ascending: s.stmt.Ascending}
	inputs := make([]*ReadEdge, len(fields))
	for i, f := range fields {
		f.Output.Node = zip
		inputs[i] = f.Output
		// Set the column name and type for later reference.
		columns[i] = influxql.VarRef{
			Val:  names[i+1],
			Type: f.Output.Input.Node.Type(),
		}
	}
	zip.InputNodes = inputs

	var in *ReadEdge
	zip.Output, in = AddEdge(zip, nil)

	// If there is a condition, filter the results of the subquery.
	if s.condition != nil {
		// Determine the field names we care about and find their mapping.
		fields := make(map[string]influxql.IteratorMap)
		names := influxql.ExprNames(s.condition)
	NAMES:
		for _, name := range names {
			if name.Type != influxql.Tag {
				for i, col := range columns {
					if name.Val == col.Val {
						fields[col.Val] = influxql.FieldMap(i)
						continue NAMES
					}
				}
			}
			if name.Type == influxql.Unknown || name.Type == influxql.Tag {
				if _, ok := dimensions[name.Val]; ok {
					fields[name.Val] = influxql.TagMap(name.Val)
					continue NAMES
				}
			}
			fields[name.Val] = influxql.NullMap{}
		}
		// Filter the results of the subquery using the condition.
		filter := &Filter{
			Condition: s.condition,
			Fields:    fields,
			Input:     in,
		}
		filter.Output, in = in.Append(filter)
	}

	// Pack all of the output fields into the iterator mapper.
	mapper := &IteratorMapper{
		AuxiliaryFields: s.auxFields,
		Dimensions:      s.dimensions,
		Columns:         columns,
		Input:           in,
	}
	in.Node = mapper

	return &subqueryEngine{
		fields:     fields,
		columns:    columns,
		dimensions: dimensions,
		mapper:     mapper,
	}, nil
}

type subqueryEngine struct {
	fields     []*compiledField
	columns    []influxql.VarRef
	dimensions map[string]struct{}
	mapper     *IteratorMapper
}

func (s *subqueryEngine) FieldDimensions() (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	fields = make(map[string]influxql.DataType)
	dimensions = make(map[string]struct{})

	// Iterate over each of the fields and retrieve the type from the graph.
	// Since linking has already been performed, everything should have types.
	for _, col := range s.columns {
		fields[col.Val] = col.Type
	}

	// Copy the dimensions of the subquery into the dimensions.
	for d := range s.dimensions {
		dimensions[d] = struct{}{}
	}
	return fields, dimensions, nil
}

func (s *subqueryEngine) MapType(field string) influxql.DataType {
	// Find the appropriate column for this field.
	for _, name := range s.columns {
		if name.Val == field {
			return name.Type
		}
	}
	if _, ok := s.dimensions[field]; ok {
		return influxql.Tag
	}
	return influxql.Unknown
}

func (s *subqueryEngine) Close() error {
	return nil
}

func (s *subqueryEngine) resolve(ref *influxql.VarRef, out *WriteEdge) {
	if ref == nil {
		s.mapper.Aux(out)
		return
	}

	clone := *ref
	field := &AuxiliaryField{Ref: &clone, Output: out}
	out.Node = field
	out, field.Input = AddEdge(s.mapper, field)

	// Search for a field matching the reference.
	for i, name := range s.columns {
		if name.Val == ref.Val {
			// Match this output edge with this field.
			s.mapper.Field(i, out)
			field.Ref.Type = name.Type
			return
		}
	}

	// Search for a tag matching the reference.
	if _, ok := s.dimensions[ref.Val]; ok {
		s.mapper.Tag(ref.Val, out)
		field.Ref.Type = influxql.Tag
		return
	}

	// If all else fails, attach it to a nil reference.
	n := &Nil{Output: out}
	out.Node = n
}

type storageEngines struct {
	sources   []storage
	ascending bool
}

func (a *storageEngines) FieldDimensions() (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	fields = make(map[string]influxql.DataType)
	dimensions = make(map[string]struct{})

	for _, s := range a.sources {
		f, d, err := s.FieldDimensions()
		if err != nil {
			return nil, nil, err
		}

		for k, typ := range f {
			if _, ok := fields[k]; typ != influxql.Unknown && (!ok || typ < fields[k]) {
				fields[k] = typ
			}
		}
		for k := range d {
			dimensions[k] = struct{}{}
		}
	}
	return fields, dimensions, nil
}

func (a *storageEngines) MapType(field string) influxql.DataType {
	var typ influxql.DataType
	for _, s := range a.sources {
		if t := s.MapType(field); typ.LessThan(t) {
			typ = t
		}
	}
	return typ
}

func (a *storageEngines) resolve(ref *influxql.VarRef, out *WriteEdge) {
	merge := &Merge{Ascending: a.ascending, Output: out}
	out.Node = merge
	for _, s := range a.sources {
		out := merge.AddInput(nil)
		s.resolve(ref, out)
	}
}

func (a *storageEngines) Close() error {
	for _, s := range a.sources {
		s.Close()
	}
	return nil
}
