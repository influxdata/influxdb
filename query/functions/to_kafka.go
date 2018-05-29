package functions

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/cespare/xxhash"
	"github.com/influxdata/line-protocol"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/plan"
	"github.com/influxdata/platform/query/semantic"
	"github.com/pkg/errors"
	kafka "github.com/segmentio/kafka-go"
)

const (
	// ToKafkaKind is the Kind for the ToKafka ifql function
	ToKafkaKind = "toKafka"
)

type ToKafkaOpSpec struct {
	Brokers      []string `json:"brokers"`
	Topic        string   `json:"topic"`
	Balancer     string   `json:"balancer"`
	Name         string   `json:"name"`
	NameColumn   string   `json:"name_column"` // either name or name_column must be set, if none is set try to use the "_measurement" column.
	TimeColumn   string   `json:"time_column"`
	TagColumns   []string `json:"tag_columns"`
	ValueColumns []string `json:"value_columns"`
	MsgBufSize   int      `json:"msg_buffer_size"` // the maximim number of messages to buffer before sending to kafka, the library we use defaults to 100
}

func init() {
	query.RegisterFunction(ToKafkaKind, createToKafkaOpSpec, ToKafkaSignature)
	query.RegisterOpSpec(ToKafkaKind,
		func() query.OperationSpec { return &ToKafkaOpSpec{} })
	plan.RegisterProcedureSpec(ToKafkaKind, newToKafkaProcedure, ToKafkaKind)
	execute.RegisterTransformation(ToKafkaKind, createToKafkaTransformation)
}

// DefaultKafkaWriterFactory is a terrible name for a way to make a kafkaWriter that is injectable for testing
var DefaultKafkaWriterFactory = func(conf kafka.WriterConfig) KafkaWriter {
	return kafka.NewWriter(conf)
}

// KafkaWriter is an interface for what we need fromDefaultKafkaWriterFactory
type KafkaWriter interface {
	io.Closer
	WriteMessages(context.Context, ...kafka.Message) error
}

// ReadArgs loads a query.Arguments into ToKafkaOpSpec.  It sets several default values.
// If the time_column isn't set, it defaults to execute.TimeColLabel.
// If the value_column isn't set it defaults to a []string{execute.DefaultValueColLabel}.
func (o *ToKafkaOpSpec) ReadArgs(args query.Arguments) error {
	var err error
	var ok bool

	brokers, err := args.GetRequiredArray("brokers", semantic.String)
	if err != nil {
		return err
	}
	l := brokers.Len()

	o.Brokers = make([]string, l)
	if brokers.Len() < 1 {
		return errors.New("at least one broker is required")
	}
	for i := 0; i < l; i++ {
		o.Brokers[i] = brokers.Get(i).Str()
	}

	o.Topic, err = args.GetRequiredString("topic")
	if err != nil {
		return err
	}
	if len(o.Topic) == 0 {
		return errors.New("invalid topic name")
	}

	o.Balancer, _, err = args.GetString("balancer")
	if err != nil {
		return err
	}

	o.Name, ok, err = args.GetString("name")
	if err != nil {
		return err
	}
	if !ok {
		o.NameColumn, ok, err = args.GetString("name_column")
		if err != nil {
			return err
		}
		if !ok {
			o.NameColumn = "_measurement"
		}
	}
	o.TimeColumn, ok, err = args.GetString("time_column")
	if err != nil {
		return err
	}
	if !ok {
		o.TimeColumn = execute.DefaultTimeColLabel
	}
	tagColumns, ok, err := args.GetArray("tag_columns", semantic.String)
	if err != nil {
		return err
	}
	o.TagColumns = o.TagColumns[:0]
	if ok {
		for i := 0; i < tagColumns.Len(); i++ {
			o.TagColumns = append(o.TagColumns, tagColumns.Get(i).Str())
		}
		sort.Strings(o.TagColumns)
	}
	valueColumns, ok, err := args.GetArray("value_columns", semantic.String)
	if err != nil {
		return err
	}
	o.ValueColumns = o.ValueColumns[:0]
	if !ok || valueColumns.Len() == 0 {
		o.ValueColumns = append(o.ValueColumns, execute.DefaultValueColLabel)
	} else {
		for i := 0; i < valueColumns.Len(); i++ {
			o.TagColumns = append(o.ValueColumns, valueColumns.Get(i).Str())
		}
		sort.Strings(o.TagColumns)
	}

	msgBufSize, ok, err := args.GetInt("msg_buffer_size")
	o.MsgBufSize = int(msgBufSize)
	if err != nil {
		return err
	}
	if o.MsgBufSize < 0 || !ok {
		o.MsgBufSize = 0 // so the library will set it  to the default
	}

	return err
}
func createToKafkaOpSpec(args query.Arguments, a *query.Administration) (query.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}
	s := new(ToKafkaOpSpec)
	if err := s.ReadArgs(args); err != nil {
		return nil, err
	}
	return s, nil
}

var ToKafkaSignature = query.DefaultFunctionSignature()

func (ToKafkaOpSpec) Kind() query.OperationKind {
	return ToKafkaKind
}

type ToKafkaProcedureSpec struct {
	Spec     *ToKafkaOpSpec
	balancer kafka.Balancer
}

func (o *ToKafkaProcedureSpec) Kind() plan.ProcedureKind {
	return ToKafkaKind
}
func (o *ToKafkaProcedureSpec) Copy() plan.ProcedureSpec {
	s := o.Spec
	res := &ToKafkaProcedureSpec{
		Spec: &ToKafkaOpSpec{
			Brokers:      append([]string(nil), s.Brokers...),
			Topic:        s.Topic,
			Balancer:     s.Balancer,
			Name:         s.Name,
			NameColumn:   s.NameColumn,
			TimeColumn:   s.TimeColumn,
			TagColumns:   append([]string(nil), s.TagColumns...),
			ValueColumns: append([]string(nil), s.ValueColumns...),
		},
	}
	switch s.Balancer {
	case "hash", "": //hash is default for compatibility with enterprise
		res.balancer = &kafka.Hash{}

	case "round-robin":
		res.balancer = &kafka.RoundRobin{}

	case "least-bytes":
		res.balancer = &kafka.LeastBytes{}
	}
	return res
}
func newToKafkaProcedure(qs query.OperationSpec, a plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*ToKafkaOpSpec)
	if !ok && spec != nil {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}
	return &ToKafkaProcedureSpec{Spec: spec}, nil
}
func createToKafkaTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*ToKafkaProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	cache := execute.NewBlockBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	t := NewToKafkaTransformation(d, cache, s)
	return t, d, nil
}

type ToKafkaTransformation struct {
	d     execute.Dataset
	cache execute.BlockBuilderCache
	spec  *ToKafkaProcedureSpec
}

func (t *ToKafkaTransformation) RetractBlock(id execute.DatasetID, key query.PartitionKey) error {
	return t.d.RetractBlock(key)
}
func NewToKafkaTransformation(d execute.Dataset, cache execute.BlockBuilderCache, spec *ToKafkaProcedureSpec) *ToKafkaTransformation {
	return &ToKafkaTransformation{
		d:     d,
		cache: cache,
		spec:  spec,
	}
}

type toKafkaMetric struct {
	tags   []*protocol.Tag
	fields []*protocol.Field
	name   string
	t      time.Time
}

func (m *toKafkaMetric) TagList() []*protocol.Tag {
	return m.tags
}
func (m *toKafkaMetric) FieldList() []*protocol.Field {
	return m.fields
}
func (m *toKafkaMetric) truncateTagsAndFields() {
	m.fields = m.fields[:0]
	m.tags = m.tags[:0]
}
func (m *toKafkaMetric) Name() string {
	return m.name
}
func (m *toKafkaMetric) Time() time.Time {
	return m.t
}

func (t *ToKafkaTransformation) Process(id execute.DatasetID, b query.Block) (err error) {
	w := DefaultKafkaWriterFactory(kafka.WriterConfig{
		Brokers:       t.spec.Spec.Brokers,
		Topic:         t.spec.Spec.Topic,
		Balancer:      t.spec.balancer,
		BatchSize:     t.spec.Spec.MsgBufSize,
		QueueCapacity: t.spec.Spec.MsgBufSize,
	})

	defer func() {
		err2 := w.Close()
		// don't overwrite current error
		if err != nil {
			return
		}
		if err2 != nil {
			// allow Process to return the error from the defered Close()
			err = err2
			return
		}
	}()
	pr, pw := io.Pipe() // TODO: replce the pipe with something faster
	// I'd like a linereader in line-protocol
	m := &toKafkaMetric{}
	e := protocol.NewEncoder(pw)
	e.FailOnFieldErr(true)
	e.SetFieldSortOrder(protocol.SortFields)
	cols := b.Cols()
	labels := make(map[string]idxType, len(cols))
	for i, col := range cols {
		labels[col.Label] = idxType{Idx: i, Type: col.Type}
	}
	// do time
	timeColLabel := t.spec.Spec.TimeColumn
	timeColIdx, ok := labels[timeColLabel]
	if !ok {
		return errors.New("Could not get time column")
	}
	if timeColIdx.Type != query.TTime {
		return fmt.Errorf("column %s is not of type %s", timeColLabel, timeColIdx.Type)
	}
	var measurementNameCol string
	if t.spec.Spec.Name == "" {
		measurementNameCol = t.spec.Spec.NameColumn
	}
	// check if each col is a tag or value and cache this value for the loop
	colMetadatas := b.Cols()
	isTag := make([]bool, len(colMetadatas))
	isValue := make([]bool, len(colMetadatas))
	for i, col := range colMetadatas {
		isValue[i] = sort.SearchStrings(t.spec.Spec.ValueColumns, col.Label) < len(t.spec.Spec.ValueColumns) && t.spec.Spec.ValueColumns[sort.SearchStrings(t.spec.Spec.ValueColumns, col.Label)] == col.Label
		isTag[i] = sort.SearchStrings(t.spec.Spec.TagColumns, col.Label) < len(t.spec.Spec.TagColumns) && t.spec.Spec.TagColumns[sort.SearchStrings(t.spec.Spec.TagColumns, col.Label)] == col.Label
	}
	m.name = t.spec.Spec.Name
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		err = b.Do(func(er query.ColReader) error {
			l := er.Len()
			for i := 0; i < l; i++ {
				m.truncateTagsAndFields()
				for j, col := range er.Cols() {
					switch {
					case col.Label == timeColLabel:
						m.t = er.Times(j)[i].Time()
					case measurementNameCol != "" && measurementNameCol == col.Label:
						if col.Type != query.TString {
							return errors.New("invalid type for measurement column")
						}
						m.name = er.Strings(j)[i]
					case isTag[j]:
						if col.Type != query.TString {
							return errors.New("invalid type for measurement column")
						}
						m.tags = append(m.tags, &protocol.Tag{Key: col.Label, Value: er.Strings(j)[i]})
					case isValue[j]:
						switch col.Type {
						case query.TFloat:
							m.fields = append(m.fields, &protocol.Field{Key: col.Label, Value: er.Floats(j)[i]})
						case query.TInt:
							m.fields = append(m.fields, &protocol.Field{Key: col.Label, Value: er.Ints(j)[i]})
						case query.TUInt:
							m.fields = append(m.fields, &protocol.Field{Key: col.Label, Value: er.UInts(j)[i]})
						case query.TString:
							m.fields = append(m.fields, &protocol.Field{Key: col.Label, Value: er.Strings(j)[i]})
						case query.TTime:
							m.fields = append(m.fields, &protocol.Field{Key: col.Label, Value: er.Times(j)[i]})
						case query.TBool:
							m.fields = append(m.fields, &protocol.Field{Key: col.Label, Value: er.Bools(j)[i]})
						default:
							return fmt.Errorf("invalid type for column %s", col.Label)
						}
					}
				}
				_, err := e.Encode(m)
				if err != nil {
					return err
				}
			}
			return nil
		})
		pw.Close()
		wg.Done()
	}()
	// write the data to kafka
	{
		scan := bufio.NewScanner(pr)
		msgBuf := make([]kafka.Message, 128)
		i := 0
		// todo, make this a little more async
		for scan.Scan() {
			v := append([]byte(nil), scan.Bytes()...) // we do this since scan.Bytes()'s result can be overwritten by calls to Scan()
			if cap(msgBuf[i].Key) != 8 {
				msgBuf[i].Key = make([]byte, 8)
			}
			binary.LittleEndian.PutUint64(msgBuf[i].Key, xxhash.Sum64(v))
			msgBuf[i].Value = v
			if i == t.spec.Spec.MsgBufSize-1 {
				if err = w.WriteMessages(context.Background(), msgBuf...); err != nil {
					return err
				}
				msgBuf = msgBuf[:0]
				i = 0
			}
			i++
		}
		// send the remainder of the messages
		if len(msgBuf) > 0 {
			err = w.WriteMessages(context.Background(), msgBuf[:i]...)
		}
	}
	wg.Wait()
	return err
}

func (t *ToKafkaTransformation) UpdateWatermark(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateWatermark(pt)
}

func (t *ToKafkaTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateProcessingTime(pt)
}

func (t *ToKafkaTransformation) Finish(id execute.DatasetID, err error) {
	t.d.Finish(err)
}
