package influxql

type iteratorMapper struct {
	e         *Emitter
	buf       []interface{}
	fields    []IteratorMap // which iterator to use for an aux field
	auxFields []interface{}
}

// IteratorMap is an interface that provides a means of retrieving an iterator by an arbitrary key.
// Implementations might document how the mapping is done in each case.
type IteratorMap interface {
	Value(tags Tags, buf []interface{}) interface{}
}

// FieldMap is an IteratorMap that provides iterators by index
type FieldMap int

// Value provides the iterator in the Nth position in buf, being N the receiver's value.
func (i FieldMap) Value(tags Tags, buf []interface{}) interface{} { return buf[i] }

// TagMap is an IteratorMap that provides iterators by tag.
type TagMap string

// Value provides the iterator mapped to the receiver's tag.
func (s TagMap) Value(tags Tags, buf []interface{}) interface{} { return tags.Value(string(s)) }

// NewIteratorMapper creates an empty iteratorMapper ready for use.
func NewIteratorMapper(itrs []Iterator, fields []IteratorMap, opt IteratorOptions) Iterator {
	e := NewEmitter(itrs, opt.Ascending, 0)
	e.OmitTime = true
	return &iteratorMapper{
		e:         e,
		buf:       make([]interface{}, len(itrs)),
		fields:    fields,
		auxFields: make([]interface{}, len(fields)),
	}
}

func (itr *iteratorMapper) Next() (*FloatPoint, error) {
	t, name, tags, err := itr.e.loadBuf()
	if err != nil || t == ZeroTime {
		return nil, err
	}

	itr.e.readInto(t, name, tags, itr.buf)
	for i, f := range itr.fields {
		itr.auxFields[i] = f.Value(tags, itr.buf)
	}
	return &FloatPoint{
		Name: name,
		Tags: tags,
		Time: t,
		Aux:  itr.auxFields,
	}, nil
}

func (itr *iteratorMapper) Stats() IteratorStats {
	stats := IteratorStats{}
	for _, itr := range itr.e.itrs {
		stats.Add(itr.Stats())
	}
	return stats
}

func (itr *iteratorMapper) Close() error {
	return itr.e.Close()
}
