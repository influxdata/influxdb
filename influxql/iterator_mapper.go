package influxql

type iteratorMapper struct {
	e         *Emitter
	buf       []interface{}
	fields    []IteratorMap // which iterator to use for an aux field
	auxFields []interface{}
}

type IteratorMap interface {
	Value(tags Tags, buf []interface{}) interface{}
}

type FieldMap int

func (i FieldMap) Value(tags Tags, buf []interface{}) interface{} { return buf[i] }

type TagMap string

func (s TagMap) Value(tags Tags, buf []interface{}) interface{} { return tags.Value(string(s)) }

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
