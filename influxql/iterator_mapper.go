package influxql

type IteratorMap interface {
	Value(tags Tags, buf []interface{}) interface{}
}

type FieldMap int

func (i FieldMap) Value(tags Tags, buf []interface{}) interface{} { return buf[i] }

type TagMap string

func (s TagMap) Value(tags Tags, buf []interface{}) interface{} { return tags.Value(string(s)) }

type NullMap struct{}

func (NullMap) Value(tags Tags, buf []interface{}) interface{} { return nil }
