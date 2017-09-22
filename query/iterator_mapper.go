package query

import "fmt"

type IteratorMap interface {
	Value(tags Tags, buf []interface{}) interface{}
}

type FieldMap int

func (i FieldMap) Value(tags Tags, buf []interface{}) interface{} { return buf[i] }

type TagMap string

func (s TagMap) Value(tags Tags, buf []interface{}) interface{} { return tags.Value(string(s)) }

type NullMap struct{}

func (NullMap) Value(tags Tags, buf []interface{}) interface{} { return nil }

func NewIteratorMapper(itrs []Iterator, driver IteratorMap, fields []IteratorMap, opt IteratorOptions) Iterator {
	if driver != nil {
		switch driver := driver.(type) {
		case FieldMap:
			switch itrs[int(driver)].(type) {
			case FloatIterator:
				return newFloatIteratorMapper(itrs, driver, fields, opt)
			case IntegerIterator:
				return newIntegerIteratorMapper(itrs, driver, fields, opt)
			case StringIterator:
				return newStringIteratorMapper(itrs, driver, fields, opt)
			case BooleanIterator:
				return newBooleanIteratorMapper(itrs, driver, fields, opt)
			default:
				panic(fmt.Sprintf("unable to map iterator type: %T", itrs[int(driver)]))
			}
		case TagMap:
			return newStringIteratorMapper(itrs, driver, fields, opt)
		default:
			panic(fmt.Sprintf("unable to create iterator mapper with driveression type: %T", driver))
		}
	}
	return newFloatIteratorMapper(itrs, nil, fields, opt)
}
