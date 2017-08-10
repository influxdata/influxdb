package query

import "sort"

type KeyValue struct {
	Key   string
	Value interface{}
}

type Cost struct {
	values map[string]interface{}
}

func (c *Cost) Add(k string, v interface{}) {
	if c.values == nil {
		c.values = make(map[string]interface{})
	}
	c.values[k] = v
}

func (c *Cost) Get() []KeyValue {
	if len(c.values) == 0 {
		return nil
	}
	keys := make([]string, 0, len(c.values))
	for k := range c.values {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	values := make([]KeyValue, len(keys))
	for i, key := range keys {
		values[i] = KeyValue{
			Key:   key,
			Value: c.values[key],
		}
	}
	return values
}
