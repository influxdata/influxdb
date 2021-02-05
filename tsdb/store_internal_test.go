package tsdb

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
)

func TestStore_mergeTagValues(t *testing.T) {
	examples := []struct {
		in  []tagValues
		out TagValues
	}{
		{},
		{in: make([]tagValues, 4), out: TagValues{Values: []KeyValue{}}},
		{
			in:  []tagValues{createtagValues("m0", map[string][]string{"host": {"server-a", "server-b", "server-c"}})},
			out: createTagValues("m0", map[string][]string{"host": {"server-a", "server-b", "server-c"}}),
		},
		{
			in: []tagValues{
				createtagValues("m0", map[string][]string{"host": {"server-a", "server-b", "server-c"}}),
				createtagValues("m0", map[string][]string{"host": {"server-a", "server-b", "server-c"}}),
			},
			out: createTagValues("m0", map[string][]string{"host": {"server-a", "server-b", "server-c"}}),
		},
		{
			in: []tagValues{
				createtagValues("m0", map[string][]string{"host": {"server-a", "server-b", "server-c"}}),
				createtagValues("m0", map[string][]string{"host": {"server-a", "server-d", "server-e"}}),
			},
			out: createTagValues("m0", map[string][]string{"host": {"server-a", "server-b", "server-c", "server-d", "server-e"}}),
		},
		{
			in: []tagValues{
				createtagValues("m0", map[string][]string{"host": {"server-a"}}),
				createtagValues("m0", map[string][]string{}),
				createtagValues("m0", map[string][]string{"host": {"server-a"}}),
			},
			out: createTagValues("m0", map[string][]string{"host": {"server-a"}}),
		},
		{
			in: []tagValues{
				createtagValues("m0", map[string][]string{"host": {"server-q", "server-z"}}),
				createtagValues("m0", map[string][]string{"host": {"server-a", "server-b", "server-c"}}),
				createtagValues("m0", map[string][]string{"host": {"server-a", "server-d", "server-e"}}),
				createtagValues("m0", map[string][]string{"host": {"server-e", "server-q", "server-z"}}),
				createtagValues("m0", map[string][]string{"host": {"server-a"}}),
			},
			out: createTagValues("m0", map[string][]string{"host": {"server-a", "server-b", "server-c", "server-d", "server-e", "server-q", "server-z"}}),
		},
		{
			in: []tagValues{
				createtagValues("m0", map[string][]string{"a": {"0", "1"}, "host1": {"server-q", "server-z"}}),
				createtagValues("m0", map[string][]string{"a": {"0", "2"}, "host2": {"server-a", "server-b", "server-c"}}),
				createtagValues("m0", map[string][]string{"a": {"0", "3"}, "host3": {"server-a", "server-d", "server-e"}}),
				createtagValues("m0", map[string][]string{"a": {"0", "4"}, "host4": {"server-e", "server-q", "server-z"}}),
				createtagValues("m0", map[string][]string{"a": {"0", "5"}, "host5": {"server-a"}}),
			},
			out: createTagValues("m0", map[string][]string{
				"a":     {"0", "1", "2", "3", "4", "5"},
				"host1": {"server-q", "server-z"},
				"host2": {"server-a", "server-b", "server-c"},
				"host3": {"server-a", "server-d", "server-e"},
				"host4": {"server-e", "server-q", "server-z"},
				"host5": {"server-a"},
			}),
		},
		{
			in: []tagValues{
				createtagValues("m0", map[string][]string{"region": {"east-1", "west-1"}, "host": {"server-a", "server-b", "server-c"}}),
				createtagValues("m0", map[string][]string{"region": {"north-1", "west-1"}, "host": {"server-a", "server-d", "server-e"}}),
			},
			out: createTagValues("m0", map[string][]string{
				"host":   {"server-a", "server-b", "server-c", "server-d", "server-e"},
				"region": {"east-1", "north-1", "west-1"},
			}),
		},
		{
			in: []tagValues{
				createtagValues("m0", map[string][]string{"region": {"east-1", "west-1"}, "host": {"server-a", "server-b", "server-c"}}),
				createtagValues("m0", map[string][]string{"city": {"Baltimore", "Las Vegas"}}),
			},
			out: createTagValues("m0", map[string][]string{
				"city":   {"Baltimore", "Las Vegas"},
				"host":   {"server-a", "server-b", "server-c"},
				"region": {"east-1", "west-1"},
			}),
		},
		{
			in: []tagValues{
				createtagValues("m0", map[string][]string{"city": {"Baltimore", "Las Vegas"}}),
				createtagValues("m0", map[string][]string{"region": {"east-1", "west-1"}, "host": {"server-a", "server-b", "server-c"}}),
			},
			out: createTagValues("m0", map[string][]string{
				"city":   {"Baltimore", "Las Vegas"},
				"host":   {"server-a", "server-b", "server-c"},
				"region": {"east-1", "west-1"},
			}),
		},
		{
			in: []tagValues{
				createtagValues("m0", map[string][]string{"region": {"east-1", "west-1"}, "host": {"server-a", "server-b", "server-c"}}),
				createtagValues("m0", map[string][]string{}),
			},
			out: createTagValues("m0", map[string][]string{
				"host":   {"server-a", "server-b", "server-c"},
				"region": {"east-1", "west-1"},
			}),
		},
	}

	buf := make([][2]int, 10)
	for i, example := range examples {
		t.Run(fmt.Sprintf("example_%d", i+1), func(t *testing.T) {
			if got, exp := mergeTagValues(buf, example.in...), example.out; !reflect.DeepEqual(got, exp) {
				t.Fatalf("\ngot\n %#v\n\n expected\n %#v", got, exp)
			}
		})
	}
}

// Helper to create some tagValues.
func createtagValues(mname string, kvs map[string][]string) tagValues {
	out := tagValues{
		name:   []byte(mname),
		keys:   make([]string, 0, len(kvs)),
		values: make([][]string, len(kvs)),
	}

	for k := range kvs {
		out.keys = append(out.keys, k)
	}
	sort.Strings(out.keys)

	for i, k := range out.keys {
		values := kvs[k]
		sort.Strings(values)
		out.values[i] = values
	}
	return out
}

// Helper to create some TagValues
func createTagValues(mname string, kvs map[string][]string) TagValues {
	var sz int
	for _, v := range kvs {
		sz += len(v)
	}

	out := TagValues{
		Measurement: mname,
		Values:      make([]KeyValue, 0, sz),
	}

	for tk, tvs := range kvs {
		for _, tv := range tvs {
			out.Values = append(out.Values, KeyValue{Key: tk, Value: tv})
		}
		// We have to sort the KeyValues since that's how they're provided from
		// the Store.
		sort.Sort(KeyValues(out.Values))
	}

	return out
}
