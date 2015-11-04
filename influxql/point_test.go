package influxql_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/influxdb/influxdb/influxql"
)

// Ensure that tags can return a unique id.
func TestTags_ID(t *testing.T) {
	tags := influxql.NewTags(map[string]string{"foo": "bar", "baz": "bat"})
	if id := tags.ID(); id != "baz\x00foo\x00bat\x00bar" {
		t.Fatalf("unexpected id: %q", id)
	}
}

// Ensure that a subset can be created from a tag set.
func TestTags_Subset(t *testing.T) {
	tags := influxql.NewTags(map[string]string{"a": "0", "b": "1", "c": "2"})
	subset := tags.Subset([]string{"b", "c", "d"})
	if keys := subset.Keys(); !reflect.DeepEqual(keys, []string{"b", "c", "d"}) {
		t.Fatalf("unexpected keys: %+v", keys)
	} else if v := subset.Value("a"); v != "" {
		t.Fatalf("unexpected 'a' value: %s", v)
	} else if v := subset.Value("b"); v != "1" {
		t.Fatalf("unexpected 'b' value: %s", v)
	} else if v := subset.Value("c"); v != "2" {
		t.Fatalf("unexpected 'c' value: %s", v)
	} else if v := subset.Value("d"); v != "" {
		t.Fatalf("unexpected 'd' value: %s", v)
	}
}

// ParseTags returns an instance of Tags for a comma-delimited list of key/values.
func ParseTags(s string) influxql.Tags {
	m := make(map[string]string)
	for _, kv := range strings.Split(s, ",") {
		a := strings.Split(kv, "=")
		m[a[0]] = a[1]
	}
	return influxql.NewTags(m)
}
