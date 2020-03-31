package tsdb_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
)

// Ensure tags can be marshaled into a byte slice.
func TestMakeTagsKey(t *testing.T) {
	for i, tt := range []struct {
		keys   []string
		tags   models.Tags
		result []byte
	}{
		{
			keys:   nil,
			tags:   nil,
			result: nil,
		},
		{
			keys:   []string{"foo"},
			tags:   models.NewTags(map[string]string{"foo": "bar"}),
			result: []byte(`foo|bar`),
		},
		{
			keys:   []string{"foo"},
			tags:   models.NewTags(map[string]string{"baz": "battttt"}),
			result: []byte(``),
		},
		{
			keys:   []string{"baz", "foo"},
			tags:   models.NewTags(map[string]string{"baz": "battttt"}),
			result: []byte(`baz|battttt`),
		},
		{
			keys:   []string{"baz", "foo", "zzz"},
			tags:   models.NewTags(map[string]string{"foo": "bar"}),
			result: []byte(`foo|bar`),
		},
		{
			keys:   []string{"baz", "foo"},
			tags:   models.NewTags(map[string]string{"foo": "bar", "baz": "battttt"}),
			result: []byte(`baz|foo|battttt|bar`),
		},
		{
			keys:   []string{"baz"},
			tags:   models.NewTags(map[string]string{"baz": "battttt", "foo": "bar"}),
			result: []byte(`baz|battttt`),
		},
	} {
		result := tsdb.MakeTagsKey(tt.keys, tt.tags)
		if !bytes.Equal(result, tt.result) {
			t.Fatalf("%d. unexpected result: exp=%s, got=%s", i, tt.result, result)
		}
	}
}

func BenchmarkMakeTagsKey_KeyN1(b *testing.B)  { benchmarkMakeTagsKey(b, 1) }
func BenchmarkMakeTagsKey_KeyN3(b *testing.B)  { benchmarkMakeTagsKey(b, 3) }
func BenchmarkMakeTagsKey_KeyN5(b *testing.B)  { benchmarkMakeTagsKey(b, 5) }
func BenchmarkMakeTagsKey_KeyN10(b *testing.B) { benchmarkMakeTagsKey(b, 10) }

func makeTagsAndKeys(keyN int) ([]string, models.Tags) {
	const keySize, valueSize = 8, 15

	// Generate tag map.
	keys := make([]string, keyN)
	tags := make(map[string]string)
	for i := 0; i < keyN; i++ {
		keys[i] = fmt.Sprintf("%0*d", keySize, i)
		tags[keys[i]] = fmt.Sprintf("%0*d", valueSize, i)
	}

	return keys, models.NewTags(tags)
}

func benchmarkMakeTagsKey(b *testing.B, keyN int) {
	keys, tags := makeTagsAndKeys(keyN)

	// Unmarshal map into byte slice.
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tsdb.MakeTagsKey(keys, tags)
	}
}
