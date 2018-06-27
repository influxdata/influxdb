package tsdb_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
)

// Ensure tags can be marshaled into a byte slice.
func TestMarshalTags(t *testing.T) {
	for i, tt := range []struct {
		tags   map[string]string
		result []byte
	}{
		{
			tags:   nil,
			result: nil,
		},
		{
			tags:   map[string]string{"foo": "bar"},
			result: []byte(`foo|bar`),
		},
		{
			tags:   map[string]string{"foo": "bar", "baz": "battttt"},
			result: []byte(`baz|foo|battttt|bar`),
		},
		{
			tags:   map[string]string{"baz": "battttt", "foo": "bar"},
			result: []byte(`baz|foo|battttt|bar`),
		},
	} {
		result := tsdb.MarshalTags(tt.tags)
		if !bytes.Equal(result, tt.result) {
			t.Fatalf("%d. unexpected result: exp=%s, got=%s", i, tt.result, result)
		}
	}
}

func BenchmarkMarshalTags_KeyN1(b *testing.B)  { benchmarkMarshalTags(b, 1) }
func BenchmarkMarshalTags_KeyN3(b *testing.B)  { benchmarkMarshalTags(b, 3) }
func BenchmarkMarshalTags_KeyN5(b *testing.B)  { benchmarkMarshalTags(b, 5) }
func BenchmarkMarshalTags_KeyN10(b *testing.B) { benchmarkMarshalTags(b, 10) }

func benchmarkMarshalTags(b *testing.B, keyN int) {
	const keySize, valueSize = 8, 15

	// Generate tag map.
	tags := make(map[string]string)
	for i := 0; i < keyN; i++ {
		tags[fmt.Sprintf("%0*d", keySize, i)] = fmt.Sprintf("%0*d", valueSize, i)
	}

	// Unmarshal map into byte slice.
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tsdb.MarshalTags(tags)
	}
}

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

type TestSeries struct {
	Measurement string
	Key         string
	Tags        models.Tags
}

func genTestSeries(mCnt, tCnt, vCnt int) []*TestSeries {
	measurements := genStrList("measurement", mCnt)
	tagSets := NewTagSetGenerator(tCnt, vCnt).AllSets()
	series := make([]*TestSeries, 0, mCnt*len(tagSets))
	for _, m := range measurements {
		for _, ts := range tagSets {
			series = append(series, &TestSeries{
				Measurement: m,
				Key:         fmt.Sprintf("%s:%s", m, string(tsdb.MarshalTags(ts))),
				Tags:        models.NewTags(ts),
			})
		}
	}
	return series
}

type TagValGenerator struct {
	Key  string
	Vals []string
	idx  int
}

func NewTagValGenerator(tagKey string, nVals int) *TagValGenerator {
	tvg := &TagValGenerator{Key: tagKey, Vals: make([]string, 0, nVals)}
	for i := 0; i < nVals; i++ {
		tvg.Vals = append(tvg.Vals, fmt.Sprintf("tagValue%d", i))
	}
	return tvg
}

func (tvg *TagValGenerator) First() string {
	tvg.idx = 0
	return tvg.Curr()
}

func (tvg *TagValGenerator) Curr() string {
	return tvg.Vals[tvg.idx]
}

func (tvg *TagValGenerator) Next() string {
	tvg.idx++
	if tvg.idx >= len(tvg.Vals) {
		tvg.idx--
		return ""
	}
	return tvg.Curr()
}

type TagSet map[string]string

type TagSetGenerator struct {
	TagVals []*TagValGenerator
}

func NewTagSetGenerator(nSets int, nTagVals ...int) *TagSetGenerator {
	tsg := &TagSetGenerator{TagVals: make([]*TagValGenerator, 0, nSets)}
	for i := 0; i < nSets; i++ {
		nVals := nTagVals[0]
		if i < len(nTagVals) {
			nVals = nTagVals[i]
		}
		tagKey := fmt.Sprintf("tagKey%d", i)
		tsg.TagVals = append(tsg.TagVals, NewTagValGenerator(tagKey, nVals))
	}
	return tsg
}

func (tsg *TagSetGenerator) First() TagSet {
	for _, tsv := range tsg.TagVals {
		tsv.First()
	}
	return tsg.Curr()
}

func (tsg *TagSetGenerator) Curr() TagSet {
	ts := TagSet{}
	for _, tvg := range tsg.TagVals {
		ts[tvg.Key] = tvg.Curr()
	}
	return ts
}

func (tsg *TagSetGenerator) Next() TagSet {
	val := ""
	for _, tsv := range tsg.TagVals {
		if val = tsv.Next(); val != "" {
			break
		} else {
			tsv.First()
		}
	}

	if val == "" {
		return nil
	}

	return tsg.Curr()
}

func (tsg *TagSetGenerator) AllSets() []TagSet {
	allSets := []TagSet{}
	for ts := tsg.First(); ts != nil; ts = tsg.Next() {
		allSets = append(allSets, ts)
	}
	return allSets
}

func genStrList(prefix string, n int) []string {
	lst := make([]string, 0, n)
	for i := 0; i < n; i++ {
		lst = append(lst, fmt.Sprintf("%s%d", prefix, i))
	}
	return lst
}
