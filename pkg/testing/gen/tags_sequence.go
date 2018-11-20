package gen

import (
	"fmt"
	"math"
	"sort"

	"github.com/influxdata/influxdb/models"
)

type TagsSequence interface {
	Next() bool
	Value() models.Tags
	Count() int
}

type TagsValuesSequence struct {
	tags models.Tags
	vals []Sequence
	n    int
	max  int
}

func NewTagsValuesSequenceKeysValues(keys []string, vals []Sequence) *TagsValuesSequence {
	tm := make(map[string]string, len(keys))
	for _, k := range keys {
		tm[k] = ""
	}

	count := 1
	for i := range vals {
		count *= vals[i].Count()
	}

	// models.Tags are ordered, so ensure vals are ordered with respect to keys
	sort.Sort(keyValues{keys, vals})

	return &TagsValuesSequence{
		tags: models.NewTags(tm),
		vals: vals,
		max:  count,
	}
}

func NewTagsValuesSequenceValues(prefix string, vals []Sequence) *TagsValuesSequence {
	keys := make([]string, len(vals))
	// max tag width
	tw := int(math.Ceil(math.Log10(float64(len(vals)))))
	tf := fmt.Sprintf("%s%%0%dd", prefix, tw)
	for i := range vals {
		keys[i] = fmt.Sprintf(tf, i)
	}
	return NewTagsValuesSequenceKeysValues(keys, vals)
}

func (s *TagsValuesSequence) Next() bool {
	if s.n >= s.max {
		return false
	}

	for i := range s.vals {
		s.tags[i].Value = []byte(s.vals[i].Value())
	}

	s.n++
	i := s.n
	for j := len(s.vals) - 1; j >= 0; j-- {
		v := s.vals[j]
		v.Next()
		c := v.Count()
		if r := i % c; r != 0 {
			break
		}
		i /= c
	}

	return true
}

func (s *TagsValuesSequence) Value() models.Tags { return s.tags }
func (s *TagsValuesSequence) Count() int         { return s.max }

type keyValues struct {
	keys []string
	vals []Sequence
}

func (k keyValues) Len() int           { return len(k.keys) }
func (k keyValues) Less(i, j int) bool { return k.keys[i] < k.keys[j] }
func (k keyValues) Swap(i, j int) {
	k.keys[i], k.keys[j] = k.keys[j], k.keys[i]
	k.vals[i], k.vals[j] = k.vals[j], k.vals[i]
}
