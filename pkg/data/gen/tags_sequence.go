package gen

import (
	"fmt"
	"math"
	"math/rand"
	"sort"

	"github.com/influxdata/influxdb/models"
)

type TagsSequence interface {
	Next() bool
	Value() models.Tags
	Count() int
}

type tagsValuesSequence struct {
	tags   models.Tags
	vals   []CountableSequence
	n      int
	count  int
	sample float64
	src    rand.Source
	nextFn func(*tagsValuesSequence) bool
}

type tagsValuesOption func(s *tagsValuesSequence)

func TagValuesLimitOption(n int) tagsValuesOption {
	return func(s *tagsValuesSequence) {
		if n >= s.count {
			return
		}

		s.src = rand.NewSource(20040409)
		s.sample = float64(n) / float64(s.count)
	}
}

func TagValuesSampleOption(n float64) tagsValuesOption {
	return func(s *tagsValuesSequence) {
		if n <= 0.0 || n > 1.0 {
			panic("expect: 0.0 < n ≤ 1.0")
		}

		s.src = rand.NewSource(int64(float64(math.MaxInt64>>1) * n))
		s.sample = n
		s.nextFn = (*tagsValuesSequence).nextSample
	}
}

func NewTagsValuesSequenceKeysValues(keys []string, vals []CountableSequence, opts ...tagsValuesOption) TagsSequence {
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

	s := &tagsValuesSequence{
		tags:   models.NewTags(tm),
		vals:   vals,
		count:  count,
		nextFn: (*tagsValuesSequence).next,
	}

	for _, o := range opts {
		o(s)
	}

	return s
}

func NewTagsValuesSequenceValues(prefix string, vals []CountableSequence) TagsSequence {
	keys := make([]string, len(vals))
	// max tag width
	tw := int(math.Ceil(math.Log10(float64(len(vals)))))
	tf := fmt.Sprintf("%s%%0%dd", prefix, tw)
	for i := range vals {
		keys[i] = fmt.Sprintf(tf, i)
	}
	return NewTagsValuesSequenceKeysValues(keys, vals)
}

func NewTagsValuesSequenceCounts(prefix string, counts []int) TagsSequence {
	tv := make([]CountableSequence, len(counts))
	for i := range counts {
		tv[i] = NewCounterByteSequenceCount(counts[i])
	}
	return NewTagsValuesSequenceValues(prefix, tv)
}

func (s *tagsValuesSequence) next() bool {
	if s.n >= s.count {
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

func (s *tagsValuesSequence) skip() bool {
	return (float64(s.src.Int63()>>10))*(1.0/9007199254740992.0) > s.sample
}

func (s *tagsValuesSequence) nextSample() bool {
	if s.n >= s.count {
		return false
	}

	for i := range s.vals {
		s.tags[i].Value = []byte(s.vals[i].Value())
	}

	for {
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

		if !s.skip() {
			break
		}
	}

	return true
}

func (s *tagsValuesSequence) Next() bool {
	return s.nextFn(s)
}

func (s *tagsValuesSequence) Value() models.Tags { return s.tags }
func (s *tagsValuesSequence) Count() int         { return s.count }

type keyValues struct {
	keys []string
	vals []CountableSequence
}

func (k keyValues) Len() int           { return len(k.keys) }
func (k keyValues) Less(i, j int) bool { return k.keys[i] < k.keys[j] }
func (k keyValues) Swap(i, j int) {
	k.keys[i], k.keys[j] = k.keys[j], k.keys[i]
	k.vals[i], k.vals[j] = k.vals[j], k.vals[i]
}
