package reads

import (
	"bytes"
	"strings"

	"github.com/influxdata/influxdb/models"
)

// tagsKeyMerger is responsible for determining a merged set of tag keys
type keyMerger struct {
	i    int
	tmp  [][]byte
	keys [2][][]byte
}

func (km *keyMerger) clear() {
	km.i = 0
	km.keys[0] = km.keys[0][:0]
	if km.tmp != nil {
		tmp := km.tmp[:cap(km.tmp)]
		for i := range tmp {
			tmp[i] = nil
		}
	}
}

func (km *keyMerger) get() [][]byte { return km.keys[km.i&1] }

func (km *keyMerger) String() string {
	var s []string
	for _, k := range km.get() {
		s = append(s, string(k))
	}
	return strings.Join(s, ",")
}

func (km *keyMerger) mergeTagKeys(tags models.Tags) {
	if cap(km.tmp) < len(tags) {
		km.tmp = make([][]byte, len(tags))
	} else {
		km.tmp = km.tmp[:len(tags)]
	}

	for i := range tags {
		km.tmp[i] = tags[i].Key
	}

	km.mergeKeys(km.tmp)
}

func (km *keyMerger) mergeKeys(in [][]byte) {
	keys := km.keys[km.i&1]
	i, j := 0, 0
	for i < len(keys) && j < len(in) && bytes.Equal(keys[i], in[j]) {
		i++
		j++
	}

	if j == len(in) {
		// no new tags
		return
	}

	km.i = (km.i + 1) & 1
	l := len(keys) + len(in)
	if cap(km.keys[km.i]) < l {
		km.keys[km.i] = make([][]byte, l)
	} else {
		km.keys[km.i] = km.keys[km.i][:l]
	}

	keya := km.keys[km.i]

	// back up the pointers
	if i > 0 {
		i--
		j--
	}

	k := i
	copy(keya[:k], keys[:k])

	for i < len(keys) && j < len(in) {
		cmp := bytes.Compare(keys[i], in[j])
		if cmp < 0 {
			keya[k] = keys[i]
			i++
		} else if cmp > 0 {
			keya[k] = in[j]
			j++
		} else {
			keya[k] = keys[i]
			i++
			j++
		}
		k++
	}

	if i < len(keys) {
		k += copy(keya[k:], keys[i:])
	}

	if j < len(in) {
		k += copy(keya[k:], in[j:])
	}

	km.keys[km.i] = keya[:k]
}
