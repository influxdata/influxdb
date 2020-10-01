package fields

import "sort"

type Fields []Field

// Merge merges other with the current set, replacing any matching keys from other.
func (fs *Fields) Merge(other Fields) {
	var list []Field
	i, j := 0, 0
	for i < len(*fs) && j < len(other) {
		if (*fs)[i].key < other[j].key {
			list = append(list, (*fs)[i])
			i++
		} else if (*fs)[i].key > other[j].key {
			list = append(list, other[j])
			j++
		} else {
			// equal, then "other" replaces existing key
			list = append(list, other[j])
			i++
			j++
		}
	}

	if i < len(*fs) {
		list = append(list, (*fs)[i:]...)
	} else if j < len(other) {
		list = append(list, other[j:]...)
	}

	*fs = list
}

// New creates a new set of fields, sorted by Key.
// Duplicate keys are removed.
func New(args ...Field) Fields {
	fields := Fields(args)
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].key < fields[j].key
	})

	// deduplicate
	// loop invariant: fields[:i] has no duplicates
	for i := 0; i < len(fields)-1; i++ {
		j := i + 1
		// find all duplicate keys
		for j < len(fields) && fields[i].key == fields[j].key {
			j++
		}

		d := (j - 1) - i // number of duplicate keys
		if d > 0 {
			// copy over duplicate keys in order to maintain loop invariant
			copy(fields[i+1:], fields[j:])
			fields = fields[:len(fields)-d]
		}
	}

	return fields
}
