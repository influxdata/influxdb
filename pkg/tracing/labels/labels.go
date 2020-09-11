package labels

import "sort"

type Label struct {
	Key, Value string
}

// The Labels type represents a set of labels, sorted by Key.
type Labels []Label

// Merge merges other with the current set, replacing any matching keys from other.
func (ls *Labels) Merge(other Labels) {
	var list []Label
	i, j := 0, 0
	for i < len(*ls) && j < len(other) {
		if (*ls)[i].Key < other[j].Key {
			list = append(list, (*ls)[i])
			i++
		} else if (*ls)[i].Key > other[j].Key {
			list = append(list, other[j])
			j++
		} else {
			// equal, then "other" replaces existing key
			list = append(list, other[j])
			i++
			j++
		}
	}

	if i < len(*ls) {
		list = append(list, (*ls)[i:]...)
	} else if j < len(other) {
		list = append(list, other[j:]...)
	}

	*ls = list
}

// New takes an even number of strings representing key-value pairs
// and creates a new slice of Labels. Duplicates are removed, however,
// there is no guarantee which will be removed
func New(args ...string) Labels {
	if len(args)%2 != 0 {
		panic("uneven number of arguments to label.Labels")
	}
	var labels Labels
	for i := 0; i+1 < len(args); i += 2 {
		labels = append(labels, Label{Key: args[i], Value: args[i+1]})
	}

	sort.Slice(labels, func(i, j int) bool {
		return labels[i].Key < labels[j].Key
	})

	// deduplicate
	// loop invariant: labels[:i] has no duplicates
	for i := 0; i < len(labels)-1; i++ {
		j := i + 1
		// find all duplicate keys
		for j < len(labels) && labels[i].Key == labels[j].Key {
			j++
		}

		d := (j - 1) - i // number of duplicate keys
		if d > 0 {
			// copy over duplicate keys in order to maintain loop invariant
			copy(labels[i+1:], labels[j:])
			labels = labels[:len(labels)-d]
		}
	}

	return labels
}
