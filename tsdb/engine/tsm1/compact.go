package tsm1

import (
	"fmt"
	"sort"
	"strings"
)

// KeyIterator allows iteration over set of keys and values in sorted order.
type KeyIterator interface {
	Next() bool
	Read() (string, []Value, error)
}

// walKeyIterator allows WAL segments to be iterated over in sorted order.
type walKeyIterator struct {
	k      string
	order  []string
	series map[string]Values
}

func (k *walKeyIterator) Next() bool {
	if len(k.order) == 0 {
		return false
	}
	k.k = k.order[0]
	k.order = k.order[1:]
	return true
}

func (k *walKeyIterator) Read() (string, []Value, error) {
	return k.k, k.series[k.k], nil
}

func NewWALKeyIterator(readers ...*WALSegmentReader) (KeyIterator, error) {
	series := map[string]Values{}
	order := []string{}

	// Iterate over each reader in order.  Later readers will overwrite earlier ones if values
	// overlap.
	for _, r := range readers {
		for r.Next() {
			entry, err := r.Read()
			if err != nil {
				return nil, err
			}

			switch t := entry.(type) {
			case *WriteWALEntry:
				// Each point needs to be decomposed from a time with multiple fields, to a time, value tuple
				for _, p := range t.Points {
					// Create a series key for each field
					for k, v := range p.Fields() {
						key := fmt.Sprintf("%s%s%s", p.Key(), keyFieldSeparator, k)

						// Just append each point as we see it.  Dedup and sorting happens later.
						series[key] = append(series[key], NewValue(p.Time(), v))
					}
				}

			case *DeleteWALEntry:
				// Each key is a series, measurement + tagset string
				for _, k := range t.Keys {
					// seriesKey is specific to a field, measurment + tagset string + sep + field name
					for seriesKey := range series {
						key := seriesKey[:strings.Index(seriesKey, keyFieldSeparator)]
						//  If the delete series key matches the portion before the separator, we delete what we have
						if key == k {
							delete(series, seriesKey)
						}
					}
				}
			}
		}
	}

	// Need to create the order that we'll iterate over (sorted key), as well as
	// sort and dedup all the points for each key.
	for k, v := range series {
		order = append(order, k)
		series[k] = v.Deduplicate()
	}
	sort.Strings(order)

	return &walKeyIterator{
		series: series,
		order:  order,
	}, nil
}
