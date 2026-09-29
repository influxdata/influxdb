package models

import (
	"encoding/json"
	"sort"
)

// Row represents a single row returned from the execution of a statement.
type Row struct {
	Name    string            `json:"name,omitempty"`
	Tags    map[string]string `json:"tags,omitempty"`
	Columns []string          `json:"columns,omitempty"`
	Values  [][]interface{}   `json:"values,omitempty"`
	Partial bool              `json:"partial,omitempty"`
	// GroupingKey is a single byte placed after Partial so it occupies
	// existing padding: a Row is no larger for queries that do not group by
	// date_part, which may return millions of series.
	GroupingKey GroupingKey `json:"grouping_keys,omitempty"`
}

// SameSeries returns true if r contains values for the same series as o.
func (r *Row) SameSeries(o *Row) bool {
	return r.tagsHash() == o.tagsHash() && r.Name == o.Name && r.GroupingKey == o.GroupingKey
}

// DatePartNames are the canonical date_part part names, indexed by
// query.DatePartExpr. They live here so a Row can name its GROUP BY date_part
// dimension in a single byte.
var DatePartNames = [...]string{
	"year", "quarter", "month", "week", "day", "hour", "minute", "second",
	"millisecond", "microsecond", "nanosecond", "dow", "doy", "epoch", "isodow",
}

// GroupingKey identifies the GROUP BY date_part dimension a Row belongs to: the
// index of its part in DatePartNames plus one. The zero value means the row
// has none. It is encoded in JSON as a one-element list of the part name.
type GroupingKey uint8

// String returns the part name, or "" for the zero value or an unknown key.
func (k GroupingKey) String() string {
	if k == 0 || int(k) > len(DatePartNames) {
		return ""
	}
	return DatePartNames[k-1]
}

// MarshalJSON encodes k as a one-element list of its part name.
func (k GroupingKey) MarshalJSON() ([]byte, error) {
	return json.Marshal([]string{k.String()})
}

// UnmarshalJSON decodes a list of part names. A part this client does not
// know decodes to the zero value rather than failing the whole response.
func (k *GroupingKey) UnmarshalJSON(data []byte) error {
	var names []string
	if err := json.Unmarshal(data, &names); err != nil {
		return err
	}
	*k = 0
	if len(names) == 0 {
		return nil
	}
	for i, name := range DatePartNames {
		if name == names[0] {
			*k = GroupingKey(i + 1)
			return nil
		}
	}
	return nil
}

// tagsHash returns a hash of tag key/value pairs.
func (r *Row) tagsHash() uint64 {
	h := NewInlineFNV64a()
	keys := r.tagsKeys()
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte(r.Tags[k]))
	}
	return h.Sum64()
}

// tagKeys returns a sorted list of tag keys.
func (r *Row) tagsKeys() []string {
	a := make([]string, 0, len(r.Tags))
	for k := range r.Tags {
		a = append(a, k)
	}
	sort.Strings(a)
	return a
}

// Rows represents a collection of rows. Rows implements sort.Interface.
type Rows []*Row

// Len implements sort.Interface.
func (p Rows) Len() int { return len(p) }

// Less implements sort.Interface.
func (p Rows) Less(i, j int) bool {
	// Sort by name first.
	if p[i].Name != p[j].Name {
		return p[i].Name < p[j].Name
	}

	// Sort by tag set hash. Tags don't have a meaningful sort order so we
	// just compute a hash and sort by that instead. This allows the tests
	// to receive rows in a predictable order every time.
	return p[i].tagsHash() < p[j].tagsHash()
}

// Swap implements sort.Interface.
func (p Rows) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
