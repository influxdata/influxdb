package query

// TagSubset identifies the tag subset a point belongs to at the current level
// of the query. HasTags distinguishes "no tag dimensions in the GROUP BY" from
// a tag subset whose ID happens to be empty, so composite grouping keys stay
// unambiguous.
type TagSubset struct {
	ID      string
	HasTags bool
}

// DimensionGrouper resolves additional grouping keys from a point's auxiliary data.
// Implementations handle specific GROUP BY function types (e.g. date_part).
type DimensionGrouper interface {
	// ResolveKeys examines a point's aux values and returns grouping entries
	// composed with the point's tag subset.
	ResolveKeys(aux []interface{}, tags TagSubset) ([]GroupingEntry, error)

	// DecodeEntry reconstructs an aux-transportable value from an encoded key
	// so it can be appended to a point's Aux slice for multi-level reduces.
	DecodeEntry(encodedKey string) (interface{}, error)
}

type GroupingEntry struct {
	DimKey     string
	EncodedKey string
}
