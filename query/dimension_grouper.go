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

// GroupingEntry is one resolved grouping dimension for a point.
//
// DimKey is the in-memory grouping/series-ordering key and is always computed.
// Expr and Val are the encode inputs for the aux-transport key; the encoded key
// is only needed when a new reduce bucket is created (a map miss), so it is
// computed lazily via EncodedKey() rather than eagerly per point — on the common
// bucket-hit path it is never built, avoiding a per-point allocation.
type GroupingEntry struct {
	DimKey string
	Expr   DatePartExpr
	Val    int64
}
