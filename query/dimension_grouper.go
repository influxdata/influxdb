package query

// DimensionGrouper resolves additional grouping keys from a point's auxiliary data.
// Implementations handle specific GROUP BY function types (e.g. date_part).
type DimensionGrouper interface {
	// ResolveKeys examines a point's aux values and returns grouping entries.
	// tagID is the current tag subset ID; hasTags indicates whether tag
	// dimensions are present (used to build composite keys).
	ResolveKeys(aux []interface{}, tagID string, hasTags bool) ([]GroupingEntry, error)

	// DecodeEntry reconstructs an aux-transportable value from an encoded key
	// so it can be appended to a point's Aux slice for multi-level reduces.
	DecodeEntry(encodedKey string) (interface{}, error)
}

type GroupingEntry struct {
	DimKey     string
	EncodedKey string
}
