package storage

const (
	// maxAnnotationLength is the max length of byte array or string allowed in the annotations
	maxAnnotationLength = 256
)

func truncateString(value string) string {
	// we ignore the problem of utf8 runes possibly being sliced in the middle,
	// as it is rather expensive to iterate through each tag just to find rune
	// boundaries.
	if len(value) > maxAnnotationLength {
		return value[:maxAnnotationLength]
	}
	return value
}
