package readservice

import "github.com/influxdata/platform/models"

const (
	fieldKey       = "_field"
	measurementKey = "_measurement"
)

var (
	fieldKeyBytes       = []byte(fieldKey)
	measurementKeyBytes = []byte(measurementKey)
)

func normalizeTags(tags models.Tags) {
	for i, tag := range tags {
		if len(tag.Key) == 2 && tag.Key[0] == '_' {
			switch tag.Key[1] {
			case 'f':
				tags[i].Key = fieldKeyBytes
			case 'm':
				tags[i].Key = measurementKeyBytes
			}
		}
	}
}
