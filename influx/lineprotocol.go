package influx

import (
	"fmt"
	"sort"
	"strings"

	"github.com/influxdata/chronograf"
)

var (
	escapeMeasurement = strings.NewReplacer(
		`,` /* to */, `\,`,
		` ` /* to */, `\ `,
	)
	escapeKeys = strings.NewReplacer(
		`,` /* to */, `\,`,
		`"` /* to */, `\"`,
		` ` /* to */, `\ `,
		`=` /* to */, `\=`,
	)
	escapeTagValues = strings.NewReplacer(
		`,` /* to */, `\,`,
		`"` /* to */, `\"`,
		` ` /* to */, `\ `,
		`=` /* to */, `\=`,
	)
	escapeFieldStrings = strings.NewReplacer(
		`"` /* to */, `\"`,
		`\` /* to */, `\\`,
	)
)

func toLineProtocol(point *chronograf.Point) string {
	measurement := escapeMeasurement.Replace(point.Measurement)
	tags := []string{}
	for tag, value := range point.Tags {
		if value != "" {
			t := fmt.Sprintf("%s=%s", escapeKeys.Replace(tag), escapeTagValues.Replace(value))
			tags = append(tags, t)
		}
	}
	// it is faster to insert data into influx db if the tags are sorted
	sort.Strings(tags)

	fields := []string{}
	for field, value := range point.Fields {
		var format string
		switch v := value.(type) {
		case int64, int32, int16, int8, int:
			format = fmt.Sprintf("%s=%di", escapeKeys.Replace(field), v)
		case uint64, uint32, uint16, uint8, uint:
			format = fmt.Sprintf("%s=%du", escapeKeys.Replace(field), v)
		case float64, float32:
			format = fmt.Sprintf("%s=%f", escapeKeys.Replace(field), v)
		case string:
			format = fmt.Sprintf(`%s="%s"`, escapeKeys.Replace(field), escapeFieldStrings.Replace(v))
		case bool:
			format = fmt.Sprintf("%s=%t", escapeKeys.Replace(field), v)
		}
		if format != "" {
			fields = append(fields, format)
		}
	}
	return fmt.Sprintf("%s,%s %s %d\n",
		measurement,
		strings.Join(tags, ","),
		strings.Join(fields, ","),
		point.Time)
}
