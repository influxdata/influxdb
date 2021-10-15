package datatypes

import "strings"

// AggregateNameMap is a set of uppercase aggregate names.
var AggregateNameMap = make(map[string]int32)

func init() {
	for k, v := range Aggregate_AggregateType_value {
		name := strings.ToUpper(strings.TrimPrefix(k, "AggregateType"))
		AggregateNameMap[name] = v
	}
}
