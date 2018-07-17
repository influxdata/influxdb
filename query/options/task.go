package options

import (
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/values"
)

func init() {
	query.RegisterBuiltInOption("task", taskObject())
}

func taskObject() values.Object {
	obj := values.NewObject()

	obj.Set("name", values.NewStringValue(""))
	obj.Set("cron", values.NewStringValue(""))
	obj.Set("every", values.NewDurationValue(0))
	obj.Set("delay", values.NewDurationValue(0))
	obj.Set("concurrency", values.NewIntValue(0))
	obj.Set("retry", values.NewIntValue(0))
	return obj
}
