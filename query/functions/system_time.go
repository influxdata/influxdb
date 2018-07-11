package functions

import (
	"time"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/semantic"
	"github.com/influxdata/platform/query/values"
)

var systemTimeFuncName = "systemTime"

func init() {
	nowFunc := SystemTime()
	query.RegisterBuiltInValue(systemTimeFuncName, nowFunc)
}

// SystemTime return a function value that when called will give the current system time
func SystemTime() values.Value {
	name := systemTimeFuncName
	ftype := semantic.NewFunctionType(semantic.FunctionSignature{
		ReturnType: semantic.Time,
	})
	call := func(args values.Object) (values.Value, error) {
		return values.NewTimeValue(values.ConvertTime(time.Now().UTC())), nil
	}
	sideEffect := false
	return values.NewFunction(name, ftype, call, sideEffect)
}
