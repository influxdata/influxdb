package checkers

import (
	"fmt"
	. "launchpad.net/gocheck"
)

type inRangeChecker struct {
	*CheckerInfo
}

var InRange Checker = &inRangeChecker{
	&CheckerInfo{Name: "InRange", Params: []string{"obtained", "expected greater than", "expected less than"}},
}

func (checker *inRangeChecker) Check(params []interface{}, names []string) (result bool, error string) {
	defer func() {
		if v := recover(); v != nil {
			result = false
			error = fmt.Sprint(v)
		}
	}()
	switch params[0].(type) {
	default:
		return false, "can't compare range for type"
	case int:
		p1 := params[0].(int)
		p2 := params[1].(int)
		p3 := params[2].(int)
		if p2 > p1 {
			return false, ""
		}
		if p3 < p1 {
			return false, ""
		}
	case int64:
		p1 := params[0].(int64)
		p2 := params[1].(int64)
		p3 := params[2].(int64)
		if p2 > p1 {
			return false, ""
		}
		if p3 < p1 {
			return false, ""
		}
	case float64:
		p1 := params[0].(float64)
		p2 := params[1].(float64)
		p3 := params[2].(float64)

		if p2 > p1 {
			return false, ""
		}
		if p3 < p1 {
			return false, ""
		}
	}

	return true, ""
}
