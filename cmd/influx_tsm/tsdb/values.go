package tsdb

import (
	"fmt"
	"time"
)

type Value struct {
	T   int64
	Val interface{}
}

func (v *Value) Time() time.Time {
	return time.Unix(0, v.T)
}

func (v *Value) UnixNano() int64 {
	return v.T
}

func (v *Value) Value() interface{} {
	return v.Val
}

func (v *Value) String() string {
	return fmt.Sprintf("%v %v", v.Time(), v.Val)
}

func (v *Value) Size() int {
	switch vv := v.Val.(type) {
	case int64, float64:
		return 16
	case bool:
		return 9
	case string:
		return 8 + len(vv)
	}
	return 0
}
