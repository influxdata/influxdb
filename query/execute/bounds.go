package execute

import (
	"fmt"
	"math"
	"time"

	"github.com/influxdata/platform/query/values"
)

type Time = values.Time
type Duration = values.Duration

const (
	MaxTime = math.MaxInt64
	MinTime = math.MinInt64
)

type Bounds struct {
	Start Time
	Stop  Time
}

var AllTime = Bounds{
	Start: MinTime,
	Stop:  MaxTime,
}

func (b Bounds) String() string {
	return fmt.Sprintf("[%v, %v)", b.Start, b.Stop)
}

func (b Bounds) Contains(t Time) bool {
	return t >= b.Start && t < b.Stop
}

func (b Bounds) Overlaps(o Bounds) bool {
	return b.Contains(o.Start) || (b.Contains(o.Stop) && o.Stop > b.Start) || o.Contains(b.Start)
}

func (b Bounds) Equal(o Bounds) bool {
	return b == o
}

func (b Bounds) Shift(d Duration) Bounds {
	return Bounds{Start: b.Start.Add(d), Stop: b.Stop.Add(d)}
}

func Now() Time {
	return values.ConvertTime(time.Now())
}
