package influxdb

import (
	"fmt"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/values"
)

const ReadRangePhysKind = "ReadRangePhysKind"

type ReadRangePhysSpec struct {
	plan.DefaultCost

	Bucket   string
	BucketID string

	Bounds flux.Bounds
}

func (s *ReadRangePhysSpec) Kind() plan.ProcedureKind {
	return ReadRangePhysKind
}
func (s *ReadRangePhysSpec) Copy() plan.ProcedureSpec {
	ns := new(ReadRangePhysSpec)

	ns.Bucket = s.Bucket
	ns.BucketID = s.BucketID

	ns.Bounds = s.Bounds

	return ns
}

func (s *ReadRangePhysSpec) PostPhysicalValidate(id plan.NodeID) error {
	if s.Bounds.Start.IsZero() && s.Bounds.Stop.IsZero() {
		var bucket string
		if len(s.Bucket) > 0 {
			bucket = s.Bucket
		} else {
			bucket = s.BucketID
		}
		return fmt.Errorf(`%s: results from "%s" must be bounded`, id, bucket)
	}
	return nil
}

// TimeBounds implements plan.BoundsAwareProcedureSpec.
func (s *ReadRangePhysSpec) TimeBounds(predecessorBounds *plan.Bounds) *plan.Bounds {
	return &plan.Bounds{
		Start: values.ConvertTime(s.Bounds.Start.Time(s.Bounds.Now)),
		Stop:  values.ConvertTime(s.Bounds.Stop.Time(s.Bounds.Now)),
	}
}
