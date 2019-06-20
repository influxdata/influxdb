package influxdb

import (
	"context"
	"errors"
	"strings"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxql"
)

const (
	ReadRangePhysKind     = "ReadRangePhysKind"
	ReadGroupPhysKind     = "ReadGroupPhysKind"
	ReadTagKeysPhysKind   = "ReadTagKeysPhysKind"
	ReadTagValuesPhysKind = "ReadTagValuesPhysKind"
)

type ReadGroupPhysSpec struct {
	plan.DefaultCost
	ReadRangePhysSpec

	GroupMode flux.GroupMode
	GroupKeys []string

	AggregateMethod string
}

func (s *ReadGroupPhysSpec) Kind() plan.ProcedureKind {
	return ReadGroupPhysKind
}

func (s *ReadGroupPhysSpec) Copy() plan.ProcedureSpec {
	ns := new(ReadGroupPhysSpec)
	ns.ReadRangePhysSpec = *s.ReadRangePhysSpec.Copy().(*ReadRangePhysSpec)

	ns.GroupMode = s.GroupMode
	ns.GroupKeys = s.GroupKeys

	ns.AggregateMethod = s.AggregateMethod
	return ns
}

type ReadRangePhysSpec struct {
	plan.DefaultCost

	Bucket   string
	BucketID string

	// FilterSet is set to true if there is a filter.
	FilterSet bool
	// Filter is the filter to use when calling into
	// storage. It must be possible to push down this
	// filter.
	Filter *semantic.FunctionExpression

	Bounds flux.Bounds
}

func (s *ReadRangePhysSpec) Kind() plan.ProcedureKind {
	return ReadRangePhysKind
}
func (s *ReadRangePhysSpec) Copy() plan.ProcedureSpec {
	ns := new(ReadRangePhysSpec)

	ns.Bucket = s.Bucket
	ns.BucketID = s.BucketID

	ns.FilterSet = s.FilterSet
	if ns.FilterSet {
		ns.Filter = s.Filter.Copy().(*semantic.FunctionExpression)
	}

	ns.Bounds = s.Bounds

	return ns
}

func (s *ReadRangePhysSpec) LookupDatabase(ctx context.Context, deps Dependencies, a execute.Administration) (string, string, error) {
	if len(s.BucketID) != 0 {
		return "", "", errors.New("cannot refer to buckets by their id in 1.x")
	}

	var db, rp string
	if i := strings.IndexByte(s.Bucket, '/'); i == -1 {
		db = s.Bucket
	} else {
		rp = s.Bucket[i+1:]
		db = s.Bucket[:i]
	}

	// validate and resolve db/rp
	di := deps.MetaClient.Database(db)
	if di == nil {
		return "", "", errors.New("no database")
	}

	if deps.AuthEnabled {
		user := meta.UserFromContext(a.Context())
		if user == nil {
			return "", "", errors.New("createFromSource: no user")
		}
		if err := deps.Authorizer.AuthorizeDatabase(user, influxql.ReadPrivilege, db); err != nil {
			return "", "", err
		}
	}

	if rp == "" {
		rp = di.DefaultRetentionPolicy
	}

	if rpi := di.RetentionPolicy(rp); rpi == nil {
		return "", "", errors.New("invalid retention policy")
	}
	return db, rp, nil
}

// TimeBounds implements plan.BoundsAwareProcedureSpec.
func (s *ReadRangePhysSpec) TimeBounds(predecessorBounds *plan.Bounds) *plan.Bounds {
	return &plan.Bounds{
		Start: values.ConvertTime(s.Bounds.Start.Time(s.Bounds.Now)),
		Stop:  values.ConvertTime(s.Bounds.Stop.Time(s.Bounds.Now)),
	}
}

type ReadTagKeysPhysSpec struct {
	ReadRangePhysSpec
}

func (s *ReadTagKeysPhysSpec) Kind() plan.ProcedureKind {
	return ReadTagKeysPhysKind
}

func (s *ReadTagKeysPhysSpec) Copy() plan.ProcedureSpec {
	ns := new(ReadTagKeysPhysSpec)
	ns.ReadRangePhysSpec = *s.ReadRangePhysSpec.Copy().(*ReadRangePhysSpec)
	return ns
}

type ReadTagValuesPhysSpec struct {
	ReadRangePhysSpec
	TagKey string
}

func (s *ReadTagValuesPhysSpec) Kind() plan.ProcedureKind {
	return ReadTagValuesPhysKind
}

func (s *ReadTagValuesPhysSpec) Copy() plan.ProcedureSpec {
	ns := new(ReadTagValuesPhysSpec)
	ns.ReadRangePhysSpec = *s.ReadRangePhysSpec.Copy().(*ReadRangePhysSpec)
	ns.TagKey = s.TagKey
	return ns
}
