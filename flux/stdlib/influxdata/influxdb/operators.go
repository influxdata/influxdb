package influxdb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxql"
)

const (
	ReadRangePhysKind           = "ReadRangePhysKind"
	ReadGroupPhysKind           = "ReadGroupPhysKind"
	ReadWindowAggregatePhysKind = "ReadWindowAggregatePhysKind"
	ReadTagKeysPhysKind         = "ReadTagKeysPhysKind"
	ReadTagValuesPhysKind       = "ReadTagValuesPhysKind"
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

	// Predicate is the filtering predicate for calling into storage.
	// It must not be mutated.
	Predicate *datatypes.Predicate

	Bounds flux.Bounds
}

func (s *ReadRangePhysSpec) Kind() plan.ProcedureKind {
	return ReadRangePhysKind
}
func (s *ReadRangePhysSpec) Copy() plan.ProcedureSpec {
	ns := new(ReadRangePhysSpec)

	ns.Bucket = s.Bucket
	ns.BucketID = s.BucketID

	ns.Predicate = s.Predicate

	ns.Bounds = s.Bounds

	return ns
}

func lookupDatabase(ctx context.Context, bucketName string, deps StorageDependencies, privilege influxql.Privilege) (string, string, error) {
	var db, rp string
	if i := strings.IndexByte(bucketName, '/'); i == -1 {
		db = bucketName
	} else {
		rp = bucketName[i+1:]
		db = bucketName[:i]
	}
	// validate and resolve db/rp
	di := deps.MetaClient.Database(db)
	if di == nil {
		return "", "", errors.New("no database")
	}
	if deps.AuthEnabled {
		user := meta.UserFromContext(ctx)
		if user == nil {
			return "", "", errors.New("no user for auth-enabled flux access")
		}
		if err := deps.Authorizer.AuthorizeDatabase(user, privilege, db); err != nil {
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

func (s *ReadRangePhysSpec) LookupDatabase(_ context.Context, deps StorageDependencies, a execute.Administration) (string, string, error) {
	if len(s.BucketID) != 0 {
		return "", "", errors.New("cannot refer to buckets by their id in 1.x")
	}
	return lookupDatabase(a.Context(), s.Bucket, deps, influxql.ReadPrivilege)
}

// TimeBounds implements plan.BoundsAwareProcedureSpec.
func (s *ReadRangePhysSpec) TimeBounds(predecessorBounds *plan.Bounds) *plan.Bounds {
	return &plan.Bounds{
		Start: values.ConvertTime(s.Bounds.Start.Time(s.Bounds.Now)),
		Stop:  values.ConvertTime(s.Bounds.Stop.Time(s.Bounds.Now)),
	}
}

type ReadWindowAggregatePhysSpec struct {
	plan.DefaultCost
	ReadRangePhysSpec

	WindowEvery flux.Duration
	Offset      flux.Duration
	Aggregates  []plan.ProcedureKind
	CreateEmpty bool
	TimeColumn  string
}

func (s *ReadWindowAggregatePhysSpec) PlanDetails() string {
	return fmt.Sprintf("every = %v, aggregates = %v, createEmpty = %v, timeColumn = \"%s\"", s.WindowEvery, s.Aggregates, s.CreateEmpty, s.TimeColumn)
}

func (s *ReadWindowAggregatePhysSpec) Kind() plan.ProcedureKind {
	return ReadWindowAggregatePhysKind
}

func (s *ReadWindowAggregatePhysSpec) Copy() plan.ProcedureSpec {
	ns := new(ReadWindowAggregatePhysSpec)

	ns.ReadRangePhysSpec = *s.ReadRangePhysSpec.Copy().(*ReadRangePhysSpec)
	ns.WindowEvery = s.WindowEvery
	ns.Offset = s.Offset
	ns.Aggregates = s.Aggregates
	ns.CreateEmpty = s.CreateEmpty
	ns.TimeColumn = s.TimeColumn

	return ns
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
