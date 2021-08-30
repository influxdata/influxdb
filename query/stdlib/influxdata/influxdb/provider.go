package influxdb

import (
	"context"
	"fmt"

	arrowmemory "github.com/apache/arrow/go/arrow/memory"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/dependencies/influxdb"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
)

type (
	Config       = influxdb.Config
	Predicate    = influxdb.Predicate
	PredicateSet = influxdb.PredicateSet
)

// Provider is an implementation of influxdb.Provider that exposes the
// query.StorageReader to flux and, if a host or org were specified, it
// delegates to the influxdb.HttpProvider.
type Provider struct {
	influxdb.HttpProvider
	Reader       query.StorageReader
	BucketLookup BucketLookup
}

func (p Provider) SeriesCardinalityReaderFor(ctx context.Context, conf influxdb.Config, bounds flux.Bounds, predicateSet influxdb.PredicateSet) (influxdb.Reader, error) {
	// If an organization is specified, it must be retrieved through the http
	// provider.
	if conf.Org.IsValid() || conf.Host != "" {
		return p.HttpProvider.SeriesCardinalityReaderFor(ctx, conf, bounds, predicateSet)
	}

	if !p.Reader.SupportReadSeriesCardinality(ctx) {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "series cardinality option is not supported",
		}
	}

	spec, err := p.readFilterSpec(ctx, conf, bounds, predicateSet)
	if err != nil {
		return nil, err
	}
	return seriesCardinalityReader{
		reader: p.Reader,
		spec: query.ReadSeriesCardinalitySpec{
			ReadFilterSpec: spec,
		},
	}, nil
}

// readFilterSpec will construct a query.ReadFilterSpec from the context and the
// configuration parameters.
func (p Provider) readFilterSpec(ctx context.Context, conf influxdb.Config, bounds flux.Bounds, predicateSet influxdb.PredicateSet) (query.ReadFilterSpec, error) {
	// Retrieve the organization id from the request context. Do not use the
	// configuration.
	req := query.RequestFromContext(ctx)
	if req == nil {
		return query.ReadFilterSpec{}, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "missing request on context",
		}
	}

	orgID := req.OrganizationID
	bucketID, err := p.lookupBucketID(ctx, orgID, conf.Bucket)
	if err != nil {
		return query.ReadFilterSpec{}, err
	}

	spec := query.ReadFilterSpec{
		OrganizationID: orgID,
		BucketID:       bucketID,
		Bounds: execute.Bounds{
			Start: values.ConvertTime(bounds.Start.Time(bounds.Now)),
			Stop:  values.ConvertTime(bounds.Stop.Time(bounds.Now)),
		},
	}

	if len(predicateSet) > 0 {
		predicates := make([]*datatypes.Predicate, 0, len(predicateSet))
		for _, predicate := range predicateSet {
			fn, ok := predicate.Fn.GetFunctionBodyExpression()
			if !ok {
				return query.ReadFilterSpec{}, &flux.Error{
					Code: codes.Invalid,
					Msg:  "predicate body cannot be pushed down",
				}
			}

			p, err := ToStoragePredicate(fn, "r")
			if err != nil {
				return query.ReadFilterSpec{}, err
			}
			predicates = append(predicates, p)
		}

		mergedPredicate, err := mergePredicates(ast.AndOperator, predicates...)
		if err != nil {
			return query.ReadFilterSpec{}, err
		}
		spec.Predicate = mergedPredicate
	}
	return spec, nil
}

func (p Provider) lookupBucketID(ctx context.Context, orgID platform.ID, bucket influxdb.NameOrID) (platform.ID, error) {
	// Determine bucketID
	switch {
	case bucket.Name != "":
		b, ok := p.BucketLookup.Lookup(ctx, orgID, bucket.Name)
		if !ok {
			return 0, &flux.Error{
				Code: codes.NotFound,
				Msg:  fmt.Sprintf("could not find bucket %q", bucket.Name),
			}
		}
		return b, nil
	case len(bucket.ID) != 0:
		var b platform.ID
		if err := b.DecodeFromString(bucket.ID); err != nil {
			return 0, &flux.Error{
				Code: codes.Invalid,
				Msg:  "invalid bucket id",
				Err:  err,
			}
		}
		return b, nil
	default:
		return 0, &flux.Error{
			Code: codes.Invalid,
			Msg:  "no bucket name or id have been specified",
		}
	}
}

type seriesCardinalityReader struct {
	reader query.StorageReader
	spec   query.ReadSeriesCardinalitySpec
}

func (s seriesCardinalityReader) Read(ctx context.Context, f func(flux.Table) error, mem arrowmemory.Allocator) error {
	alloc, ok := mem.(*memory.Allocator)
	if !ok {
		alloc = &memory.Allocator{
			Allocator: mem,
		}
	}

	reader, err := s.reader.ReadSeriesCardinality(ctx, s.spec, alloc)
	if err != nil {
		return err
	}

	return reader.Do(f)
}
