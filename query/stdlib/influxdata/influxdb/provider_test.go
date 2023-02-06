package influxdb_test

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow/go/v7/arrow/memory"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/execute/table"
	"github.com/influxdata/flux/execute/table/static"
	influxdb2 "github.com/influxdata/influxdb/v2"
	context2 "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/query/stdlib/influxdata/influxdb"
	storageflux "github.com/influxdata/influxdb/v2/storage/flux"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
	"github.com/influxdata/influxdb/v2/v1/services/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

var (
	orgID    = platform.ID(10)
	bucketID = platform.ID(1) // mock BucketLookup returns the name "my-bucket" for id 1
)

func TestProvider_SeriesCardinalityReader(t *testing.T) {
	t.Parallel()

	store := &mock.ReadsStore{
		ReadSeriesCardinalityFn: func(ctx context.Context, req *datatypes.ReadSeriesCardinalityRequest) (cursors.Int64Iterator, error) {
			source, err := storage.GetReadSource(req.GetReadSource())
			if err != nil {
				return nil, err
			}

			if want, got := orgID, platform.ID(source.GetOrgID()); want != got {
				t.Errorf("unexpected org id -want/+got:\n\t- %d\n\t+ %d", want, got)
			}
			if want, got := bucketID, platform.ID(source.GetBucketID()); want != got {
				t.Errorf("unexpected org id -want/+got:\n\t- %d\n\t+ %d", want, got)
			}

			if want, got := req.Range.GetStart(), int64(1000000000); want != got {
				t.Errorf("unexpected start range -want/+got:\n\t- %d\n\t+ %d", want, got)
			}
			if want, got := req.Range.GetEnd(), int64(2000000000); want != got {
				t.Errorf("unexpected end range -want/+got:\n\t- %d\n\t+ %d", want, got)
			}

			if req.Predicate != nil {
				t.Error("expected predicate to be nil")
			}
			return cursors.NewInt64SliceIterator([]int64{4}), nil
		},
		SupportReadSeriesCardinalityFn: func(ctx context.Context) bool {
			return true
		},
		GetSourceFn: func(orgID, bucketID uint64) proto.Message {
			return &storage.ReadSource{
				BucketID: bucketID,
				OrgID:    orgID,
			}
		},
	}

	provider := influxdb.Provider{
		Reader:       storageflux.NewReader(store),
		BucketLookup: mock.BucketLookup{},
	}

	ctx := query.ContextWithRequest(
		context.Background(),
		&query.Request{
			OrganizationID: orgID,
		},
	)

	reader, err := provider.SeriesCardinalityReaderFor(
		ctx,
		influxdb.Config{
			Bucket: influxdb.NameOrID{
				Name: "my-bucket",
			},
		},
		flux.Bounds{
			Start: flux.Time{
				Absolute: time.Unix(1, 0),
			},
			Stop: flux.Time{
				Absolute: time.Unix(2, 0),
			},
		},
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	want := static.Table{
		static.Ints("_value", 4),
	}

	got := table.Iterator{}
	if err := reader.Read(ctx, func(tbl flux.Table) error {
		cpy, err := execute.CopyTable(tbl)
		if err != nil {
			return err
		}
		got = append(got, cpy)
		return nil
	}, memory.DefaultAllocator); err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	if diff := table.Diff(want, got); err != nil {
		t.Errorf("unexpected output -want/+got:\n%s", diff)
	}
}

func TestProvider_SeriesCardinalityReader_Unsupported(t *testing.T) {
	t.Parallel()

	store := &mock.ReadsStore{
		ReadSeriesCardinalityFn: func(ctx context.Context, req *datatypes.ReadSeriesCardinalityRequest) (cursors.Int64Iterator, error) {
			return nil, &errors.Error{
				Code: errors.EInvalid,
				Msg:  "unexpected read",
			}
		},
		SupportReadSeriesCardinalityFn: func(ctx context.Context) bool {
			return false
		},
	}

	provider := influxdb.Provider{
		Reader:       storageflux.NewReader(store),
		BucketLookup: mock.BucketLookup{},
	}

	ctx := query.ContextWithRequest(
		context.Background(),
		&query.Request{
			OrganizationID: orgID,
		},
	)

	wantErr := &errors.Error{
		Code: errors.EInvalid,
		Msg:  "series cardinality option is not supported",
	}

	_, gotErr := provider.SeriesCardinalityReaderFor(
		ctx,
		influxdb.Config{
			Bucket: influxdb.NameOrID{
				Name: "my-bucket",
			},
		},
		flux.Bounds{
			Start: flux.Time{
				Absolute: time.Unix(1, 0),
			},
			Stop: flux.Time{
				Absolute: time.Unix(2, 0),
			},
		},
		nil,
	)

	require.Equal(t, wantErr, gotErr)
}

func TestProvider_SeriesCardinalityReader_MissingRequestContext(t *testing.T) {
	t.Parallel()

	store := &mock.ReadsStore{
		ReadSeriesCardinalityFn: func(ctx context.Context, req *datatypes.ReadSeriesCardinalityRequest) (cursors.Int64Iterator, error) {
			return nil, &errors.Error{
				Code: errors.EInvalid,
				Msg:  "unexpected read",
			}
		},
		SupportReadSeriesCardinalityFn: func(ctx context.Context) bool {
			return true
		},
	}

	provider := influxdb.Provider{
		Reader:       storageflux.NewReader(store),
		BucketLookup: mock.BucketLookup{},
	}

	wantErr := &errors.Error{
		Code: errors.EInvalid,
		Msg:  "missing request on context",
	}

	_, gotErr := provider.SeriesCardinalityReaderFor(
		context.Background(),
		influxdb.Config{
			Bucket: influxdb.NameOrID{
				Name: "my-bucket",
			},
		},
		flux.Bounds{
			Start: flux.Time{
				Absolute: time.Unix(1, 0),
			},
			Stop: flux.Time{
				Absolute: time.Unix(2, 0),
			},
		},
		nil,
	)

	require.Equal(t, wantErr, gotErr)
}

func TestWriterFor(t *testing.T) {
	t.Parallel()

	auth := influxdb2.Authorization{
		Status: influxdb2.Active,
		Permissions: []influxdb2.Permission{
			{
				Action: influxdb2.WriteAction,
				Resource: influxdb2.Resource{
					Type: influxdb2.BucketsResourceType,
				},
			},
		},
	}

	provider := influxdb.Provider{
		Reader:       storageflux.NewReader(&mock.ReadsStore{}),
		BucketLookup: mock.BucketLookup{},
	}

	conf := influxdb.Config{
		Bucket: influxdb.NameOrID{
			Name: "my-bucket",
		},
	}

	ctx := context.Background()
	req := query.Request{
		OrganizationID: platform.ID(2),
	}
	ctx = query.ContextWithRequest(ctx, &req)
	ctx = context2.SetAuthorizer(ctx, &auth)

	_, gotErr := provider.WriterFor(ctx, conf)

	require.Nil(t, gotErr)
}

func TestWriterFor_Error(t *testing.T) {
	t.Parallel()

	auth := influxdb2.Authorization{
		Status: influxdb2.Active,
		Permissions: []influxdb2.Permission{
			{
				Action: influxdb2.ReadAction,
				Resource: influxdb2.Resource{
					Type: influxdb2.BucketsResourceType,
				},
			},
		},
	}

	provider := influxdb.Provider{
		Reader:       storageflux.NewReader(&mock.ReadsStore{}),
		BucketLookup: mock.BucketLookup{},
	}

	conf := influxdb.Config{
		Bucket: influxdb.NameOrID{
			Name: "my-bucket",
		},
	}

	ctx := context.Background()
	req := query.Request{
		OrganizationID: platform.ID(2),
	}
	ctx = query.ContextWithRequest(ctx, &req)
	ctx = context2.SetAuthorizer(ctx, &auth)

	_, gotErr := provider.WriterFor(ctx, conf)

	wantErr := &errors.Error{
		Code: errors.EForbidden,
		Msg:  "user not authorized to write",
	}

	require.Equal(t, wantErr, gotErr)
}
