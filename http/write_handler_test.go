package http

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/http/metric"
	httpmock "github.com/influxdata/influxdb/v2/http/mock"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/mock"
	influxtesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestWriteService_WriteTo(t *testing.T) {
	type args struct {
		org      string
		orgId    platform.ID
		bucket   string
		bucketId platform.ID
		r        io.Reader
	}

	orgId := platform.ID(1)
	org := "org"
	bucketId := platform.ID(2)
	bucket := "bucket"

	tests := []struct {
		name        string
		args        args
		status      int
		want        string
		wantFilters influxdb.BucketFilter
		wantErr     bool
	}{
		{
			name: "write with org and bucket IDs",
			args: args{
				orgId:    orgId,
				bucketId: bucketId,
				r:        strings.NewReader("m,t1=v1 f1=2"),
			},
			status: http.StatusNoContent,
			want:   "m,t1=v1 f1=2",
			wantFilters: influxdb.BucketFilter{
				ID:             &bucketId,
				OrganizationID: &orgId,
			},
		},
		{
			name: "write with org ID and bucket name",
			args: args{
				orgId:  orgId,
				bucket: bucket,
				r:      strings.NewReader("m,t1=v1 f1=2"),
			},
			status: http.StatusNoContent,
			want:   "m,t1=v1 f1=2",
			wantFilters: influxdb.BucketFilter{
				Name:           &bucket,
				OrganizationID: &orgId,
			},
		},
		{
			name: "write with org name and bucket ID",
			args: args{
				org:      org,
				bucketId: bucketId,
				r:        strings.NewReader("m,t1=v1 f1=2"),
			},
			status: http.StatusNoContent,
			want:   "m,t1=v1 f1=2",
			wantFilters: influxdb.BucketFilter{
				ID:  &bucketId,
				Org: &org,
			},
		},
		{
			name: "write with org and bucket names",
			args: args{
				org:    org,
				bucket: bucket,
				r:      strings.NewReader("m,t1=v1 f1=2"),
			},
			status: http.StatusNoContent,
			want:   "m,t1=v1 f1=2",
			wantFilters: influxdb.BucketFilter{
				Name: &bucket,
				Org:  &org,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var org, bucket *string
			var orgId, bucketId *platform.ID
			var lp []byte
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				orgStr := r.URL.Query().Get("org")
				bucketStr := r.URL.Query().Get("bucket")
				var err error
				if orgId, err = platform.IDFromString(orgStr); err != nil {
					org = &orgStr
				}
				if bucketId, err = platform.IDFromString(bucketStr); err != nil {
					bucket = &bucketStr
				}
				defer r.Body.Close()
				in, _ := gzip.NewReader(r.Body)
				defer in.Close()
				lp, _ = io.ReadAll(in)
				w.WriteHeader(tt.status)
			}))
			s := &WriteService{
				Addr: ts.URL,
			}
			err := s.WriteTo(
				context.Background(),
				influxdb.BucketFilter{ID: &tt.args.bucketId, Name: &tt.args.bucket, OrganizationID: &tt.args.orgId, Org: &tt.args.org},
				tt.args.r,
			)
			require.Equalf(t, err != nil, tt.wantErr, "error didn't match expectations: %v", err)
			require.Equal(t, tt.wantFilters.OrganizationID, orgId)
			require.Equal(t, tt.wantFilters.Org, org)
			require.Equal(t, tt.wantFilters.ID, bucketId)
			require.Equal(t, tt.wantFilters.Name, bucket)
			require.Equal(t, tt.want, string(lp))
		})
	}
}

func TestWriteHandler_handleWrite(t *testing.T) {
	// state is the internal state of org and bucket services
	type state struct {
		org       *influxdb.Organization // org to return in org service
		orgErr    error                  // err to return in org service
		bucket    *influxdb.Bucket       // bucket to return in bucket service
		bucketErr error                  // err to return in bucket service
		writeErr  error                  // err to return from the points writer
		opts      []WriteHandlerOption   // write handle configured options
	}

	// want is the expected output of the HTTP endpoint
	type wants struct {
		body string
		code int
	}

	// request is sent to the HTTP endpoint
	type request struct {
		auth   influxdb.Authorizer
		org    string
		bucket string
		body   string
	}

	tests := []struct {
		name    string
		request request
		state   state
		wants   wants
	}{
		{
			name: "simple body is accepted",
			request: request{
				org:    "043e0780ee2b1000",
				bucket: "04504b356e23b000",
				body:   "m1,t1=v1 f1=1",
				auth:   bucketWritePermission("043e0780ee2b1000", "04504b356e23b000"),
			},
			state: state{
				org:    testOrg("043e0780ee2b1000"),
				bucket: testBucket("043e0780ee2b1000", "04504b356e23b000"),
			},
			wants: wants{
				code: 204,
			},
		},
		{
			name: "partial write error is unprocessable",
			request: request{
				org:    "043e0780ee2b1000",
				bucket: "04504b356e23b000",
				body:   "m1,t1=v1 f1=1",
				auth:   bucketWritePermission("043e0780ee2b1000", "04504b356e23b000"),
			},
			state: state{
				org:      testOrg("043e0780ee2b1000"),
				bucket:   testBucket("043e0780ee2b1000", "04504b356e23b000"),
				writeErr: tsdb.PartialWriteError{Reason: "bad points", Dropped: 1},
			},
			wants: wants{
				code: 422,
				body: `{"code":"unprocessable entity","message":"failure writing points to database: partial write: bad points dropped=1"}`,
			},
		},
		{
			name: "points writer error is an internal error",
			request: request{
				org:    "043e0780ee2b1000",
				bucket: "04504b356e23b000",
				body:   "m1,t1=v1 f1=1",
				auth:   bucketWritePermission("043e0780ee2b1000", "04504b356e23b000"),
			},
			state: state{
				org:      testOrg("043e0780ee2b1000"),
				bucket:   testBucket("043e0780ee2b1000", "04504b356e23b000"),
				writeErr: fmt.Errorf("error"),
			},
			wants: wants{
				code: 500,
				body: `{"code":"internal error","message":"unexpected error writing points to database: error"}`,
			},
		},
		{
			name: "empty request body is ok",
			request: request{
				org:    "043e0780ee2b1000",
				bucket: "04504b356e23b000",
				auth:   bucketWritePermission("043e0780ee2b1000", "04504b356e23b000"),
			},
			state: state{
				org:    testOrg("043e0780ee2b1000"),
				bucket: testBucket("043e0780ee2b1000", "04504b356e23b000"),
			},
			wants: wants{
				code: 204,
			},
		},
		{
			name: "org error returns 404 error",
			request: request{
				org:    "043e0780ee2b1000",
				bucket: "04504b356e23b000",
				body:   "m1,t1=v1 f1=1",
				auth:   bucketWritePermission("043e0780ee2b1000", "04504b356e23b000"),
			},
			state: state{
				orgErr: &errors.Error{Code: errors.ENotFound, Msg: "not found"},
			},
			wants: wants{
				code: 404,
				body: `{"code":"not found","message":"not found"}`,
			},
		},
		{
			name: "missing bucket returns 404",
			request: request{
				org:    "043e0780ee2b1000",
				bucket: "",
				body:   "m1,t1=v1 f1=1",
				auth:   bucketWritePermission("043e0780ee2b1000", "04504b356e23b000"),
			},
			state: state{
				org:    testOrg("043e0780ee2b1000"),
				bucket: testBucket("043e0780ee2b1000", "04504b356e23b000"),
			},
			wants: wants{
				code: 404,
				body: `{"code":"not found","message":"bucket not found"}`,
			},
		},
		{
			name: "bucket error returns 404 error",
			request: request{
				org:    "043e0780ee2b1000",
				bucket: "04504b356e23b000",
				body:   "m1,t1=v1 f1=1",
				auth:   bucketWritePermission("043e0780ee2b1000", "04504b356e23b000"),
			},
			state: state{
				org:       testOrg("043e0780ee2b1000"),
				bucketErr: &errors.Error{Code: errors.ENotFound, Msg: "not found"},
			},
			wants: wants{
				code: 404,
				body: `{"code":"not found","message":"not found"}`,
			},
		},
		{
			name: "500 when bucket service returns internal error",
			request: request{
				org:    "043e0780ee2b1000",
				bucket: "04504b356e23b000",
				auth:   bucketWritePermission("043e0780ee2b1000", "04504b356e23b000"),
			},
			state: state{
				org:       testOrg("043e0780ee2b1000"),
				bucketErr: &errors.Error{Code: errors.EInternal, Msg: "internal error"},
			},
			wants: wants{
				code: 500,
				body: `{"code":"internal error","message":"internal error"}`,
			},
		},
		{
			name: "invalid line protocol returns 400",
			request: request{
				org:    "043e0780ee2b1000",
				bucket: "04504b356e23b000",
				auth:   bucketWritePermission("043e0780ee2b1000", "04504b356e23b000"),
				body:   "invalid",
			},
			state: state{
				org:    testOrg("043e0780ee2b1000"),
				bucket: testBucket("043e0780ee2b1000", "04504b356e23b000"),
			},
			wants: wants{
				code: 400,
				body: `{"code":"invalid","message":"unable to parse 'invalid': missing fields"}`,
			},
		},
		{
			name: "forbidden to write with insufficient permission",
			request: request{
				org:    "043e0780ee2b1000",
				bucket: "04504b356e23b000",
				body:   "m1,t1=v1 f1=1",
				auth:   bucketWritePermission("043e0780ee2b1000", "000000000000000a"),
			},
			state: state{
				org:    testOrg("043e0780ee2b1000"),
				bucket: testBucket("043e0780ee2b1000", "04504b356e23b000"),
			},
			wants: wants{
				code: 403,
				body: `{"code":"forbidden","message":"insufficient permissions for write"}`,
			},
		},
		{
			// authorization extraction happens in a different middleware.
			name: "no authorizer is an internal error",
			request: request{
				org:    "043e0780ee2b1000",
				bucket: "04504b356e23b000",
			},
			state: state{
				org:    testOrg("043e0780ee2b1000"),
				bucket: testBucket("043e0780ee2b1000", "04504b356e23b000"),
			},
			wants: wants{
				code: 500,
				body: `{"code":"internal error","message":"authorizer not found on context"}`,
			},
		},
		{
			name: "large requests rejected",
			request: request{
				org:    "043e0780ee2b1000",
				bucket: "04504b356e23b000",
				body:   "m1,t1=v1 f1=1",
				auth:   bucketWritePermission("043e0780ee2b1000", "04504b356e23b000"),
			},
			state: state{
				org:    testOrg("043e0780ee2b1000"),
				bucket: testBucket("043e0780ee2b1000", "04504b356e23b000"),
				opts:   []WriteHandlerOption{WithMaxBatchSizeBytes(5)},
			},
			wants: wants{
				code: 413,
				body: `{"code":"request too large","message":"unable to read data: points batch is too large"}`,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orgs := mock.NewOrganizationService()
			orgs.FindOrganizationF = func(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
				return tt.state.org, tt.state.orgErr
			}
			buckets := mock.NewBucketService()
			buckets.FindBucketFn = func(context.Context, influxdb.BucketFilter) (*influxdb.Bucket, error) {
				return tt.state.bucket, tt.state.bucketErr
			}

			b := &APIBackend{
				HTTPErrorHandler:    kithttp.NewErrorHandler(zaptest.NewLogger(t)),
				Logger:              zaptest.NewLogger(t),
				OrganizationService: orgs,
				BucketService:       buckets,
				PointsWriter:        &mock.PointsWriter{Err: tt.state.writeErr},
				WriteEventRecorder:  &metric.NopEventRecorder{},
			}
			writeHandler := NewWriteHandler(zaptest.NewLogger(t), NewWriteBackend(zaptest.NewLogger(t), b), tt.state.opts...)
			handler := httpmock.NewAuthMiddlewareHandler(writeHandler, tt.request.auth)

			r := httptest.NewRequest(
				"POST",
				"http://localhost:8086/api/v2/write",
				strings.NewReader(tt.request.body),
			)

			params := r.URL.Query()
			params.Set("org", tt.request.org)
			params.Set("bucket", tt.request.bucket)
			r.URL.RawQuery = params.Encode()

			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if got, want := w.Code, tt.wants.code; got != want {
				t.Errorf("unexpected status code: got %d want %d", got, want)
			}

			if got, want := w.Body.String(), tt.wants.body; got != want {
				t.Errorf("unexpected body: got %s want %s", got, want)
			}
		})
	}
}

func bucketWritePermission(org, bucket string) *influxdb.Authorization {
	oid := influxtesting.MustIDBase16(org)
	bid := influxtesting.MustIDBase16(bucket)
	return &influxdb.Authorization{
		OrgID:  oid,
		Status: influxdb.Active,
		Permissions: []influxdb.Permission{
			{
				Action: influxdb.WriteAction,
				Resource: influxdb.Resource{
					Type:  influxdb.BucketsResourceType,
					OrgID: &oid,
					ID:    &bid,
				},
			},
		},
	}
}

func testOrg(org string) *influxdb.Organization {
	oid := influxtesting.MustIDBase16(org)
	return &influxdb.Organization{
		ID: oid,
	}
}

func testBucket(org, bucket string) *influxdb.Bucket {
	oid := influxtesting.MustIDBase16(org)
	bid := influxtesting.MustIDBase16(bucket)

	return &influxdb.Bucket{
		ID:    bid,
		OrgID: oid,
	}
}
