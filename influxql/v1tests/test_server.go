package v1tests

import (
	"context"
	"strings"
	"testing"

	"github.com/influxdata/influxdb/v2"
	icontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/tests"
	"github.com/influxdata/influxdb/v2/tests/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestServer struct {
	db   string
	rp   string
	p    *tests.Pipeline
	fx   pipeline.BaseFixture
	auth *influxdb.Authorization
}

func NewTestServer(ctx context.Context, t *testing.T, db, rp string, writes ...string) *TestServer {
	require.Greater(t, len(writes), 0)

	p := OpenServer(t)
	t.Cleanup(func() {
		_ = p.Close()
	})

	fx := pipeline.NewBaseFixture(t, p.Pipeline, p.DefaultOrgID, p.DefaultBucketID)

	// write test data
	err := fx.Admin.WriteTo(ctx, influxdb.BucketFilter{ID: &p.DefaultBucketID, OrganizationID: &p.DefaultOrgID}, strings.NewReader(strings.Join(writes, "\n")))
	require.NoError(t, err)

	p.Flush()

	writeOrg, err := influxdb.NewPermissionAtID(p.DefaultOrgID, influxdb.WriteAction, influxdb.OrgsResourceType, p.DefaultOrgID)
	require.NoError(t, err)

	bucketWritePerm, err := influxdb.NewPermissionAtID(p.DefaultBucketID, influxdb.WriteAction, influxdb.BucketsResourceType, p.DefaultOrgID)
	require.NoError(t, err)

	bucketReadPerm, err := influxdb.NewPermissionAtID(p.DefaultBucketID, influxdb.ReadAction, influxdb.BucketsResourceType, p.DefaultOrgID)
	require.NoError(t, err)

	auth := tests.MakeAuthorization(p.DefaultOrgID, p.DefaultUserID, []influxdb.Permission{*writeOrg, *bucketWritePerm, *bucketReadPerm})
	ctx = icontext.SetAuthorizer(ctx, auth)
	err = p.Launcher.
		DBRPMappingService().
		Create(ctx, &influxdb.DBRPMapping{
			Database:        db,
			RetentionPolicy: rp,
			Default:         true,
			OrganizationID:  p.DefaultOrgID,
			BucketID:        p.DefaultBucketID,
		})
	require.NoError(t, err)

	return &TestServer{
		p:    p.Pipeline,
		db:   db,
		rp:   rp,
		fx:   fx,
		auth: auth,
	}
}

func (qr *TestServer) Execute(ctx context.Context, t *testing.T, query Query) {
	t.Helper()
	ctx = icontext.SetAuthorizer(ctx, qr.auth)
	if query.skip != "" {
		t.Skipf("SKIP:: %s", query.skip)
	}
	err := query.Execute(ctx, t, qr.db, qr.fx.Admin)
	assert.NoError(t, err)
	assert.Equal(t, query.exp, query.got,
		"%s: unexpected results\nquery:  %s\nparams:  %v\nexp:    %s\nactual: %s\n",
		query.name, query.command, query.params, query.exp, query.got)
}
