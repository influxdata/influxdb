package v1tests

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
	icontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/tests"
	"github.com/influxdata/influxdb/v2/tests/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

func OpenServer(t *testing.T, extra ...tests.PipelineOption) *tests.DefaultPipeline {
	t.Helper()

	defaults := []tests.PipelineOption{
		tests.WithLogger(zaptest.NewLogger(t, zaptest.Level(zapcore.ErrorLevel))),
	}

	p := tests.NewDefaultPipeline(t, append(defaults, extra...)...)
	p.MustOpen()
	return p
}

type Query struct {
	name       string
	command    string
	params     url.Values
	exp, got   string
	skip       string
	skipOthers bool // set to true to only run this test
	repeat     int
	once       bool
}

// Execute runs the command and returns an err if it fails
func (q *Query) Execute(ctx context.Context, t *testing.T, db string, c *tests.Client) (err error) {
	t.Helper()

	params := [][2]string{{"q", q.command}}
	if qdb := q.params.Get("db"); len(qdb) > 0 {
		params = append(params, [2]string{"db", qdb})
	}

	if epoch := q.params.Get("epoch"); len(epoch) > 0 {
		params = append(params, [2]string{"epoch", epoch})
	}

	if parameters := q.params.Get("params"); len(parameters) > 0 {
		params = append(params, [2]string{"params", parameters})
	}

	if chunked := q.params.Get("chunked"); len(chunked) > 0 {
		params = append(params, [2]string{"chunked", chunked})
	}

	if chunkSize := q.params.Get("chunk_size"); len(chunkSize) > 0 {
		params = append(params, [2]string{"chunk_size", chunkSize})
	}

	err = c.Client.Get("/query").
		QueryParams(params...).
		Header("Accept", "application/json").
		RespFn(func(resp *http.Response) error {
			b, err := ioutil.ReadAll(resp.Body)
			q.got = strings.TrimSpace(string(b))
			return err
		}).
		Do(ctx)

	return
}

type Write struct {
	data string
}

type Writes []*Write

type Test struct {
	orgID    influxdb.ID
	bucketID influxdb.ID
	db       string
	rp       string
	writes   Writes
	queries  []*Query
}

func NewTest(db, rp string) Test {
	return Test{
		db: db,
		rp: rp,
	}
}

func (qt *Test) Run(ctx context.Context, t *testing.T, p *tests.DefaultPipeline) {
	t.Helper()
	fx, auth := qt.init(ctx, t, p)
	ctx = icontext.SetAuthorizer(ctx, auth)

	skipOthers := false
	for _, query := range qt.queries {
		skipOthers = skipOthers || query.skipOthers
	}

	var queries []*Query
	if skipOthers {
		queries = make([]*Query, 0, len(qt.queries))
		for _, query := range qt.queries {
			if query.skipOthers {
				queries = append(queries, query)
			}
		}
	} else {
		queries = qt.queries
	}

	for _, query := range queries {
		t.Run(query.name, func(t *testing.T) {
			if query.skip != "" {
				t.Skipf("SKIP:: %s", query.skip)
			}
			err := query.Execute(ctx, t, qt.db, fx.Admin)
			assert.NoError(t, err)
			assert.Equal(t, query.exp, query.got,
				"%s: unexpected results\nquery:  %s\nparams:  %v\nexp:    %s\nactual: %s\n",
				query.name, query.command, query.params, query.exp, query.got)
		})
	}
}

func (qt *Test) addQueries(q ...*Query) {
	qt.queries = append(qt.queries, q...)
}

func (qt *Test) init(ctx context.Context, t *testing.T, p *tests.DefaultPipeline) (fx pipeline.BaseFixture, auth *influxdb.Authorization) {
	t.Helper()

	require.Greater(t, len(qt.writes), 0)

	qt.orgID = p.DefaultOrgID
	qt.bucketID = p.DefaultBucketID

	fx = pipeline.NewBaseFixture(t, p.Pipeline, qt.orgID, qt.bucketID)

	qt.writeTestData(ctx, t, fx.Admin)
	p.Flush()

	writeOrg, err := influxdb.NewPermissionAtID(qt.orgID, influxdb.WriteAction, influxdb.OrgsResourceType, qt.orgID)
	require.NoError(t, err)

	bucketWritePerm, err := influxdb.NewPermissionAtID(qt.bucketID, influxdb.WriteAction, influxdb.BucketsResourceType, qt.orgID)
	require.NoError(t, err)

	bucketReadPerm, err := influxdb.NewPermissionAtID(qt.bucketID, influxdb.ReadAction, influxdb.BucketsResourceType, qt.orgID)
	require.NoError(t, err)

	auth = tests.MakeAuthorization(qt.orgID, p.DefaultUserID, []influxdb.Permission{*writeOrg, *bucketWritePerm, *bucketReadPerm})
	ctx = icontext.SetAuthorizer(ctx, auth)
	err = p.Launcher.
		DBRPMappingServiceV2().
		Create(ctx, &influxdb.DBRPMappingV2{
			Database:        qt.db,
			RetentionPolicy: qt.rp,
			Default:         true,
			OrganizationID:  qt.orgID,
			BucketID:        qt.bucketID,
		})
	require.NoError(t, err)

	return
}

func (qt *Test) writeTestData(ctx context.Context, t *testing.T, c *tests.Client) {
	t.Helper()
	for _, w := range qt.writes {
		err := c.Write(ctx, qt.orgID, qt.bucketID, strings.NewReader(w.data))
		require.NoError(t, err)
	}
}

func maxInt64() string {
	maxInt64, _ := json.Marshal(^int64(0))
	return string(maxInt64)
}

func now() time.Time {
	return time.Now().UTC()
}

func yesterday() time.Time {
	return now().Add(-1 * time.Hour * 24)
}

func mustParseTime(layout, value string) time.Time {
	tm, err := time.Parse(layout, value)
	if err != nil {
		panic(err)
	}
	return tm
}

func mustParseLocation(tzname string) *time.Location {
	loc, err := time.LoadLocation(tzname)
	if err != nil {
		panic(err)
	}
	return loc
}

var LosAngeles = mustParseLocation("America/Los_Angeles")
