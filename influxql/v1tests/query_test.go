package v1tests

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
	icontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/tests"
	"github.com/stretchr/testify/require"
)

// Ensure parameterized queries can be executed
func TestServer_Query_Parameterized(t *testing.T) {
	t.Parallel()
	s := OpenServer(t)
	defer s.Close()

	writes := []string{
		fmt.Sprintf(`cpu,host=foo value=1.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=bar value=1.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-02T01:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	minTime := mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:00Z").UnixNano()
	maxTime := mustParseTime(time.RFC3339Nano, "2000-01-01T02:00:00Z").UnixNano()

	test.addQueries([]*Query{
		{
			name:    "parameterized time",
			params:  url.Values{"db": []string{"db0"}, "params": []string{fmt.Sprintf(`{"0": %d, "1": %d}`, minTime, maxTime)}},
			command: `SELECT value FROM cpu WHERE time >= $0 AND time < $1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T01:00:00Z",1]]}]}]}`,
		},
		{
			name:    "parameterized tag",
			params:  url.Values{"db": []string{"db0"}, "params": []string{`{"0": "foo"}`}},
			command: `SELECT value FROM cpu WHERE host = $0`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T01:00:00Z",1]]}]}]}`,
		},
	}...)

	ctx := context.Background()
	test.Run(ctx, t, s)
}

// Ensure queries are properly chunked
func TestServer_Query_Chunked(t *testing.T) {
	t.Parallel()
	s := OpenServer(t)
	defer s.Close()

	writes := []string{
		fmt.Sprintf(`cpu,host=foo value=1.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=bar value=1.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-02T01:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		{
			name:    "query is chunked",
			params:  url.Values{"db": []string{"db0"}, "chunked": []string{"true"}, "chunk_size": []string{"1"}},
			command: `SELECT value FROM cpu`,
			exp: `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T01:00:00Z",1]],"partial":true}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-02T01:00:00Z",1]]}]}]}`,
		},
		{
			name:    "query is not chunked",
			params:  url.Values{"db": []string{"db0"}, "chunked": []string{"false"}, "chunk_size": []string{"1"}},
			command: `SELECT value FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T01:00:00Z",1],["2000-01-02T01:00:00Z",1]]}]}]}`,
		},
	}...)

	ctx := context.Background()
	test.Run(ctx, t, s)
}

// Ensure a more complex group-by is correct
func TestServer_Query_ComplexGroupby(t *testing.T) {
	t.Parallel()
	s := OpenServer(t)
	defer s.Close()

	r := rand.New(rand.NewSource(1000))
	abc := []string{"a", "b", "c"}
	startDate := time.Date(2021, 5, 10, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2021, 5, 15, 0, 0, 0, 0, time.UTC)
	writes := make([]string,0)
	for date := startDate; date.Before(endDate); date = date.Add(1 * time.Hour) {
		line := fmt.Sprintf(`m0,tenant_id=t%s,env=e%s total_count=%d %d`,
			abc[r.Intn(3)], abc[r.Intn(3)], 10+r.Intn(5), date.UnixNano())
		writes = append(writes, line)
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	minTime := startDate.UnixNano()
	maxTime := endDate.UnixNano()

	test.addQueries([]*Query{
		{
			name:    "parameterized time",
			params:  url.Values{"db": []string{"db0"}, "params": []string{fmt.Sprintf(`{"0": %d, "1": %d}`, minTime, maxTime)}},
			command: `SELECT SUM(ncount) as scount FROM (SELECT NON_NEGATIVE_DIFFERENCE(total_count) as ncount FROM m0 WHERE time >= $0 AND time <= $1 AND tenant_id='tb' GROUP BY env) WHERE time >= $0 AND time <= $1 GROUP BY time(1d)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"m0","columns":["time","scount"],"values":[["2021-05-10T00:00:00Z",10],["2021-05-11T00:00:00Z",5],["2021-05-12T00:00:00Z",3],["2021-05-13T00:00:00Z",7],["2021-05-14T00:00:00Z",4],["2021-05-15T00:00:00Z",null]]}]}]}`,
		},
	}...)

	ctx := context.Background()
	test.Run(ctx, t, s)
}


func TestServer_Query_ShowDatabases(t *testing.T) {
	t.Parallel()
	s := OpenServer(t)
	defer s.MustClose()

	ctx := context.Background()
	ctx = icontext.SetAuthorizer(ctx, tests.MakeAuthorization(s.DefaultOrgID, s.DefaultUserID, influxdb.OperPermissions()))

	// create some buckets and mappings
	buckets := []struct {
		name string
		db   string
		rp   string
	}{
		{"my-bucket", "my-bucket", "autogen"},
		{"telegraf/autogen", "telegraf", "autogen"},
		{"telegraf/1_week", "telegraf", "1_week"},
		{"telegraf/1_month", "telegraf", "1_month"},
	}

	for _, bi := range buckets {
		b := influxdb.Bucket{
			OrgID:           s.DefaultOrgID,
			Type:            influxdb.BucketTypeUser,
			Name:            bi.name,
			RetentionPeriod: 0,
		}
		err := s.Launcher.
			Launcher.
			BucketService().
			CreateBucket(ctx, &b)
		require.NoError(t, err)

		err = s.Launcher.
			DBRPMappingServiceV2().
			Create(ctx, &influxdb.DBRPMappingV2{
				Database:        bi.db,
				RetentionPolicy: bi.rp,
				Default:         true,
				OrganizationID:  s.DefaultOrgID,
				BucketID:        b.ID,
			})
		require.NoError(t, err)
	}

	test := NewEmptyTest()
	test.addQueries(
		&Query{
			name:    "show databases does not return duplicates",
			command: "SHOW DATABASES",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"databases","columns":["name"],"values":[["my-bucket"],["telegraf"]]}]}]}`,
		},
	)

	test.Run(context.Background(), t, s)
}
