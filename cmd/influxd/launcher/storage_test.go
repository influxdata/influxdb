package launcher_test

import (
	"context"
	"fmt"
	"io/ioutil"
	nethttp "net/http"
	"strings"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	"github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"github.com/stretchr/testify/require"
)

func TestStorage_WriteAndQuery(t *testing.T) {
	l := launcher.NewTestLauncher()
	l.RunOrFail(t, ctx)
	defer l.ShutdownOrFail(t, ctx)

	org1 := l.OnBoardOrFail(t, &influxdb.OnboardingRequest{
		User:     "USER-1",
		Password: "PASSWORD-1",
		Org:      "ORG-01",
		Bucket:   "BUCKET",
	})
	org2 := l.OnBoardOrFail(t, &influxdb.OnboardingRequest{
		User:     "USER-2",
		Password: "PASSWORD-1",
		Org:      "ORG-02",
		Bucket:   "BUCKET",
	})

	// Execute single write against the server.
	l.WriteOrFail(t, org1, `m,k=v1 f=100i 946684800000000000`)
	l.WriteOrFail(t, org2, `m,k=v2 f=200i 946684800000000000`)

	qs := `from(bucket:"BUCKET") |> range(start:2000-01-01T00:00:00Z,stop:2000-01-02T00:00:00Z)`

	exp := `,result,table,_start,_stop,_time,_value,_field,_measurement,k` + "\r\n" +
		`,_result,0,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00Z,100,f,m,v1` + "\r\n\r\n"
	if got := l.FluxQueryOrFail(t, org1.Org, org1.Auth.Token, qs); !cmp.Equal(got, exp) {
		t.Errorf("unexpected query results -got/+exp\n%s", cmp.Diff(got, exp))
	}

	exp = `,result,table,_start,_stop,_time,_value,_field,_measurement,k` + "\r\n" +
		`,_result,0,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00Z,200,f,m,v2` + "\r\n\r\n"
	if got := l.FluxQueryOrFail(t, org2.Org, org2.Auth.Token, qs); !cmp.Equal(got, exp) {
		t.Errorf("unexpected query results -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

// Ensure the server will write all points possible with exception of
// - field type conflict
// - field too large
func TestStorage_PartialWrite(t *testing.T) {
	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
	defer l.ShutdownOrFail(t, ctx)

	// Initial write of integer.
	l.WritePointsOrFail(t, `cpu value=1i 946684800000000000`)

	// Write mixed-field types.
	err := l.WritePoints("cpu value=2i 946684800000000001\ncpu value=3 946684800000000002\ncpu value=4i 946684800000000003")
	require.Error(t, err)

	// Write oversized field value.
	err = l.WritePoints(fmt.Sprintf(`cpu str="%s" 946684800000000004`, strings.Repeat("a", tsdb.MaxFieldValueLength+1)))
	require.Error(t, err)

	// Write biggest field value.
	l.WritePointsOrFail(t, fmt.Sprintf(`cpu str="%s" 946684800000000005`, strings.Repeat("a", tsdb.MaxFieldValueLength)))

	// Ensure the valid points were written.
	qs := `from(bucket:"BUCKET") |> range(start:2000-01-01T00:00:00Z,stop:2000-01-02T00:00:00Z) |> keep(columns: ["_time","_field"])`

	exp := `,result,table,_time,_field` + "\r\n" +
		`,_result,0,2000-01-01T00:00:00.000000005Z,str` + "\r\n" + // str=max-length string
		`,_result,1,2000-01-01T00:00:00Z,value` + "\r\n" + // value=1
		`,_result,1,2000-01-01T00:00:00.000000001Z,value` + "\r\n" + // value=2
		`,_result,1,2000-01-01T00:00:00.000000003Z,value` + "\r\n\r\n" // value=4

	buf, err := http.SimpleQuery(l.URL(), qs, l.Org.Name, l.Auth.Token)
	require.NoError(t, err)
	require.Equal(t, exp, string(buf))
}

func TestStorage_DisableMaxFieldValueSize(t *testing.T) {
	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t, func(o *launcher.InfluxdOpts) {
		o.StorageConfig.Data.SkipFieldSizeValidation = true
	})
	defer l.ShutdownOrFail(t, ctx)

	// Write a normally-oversized field value.
	l.WritePointsOrFail(t, fmt.Sprintf(`cpu str="%s" 946684800000000000`, strings.Repeat("a", tsdb.MaxFieldValueLength+1)))

	// Check that the point can be queried.
	qs := `from(bucket:"BUCKET") |> range(start:2000-01-01T00:00:00Z,stop:2000-01-02T00:00:00Z) |> keep(columns: ["_value"])`
	exp := `,result,table,_value` + "\r\n" +
		fmt.Sprintf(`,_result,0,%s`, strings.Repeat("a", tsdb.MaxFieldValueLength+1)) + "\r\n\r\n"

	buf, err := http.SimpleQuery(l.URL(), qs, l.Org.Name, l.Auth.Token)
	require.NoError(t, err)
	require.Equal(t, exp, string(buf))
}

func TestLauncher_WriteAndQuery(t *testing.T) {
	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
	defer l.ShutdownOrFail(t, ctx)

	// Execute single write against the server.
	resp, err := nethttp.DefaultClient.Do(l.MustNewHTTPRequest("POST", fmt.Sprintf("/api/v2/write?org=%s&bucket=%s", l.Org.ID, l.Bucket.ID), `m,k=v f=100i 946684800000000000`))
	if err != nil {
		t.Fatal(err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != nethttp.StatusNoContent {
		t.Fatalf("unexpected status code: %d, body: %s, headers: %v", resp.StatusCode, body, resp.Header)
	}

	// Query server to ensure write persists.
	qs := `from(bucket:"BUCKET") |> range(start:2000-01-01T00:00:00Z,stop:2000-01-02T00:00:00Z)`
	exp := `,result,table,_start,_stop,_time,_value,_field,_measurement,k` + "\r\n" +
		`,_result,0,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00Z,100,f,m,v` + "\r\n\r\n"

	buf, err := http.SimpleQuery(l.URL(), qs, l.Org.Name, l.Auth.Token)
	if err != nil {
		t.Fatalf("unexpected error querying server: %v", err)
	}
	if diff := cmp.Diff(string(buf), exp); diff != "" {
		t.Fatal(diff)
	}
}

func TestLauncher_BucketDelete(t *testing.T) {
	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
	defer l.ShutdownOrFail(t, ctx)

	// Execute single write against the server.
	resp, err := nethttp.DefaultClient.Do(l.MustNewHTTPRequest("POST", fmt.Sprintf("/api/v2/write?org=%s&bucket=%s", l.Org.ID, l.Bucket.ID), `m,k=v f=100i 946684800000000000`))
	if err != nil {
		t.Fatal(err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != nethttp.StatusNoContent {
		t.Fatalf("unexpected status code: %d, body: %s, headers: %v", resp.StatusCode, body, resp.Header)
	}

	// Query server to ensure write persists.
	qs := `from(bucket:"BUCKET") |> range(start:2000-01-01T00:00:00Z,stop:2000-01-02T00:00:00Z)`
	exp := `,result,table,_start,_stop,_time,_value,_field,_measurement,k` + "\r\n" +
		`,_result,0,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00Z,100,f,m,v` + "\r\n\r\n"

	buf, err := http.SimpleQuery(l.URL(), qs, l.Org.Name, l.Auth.Token)
	if err != nil {
		t.Fatalf("unexpected error querying server: %v", err)
	}
	if diff := cmp.Diff(string(buf), exp); diff != "" {
		t.Fatal(diff)
	}

	// Verify the cardinality in the engine.
	engine := l.Launcher.Engine()
	if got, exp := engine.SeriesCardinality(ctx, l.Bucket.ID), int64(1); got != exp {
		t.Fatalf("got %d, exp %d", got, exp)
	}

	// Delete the bucket.
	if resp, err = nethttp.DefaultClient.Do(l.MustNewHTTPRequest("DELETE", fmt.Sprintf("/api/v2/buckets/%s", l.Bucket.ID), "")); err != nil {
		t.Fatal(err)
	}

	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		t.Fatal(err)
	}

	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != nethttp.StatusNoContent {
		t.Fatalf("unexpected status code: %d, body: %s, headers: %v", resp.StatusCode, body, resp.Header)
	}

	// Verify that the data has been removed from the storage engine.
	if got, exp := engine.SeriesCardinality(ctx, l.Bucket.ID), int64(0); got != exp {
		t.Fatalf("after bucket delete got %d, exp %d", got, exp)
	}
}

func TestLauncher_DeleteWithPredicate(t *testing.T) {
	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
	defer l.ShutdownOrFail(t, ctx)

	// Write data to server.
	if resp, err := nethttp.DefaultClient.Do(l.MustNewHTTPRequest("POST", fmt.Sprintf("/api/v2/write?org=%s&bucket=%s", l.Org.ID, l.Bucket.ID),
		"cpu,region=us-east-1 v=1 946684800000000000\n"+
			"cpu,region=us-west-1 v=1 946684800000000000\n"+
			"mem,region=us-west-1 v=1 946684800000000000\n",
	)); err != nil {
		t.Fatal(err)
	} else if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}

	// Execute single write against the server.
	s := http.DeleteService{
		Addr:  l.URL().String(),
		Token: l.Auth.Token,
	}
	if err := s.DeleteBucketRangePredicate(context.Background(), http.DeleteRequest{
		OrgID:     l.Org.ID.String(),
		BucketID:  l.Bucket.ID.String(),
		Start:     "2000-01-01T00:00:00Z",
		Stop:      "2000-01-02T00:00:00Z",
		Predicate: `_measurement="cpu" AND region="us-west-1"`,
	}); err != nil {
		t.Fatal(err)
	}

	// Query server to ensure write persists.
	qs := `from(bucket:"BUCKET") |> range(start:2000-01-01T00:00:00Z,stop:2000-01-02T00:00:00Z)`
	exp := `,result,table,_start,_stop,_time,_value,_field,_measurement,region` + "\r\n" +
		`,_result,0,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00Z,1,v,cpu,us-east-1` + "\r\n" +
		`,_result,1,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00Z,1,v,mem,us-west-1` + "\r\n\r\n"

	buf, err := http.SimpleQuery(l.URL(), qs, l.Org.Name, l.Auth.Token)
	if err != nil {
		t.Fatalf("unexpected error querying server: %v", err)
	} else if diff := cmp.Diff(string(buf), exp); diff != "" {
		t.Fatal(diff)
	}
}

func TestLauncher_FluxCardinality(t *testing.T) {
	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
	defer l.ShutdownOrFail(t, ctx)

	// Write data to server.
	if resp, err := nethttp.DefaultClient.Do(l.MustNewHTTPRequest("POST", fmt.Sprintf("/api/v2/write?org=%s&bucket=%s", l.Org.ID, l.Bucket.ID),
		"cpu,region=us-east-1 v=1 946684800000000000\n"+
			"cpu,region=us-west-1 v=1 946684800000000000\n"+
			"mem,region=us-west-1 v=1 946684800000000000\n"+
			"mem,region=us-south-1 v=2 996684800000000000\n",
	)); err != nil {
		t.Fatal(err)
	} else if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}

	// Specific time values for tests bracketing shards with time ranges
	mc := l.Engine().MetaClient()
	sgs, err := mc.ShardGroupsByTimeRange(l.Bucket.ID.String(), meta.DefaultRetentionPolicyName, time.Unix(0, 946684800000000000), time.Unix(0, 996684800000000000))
	require.NoError(t, err)
	require.Equal(t, 2, len(sgs))

	sg1Start := sgs[0].StartTime
	sg2End := sgs[1].EndTime
	sg2Start := sgs[1].StartTime
	preSg1Start := sg1Start.Add(-1 * time.Minute)

	lastPoint := time.Unix(0, 996684800000000000)
	// a point in the middle of the later shard group, after the data but before
	// the end of the group
	afterLastPoint := lastPoint.Add(1 * time.Minute)
	require.True(t, afterLastPoint.Before(sg2End))
	require.True(t, afterLastPoint.After(sg2Start))

	// similar, but before the data
	beforeLastPoint := lastPoint.Add(-1 * time.Minute)
	require.True(t, beforeLastPoint.Before(sg2End))
	require.True(t, beforeLastPoint.After(sg2Start))

	tests := []struct {
		name  string
		query string
		exp   string
	}{
		{
			name: "boolean literal predicate",
			query: `import "influxdata/influxdb"
			influxdb.cardinality(
				bucket: "BUCKET",
				start: 2000-01-01T00:00:00Z,
				stop: 2000-01-02T00:00:00Z,
				predicate: (r) => true
			)`,
			exp: `,result,table,_value` + "\r\n" +
				`,_result,0,3` + "\r\n\r\n",
		},
		{
			name: "nil predicate",
			query: `import "influxdata/influxdb"
			influxdb.cardinality(
				bucket: "BUCKET",
				start: 2000-01-01T00:00:00Z,
				stop: 2000-01-02T00:00:00Z,
			)`,
			exp: `,result,table,_value` + "\r\n" +
				`,_result,0,3` + "\r\n\r\n",
		},
		{
			name: "nil predicate with large time range",
			query: `import "influxdata/influxdb"
			influxdb.cardinality(
				bucket: "BUCKET",
				start: 1990-01-01T00:00:00Z,
				stop: 2010-01-01T00:00:00Z,
			)`,
			exp: `,result,table,_value` + "\r\n" +
				`,_result,0,4` + "\r\n\r\n",
		},
		{
			name: "single measurement match",
			query: `import "influxdata/influxdb"
			influxdb.cardinality(
				bucket: "BUCKET",
				start: 2000-01-01T00:00:00Z,
				stop: 2000-01-02T00:00:00Z,
				predicate: (r) => r._measurement == "cpu"
			)`,
			exp: `,result,table,_value` + "\r\n" +
				`,_result,0,2` + "\r\n\r\n",
		},
		{
			name: "multiple measurement match",
			query: `import "influxdata/influxdb"
			influxdb.cardinality(
				bucket: "BUCKET",
				start: 2000-01-01T00:00:00Z,
				stop: 2000-01-02T00:00:00Z,
				predicate: (r) => r._measurement == "cpu" or r._measurement == "mem"
			)`,
			exp: `,result,table,_value` + "\r\n" +
				`,_result,0,3` + "\r\n\r\n",
		},
		{
			name: "predicate matches nothing",
			query: `import "influxdata/influxdb"
			influxdb.cardinality(
				bucket: "BUCKET",
				start: 2000-01-01T00:00:00Z,
				stop: 2000-01-02T00:00:00Z,
				predicate: (r) => r._measurement == "cpu" and r._measurement == "mem"
			)`,
			exp: `,result,table,_value` + "\r\n" +
				`,_result,0,0` + "\r\n\r\n",
		},
		{
			name: "time range matches nothing",
			query: `import "influxdata/influxdb"
			influxdb.cardinality(
				bucket: "BUCKET",
				start: 2000-04-01T00:00:00Z,
				stop: 2000-05-02T00:00:00Z,
			)`,
			exp: `,result,table,_value` + "\r\n" +
				`,_result,0,0` + "\r\n\r\n",
		},
		{
			name: "large time range - all shards are within the window",
			query: `import "influxdata/influxdb"
			influxdb.cardinality(
				bucket: "BUCKET",
				start: 1990-01-01T00:00:00Z,
				stop: 2010-01-01T00:00:00Z,
				predicate: (r) => r._measurement == "cpu"
			)`,
			exp: `,result,table,_value` + "\r\n" +
				`,_result,0,2` + "\r\n\r\n",
		},
		{
			name: "start range is inclusive",
			query: fmt.Sprintf(`import "influxdata/influxdb"
			influxdb.cardinality(
				bucket: "BUCKET",
				start: %s,
				stop: 2010-01-01T00:00:00Z,
				predicate: (r) => r._measurement == "mem"
			)`, time.Unix(0, 946684800000000000).Format(time.RFC3339Nano),
			),
			exp: `,result,table,_value` + "\r\n" +
				`,_result,0,2` + "\r\n\r\n",
		},
		{
			name: "stop range is exclusive",
			query: fmt.Sprintf(`import "influxdata/influxdb"
			influxdb.cardinality(
				bucket: "BUCKET",
				start: 1990-01-01T00:00:00Z,
				stop: %s,
				predicate: (r) => r._measurement == "mem"
			)`, lastPoint.Format(time.RFC3339Nano),
			),
			exp: `,result,table,_value` + "\r\n" +
				`,_result,0,1` + "\r\n\r\n",
		},
		{
			name: "one shard is entirely in the time range, other is partially, range includes data in partial shard",
			query: fmt.Sprintf(`import "influxdata/influxdb"
			influxdb.cardinality(
				bucket: "BUCKET",
				start: %s,
				stop: %s,
				predicate: (r) => r._measurement == "mem"
			)`, preSg1Start.Format(time.RFC3339Nano), afterLastPoint.Format(time.RFC3339Nano),
			),
			exp: `,result,table,_value` + "\r\n" +
				`,_result,0,2` + "\r\n\r\n",
		},
		{
			name: "one shard is entirely in the time range, other is partially, range does not include data in partial shard",
			query: fmt.Sprintf(`import "influxdata/influxdb"
			influxdb.cardinality(
				bucket: "BUCKET",
				start: %s,
				stop: %s,
				predicate: (r) => r._measurement == "mem"
			)`, preSg1Start.Format(time.RFC3339Nano), beforeLastPoint.Format(time.RFC3339Nano),
			),
			exp: `,result,table,_value` + "\r\n" +
				`,_result,0,1` + "\r\n\r\n",
		},
	}

	for _, tt := range tests {
		buf, err := http.SimpleQuery(l.URL(), tt.query, l.Org.Name, l.Auth.Token)
		if err != nil {
			t.Fatalf("unexpected error querying server: %v", err)
		} else if diff := cmp.Diff(string(buf), tt.exp); diff != "" {
			t.Fatal(diff)
		}
	}
}

func TestLauncher_UpdateRetentionPolicy(t *testing.T) {
	durPtr := func(d time.Duration) *time.Duration {
		return &d
	}

	testCases := []struct {
		name            string
		initRp          time.Duration
		initSgd         time.Duration
		derivedSgd      *time.Duration
		newRp           *time.Duration
		newSgd          *time.Duration
		expectInitErr   bool
		expectUpdateErr bool
	}{
		{
			name:       "infinite to 1w",
			derivedSgd: durPtr(humanize.Week),
			newRp:      durPtr(humanize.Week),
		},
		{
			name:       "1w to 1d",
			initRp:     humanize.Week,
			derivedSgd: durPtr(humanize.Day),
			newRp:      durPtr(humanize.Day),
		},
		{
			name:       "1d to 1h",
			initRp:     humanize.Day,
			derivedSgd: durPtr(time.Hour),
			newRp:      durPtr(time.Hour),
		},
		{
			name:       "infinite, update shard duration",
			initSgd:    humanize.Month,
			derivedSgd: durPtr(humanize.Month),
			newSgd:     durPtr(humanize.Week),
		},
		{
			name:    "1w, update shard duration",
			initRp:  humanize.Week,
			initSgd: humanize.Week,
			newSgd:  durPtr(time.Hour),
		},
		{
			name:    "1d, update shard duration",
			initRp:  humanize.Day,
			initSgd: 3 * time.Hour,
			newSgd:  durPtr(1*time.Hour + 30*time.Minute),
		},
		{
			name:       "infinite, update both retention and shard duration",
			derivedSgd: durPtr(humanize.Week),
			newRp:      durPtr(time.Hour),
			newSgd:     durPtr(time.Hour),
		},
		{
			name:          "init shard duration larger than RP",
			initRp:        time.Hour,
			initSgd:       humanize.Day,
			expectInitErr: true,
		},
		{
			name:            "updated shard duration larger than RP",
			initRp:          humanize.Day,
			initSgd:         time.Hour,
			newSgd:          durPtr(humanize.Week),
			expectUpdateErr: true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			l := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
			defer l.ShutdownOrFail(t, ctx)
			bucketService := l.BucketService(t)

			bucket := &influxdb.Bucket{
				OrgID:              l.Org.ID,
				RetentionPeriod:    tc.initRp,
				ShardGroupDuration: tc.initSgd,
			}
			err := bucketService.CreateBucket(ctx, bucket)
			if tc.expectInitErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			defer bucketService.DeleteBucket(ctx, bucket.ID)

			bucket, err = bucketService.FindBucketByID(ctx, bucket.ID)
			require.NoError(t, err)

			expectedSgd := tc.initSgd
			if tc.derivedSgd != nil {
				expectedSgd = *tc.derivedSgd
			}
			require.Equal(t, tc.initRp, bucket.RetentionPeriod)
			require.Equal(t, expectedSgd, bucket.ShardGroupDuration)

			bucket, err = bucketService.UpdateBucket(ctx, bucket.ID, influxdb.BucketUpdate{
				RetentionPeriod:    tc.newRp,
				ShardGroupDuration: tc.newSgd,
			})
			if tc.expectUpdateErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			bucket, err = bucketService.FindBucketByID(ctx, bucket.ID)
			require.NoError(t, err)

			expectedRp := tc.initRp
			if tc.newRp != nil {
				expectedRp = *tc.newRp
			}
			if tc.newSgd != nil {
				expectedSgd = *tc.newSgd
			}
			require.Equal(t, expectedRp, bucket.RetentionPeriod)
			require.Equal(t, expectedSgd, bucket.ShardGroupDuration)
		})
	}
}

func TestLauncher_OverlappingShards(t *testing.T) {
	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
	defer l.ShutdownOrFail(t, ctx)

	bkt := influxdb.Bucket{Name: "test", ShardGroupDuration: time.Hour, OrgID: l.Org.ID}
	require.NoError(t, l.BucketService(t).CreateBucket(ctx, &bkt))

	req := l.MustNewHTTPRequest("POST", fmt.Sprintf("/api/v2/write?org=%s&bucket=%s", l.Org.ID, bkt.ID),
		"m,s=0 n=0 1626416520000000000\nm,s=0 n=1 1626420120000000000\n")
	resp, err := nethttp.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	newDur := humanize.Day
	_, err = l.BucketService(t).UpdateBucket(ctx, bkt.ID, influxdb.BucketUpdate{ShardGroupDuration: &newDur})
	require.NoError(t, err)

	req = l.MustNewHTTPRequest("POST", fmt.Sprintf("/api/v2/write?org=%s&bucket=%s", l.Org.ID, bkt.ID),
		// NOTE: The 3rd point's timestamp is chronologically earlier than the other two points, but it
		// must come after the others in the request to trigger the overlapping-shard bug. If it comes
		// first in the request, the bug is avoided because:
		//   1. The point-writer sees there is no shard for the earlier point, and creates a new 24h shard-group
		//   2. The new 24 group covers the timestamps of the remaining 2 points, so the writer doesn't bother looking
		//      for existing shards that also cover the timestamp
		//   3. With only 1 shard mapped to the 3 points, there is no overlap to trigger the bug
		"m,s=0 n=0 1626416520000000000\nm,s=0 n=1 1626420120000000000\nm,s=1 n=1 1626412920000000000\n")
	resp, err = nethttp.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	query := `from(bucket:"test") |> range(start:2000-01-01T00:00:00Z,stop:2050-01-01T00:00:00Z)` +
		` |> drop(columns:["_start","_stop"])`
	exp := `,result,table,_time,_value,_field,_measurement,s` + "\r\n" +
		`,_result,0,2021-07-16T06:22:00Z,0,n,m,0` + "\r\n" +
		`,_result,0,2021-07-16T07:22:00Z,1,n,m,0` + "\r\n" +
		`,_result,1,2021-07-16T05:22:00Z,1,n,m,1` + "\r\n\r\n"

	buf, err := http.SimpleQuery(l.URL(), query, l.Org.Name, l.Auth.Token)
	require.NoError(t, err)
	require.Equal(t, exp, string(buf))
}
