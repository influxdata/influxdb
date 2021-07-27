package launcher_test

import (
	"context"
	"fmt"
	"io/ioutil"
	nethttp "net/http"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	"github.com/influxdata/influxdb/v2/http"
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
