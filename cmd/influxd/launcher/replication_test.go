package launcher_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"math/rand"
	nethttp "net/http"
	"net/http/httptest"
	"net/http/httputil"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influx-cli/v2/api"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	"github.com/stretchr/testify/require"
)

func TestValidateReplication_Valid(t *testing.T) {
	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
	defer l.ShutdownOrFail(t, ctx)
	client := l.APIClient(t)

	// Create a "remote" connection to the launcher from itself.
	remote, err := client.RemoteConnectionsApi.PostRemoteConnection(ctx).
		RemoteConnectionCreationRequest(api.RemoteConnectionCreationRequest{
			Name:             "self",
			OrgID:            l.Org.ID.String(),
			RemoteURL:        l.URL().String(),
			RemoteAPIToken:   l.Auth.Token,
			RemoteOrgID:      api.PtrString(l.Org.ID.String()),
			AllowInsecureTLS: false,
		}).Execute()
	require.NoError(t, err)

	// Validate the replication before creating it.
	createReq := api.ReplicationCreationRequest{
		Name:              "test",
		OrgID:             l.Org.ID.String(),
		RemoteID:          remote.Id,
		LocalBucketID:     l.Bucket.ID.String(),
		RemoteBucketID:    api.PtrString(l.Bucket.ID.String()),
		MaxQueueSizeBytes: influxdb.DefaultReplicationMaxQueueSizeBytes,
	}
	_, err = client.ReplicationsApi.PostReplication(ctx).ReplicationCreationRequest(createReq).Validate(true).Execute()
	require.NoError(t, err)

	// Create the replication.
	replication, err := client.ReplicationsApi.PostReplication(ctx).ReplicationCreationRequest(createReq).Execute()
	require.NoError(t, err)

	// Ensure the replication is marked as valid.
	require.NoError(t, client.ReplicationsApi.PostValidateReplicationByID(ctx, replication.Id).Execute())

	// Create a new auth token that can only write to the bucket.
	auth := influxdb.Authorization{
		Status: "active",
		OrgID:  l.Org.ID,
		UserID: l.User.ID,
		Permissions: []influxdb.Permission{{
			Action: "write",
			Resource: influxdb.Resource{
				Type:  influxdb.BucketsResourceType,
				ID:    &l.Bucket.ID,
				OrgID: &l.Org.ID,
			},
		}},
		CRUDLog: influxdb.CRUDLog{},
	}
	require.NoError(t, l.AuthorizationService(t).CreateAuthorization(ctx, &auth))

	// Update the remote to use the new token.
	_, err = client.RemoteConnectionsApi.PatchRemoteConnectionByID(ctx, remote.Id).
		RemoteConnenctionUpdateRequest(api.RemoteConnenctionUpdateRequest{RemoteAPIToken: &auth.Token}).
		Execute()
	require.NoError(t, err)

	// Ensure the replication is still valid.
	require.NoError(t, client.ReplicationsApi.PostValidateReplicationByID(ctx, replication.Id).Execute())
}

func TestValidateReplication_Invalid(t *testing.T) {
	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
	defer l.ShutdownOrFail(t, ctx)
	client := l.APIClient(t)

	// Create a "remote" connection to the launcher from itself,
	// but with a bad auth token.
	remote, err := client.RemoteConnectionsApi.PostRemoteConnection(ctx).
		RemoteConnectionCreationRequest(api.RemoteConnectionCreationRequest{
			Name:             "self",
			OrgID:            l.Org.ID.String(),
			RemoteURL:        l.URL().String(),
			RemoteAPIToken:   "foo",
			RemoteOrgID:      api.PtrString(l.Org.ID.String()),
			AllowInsecureTLS: false,
		}).Execute()
	require.NoError(t, err)

	// Validate the replication before creating it. This should fail because of the bad
	// auth token in the linked remote.
	createReq := api.ReplicationCreationRequest{
		Name:              "test",
		OrgID:             l.Org.ID.String(),
		RemoteID:          remote.Id,
		LocalBucketID:     l.Bucket.ID.String(),
		RemoteBucketID:    api.PtrString(l.Bucket.ID.String()),
		MaxQueueSizeBytes: influxdb.DefaultReplicationMaxQueueSizeBytes,
	}
	_, err = client.ReplicationsApi.PostReplication(ctx).ReplicationCreationRequest(createReq).Validate(true).Execute()
	require.Error(t, err)

	// Create the replication even though it failed validation.
	replication, err := client.ReplicationsApi.PostReplication(ctx).ReplicationCreationRequest(createReq).Execute()
	require.NoError(t, err)

	// Ensure the replication is marked as invalid.
	require.Error(t, client.ReplicationsApi.PostValidateReplicationByID(ctx, replication.Id).Execute())

	// Create a new auth token that can only write to the bucket.
	auth := influxdb.Authorization{
		Status: "active",
		OrgID:  l.Org.ID,
		UserID: l.User.ID,
		Permissions: []influxdb.Permission{{
			Action: "write",
			Resource: influxdb.Resource{
				Type:  influxdb.BucketsResourceType,
				ID:    &l.Bucket.ID,
				OrgID: &l.Org.ID,
			},
		}},
		CRUDLog: influxdb.CRUDLog{},
	}
	require.NoError(t, l.AuthorizationService(t).CreateAuthorization(ctx, &auth))

	// Update the remote to use the new token.
	_, err = client.RemoteConnectionsApi.PatchRemoteConnectionByID(ctx, remote.Id).
		RemoteConnenctionUpdateRequest(api.RemoteConnenctionUpdateRequest{RemoteAPIToken: &auth.Token}).
		Execute()
	require.NoError(t, err)

	// Ensure the replication is now valid.
	require.NoError(t, client.ReplicationsApi.PostValidateReplicationByID(ctx, replication.Id).Execute())

	// Create a new bucket.
	bucket2 := influxdb.Bucket{
		OrgID:              l.Org.ID,
		Name:               "bucket2",
		RetentionPeriod:    0,
		ShardGroupDuration: 0,
	}
	require.NoError(t, l.BucketService(t).CreateBucket(ctx, &bucket2))
	bucket2Id := bucket2.ID.String()

	// Updating the replication to point at the new bucket should fail validation.
	_, err = client.ReplicationsApi.PatchReplicationByID(ctx, replication.Id).
		ReplicationUpdateRequest(api.ReplicationUpdateRequest{RemoteBucketID: &bucket2Id}).
		Validate(true).
		Execute()
	require.Error(t, err)
}

func TestReplicationStreamEndToEnd(t *testing.T) {
	// Points that will be written to the local bucket when only one replication is active.
	testPoints1 := []string{
		`m,k=v1 f=100i 946684800000000000`,
		`m,k=v2 f=200i 946684800000000000`,
	}

	// Flux script to write points that to the local bucket when both replications are active.
	testPoints2 := `import "csv"
csvData = "#datatype,string,long,dateTime:RFC3339,string,long,string,string
#group,false,false,false,true,false,true,true
#default,,,,,,,
,result,table,_time,k,_value,_measurement,_field
,,0,2000-01-01T00:00:00Z,v3,300,m,f
,,0,2000-01-01T00:00:00Z,v4,400,m,f
"
csv.from(csv: csvData) |> to(bucket: %q)
`

	// Format string to be used as a flux query to get data from a bucket.
	qs := `from(bucket:%q) |> range(start:2000-01-01T00:00:00Z,stop:2000-01-02T00:00:00Z)`

	// Data that should be in a bucket which received all the testPoints1.
	exp1 := `,result,table,_start,_stop,_time,_value,_field,_measurement,k` + "\r\n" +
		`,_result,0,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00Z,100,f,m,v1` + "\r\n" +
		`,_result,1,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00Z,200,f,m,v2` + "\r\n\r\n"

	// Data that should be in a bucket which received all the points from testPoints1 and testPoints2.
	exp2 := `,result,table,_start,_stop,_time,_value,_field,_measurement,k` + "\r\n" +
		`,_result,0,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00Z,100,f,m,v1` + "\r\n" +
		`,_result,1,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00Z,200,f,m,v2` + "\r\n" +
		`,_result,2,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00Z,300,f,m,v3` + "\r\n" +
		`,_result,3,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00Z,400,f,m,v4` + "\r\n\r\n"

	// Data that should be in a bucket which received points only from testPoints2.
	exp3 := `,result,table,_start,_stop,_time,_value,_field,_measurement,k` + "\r\n" +
		`,_result,0,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00Z,300,f,m,v3` + "\r\n" +
		`,_result,1,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00Z,400,f,m,v4` + "\r\n\r\n"

	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
	defer l.ShutdownOrFail(t, ctx)
	client := l.APIClient(t)

	localBucketName := l.Bucket.Name
	remote1BucketName := "remote1"
	remote2BucketName := "remote2"

	// Create a proxy for use in testing. This will proxy requests to the server, and also decrement the waitGroup to
	// allow for synchronization.
	var wg sync.WaitGroup
	var mu sync.Mutex
	proxyHandler := httputil.NewSingleHostReverseProxy(l.URL())
	proxy := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		mu.Lock()
		defer mu.Unlock()
		proxyHandler.ServeHTTP(w, r)
		wg.Done()
	}))
	defer proxy.Close()

	// Create a "remote" connection to the launcher from itself via the test proxy.
	remote, err := client.RemoteConnectionsApi.PostRemoteConnection(ctx).
		RemoteConnectionCreationRequest(api.RemoteConnectionCreationRequest{
			Name:             "self",
			OrgID:            l.Org.ID.String(),
			RemoteURL:        proxy.URL,
			RemoteAPIToken:   l.Auth.Token,
			RemoteOrgID:      api.PtrString(l.Org.ID.String()),
			AllowInsecureTLS: false,
		}).Execute()
	require.NoError(t, err)

	// Create separate buckets to act as the target for remote writes
	svc := l.BucketService(t)
	remote1Bucket := &influxdb.Bucket{
		OrgID: l.Org.ID,
		Name:  remote1BucketName,
	}
	require.NoError(t, svc.CreateBucket(ctx, remote1Bucket))
	remote2Bucket := &influxdb.Bucket{
		OrgID: l.Org.ID,
		Name:  remote2BucketName,
	}
	require.NoError(t, svc.CreateBucket(ctx, remote2Bucket))

	// Create a replication for the first remote bucket.
	replicationCreateReq := api.ReplicationCreationRequest{
		Name:              "test1",
		OrgID:             l.Org.ID.String(),
		RemoteID:          remote.Id,
		LocalBucketID:     l.Bucket.ID.String(),
		RemoteBucketID:    api.PtrString(remote1Bucket.ID.String()),
		MaxQueueSizeBytes: influxdb.DefaultReplicationMaxQueueSizeBytes,
		MaxAgeSeconds:     influxdb.DefaultReplicationMaxAge,
	}

	_, err = client.ReplicationsApi.PostReplication(ctx).ReplicationCreationRequest(replicationCreateReq).Execute()
	require.NoError(t, err)

	// Write the first set of points to the launcher bucket. This is the local bucket in the replication.
	for _, p := range testPoints1 {
		wg.Add(1)
		l.WritePointsOrFail(t, p)
	}
	wg.Wait()

	// Data should now be in the local bucket and in the replication remote bucket, but not in the bucket without the
	// replication.
	require.Equal(t, exp1, l.FluxQueryOrFail(t, l.Org, l.Auth.Token, fmt.Sprintf(qs, localBucketName)))
	require.Equal(t, exp1, l.FluxQueryOrFail(t, l.Org, l.Auth.Token, fmt.Sprintf(qs, remote1BucketName)))
	require.Equal(t, "\r\n", l.FluxQueryOrFail(t, l.Org, l.Auth.Token, fmt.Sprintf(qs, remote2BucketName)))

	// Create a replication for the second remote bucket.
	replicationCreateReq = api.ReplicationCreationRequest{
		Name:              "test2",
		OrgID:             l.Org.ID.String(),
		RemoteID:          remote.Id,
		LocalBucketID:     l.Bucket.ID.String(),
		RemoteBucketID:    api.PtrString(remote2Bucket.ID.String()),
		MaxQueueSizeBytes: influxdb.DefaultReplicationMaxQueueSizeBytes,
		MaxAgeSeconds:     influxdb.DefaultReplicationMaxAge,
	}
	_, err = client.ReplicationsApi.PostReplication(ctx).ReplicationCreationRequest(replicationCreateReq).Execute()
	require.NoError(t, err)

	// Write the second set of points to the launcher bucket via flux
	wg.Add(2) // since there are two replications, the proxy server will handle 2 requests
	l.FluxQueryOrFail(t, l.Org, l.Auth.Token, fmt.Sprintf(testPoints2, l.Bucket.Name))
	wg.Wait()

	// All the data should be in the local bucket and first replicated bucket. Only part of the data should be in the
	// second replicated bucket.
	require.Equal(t, exp2, l.FluxQueryOrFail(t, l.Org, l.Auth.Token, fmt.Sprintf(qs, localBucketName)))
	require.Equal(t, exp2, l.FluxQueryOrFail(t, l.Org, l.Auth.Token, fmt.Sprintf(qs, remote1BucketName)))
	require.Equal(t, exp3, l.FluxQueryOrFail(t, l.Org, l.Auth.Token, fmt.Sprintf(qs, remote2BucketName)))
}

func TestReplicationStreamEndToEndRemoteFailures(t *testing.T) {
	// Points that will be written to the local bucket.
	testPoints := []string{
		`m,k=v0 f=100i 946684800000000000`,
		`m,k=v1 f=200i 946684800000000000`,
		`m,k=v2 f=300i 946684800000000000`,
		`m,k=v3 f=400i 946684800000000000`,
	}

	// Format string to be used as a flux query to get data from a bucket.
	qs := `from(bucket:%q) |> range(start:2000-01-01T00:00:00Z,stop:2000-01-02T00:00:00Z)`

	// Data that should be in a bucket which received all the testPoints.
	exp := `,result,table,_start,_stop,_time,_value,_field,_measurement,k` + "\r\n" +
		`,_result,0,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00Z,100,f,m,v0` + "\r\n" +
		`,_result,1,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00Z,200,f,m,v1` + "\r\n" +
		`,_result,2,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00Z,300,f,m,v2` + "\r\n" +
		`,_result,3,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00Z,400,f,m,v3` + "\r\n\r\n"

	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
	defer l.ShutdownOrFail(t, ctx)
	client := l.APIClient(t)

	localBucketName := l.Bucket.Name
	remoteBucketName := "remote"

	random := rand.New(rand.NewSource(1000))
	pointsIndex := 0

	// Create a proxy for use in testing. This will proxy requests to the server, and also decrement the waitGroup to
	// allow for synchronization.
	var wg sync.WaitGroup
	var mu sync.Mutex
	proxyHandler := httputil.NewSingleHostReverseProxy(l.URL())
	proxy := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		mu.Lock()
		defer mu.Unlock()

		// Read the current point off of the request.
		body, _ := io.ReadAll(r.Body)
		reader, _ := gzip.NewReader(bytes.NewBuffer(body))
		unzipBody, _ := io.ReadAll(reader)

		// Fix the Body on the request.
		r.Body.Close()
		r.Body = io.NopCloser(bytes.NewBuffer(body))

		// "Randomly" fail on remote write to test retries.
		if random.Intn(2) == 0 {
			// Increment the index if the proper point succeeds.
			// This is needed since the replication queue will currently send the same
			// data several times if a failure is reached, since there is not enough
			// data to warrant a call to scan.Advance().
			if strings.Contains(string(unzipBody), fmt.Sprintf("v%d", pointsIndex)) {
				pointsIndex++
			}
			proxyHandler.ServeHTTP(w, r)

			// Decrement the waitGroup if all points have succeeded in writing.
			if pointsIndex == len(testPoints) {
				wg.Done()
			}
		} else {
			w.WriteHeader(nethttp.StatusGatewayTimeout)
		}
	}))
	defer proxy.Close()

	// Create a "remote" connection to the launcher from itself via the test proxy.
	remote, err := client.RemoteConnectionsApi.PostRemoteConnection(ctx).
		RemoteConnectionCreationRequest(api.RemoteConnectionCreationRequest{
			Name:             "self",
			OrgID:            l.Org.ID.String(),
			RemoteURL:        proxy.URL,
			RemoteAPIToken:   l.Auth.Token,
			RemoteOrgID:      api.PtrString(l.Org.ID.String()),
			AllowInsecureTLS: false,
		}).Execute()
	require.NoError(t, err)

	// Create separate buckets to act as the target for remote writes.
	svc := l.BucketService(t)
	remoteBucket := &influxdb.Bucket{
		OrgID: l.Org.ID,
		Name:  remoteBucketName,
	}
	require.NoError(t, svc.CreateBucket(ctx, remoteBucket))

	// Create a replication for the remote bucket.
	replicationCreateReq := api.ReplicationCreationRequest{
		Name:              "test1",
		OrgID:             l.Org.ID.String(),
		RemoteID:          remote.Id,
		LocalBucketID:     l.Bucket.ID.String(),
		RemoteBucketID:    api.PtrString(remoteBucket.ID.String()),
		MaxQueueSizeBytes: influxdb.DefaultReplicationMaxQueueSizeBytes,
		MaxAgeSeconds:     influxdb.DefaultReplicationMaxAge,
	}

	_, err = client.ReplicationsApi.PostReplication(ctx).ReplicationCreationRequest(replicationCreateReq).Execute()
	require.NoError(t, err)

	// Write the set of points to the launcher bucket. This is the local bucket in the replication.
	wg.Add(1)
	for _, p := range testPoints {
		l.WritePointsOrFail(t, p)
	}
	wg.Wait()

	// Data should now be in the local bucket and in the replication remote bucket.
	require.Equal(t, exp, l.FluxQueryOrFail(t, l.Org, l.Auth.Token, fmt.Sprintf(qs, localBucketName)))
	require.Equal(t, exp, l.FluxQueryOrFail(t, l.Org, l.Auth.Token, fmt.Sprintf(qs, remoteBucketName)))
}

func TestReplicationsLocalWriteAndShutdownBlocking(t *testing.T) {
	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
	client := l.APIClient(t)

	// Server that only returns an error will cause the remote write to retry on loop.
	svr := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		w.WriteHeader(nethttp.StatusInternalServerError)
	}))
	defer svr.Close()

	// Create a "remote" connection to the blocking server
	remote, err := client.RemoteConnectionsApi.PostRemoteConnection(ctx).
		RemoteConnectionCreationRequest(api.RemoteConnectionCreationRequest{
			Name:             "self",
			OrgID:            l.Org.ID.String(),
			RemoteURL:        svr.URL,
			RemoteAPIToken:   "foo",
			RemoteOrgID:      api.PtrString(l.Org.ID.String()),
			AllowInsecureTLS: false,
		}).Execute()
	require.NoError(t, err)

	createReq := api.ReplicationCreationRequest{
		Name:              "test",
		OrgID:             l.Org.ID.String(),
		RemoteID:          remote.Id,
		LocalBucketID:     l.Bucket.ID.String(),
		RemoteBucketID:    api.PtrString(l.Bucket.ID.String()),
		MaxQueueSizeBytes: influxdb.DefaultReplicationMaxQueueSizeBytes,
	}

	// Create the replication
	_, err = client.ReplicationsApi.PostReplication(ctx).ReplicationCreationRequest(createReq).Execute()
	require.NoError(t, err)

	p := `m,k=v1 f=100i 946684800000000000`

	// Do a write; the remote writer will block forever
	l.WritePointsOrFail(t, p)

	// Do some more writes; these should not be blocked locally, although the remote writer will be.
	var wg sync.WaitGroup
	for idx := 0; idx < 3; idx++ {
		wg.Add(1)
		go func() {
			l.WritePointsOrFail(t, p)
			wg.Done()
		}()
	}

	writesAreDone := make(chan struct{})

	go func() {
		wg.Wait()
		// If remote writes don't block local writes, this will quickly send on the writesAreDone channel to prevent the
		// test from timing out.
		writesAreDone <- struct{}{}
	}()

	// Test timeout for local writes
	delay := 5 * time.Second
	select {
	case <-time.After(delay):
		t.Fatalf("test timed out after %s - writing was blocked by remote writer", delay)
	case <-writesAreDone:
	}

	// Try to shut down the server
	didShutdown := make(chan struct{})
	go func() {
		// If remote writes don't block the server shutdown, the server should quickly shutdown and send on the didShutdown
		// channel.
		l.ShutdownOrFail(t, context.Background())
		didShutdown <- struct{}{}
	}()

	// Test timeout for server shutdown
	delay = 10 * time.Second
	select {
	case <-time.After(delay):
		t.Fatalf("test timed out after %s - server shutdown was blocked by remote writer", delay)
	case <-didShutdown:
	}
}
