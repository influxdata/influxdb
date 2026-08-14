package launcher_test

import (
	"context"
	"encoding/json"
	"fmt"
	nethttp "net/http"
	"testing"
	"time"

	"github.com/influxdata/influx-cli/v2/clients/backup"
	"github.com/influxdata/influx-cli/v2/clients/restore"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func runBackupRestoreTests(t *testing.T, name string, testFunc func(bool, bool, *testing.T)) {
	t.Helper()
	for _, backupHashedTokens := range []bool{false, true} {
		for _, restoreHashedTokens := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/BackupHashedTokens=%t/RestoreHashedTokens=%t", name, backupHashedTokens, restoreHashedTokens),
				func() func(*testing.T) {
					return func(t *testing.T) {
						testFunc(backupHashedTokens, restoreHashedTokens, t)
					}
				}())
		}
	}
}

func TestBackupRestore_Full(t *testing.T) {
	t.Helper()
	runBackupRestoreTests(t, "TestBackupRestore_Full", runTestBackupRestore_Full)
}

func runTestBackupRestore_Full(backupHashedTokens, restoreHashedTokens bool, t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	backupDir := t.TempDir()

	// Boot a server, write some data, and take a backup.
	l1 := launcher.RunAndSetupNewLauncherOrFail(ctx, t, func(o *launcher.InfluxdOpts) {
		o.StoreType = "bolt"
		o.Testing = false
		o.LogLevel = zap.InfoLevel
		o.UseHashedTokens = backupHashedTokens
	})
	originalAuth := *l1.Auth
	l1.WritePointsOrFail(t, "m,k=v1 f=100i 946684800000000000\nm,k=v2 f=200i 946684800000000001")
	l1.BackupOrFail(t, ctx, backup.Params{Path: backupDir})

	// Create a new bucket, write data into it (+ the old bucket), and take another backup.
	b1 := influxdb.Bucket{OrgID: l1.Org.ID, Name: "bucket2"}
	require.NoError(t, l1.BucketService(t).CreateBucket(ctx, &b1))
	l1.WriteOrFail(t, &influxdb.OnboardingResults{
		Org:    l1.Org,
		Bucket: &b1,
		Auth:   l1.Auth,
	}, "m,k=v1 f=100i 946684800000000005\nm,k=v2 f=200i 946684800000000006")
	l1.WritePointsOrFail(t, "m,k=v1 f=100i 946684800000000002\nm,k=v2 f=200i 946684800000000003")
	l1.BackupOrFail(t, ctx, backup.Params{Path: backupDir})

	// Shut down the server.
	l1.ShutdownOrFail(t, ctx)

	// Boot up a second server, using a new auth token
	l2 := launcher.NewTestLauncher()
	l2.RunOrFail(t, ctx, func(o *launcher.InfluxdOpts) {
		o.StoreType = "bolt"
		o.Testing = false
		o.LogLevel = zap.InfoLevel
		o.UseHashedTokens = restoreHashedTokens
	})
	defer l2.ShutdownOrFail(t, ctx)

	onboardReq := influxdb.OnboardingRequest{
		User:     "USER",
		Password: "PASSWORD",
		Org:      "ORG",
		Bucket:   "BUCKET",
	}
	onboardRes := l2.OnBoardOrFail(t, &onboardReq)
	l2.Org = onboardRes.Org
	l2.Bucket = onboardRes.Bucket
	l2.Auth = onboardRes.Auth

	// Create a second bucket, write data into it.
	b2 := influxdb.Bucket{OrgID: onboardRes.Org.ID, Name: "2bucket"}
	require.NoError(t, l2.BucketService(t).CreateBucket(ctx, &b2))
	l2.WriteOrFail(t, &influxdb.OnboardingResults{
		Org:    onboardRes.Org,
		Bucket: &b2,
		Auth:   onboardRes.Auth,
	}, "m,k=v5 f=100i 946684800000000005\nm,k=v7 f=200i 946684800000000006")

	// Perform a full restore from the previous backups.
	restoreParams := restore.Params{Path: backupDir, Full: true}
	if backupHashedTokens {
		restoreParams.OperatorToken = originalAuth.Token
	}
	l2.RestoreOrFail(t, ctx, restoreParams)

	// A full restore also restores the original token
	l2.Auth = &originalAuth
	l2.ResetHTTPCLient()

	// Check that orgs and buckets were reset to match the original server's metadata.
	_, err := l2.OrgService(t).FindOrganizationByID(ctx, l2.Org.ID)
	require.Equal(t, errors.ENotFound, errors.ErrorCode(err))
	rbkt1, err := l2.BucketService(t).FindBucket(ctx, influxdb.BucketFilter{OrganizationID: &l1.Org.ID, ID: &l1.Bucket.ID})
	require.NoError(t, err)
	require.Equal(t, l1.Bucket.Name, rbkt1.Name)
	rbkt2, err := l2.BucketService(t).FindBucket(ctx, influxdb.BucketFilter{OrganizationID: &l1.Org.ID, ID: &b1.ID})
	require.NoError(t, err)
	require.Equal(t, b1.Name, rbkt2.Name)
	_, err = l2.BucketService(t).FindBucket(ctx, influxdb.BucketFilter{OrganizationID: &l2.Org.ID, ID: &b2.ID})
	require.Equal(t, errors.ENotFound, errors.ErrorCode(err))

	// Check that data was restored to buckets.
	q1 := `from(bucket:"BUCKET") |> range(start:2000-01-01T00:00:00Z,stop:2000-01-02T00:00:00Z)`
	exp1 := `,result,table,_start,_stop,_time,_value,_field,_measurement,k` + "\r\n" +
		`,_result,0,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00Z,100,f,m,v1` + "\r\n" +
		`,_result,0,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00.000000002Z,100,f,m,v1` + "\r\n" +
		`,_result,1,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00.000000001Z,200,f,m,v2` + "\r\n" +
		`,_result,1,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00.000000003Z,200,f,m,v2` + "\r\n\r\n"
	res1 := l2.FluxQueryOrFail(t, l2.Org, l2.Auth.Token, q1)
	require.Equal(t, exp1, res1)

	q2 := `from(bucket:"bucket2") |> range(start:2000-01-01T00:00:00Z,stop:2000-01-02T00:00:00Z)`
	exp2 := `,result,table,_start,_stop,_time,_value,_field,_measurement,k` + "\r\n" +
		`,_result,0,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00.000000005Z,100,f,m,v1` + "\r\n" +
		`,_result,1,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00.000000006Z,200,f,m,v2` + "\r\n\r\n"
	res2 := l2.FluxQueryOrFail(t, l2.Org, l2.Auth.Token, q2)
	require.Equal(t, exp2, res2)
}

func TestBackupRestore_OnConflictReplace(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	backupDir := t.TempDir()

	// Boot a server, write some data, and take a backup.
	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t, func(o *launcher.InfluxdOpts) {
		o.StoreType = "bolt"
		o.Testing = false
		o.LogLevel = zap.InfoLevel
	})
	defer l.ShutdownOrFail(t, ctx)
	l.WritePointsOrFail(t, "m,k=v1 f=100i 946684800000000000\nm,k=v2 f=200i 946684800000000001")
	l.BackupOrFail(t, ctx, backup.Params{Path: backupDir})

	// Write another point after the backup, then restore over the live bucket.
	l.WritePointsOrFail(t, "m,k=v3 f=300i 946684800000000002")
	l.RestoreOrFail(t, ctx, restore.Params{Path: backupDir, OnConflict: "replace"})

	// The bucket must keep its ID so tokens, DBRP mappings, and tasks
	// referencing it stay valid.
	rbkt, err := l.BucketService(t).FindBucket(ctx, influxdb.BucketFilter{Org: &l.Org.Name, Name: &l.Bucket.Name})
	require.NoError(t, err)
	require.Equal(t, l.Bucket.ID, rbkt.ID)

	// The bucket's contents match the backup: the post-backup point is gone.
	q := `from(bucket:"BUCKET") |> range(start:2000-01-01T00:00:00Z,stop:2000-01-02T00:00:00Z)`
	exp := `,result,table,_start,_stop,_time,_value,_field,_measurement,k` + "\r\n" +
		`,_result,0,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00Z,100,f,m,v1` + "\r\n" +
		`,_result,1,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00.000000001Z,200,f,m,v2` + "\r\n\r\n"
	res := l.FluxQueryOrFail(t, l.Org, l.Auth.Token, q)
	require.Equal(t, exp, res)
}

func TestBackupRestore_ReplaceStagedUntilUploadsComplete(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	backupDir := t.TempDir()

	// Boot a server, write some data, and take a backup.
	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t, func(o *launcher.InfluxdOpts) {
		o.StoreType = "bolt"
		o.Testing = false
		o.LogLevel = zap.InfoLevel
	})
	defer l.ShutdownOrFail(t, ctx)
	l.WritePointsOrFail(t, "m,k=v1 f=100i 946684800000000000")
	l.BackupOrFail(t, ctx, backup.Params{Path: backupDir})
	l.WritePointsOrFail(t, "m,k=v2 f=200i 946684800000000001")

	// Begin a replace restore but never upload the restored shards.
	start := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	manifest := influxdb.BucketMetadataManifest{
		OrganizationID:         l.Org.ID,
		OrganizationName:       l.Org.Name,
		BucketID:               l.Bucket.ID,
		BucketName:             l.Bucket.Name,
		DefaultRetentionPolicy: "autogen",
		RetentionPolicies: []influxdb.RetentionPolicyManifest{{
			Name:               "autogen",
			ReplicaN:           1,
			ShardGroupDuration: 7 * 24 * time.Hour,
			ShardGroups: []influxdb.ShardGroupManifest{{
				ID:        1,
				StartTime: start,
				EndTime:   start.Add(7 * 24 * time.Hour),
				Shards:    []influxdb.ShardManifest{{ID: 1}},
			}},
		}},
	}
	body, err := json.Marshal(manifest)
	require.NoError(t, err)

	req := l.NewHTTPRequestOrFail(t, "POST", "/api/v2/restore/bucketMetadata?onConflict=replace", l.Auth.Token, string(body))
	resp, err := nethttp.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, nethttp.StatusOK, resp.StatusCode)

	var mappings influxdb.RestoredBucketMappings
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&mappings))
	require.Equal(t, l.Bucket.ID, mappings.ID)
	require.Len(t, mappings.ShardMappings, 1)

	// The swap is staged until the shard uploads land, so the abandoned
	// restore leaves the bucket's data untouched.
	q := `from(bucket:"BUCKET") |> range(start:2000-01-01T00:00:00Z,stop:2000-01-02T00:00:00Z)`
	exp := `,result,table,_start,_stop,_time,_value,_field,_measurement,k` + "\r\n" +
		`,_result,0,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00Z,100,f,m,v1` + "\r\n" +
		`,_result,1,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00.000000001Z,200,f,m,v2` + "\r\n\r\n"
	require.Equal(t, exp, l.FluxQueryOrFail(t, l.Org, l.Auth.Token, q))

	// A full replace restore still succeeds after the abandoned attempt,
	// dropping its staged shards and committing the backup's contents.
	l.RestoreOrFail(t, ctx, restore.Params{Path: backupDir, OnConflict: "replace"})
	exp = `,result,table,_start,_stop,_time,_value,_field,_measurement,k` + "\r\n" +
		`,_result,0,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00Z,100,f,m,v1` + "\r\n\r\n"
	require.Equal(t, exp, l.FluxQueryOrFail(t, l.Org, l.Auth.Token, q))
}

func TestBackupRestore_Partial(t *testing.T) {
	t.Helper()
	runBackupRestoreTests(t, "TestBackupRestore_Full", runTestBackupRestore_Partial)
}

func runTestBackupRestore_Partial(backupHashedTokens, restoreHashedTokens bool, t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	backupDir := t.TempDir()

	// Boot a server, write some data, and take a backup.
	l1 := launcher.RunAndSetupNewLauncherOrFail(ctx, t, func(o *launcher.InfluxdOpts) {
		o.StoreType = "bolt"
		o.Testing = false
		o.LogLevel = zap.InfoLevel
		o.UseHashedTokens = backupHashedTokens
	})
	l1.WritePointsOrFail(t, "m,k=v1 f=100i 946684800000000000\nm,k=v2 f=200i 946684800000000001")
	l1.BackupOrFail(t, ctx, backup.Params{Path: backupDir})

	// Create a new bucket, write data into it (+ the old bucket), and take another backup.
	b1 := influxdb.Bucket{OrgID: l1.Org.ID, Name: "bucket2"}
	require.NoError(t, l1.BucketService(t).CreateBucket(ctx, &b1))
	l1.WriteOrFail(t, &influxdb.OnboardingResults{
		Org:    l1.Org,
		Bucket: &b1,
		Auth:   l1.Auth,
	}, "m,k=v1 f=100i 946684800000000005\nm,k=v2 f=200i 946684800000000006")
	l1.WritePointsOrFail(t, "m,k=v1 f=100i 946684800000000002\nm,k=v2 f=200i 946684800000000003")
	l1.BackupOrFail(t, ctx, backup.Params{Path: backupDir})

	// Shut down the server.
	l1.ShutdownOrFail(t, ctx)

	// Boot up a second server.
	l2 := launcher.NewTestLauncher()
	l2.RunOrFail(t, ctx, func(o *launcher.InfluxdOpts) {
		o.StoreType = "bolt"
		o.Testing = false
		o.LogLevel = zap.InfoLevel
		o.UseHashedTokens = restoreHashedTokens
	})
	defer l2.ShutdownOrFail(t, ctx)

	onboardReq := influxdb.OnboardingRequest{
		User:     "USER",
		Password: "PASSWORD",
		Org:      "ORG2",
		Bucket:   "BUCKET",
	}
	onboardRes := l2.OnBoardOrFail(t, &onboardReq)
	l2.Org = onboardRes.Org
	l2.Bucket = onboardRes.Bucket
	l2.Auth = onboardRes.Auth

	// Create a second bucket, write data into it.
	b2 := influxdb.Bucket{OrgID: onboardRes.Org.ID, Name: "2bucket"}
	require.NoError(t, l2.BucketService(t).CreateBucket(ctx, &b2))
	l2.WriteOrFail(t, &influxdb.OnboardingResults{
		Org:    onboardRes.Org,
		Bucket: &b2,
		Auth:   onboardRes.Auth,
	}, "m,k=v5 f=100i 946684800000000005\nm,k=v7 f=200i 946684800000000006")

	// Perform a partial restore from the previous backups.
	l2.RestoreOrFail(t, ctx, restore.Params{Path: backupDir})

	// Check that buckets from the 1st launcher were restored to the new server.
	rbkt1, err := l2.BucketService(t).FindBucket(ctx, influxdb.BucketFilter{Org: &l1.Org.Name, Name: &l1.Bucket.Name})
	require.NoError(t, err)
	require.Equal(t, l1.Bucket.Name, rbkt1.Name)
	rbkt2, err := l2.BucketService(t).FindBucket(ctx, influxdb.BucketFilter{Org: &l1.Org.Name, Name: &b1.Name})
	require.NoError(t, err)
	require.Equal(t, b1.Name, rbkt2.Name)

	// Check that data was restored to buckets.
	q1 := `from(bucket:"BUCKET") |> range(start:2000-01-01T00:00:00Z,stop:2000-01-02T00:00:00Z)`
	exp1 := `,result,table,_start,_stop,_time,_value,_field,_measurement,k` + "\r\n" +
		`,_result,0,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00Z,100,f,m,v1` + "\r\n" +
		`,_result,0,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00.000000002Z,100,f,m,v1` + "\r\n" +
		`,_result,1,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00.000000001Z,200,f,m,v2` + "\r\n" +
		`,_result,1,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00.000000003Z,200,f,m,v2` + "\r\n\r\n"
	res1 := l2.FluxQueryOrFail(t, l1.Org, l2.Auth.Token, q1)
	require.Equal(t, exp1, res1)

	q2 := `from(bucket:"bucket2") |> range(start:2000-01-01T00:00:00Z,stop:2000-01-02T00:00:00Z)`
	exp2 := `,result,table,_start,_stop,_time,_value,_field,_measurement,k` + "\r\n" +
		`,_result,0,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00.000000005Z,100,f,m,v1` + "\r\n" +
		`,_result,1,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00.000000006Z,200,f,m,v2` + "\r\n\r\n"
	res2 := l2.FluxQueryOrFail(t, l1.Org, l2.Auth.Token, q2)
	require.Equal(t, exp2, res2)

	// Check that the 2nd launcher's buckets weren't touched.
	newBucket1, err := l2.BucketService(t).FindBucket(ctx, influxdb.BucketFilter{OrganizationID: &l2.Org.ID, ID: &l2.Bucket.ID})
	require.NoError(t, err)
	require.Equal(t, l2.Bucket.Name, newBucket1.Name)
	newBucket2, err := l2.BucketService(t).FindBucket(ctx, influxdb.BucketFilter{OrganizationID: &l2.Org.ID, ID: &b2.ID})
	require.NoError(t, err)
	require.Equal(t, b2.Name, newBucket2.Name)

	q3 := `from(bucket:"2bucket") |> range(start:2000-01-01T00:00:00Z,stop:2000-01-02T00:00:00Z)`
	exp3 := `,result,table,_start,_stop,_time,_value,_field,_measurement,k` + "\r\n" +
		`,_result,0,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00.000000005Z,100,f,m,v5` + "\r\n" +
		`,_result,1,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00.000000006Z,200,f,m,v7` + "\r\n\r\n"
	res3 := l2.FluxQueryOrFail(t, l2.Org, l2.Auth.Token, q3)
	require.Equal(t, exp3, res3)
}
