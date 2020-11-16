package upgrade

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/dustin/go-humanize"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	"github.com/influxdata/influxdb/v2/internal/testutil"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Default context.
var ctx = context.Background()

func TestUpgradeRealDB(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "")
	require.Nil(t, err)

	defer os.RemoveAll(tmpdir)
	err = testutil.Unzip("testdata/v1db.zip", tmpdir)
	require.Nil(t, err)

	tl := launcher.NewTestLauncherServer(nil)
	defer tl.ShutdownOrFail(t, ctx)

	boltPath := filepath.Join(tl.Path, bolt.DefaultFilename)
	enginePath := filepath.Join(tl.Path, "engine")
	cqPath := filepath.Join(tl.Path, "cq.txt")

	v1opts := &optionsV1{dbDir: tmpdir + "/v1db"}
	v1opts.populateDirs()

	v1, err := newInfluxDBv1(v1opts)
	require.Nil(t, err)

	v2opts := &optionsV2{
		boltPath:   boltPath,
		enginePath: enginePath,
		cqPath:     cqPath,
		userName:   "my-user",
		password:   "my-password",
		orgName:    "my-org",
		bucket:     "my-bucket",
		retention:  "7d",
		token:      "my-token",
	}

	v2, err := newInfluxDBv2(ctx, v2opts, zap.NewNop())
	require.Nil(t, err)

	options.target = *v2opts
	req, err := nonInteractive()
	require.Nil(t, err)
	assert.Equal(t, req.RetentionPeriod, humanize.Week, "Retention policy should pass through")

	resp, err := setupAdmin(ctx, v2, req)
	require.Nil(t, err)

	require.NotNil(t, resp)
	require.NotNil(t, resp.Org)
	require.NotNil(t, resp.Auth)
	require.NotNil(t, resp.User)
	require.NotNil(t, resp.Bucket)
	assert.Equal(t, "my-org", resp.Org.Name)
	assert.Equal(t, "my-user", resp.User.Name)
	assert.Equal(t, "my-bucket", resp.Bucket.Name)
	assert.Equal(t, "my-token", resp.Auth.Token)

	log, err := zap.NewDevelopment()
	require.Nil(t, err)

	db2bids, err := upgradeDatabases(ctx, v1, v2, v1opts, v2opts, resp.Org.ID, log)
	require.Nil(t, err)

	err = v2.close()
	require.Nil(t, err)

	require.Len(t, db2bids, 3)
	require.Len(t, db2bids["mydb"], 2)
	require.Len(t, db2bids["test"], 1)
	require.Len(t, db2bids["empty"], 1)

	v2, err = newInfluxDBv2(ctx, &optionsV2{boltPath: boltPath, enginePath: enginePath}, log)
	require.Nil(t, err)

	orgs, _, err := v2.ts.FindOrganizations(ctx, influxdb.OrganizationFilter{})
	require.Nil(t, err)
	require.NotNil(t, orgs)
	require.Len(t, orgs, 1)
	assert.Equal(t, "my-org", orgs[0].Name)

	tl.Org = orgs[0]

	users, _, err := v2.ts.FindUsers(ctx, influxdb.UserFilter{})
	require.Nil(t, err)
	require.NotNil(t, users)
	require.Len(t, users, 1)
	assert.Equal(t, "my-user", users[0].Name)

	tl.User = users[0]

	buckets, _, err := v2.ts.FindBuckets(ctx, influxdb.BucketFilter{})
	require.Nil(t, err)

	bucketNames := []string{"my-bucket", "_tasks", "_monitoring", "mydb/autogen", "mydb/1week", "test/autogen", "empty/autogen"}
	myDbAutogenBucketId := ""
	myDb1weekBucketId := ""
	testBucketId := ""
	emptyBucketId := ""

	require.NotNil(t, buckets)
	require.Len(t, buckets, len(bucketNames))

	for _, b := range buckets {
		assert.Contains(t, bucketNames, b.Name)
		switch b.Name {
		case bucketNames[0]:
			tl.Bucket = b
		case bucketNames[3]:
			myDbAutogenBucketId = b.ID.String()
		case bucketNames[4]:
			myDb1weekBucketId = b.ID.String()
		case bucketNames[5]:
			testBucketId = b.ID.String()
		case bucketNames[6]:
			emptyBucketId = b.ID.String()
		}
	}
	assert.NoDirExists(t, filepath.Join(enginePath, "data", "_internal"))

	dbChecks := []struct {
		dbname    string
		shardsNum int
	}{
		{myDbAutogenBucketId, 3},
		{testBucketId, 5},
		{myDb1weekBucketId, 1},
		{emptyBucketId, 0},
	}

	for _, check := range dbChecks {
		db := v2.meta.Database(check.dbname)
		require.NotNil(t, db)
		assert.Len(t, db.ShardInfos(), check.shardsNum)
		if check.shardsNum > 0 {
			assert.DirExists(t, filepath.Join(enginePath, "data", check.dbname, meta.DefaultRetentionPolicyName))
		}
		assert.NoDirExists(t, filepath.Join(enginePath, "data", check.dbname, "_series"))
		for _, si := range db.ShardInfos() {
			assert.NoDirExists(t, filepath.Join(enginePath, "data", check.dbname, strconv.FormatUint(si.ID, 10), "index"))
		}
	}

	auths, _, err := v2.authSvcV2.FindAuthorizations(ctx, influxdb.AuthorizationFilter{})
	require.Nil(t, err)
	require.Len(t, auths, 1)

	tl.Auth = auths[0]

	err = v2.close()
	require.Nil(t, err)

	// start server
	err = tl.Run(ctx)
	require.Nil(t, err)

	respBody := mustRunQuery(t, tl, "test", "select count(avg) from stat")
	assert.Contains(t, respBody, `["1970-01-01T00:00:00Z",5776]`)

	respBody = mustRunQuery(t, tl, "mydb", "select count(avg) from testv1")
	assert.Contains(t, respBody, `["1970-01-01T00:00:00Z",2882]`)

	respBody = mustRunQuery(t, tl, "mydb", "select count(i) from testv1")
	assert.Contains(t, respBody, `["1970-01-01T00:00:00Z",21]`)

	respBody = mustRunQuery(t, tl, "mydb", `select count(line) from mydb."1week".log`)
	assert.Contains(t, respBody, `["1970-01-01T00:00:00Z",1]`)

	cqBytes, err := ioutil.ReadFile(cqPath)
	require.NoError(t, err)
	cqs := string(cqBytes)

	assert.Contains(t, cqs, "CREATE CONTINUOUS QUERY other_cq ON test BEGIN SELECT mean(foo) INTO test.autogen.foo FROM empty.autogen.foo GROUP BY time(1h) END")
	assert.Contains(t, cqs, "CREATE CONTINUOUS QUERY cq_3 ON test BEGIN SELECT mean(bar) INTO test.autogen.bar FROM test.autogen.foo GROUP BY time(1m) END")
	assert.Contains(t, cqs, "CREATE CONTINUOUS QUERY cq ON empty BEGIN SELECT mean(example) INTO empty.autogen.mean FROM empty.autogen.raw GROUP BY time(1h) END")
}

func mustRunQuery(t *testing.T, tl *launcher.TestLauncher, db, rawQ string) string {
	queryUrl, err := url.Parse(tl.URL() + "/query")
	require.Nil(t, err)

	params := queryUrl.Query()
	params.Set("db", db)
	params.Set("q", rawQ)
	queryUrl.RawQuery = params.Encode()

	req, err := http.NewRequest(http.MethodGet, queryUrl.String(), nil)
	require.Nil(t, err)

	req.Header.Set("Authorization", "Token "+tl.Auth.Token)
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)

	respBody, err := ioutil.ReadAll(resp.Body)
	require.Nil(t, err)

	return string(respBody)
}
