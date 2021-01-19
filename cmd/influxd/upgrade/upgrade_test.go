package upgrade

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	"github.com/influxdata/influxdb/v2/internal/testutil"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tcnksm/go-input"
	"go.uber.org/zap/zaptest"
)

func TestPathValidations(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "")
	require.Nil(t, err)

	defer os.RemoveAll(tmpdir)

	v1Dir := filepath.Join(tmpdir, "v1db")
	v2Dir := filepath.Join(tmpdir, "v2db")

	boltPath := filepath.Join(v2Dir, bolt.DefaultFilename)
	configsPath := filepath.Join(v2Dir, "configs")
	enginePath := filepath.Join(v2Dir, "engine")

	err = os.MkdirAll(filepath.Join(enginePath, "db"), 0777)
	require.Nil(t, err)

	sourceOpts := &optionsV1{
		dbDir:      v1Dir,
		configFile: "",
	}
	sourceOpts.populateDirs()

	targetOpts := &optionsV2{
		boltPath:       boltPath,
		cliConfigsPath: configsPath,
		enginePath:     enginePath,
	}

	err = sourceOpts.validatePaths()
	require.NotNil(t, err, "Must fail")
	assert.Contains(t, err.Error(), "1.x DB dir")

	err = os.MkdirAll(filepath.Join(v1Dir, "meta"), 0777)
	require.Nil(t, err)

	err = sourceOpts.validatePaths()
	require.NotNil(t, err, "Must fail")
	assert.Contains(t, err.Error(), "1.x meta.db")

	err = ioutil.WriteFile(filepath.Join(v1Dir, "meta", "meta.db"), []byte{1}, 0777)
	require.Nil(t, err)

	err = sourceOpts.validatePaths()
	require.Nil(t, err)

	err = targetOpts.validatePaths()
	require.NotNil(t, err, "Must fail")
	assert.Contains(t, err.Error(), "2.x engine")

	err = os.Remove(filepath.Join(enginePath, "db"))
	assert.Nil(t, err)

	err = ioutil.WriteFile(configsPath, []byte{1}, 0777)
	require.Nil(t, err)

	err = targetOpts.validatePaths()
	require.NotNil(t, err, "Must fail")
	assert.Contains(t, err.Error(), "2.x CLI configs")
}

func TestClearTargetPaths(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "")
	require.NoError(t, err)

	defer os.RemoveAll(tmpdir)

	v2Dir := filepath.Join(tmpdir, "v2db")
	boltPath := filepath.Join(v2Dir, bolt.DefaultFilename)
	configsPath := filepath.Join(v2Dir, "configs")
	enginePath := filepath.Join(v2Dir, "engine")
	cqPath := filepath.Join(v2Dir, "cqs")
	configPath := filepath.Join(v2Dir, "config")

	err = os.MkdirAll(filepath.Join(enginePath, "db"), 0777)
	require.NoError(t, err)
	err = ioutil.WriteFile(boltPath, []byte{1}, 0777)
	require.NoError(t, err)
	err = ioutil.WriteFile(configsPath, []byte{1}, 0777)
	require.NoError(t, err)
	err = ioutil.WriteFile(cqPath, []byte{1}, 0777)
	require.NoError(t, err)
	err = ioutil.WriteFile(configPath, []byte{1}, 0777)
	require.NoError(t, err)

	targetOpts := &optionsV2{
		boltPath:       boltPath,
		cliConfigsPath: configsPath,
		enginePath:     enginePath,
		configPath:     configPath,
		cqPath:         cqPath,
	}

	err = targetOpts.validatePaths()
	require.Error(t, err)
	err = targetOpts.clearPaths()
	require.NoError(t, err)
	err = targetOpts.validatePaths()
	require.NoError(t, err)
}

func TestDbURL(t *testing.T) {

	type testCase struct {
		name string
		conf string
		want string
	}

	var testCases = []testCase{
		{
			name: "default",
			conf: "[meta]\n[data]\n[http]\n",
			want: "http://localhost:8086",
		},
		{
			name: "custom but same as default",
			conf: "[meta]\n[data]\n[http]\nbind-address=\":8086\"\nhttps-enabled=false",
			want: "http://localhost:8086",
		},
		{
			name: "custom no host",
			conf: "[meta]\n[data]\n[http]\nbind-address=\":8186\"\nhttps-enabled=true",
			want: "https://localhost:8186",
		},
		{
			name: "custom with host",
			conf: "[meta]\n[data]\n[http]\nbind-address=\"10.0.0.1:8086\"\nhttps-enabled=true",
			want: "https://10.0.0.1:8086",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var c configV1
			_, err := toml.Decode(tc.conf, &c)
			require.NoError(t, err)
			if diff := cmp.Diff(tc.want, c.dbURL()); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestUpgradeRealDB(t *testing.T) {
	ctx := context.Background()

	tmpdir, err := ioutil.TempDir("", "")
	require.NoError(t, err)

	defer os.RemoveAll(tmpdir)
	err = testutil.Unzip("testdata/v1db.zip", tmpdir)
	require.NoError(t, err)

	tl := launcher.NewTestLauncherServer()
	defer tl.ShutdownOrFail(t, ctx)

	boltPath := filepath.Join(tl.Path, bolt.DefaultFilename)
	enginePath := filepath.Join(tl.Path, "engine")
	cqPath := filepath.Join(tl.Path, "cq.txt")
	cliConfigPath := filepath.Join(tl.Path, "influx-configs")

	v1opts := &optionsV1{dbDir: tmpdir + "/v1db"}
	v1opts.populateDirs()

	v2opts := &optionsV2{
		boltPath:       boltPath,
		enginePath:     enginePath,
		cqPath:         cqPath,
		cliConfigsPath: cliConfigPath,
		userName:       "my-user",
		password:       "my-password",
		orgName:        "my-org",
		bucket:         "my-bucket",
		retention:      "7d",
		token:          "my-token",
	}

	opts := &options{source: *v1opts, target: *v2opts, force: true}
	ui := &input.UI{Writer: &bytes.Buffer{}, Reader: &bytes.Buffer{}}

	log := zaptest.NewLogger(t)
	err = runUpgrade(ui, opts, log)
	require.NoError(t, err)

	v2, err := newInfluxDBv2(ctx, v2opts, log)
	require.NoError(t, err)

	orgs, _, err := v2.ts.FindOrganizations(ctx, influxdb.OrganizationFilter{})
	require.NoError(t, err)
	require.NotNil(t, orgs)
	require.Len(t, orgs, 1)
	assert.Equal(t, "my-org", orgs[0].Name)

	tl.Org = orgs[0]

	users, _, err := v2.ts.FindUsers(ctx, influxdb.UserFilter{})
	require.NoError(t, err)
	require.NotNil(t, users)
	require.Len(t, users, 1)
	assert.Equal(t, "my-user", users[0].Name)

	tokenNames := []string{"reader", "writer", "readerwriter"}
	compatTokens, _, err := v2.authSvc.FindAuthorizations(ctx, influxdb.AuthorizationFilter{})
	require.NoError(t, err)
	require.NotNil(t, compatTokens)
	require.Len(t, compatTokens, len(tokenNames))

	tl.User = users[0]

	buckets, _, err := v2.ts.FindBuckets(ctx, influxdb.BucketFilter{})
	require.NoError(t, err)

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
	require.NoError(t, err)
	require.Len(t, auths, 1)

	tl.Auth = auths[0]

	err = v2.close()
	require.NoError(t, err)

	// start server
	err = tl.Run(t, ctx)
	require.NoError(t, err)

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
