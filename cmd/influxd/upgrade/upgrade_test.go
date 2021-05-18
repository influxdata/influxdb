package upgrade

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/dustin/go-humanize"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	"github.com/influxdata/influxdb/v2/internal/testutil"
	"github.com/influxdata/influxdb/v2/kit/cli"
	"github.com/influxdata/influxdb/v2/sqlite"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/tcnksm/go-input"
	"go.uber.org/zap"
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
	require.Contains(t, err.Error(), "1.x DB dir")

	err = os.MkdirAll(filepath.Join(v1Dir, "meta"), 0777)
	require.Nil(t, err)

	err = sourceOpts.validatePaths()
	require.NotNil(t, err, "Must fail")
	require.Contains(t, err.Error(), "1.x meta.db")

	err = ioutil.WriteFile(filepath.Join(v1Dir, "meta", "meta.db"), []byte{1}, 0777)
	require.Nil(t, err)

	err = sourceOpts.validatePaths()
	require.Nil(t, err)

	err = targetOpts.validatePaths()
	require.NotNil(t, err, "Must fail")
	require.Contains(t, err.Error(), "2.x engine")

	err = os.Remove(filepath.Join(enginePath, "db"))
	require.Nil(t, err)

	err = ioutil.WriteFile(configsPath, []byte{1}, 0777)
	require.Nil(t, err)

	err = targetOpts.validatePaths()
	require.NotNil(t, err, "Must fail")
	require.Contains(t, err.Error(), "2.x CLI configs")
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

	v1ConfigPath := filepath.Join(tmpdir, "v1.conf")
	v1Config, err := os.Create(v1ConfigPath)
	require.NoError(t, err)
	defer v1Config.Close()

	_, err = v1Config.WriteString(fmt.Sprintf(`reporting-disabled = true
[meta]
  dir = "%[1]s/v1db/meta"

[data]
  dir = "%[1]s/v1db/data"
  wal-dir = "%[1]s/v1db/wal"

[coordinator]
  max-concurrent-queries = 0
`,
		tmpdir))
	require.NoError(t, err)
	v1Config.Close()

	tl := launcher.NewTestLauncherServer()
	boltPath := filepath.Join(tl.Path, bolt.DefaultFilename)
	enginePath := filepath.Join(tl.Path, "engine")
	cqPath := filepath.Join(tl.Path, "cq.txt")
	cliConfigPath := filepath.Join(tl.Path, "influx-configs")
	configPath := filepath.Join(tl.Path, "config.toml")

	v1opts := &optionsV1{configFile: v1ConfigPath}
	v2opts := &optionsV2{
		boltPath:       boltPath,
		enginePath:     enginePath,
		cqPath:         cqPath,
		cliConfigsPath: cliConfigPath,
		configPath:     configPath,
		userName:       "my-user",
		password:       "my-password",
		orgName:        "my-org",
		bucket:         "my-bucket",
		retention:      "7d",
		token:          "my-token",
	}

	opts := &options{source: *v1opts, target: *v2opts, force: true}
	ui := &input.UI{Writer: &bytes.Buffer{}, Reader: &bytes.Buffer{}}

	log := zaptest.NewLogger(t, zaptest.Level(zap.InfoLevel))
	err = runUpgradeE(ctx, ui, opts, log)
	require.NoError(t, err)

	v := viper.New()
	v.SetConfigFile(configPath)
	require.NoError(t, v.ReadInConfig())
	lOpts := launcher.NewOpts(v)
	// We need to specify the path to sqlite here to prevent errors from trying to write to the database
	// concurrently by parallel tests.
	lOpts.SqLitePath = filepath.Join(tl.Path, sqlite.DefaultFilename)
	cliOpts := lOpts.BindCliOpts()

	cmd := cobra.Command{
		Use: "test",
		Run: func(*cobra.Command, []string) {
			tl.RunOrFail(t, ctx, func(o *launcher.InfluxdOpts) {
				*o = *lOpts
			})
			defer tl.ShutdownOrFail(t, ctx)

			orgs, _, err := tl.OrganizationService().FindOrganizations(ctx, influxdb.OrganizationFilter{})
			require.NoError(t, err)
			require.NotNil(t, orgs)
			require.Len(t, orgs, 1)
			require.Equal(t, "my-org", orgs[0].Name)

			users, _, err := tl.UserService().FindUsers(ctx, influxdb.UserFilter{})
			require.NoError(t, err)
			require.NotNil(t, users)
			require.Len(t, users, 1)
			require.Equal(t, "my-user", users[0].Name)

			tokenNames := []string{"reader", "writer", "readerwriter"}
			compatTokens, _, err := tl.Launcher.AuthorizationV1Service().FindAuthorizations(ctx, influxdb.AuthorizationFilter{})
			require.NoError(t, err)
			require.NotNil(t, compatTokens)
			require.Len(t, compatTokens, len(tokenNames))

			buckets, _, err := tl.Launcher.BucketService().FindBuckets(ctx, influxdb.BucketFilter{})
			require.NoError(t, err)

			bucketNames := []string{"my-bucket", "_tasks", "_monitoring", "mydb/autogen", "mydb/1week", "test/autogen", "empty/autogen"}
			myDbAutogenBucketId := ""
			myDb1weekBucketId := ""
			testBucketId := ""
			emptyBucketId := ""

			require.NotNil(t, buckets)
			require.Len(t, buckets, len(bucketNames))

			for _, b := range buckets {
				require.Contains(t, bucketNames, b.Name)
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
			require.NoDirExists(t, filepath.Join(enginePath, "data", "_internal"))

			// Ensure retention policy from the setup request passed through to the bucket.
			require.Equal(t, humanize.Week, tl.Bucket.RetentionPeriod)

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
				db := tl.Launcher.Engine().MetaClient().Database(check.dbname)
				require.NotNil(t, db)
				require.Len(t, db.ShardInfos(), check.shardsNum)
				if check.shardsNum > 0 {
					require.DirExists(t, filepath.Join(enginePath, "data", check.dbname, meta.DefaultRetentionPolicyName))
				}
			}

			auths, _, err := tl.Launcher.AuthorizationService().FindAuthorizations(ctx, influxdb.AuthorizationFilter{})
			require.NoError(t, err)
			require.Len(t, auths, 1)

			respBody := mustRunQuery(t, tl, "test", "select count(avg) from stat", auths[0].Token)
			require.Contains(t, respBody, `["1970-01-01T00:00:00Z",5776]`)

			respBody = mustRunQuery(t, tl, "mydb", "select count(avg) from testv1", auths[0].Token)
			require.Contains(t, respBody, `["1970-01-01T00:00:00Z",2882]`)

			respBody = mustRunQuery(t, tl, "mydb", "select count(i) from testv1", auths[0].Token)
			require.Contains(t, respBody, `["1970-01-01T00:00:00Z",21]`)

			respBody = mustRunQuery(t, tl, "mydb", `select count(line) from mydb."1week".log`, auths[0].Token)
			require.Contains(t, respBody, `["1970-01-01T00:00:00Z",1]`)

			cqBytes, err := ioutil.ReadFile(cqPath)
			require.NoError(t, err)
			cqs := string(cqBytes)

			require.Contains(t, cqs, "CREATE CONTINUOUS QUERY other_cq ON test BEGIN SELECT mean(foo) INTO test.autogen.foo FROM empty.autogen.foo GROUP BY time(1h) END")
			require.Contains(t, cqs, "CREATE CONTINUOUS QUERY cq_3 ON test BEGIN SELECT mean(bar) INTO test.autogen.bar FROM test.autogen.foo GROUP BY time(1m) END")
			require.Contains(t, cqs, "CREATE CONTINUOUS QUERY cq ON empty BEGIN SELECT mean(example) INTO empty.autogen.mean FROM empty.autogen.raw GROUP BY time(1h) END")
		},
	}
	require.NoError(t, cli.BindOptions(v, &cmd, cliOpts))
	require.NoError(t, cmd.Execute())
}

func mustRunQuery(t *testing.T, tl *launcher.TestLauncher, db, rawQ, token string) string {
	queryUrl, err := url.Parse(tl.URL() + "/query")
	require.Nil(t, err)

	params := queryUrl.Query()
	params.Set("db", db)
	params.Set("q", rawQ)
	queryUrl.RawQuery = params.Encode()

	req, err := http.NewRequest(http.MethodGet, queryUrl.String(), nil)
	require.Nil(t, err)

	req.Header.Set("Authorization", "Token "+token)
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)

	respBody, err := ioutil.ReadAll(resp.Body)
	require.Nil(t, err)

	return string(respBody)
}
