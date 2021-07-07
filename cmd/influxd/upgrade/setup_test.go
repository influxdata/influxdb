package upgrade

import (
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influx-cli/v2/config"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestLocalConfig(t *testing.T) {

	type testCase struct {
		name       string
		sourceOpts *optionsV1
		targetOpts *optionsV2
		want       config.Config
	}

	var testCases = []testCase{
		{
			name:       "default",
			sourceOpts: &optionsV1{},
			targetOpts: &optionsV2{
				orgName: "my-org",
				token:   "my-token",
			},
			want: config.Config{
				Name:   config.DefaultConfig.Name,
				Host:   config.DefaultConfig.Host,
				Org:    "my-org",
				Token:  "my-token",
				Active: config.DefaultConfig.Active,
			},
		},
		{
			name: "v1 url",
			sourceOpts: &optionsV1{
				dbURL: "https://10.0.0.1:8086",
			},
			targetOpts: &optionsV2{
				orgName: "my-org",
				token:   "my-token",
			},
			want: config.Config{
				Name:   config.DefaultConfig.Name,
				Host:   "https://10.0.0.1:8086",
				Org:    "my-org",
				Token:  "my-token",
				Active: config.DefaultConfig.Active,
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			log := zaptest.NewLogger(t)
			// adjust paths to runtime values
			opts := tc.targetOpts
			opts.boltPath = filepath.Join(dir, bolt.DefaultFilename)
			opts.cliConfigsPath = filepath.Join(dir, "configs")
			opts.enginePath = filepath.Join(dir, "engine")
			// execute op
			err := saveLocalConfig(tc.sourceOpts, opts, log)
			require.NoError(t, err)
			// verify saved config
			localConfigSVC := config.NewLocalConfigService(opts.cliConfigsPath)
			actual, err := localConfigSVC.Active()
			require.NoError(t, err)
			if diff := cmp.Diff(tc.want, actual); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
