package upgrade

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	largs := make([]string, 0)
	largs = append(largs, "--username", "my-user")
	largs = append(largs, "--password", "my-password")
	largs = append(largs, "--org", "my-org")
	largs = append(largs, "--bucket", "my-bucket")
	largs = append(largs, "--retention", "7d")
	largs = append(largs, "--token", "my-token")
	largs = append(largs, "--v1-dir", v1Dir)
	largs = append(largs, "--bolt-path", boltPath)
	largs = append(largs, "--influx-configs-path", configsPath)
	largs = append(largs, "--engine-path", enginePath)
	largs = append(largs, "--config-file", "")

	cmd := NewCommand(viper.New())
	cmd.SetArgs(largs)
	err = cmd.Execute()
	require.NotNil(t, err, "Must fail")
	assert.Contains(t, err.Error(), "1.x metadb error")

	err = os.MkdirAll(filepath.Join(v1Dir, "meta"), 0777)
	require.Nil(t, err)

	err = ioutil.WriteFile(filepath.Join(v1Dir, "meta", "meta.db"), []byte{1}, 0777)
	require.Nil(t, err)

	cmd = NewCommand(viper.New())
	cmd.SetArgs(largs)

	err = cmd.Execute()
	require.NotNil(t, err, "Must fail")
	assert.Contains(t, err.Error(), "target engine path")
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
