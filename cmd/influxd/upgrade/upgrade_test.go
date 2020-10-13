package upgrade

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/influxdata/influxdb/v2/bolt"
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
	largs = append(largs, "--engine-path", enginePath)
	largs = append(largs, "--config-file", "")

	cmd := NewCommand()
	cmd.SetArgs(largs)

	err = cmd.Execute()
	require.NotNil(t, err, "Must fail")
	assert.Contains(t, err.Error(), "influx command path not specified")

	influxPath := "/usr/local/bin/influx" // fake
	largs = append(largs, "--influx-command-path", influxPath)
	cmd = NewCommand()
	cmd.SetArgs(largs)

	err = cmd.Execute()
	require.NotNil(t, err, "Must fail")
	assert.Contains(t, err.Error(), "1.x metadb error")

	err = os.MkdirAll(filepath.Join(v1Dir, "meta"), 0777)
	require.Nil(t, err)

	err = ioutil.WriteFile(filepath.Join(v1Dir, "meta", "meta.db"), []byte{1}, 0777)
	require.Nil(t, err)

	cmd = NewCommand()
	cmd.SetArgs(largs)

	err = cmd.Execute()
	require.NotNil(t, err, "Must fail")
	assert.Contains(t, err.Error(), "target engine path")
}
