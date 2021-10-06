package testhelper

import (
	"bytes"
	_ "embed"
	"io"
	"os"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

//go:embed influxd.bolt.testdata
var influxdBolt []byte

type TestBoltDb struct {
	f *os.File
	t *testing.T
}

func NewTestBoltDb(t *testing.T) *TestBoltDb {
	f, err := os.CreateTemp("", "")
	require.NoError(t, err)

	_, err = io.Copy(f, bytes.NewBuffer(influxdBolt))
	require.NoError(t, err)
	return &TestBoltDb{f, t}
}

func (t *TestBoltDb) Name() string {
	return t.f.Name()
}

func (t *TestBoltDb) Close() {
	require.NoError(t.t, t.f.Close())
	require.NoError(t.t, os.Remove(t.f.Name()))
}

func MustRunCommand(t *testing.T, cmd *cobra.Command, args ...string) string {
	buf := &bytes.Buffer{}
	cmd.SetArgs(args)
	cmd.SetOut(buf)
	require.NoError(t, cmd.Execute())
	return buf.String()
}

func RunCommand(t *testing.T, cmd *cobra.Command, args ...string) error {
	buf := &bytes.Buffer{}
	cmd.SetArgs(args)
	cmd.SetOut(buf)
	return cmd.Execute()
}
