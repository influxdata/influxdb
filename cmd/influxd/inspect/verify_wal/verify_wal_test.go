package verify_wal

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/stretchr/testify/require"
)

func TestVerifies_InvalidFileType(t *testing.T) {
	path, err := os.MkdirTemp("", "verify-wal")
	require.NoError(t, err)

	_, err = os.CreateTemp(path, "verifywaltest*"+".txt")
	require.NoError(t, err)
	defer os.RemoveAll(path)

	runCommand(t, path, "failed to find WAL files in directory", true)
}

func TestVerifies_InvalidNotDir(t *testing.T) {
	path, file := newTempWALInvalid(t, true)
	defer os.RemoveAll(path)

	runCommand(t, file.Name(), "invalid data directory", true)
}

func TestVerifies_InvalidEmptyFile(t *testing.T) {
	path, _ := newTempWALInvalid(t, true)
	defer os.RemoveAll(path)

	runCommand(t, path, "no WAL entries found for file", false)
}

func TestVerifies_Invalid(t *testing.T) {
	path, _ := newTempWALInvalid(t, false)
	defer os.RemoveAll(path)

	runCommand(t, path, "corrupt entry found at position", false)
}

func TestVerifies_Valid(t *testing.T) {
	path := newTempWALValid(t)
	defer os.RemoveAll(path)

	runCommand(t, path, "clean", false)
}

func runCommand(t *testing.T, dir string, expected string, expectErr bool) {
	verify := NewVerifyWALCommand()
	verify.SetArgs([]string{"--data-dir", dir})

	b := bytes.NewBufferString("")
	verify.SetOut(b)
	if expectErr {
		require.Error(t, verify.Execute())
	} else {
		require.NoError(t, verify.Execute())
	}

	out, err := io.ReadAll(b)
	require.NoError(t, err)
	require.Contains(t, string(out), expected)
}

func newTempWALValid(t *testing.T) string {
	t.Helper()

	dir, err := os.MkdirTemp("", "verify-wal")
	require.NoError(t, err)

	w := tsm1.NewWAL(dir, 0, 0)
	defer w.Close()
	require.NoError(t, w.Open())

	p1 := tsm1.NewValue(1, 1.1)
	p2 := tsm1.NewValue(1, int64(1))
	p3 := tsm1.NewValue(1, true)
	p4 := tsm1.NewValue(1, "string")
	p5 := tsm1.NewValue(1, ^uint64(0))

	values := map[string][]tsm1.Value{
		"cpu,host=A#!~#float":    {p1},
		"cpu,host=A#!~#int":      {p2},
		"cpu,host=A#!~#bool":     {p3},
		"cpu,host=A#!~#string":   {p4},
		"cpu,host=A#!~#unsigned": {p5},
	}

	_, err = w.WriteMulti(context.Background(), values)
	require.NoError(t, err)

	return dir
}

func newTempWALInvalid(t *testing.T, empty bool) (string, *os.File) {
	t.Helper()

	dir, err := os.MkdirTemp("", "verify-wal")
	require.NoError(t, err)

	file, err := os.CreateTemp(dir, "verifywaltest*."+tsm1.WALFileExtension)
	require.NoError(t, err)

	if !empty {
		writer, err := os.OpenFile(file.Name(), os.O_APPEND|os.O_WRONLY, 0644)
		require.NoError(t, err)
		defer writer.Close()

		written, err := writer.Write([]byte("foobar"))
		require.NoError(t, err)
		require.Equal(t, 6, written)
	}

	return dir, file
}
