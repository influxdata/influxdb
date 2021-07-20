package wal

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"text/tabwriter"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/stretchr/testify/require"
)

type testInfo struct {
	path        string
	expectedOut string
	expectErr   bool
	withStdErr  bool
}

func TestVerifies_InvalidFileType(t *testing.T) {
	path, err := ioutil.TempDir("", "verify-wal")
	require.NoError(t, err)

	_, err = ioutil.TempFile(path, "verifywaltest*"+".txt")
	require.NoError(t, err)
	defer os.RemoveAll(path)

	runCommand(t, testInfo{
		path:        path,
		expectedOut: "no WAL files found in directory",
		expectErr:   true,
	})
}

func TestVerifies_InvalidNotDir(t *testing.T) {
	path, file := newTempWALInvalid(t, true)
	defer os.RemoveAll(path)

	runCommand(t, testInfo{
		path:        file.Name(),
		expectedOut: "is not a directory",
		expectErr:   true,
	})
}

func TestVerifies_InvalidEmptyFile(t *testing.T) {
	path, _ := newTempWALInvalid(t, true)
	defer os.RemoveAll(path)

	runCommand(t, testInfo{
		path:        path,
		expectedOut: "no WAL entries found",
		withStdErr:  true,
	})
}

func TestVerifies_Invalid(t *testing.T) {
	path, _ := newTempWALInvalid(t, false)
	defer os.RemoveAll(path)

	runCommand(t, testInfo{
		path:        path,
		expectedOut: "corrupt entry found at position",
		withStdErr:  true,
	})
}

func TestVerifies_Valid(t *testing.T) {
	path := newTempWALValid(t)
	defer os.RemoveAll(path)

	runCommand(t, testInfo{
		path:        path,
		expectedOut: "clean",
		withStdErr:  true,
	})
}

func runCommand(t *testing.T, args testInfo) {
	verify := NewCommand()

	b := bytes.NewBufferString("")
	tw := tabwriter.NewWriter(b, 16, 8, 0, '\n', 0)
	verify.Stdout = b
	if args.withStdErr {
		verify.Stderr = b
	}

	if args.expectErr {
		err := verify.verifyWAL(tw, args.path, true)
		require.Error(t, err)
		_, err = fmt.Fprint(tw, err.Error())
		require.NoError(t, err)
	} else {
		require.NoError(t, verify.verifyWAL(tw, args.path, true))
	}

	require.NoError(t, tw.Flush())
	out, err := ioutil.ReadAll(b)
	require.NoError(t, err)
	require.Contains(t, string(out), args.expectedOut)
}

func newTempWALValid(t *testing.T) string {
	t.Helper()

	dir, err := ioutil.TempDir("", "verify-wal")
	require.NoError(t, err)

	w := tsm1.NewWAL(dir)
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

	_, err = w.WriteMulti(values)
	require.NoError(t, err)

	return dir
}

func newTempWALInvalid(t *testing.T, empty bool) (string, *os.File) {
	t.Helper()

	dir, err := ioutil.TempDir("", "verify-wal")
	require.NoError(t, err)

	file, err := ioutil.TempFile(dir, "verifywaltest*."+tsm1.WALFileExtension)
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
