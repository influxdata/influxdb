package delete_tsm

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"testing"

	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/stretchr/testify/require"
)

func Test_DeleteTSM_EmptyFile(t *testing.T) {
	dir, file := createTSMFile(t, tsmParams{})

	runCommand(t, testParams{
		file:      file,
		expectErr: true,
		expectOut: "unable to read TSM file",
	})
	require.NoError(t, os.RemoveAll(dir))
}

func Test_DeleteTSM_WrongExt(t *testing.T) {
	dir, file := createTSMFile(t, tsmParams{
		improperExt: true,
	})

	runCommand(t, testParams{
		file:      file,
		expectErr: true,
		expectOut: "is not a TSM file",
	})
	require.NoError(t, os.RemoveAll(dir))
}

func Test_DeleteTSM_NotFile(t *testing.T) {
	dir, _ := createTSMFile(t, tsmParams{})

	runCommand(t, testParams{
		file:      dir,
		expectErr: true,
		expectOut: "is a directory",
	})
	require.NoError(t, os.RemoveAll(dir))
}

func Test_DeleteTSM_SingleEntry_Valid(t *testing.T) {
	dir, file := createTSMFile(t, tsmParams{
		keys: []string{"cpu"},
	})

	runCommand(t, testParams{
		file:            file,
		shouldBeDeleted: true,
		expectOut:       "deleting block: cpu",
	})
	require.NoError(t, os.RemoveAll(dir))
}

func Test_DeleteTSM_SingleEntry_Invalid(t *testing.T) {
	dir, file := createTSMFile(t, tsmParams{
		invalid: true,
		keys:    []string{"cpu"},
	})

	runCommand(t, testParams{
		file:      file,
		expectErr: true,
		expectOut: "unable to read TSM file",
	})
	require.NoError(t, os.RemoveAll(dir))
}

func Test_DeleteTSM_ManyEntries_Valid(t *testing.T) {
	dir, file := createTSMFile(t, tsmParams{
		keys: []string{"cpu", "foobar", "mem"},
	})

	runCommand(t, testParams{
		file:      file,
		expectOut: "deleting block: cpu",
	})
	require.NoError(t, os.RemoveAll(dir))
}

func Test_DeleteTSM_ManyEntries_Invalid(t *testing.T) {
	dir, file := createTSMFile(t, tsmParams{
		invalid: true,
		keys:    []string{"cpu", "foobar", "mem"},
	})

	runCommand(t, testParams{
		file:      file,
		expectErr: true,
		expectOut: "unable to read TSM file",
	})
	require.NoError(t, os.RemoveAll(dir))
}

type testParams struct {
	file            string
	sanitize        bool // if true, run with --sanitize flag. Else run with --measurement flag
	expectOut       string
	expectErr       bool
	measurement     string
	shouldBeDeleted bool
}

func runCommand(t *testing.T, params testParams) {
	cmd := NewDeleteTSMCommand()
	args := []string{params.file}
	if params.sanitize {
		args = append(args, "--sanitize")
	} else {
		args = append(args, "--measurement", "cpu")
	}
	args = append(args, "--verbose")
	cmd.SetArgs(args)

	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cmd.SetErr(b)

	if params.expectErr {
		require.Error(t, cmd.Execute())
	} else {
		require.NoError(t, cmd.Execute())
		if params.shouldBeDeleted {
			require.NoFileExists(t, params.file)
		} else {
			file, err := os.Open(params.file)
			require.NoError(t, err)

			r, err := tsm1.NewTSMReader(file)
			require.NoError(t, err)

			require.False(t, r.Contains([]byte("cpu")))

			require.NoError(t, r.Close())
		}
	}

	out, err := io.ReadAll(b)
	require.NoError(t, err)
	require.Contains(t, string(out), params.expectOut)
}

type tsmParams struct {
	invalid     bool
	improperExt bool
	keys        []string
}

func createTSMFile(t *testing.T, params tsmParams) (string, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "deletetsm")
	require.NoError(t, err)

	var file *os.File
	if !params.improperExt {
		file, err = os.CreateTemp(dir, "*."+tsm1.TSMFileExtension)
	} else {
		file, err = os.CreateTemp(dir, "*.txt")
	}
	require.NoError(t, err)

	w, err := tsm1.NewTSMWriter(file)
	require.NoError(t, err)
	for _, key := range params.keys {
		values := []tsm1.Value{tsm1.NewValue(0, 1.0)}
		require.NoError(t, w.Write([]byte(key), values))
	}

	if len(params.keys) != 0 {
		require.NoError(t, w.WriteIndex())
	}

	if params.invalid {
		require.NoError(t, binary.Write(file, binary.BigEndian, []byte("foobar\n")))
	}
	require.NoError(t, w.Close())

	return dir, file.Name()
}
