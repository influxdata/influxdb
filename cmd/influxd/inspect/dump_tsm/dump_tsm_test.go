package dump_tsm

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"testing"

	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/stretchr/testify/require"
)

var argsKeys = []string{"index", "blocks", "all", "filter-key"}

func Test_DumpTSM_NoFile(t *testing.T) {
	runCommand(t, cmdParams{
		file: "",
		expectOut: "TSM File not specified",
	})
}

func Test_DumpTSM_EmptyFile(t *testing.T) {
	dir, file := makeTSMFile(t, tsmParams{})
	defer os.RemoveAll(dir)

	runCommand(t, cmdParams{
		file:      file,
		expectErr: true,
		expectOut: "error opening TSM file",
	})
}

func Test_DumpTSM_WrongExt(t *testing.T) {
	dir, file := makeTSMFile(t, tsmParams{
		wrongExt: true,
	})
	defer os.RemoveAll(dir)

	runCommand(t, cmdParams{
		file:      file,
		expectErr: true,
		expectOut: "is not a TSM file",
	})
}

func Test_DumpTSM_NotFile(t *testing.T) {
	dir, _ := makeTSMFile(t, tsmParams{})
	defer os.RemoveAll(dir)

	runCommand(t, cmdParams{
		file:      dir,
		expectErr: true,
		expectOut: "is a directory",
	})
}

func Test_DumpTSM_Valid(t *testing.T) {
	dir, file := makeTSMFile(t, tsmParams{
		keys: []string{"cpu"},
	})
	defer os.RemoveAll(dir)

	runCommand(t, cmdParams{
		file: file,
		expectOuts: makeExpectOut(
			[]string{"Summary:", "Index:", "Total: 1", "Size: 34"},
			[]string{"Summary:", "Blocks:", "float64", "9/19", "s8b/gor"},
			[]string{"Summary:", "Index:", "Blocks:", "Points:", "Encoding:", "Compression:"},
		),
	})
}

func Test_DumpTSM_Invalid(t *testing.T) {
	dir, file := makeTSMFile(t, tsmParams{
		invalid: true,
		keys:    []string{"cpu"},
	})
	defer os.RemoveAll(dir)

	runCommand(t, cmdParams{
		file:      file,
		expectErr: true,
		expectOut: "error opening TSM file",
	})
}

func Test_DumpTSM_ManyKeys(t *testing.T) {
	dir, file := makeTSMFile(t, tsmParams{
		keys: []string{"cpu", "foobar", "mem"},
	})
	defer os.RemoveAll(dir)

	runCommand(t, cmdParams{
		file: file,
		expectOuts: makeExpectOut(
			[]string{"Total: 3", "Size: 102"},
			[]string{"Blocks:", "float64", "s8b/gor", "9/19"},
			[]string{"cpu", "foobar", "mem", "float64"},
		),
	})
}

func Test_DumpTSM_FilterKey(t *testing.T) {
	dir, file := makeTSMFile(t, tsmParams{
		keys: []string{"cpu", "foobar", "mem"},
	})
	defer os.RemoveAll(dir)

	runCommand(t, cmdParams{
		file:   file,
		filter: "cpu",
		expectOuts: makeExpectOut(
			[]string{"Total: 3", "Size: 102"},
			[]string{"Blocks:", "float64", "s8b/gor", "9/19"},
			[]string{"cpu", "foobar", "mem", "float64"},
			[]string{"Points:\n    Total: 1", "s8b: 1 (33%)", "gor: 1 (33%)"},
		),
	})
}

func makeExpectOut(outs ...[]string) (m map[string][]string) {
	m = make(map[string][]string)
	for i, value := range outs {
		m[argsKeys[i]] = value
	}
	return
}

type cmdParams struct {
	file       string
	expectErr  bool
	expectOuts map[string][]string
	expectOut  string
	filter     string
}

func runCommand(t *testing.T, params cmdParams) {
	cmd := NewDumpTSMCommand()

	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cmd.SetErr(b)

	m := makeArgs(params.file, params.filter)

	for argsKey, args := range m {
		cmd.SetArgs(args)
		if params.expectErr {
			require.Error(t, cmd.Execute())
		} else {
			require.NoError(t, cmd.Execute())
		}

		out, err := io.ReadAll(b)
		require.NoError(t, err)

		if params.expectOut != "" {
			require.Contains(t, string(out), params.expectOut)
		} else {
			for _, value := range params.expectOuts[argsKey] {
				require.Contains(t, string(out), value)
			}
		}
	}
}

func makeArgs(path string, meas string) (args map[string][]string) {
	args = make(map[string][]string)
	args[argsKeys[0]] = []string{"--file", path, "--index"}
	args[argsKeys[1]] = []string{"--file", path, "--blocks"}
	args[argsKeys[2]] = []string{"--file", path, "--all"}
	if meas != "" {
		args[argsKeys[3]] = []string{"--file", path, "--filter-key", meas}
	}
	return
}

type tsmParams struct {
	wrongExt bool
	invalid  bool
	keys     []string
}

func makeTSMFile(t *testing.T, params tsmParams) (string, string) {
	t.Helper()

	dir, err := os.MkdirTemp("", "dumptsm")
	require.NoError(t, err)

	ext := tsm1.TSMFileExtension
	if params.wrongExt {
		ext = "txt"
	}
	file, err := os.CreateTemp(dir, "*."+ext)
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
