package dump_wal

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/golang/snappy"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func Test_DumpWal_No_Args(t *testing.T) {
	params := cmdParams{
		walPaths:    []string{},
		expectErr:   true,
		expectedOut: "requires at least 1 arg(s), only received 0",
	}

	runCommand(t, params)
}

func Test_DumpWal_Bad_Path(t *testing.T) {
	params := cmdParams{
		findDuplicates: false,
		walPaths:       []string{"badpath.wal"},
		expectErr:      true,
		expectedOut:    "open badpath.wal: no such file or directory",
	}

	runCommand(t, params)
}

func Test_DumpWal_Wrong_File_Type(t *testing.T) {
	// Creates a temporary .txt file (wrong extension)
	dir, file := newTempWal(t, false, false)
	defer os.RemoveAll(dir)

	params := cmdParams{
		walPaths:    []string{file},
		expectedOut: fmt.Sprintf("invalid wal file path, skipping %s", file),
		expectErr:   false,
	}
	runCommand(t, params)
}

func Test_DumpWal_File_Valid(t *testing.T) {
	dir, file := newTempWal(t, true, false)
	defer os.RemoveAll(dir)

	params := cmdParams{
		walPaths:     []string{file},
		expectedOuts: []string{
			"[write]",
			"cpu,host=A#!~#float 1.1 1",
			"cpu,host=A#!~#int 1i 1",
			"cpu,host=A#!~#bool true 1",
			"cpu,host=A#!~#string \"string\" 1",
			"cpu,host=A#!~#unsigned 10u 5",
		},
	}

	runCommand(t, params)
}

func Test_DumpWal_Find_Duplicates_None(t *testing.T) {
	dir, file := newTempWal(t, true, false)
	defer os.RemoveAll(dir)

	params := cmdParams{
		findDuplicates: true,
		walPaths:       []string{file},
		expectedOut:    "No duplicates or out of order timestamps found",
	}

	runCommand(t, params)
}

func Test_DumpWal_Find_Duplicates_Present(t *testing.T) {
	dir, file := newTempWal(t, true, true)
	defer os.RemoveAll(dir)

	params := cmdParams{
		findDuplicates: true,
		walPaths:       []string{file},
		expectedOut:    "cpu,host=A#!~#unsigned",
	}

	runCommand(t, params)
}

func newTempWal(t *testing.T, validExt bool, withDuplicate bool) (string, string) {
	t.Helper()

	dir, err := os.MkdirTemp("", "dump-wal")
	require.NoError(t, err)
	var file *os.File

	if !validExt {
		file, err := os.CreateTemp(dir, "dumpwaltest*.txt")
		require.NoError(t, err)
		return dir, file.Name()
	}

	file, err = os.CreateTemp(dir, "dumpwaltest*"+"."+tsm1.WALFileExtension)
	require.NoError(t, err)

	p1 := tsm1.NewValue(10, 1.1)
	p2 := tsm1.NewValue(1, int64(1))
	p3 := tsm1.NewValue(1, true)
	p4 := tsm1.NewValue(1, "string")
	p5 := tsm1.NewValue(5, uint64(10))

	values := map[string][]tsm1.Value{
		"cpu,host=A#!~#float":    {p1},
		"cpu,host=A#!~#int":      {p2},
		"cpu,host=A#!~#bool":     {p3},
		"cpu,host=A#!~#string":   {p4},
		"cpu,host=A#!~#unsigned": {p5},
	}

	if withDuplicate {
		p6 := tsm1.NewValue(1, uint64(70))
		values = map[string][]tsm1.Value{
			"cpu,host=A#!~#unsigned": {p5, p6},
		}
	}

	// Write to WAL File
	writeWalFile(t, file, values)

	return dir, file.Name()
}

func writeWalFile(t *testing.T, file *os.File, vals map[string][]tsm1.Value) {
	t.Helper()

	e := &tsm1.WriteWALEntry{Values: vals}
	b, err := e.Encode(nil)
	require.NoError(t, err)

	w := tsm1.NewWALSegmentWriter(file)
	err = w.Write(e.Type(), snappy.Encode(nil, b))
	require.NoError(t, err)

	err = w.Flush()
	require.NoError(t, err)

	err = file.Sync()
	require.NoError(t, err)
}

type cmdParams struct {
	findDuplicates   bool
	walPaths         []string
	expectedOut      string
	expectedOuts     []string
	expectErr        bool
	expectExactEqual bool
}

func initCommand(t *testing.T, params cmdParams) *cobra.Command {
	t.Helper()

	// Creates new command and sets args
	cmd := NewDumpWALCommand()

	allArgs := params.walPaths
	if params.findDuplicates == true {
		allArgs = append(allArgs, "--find-duplicates")
	}

	cmd.SetArgs(allArgs)

	return cmd
}

func getOutput(t *testing.T, cmd *cobra.Command) []byte {
	t.Helper()

	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cmd.SetErr(b)
	require.NoError(t, cmd.Execute())

	out, err := io.ReadAll(b)
	require.NoError(t, err)

	return out
}

func runCommand(t *testing.T, params cmdParams) {
	t.Helper()

	cmd := initCommand(t, params)

	if params.expectErr {
		require.EqualError(t, cmd.Execute(), params.expectedOut)
		return
	}

	// Get output
	out := getOutput(t, cmd)

	// Check output
	if params.expectExactEqual {
		require.Equal(t, string(out), params.expectedOut)
		return
	}

	if params.expectedOut != "" {
		require.Contains(t, string(out), params.expectedOut)
	} else {
		for _, output := range params.expectedOuts {
			require.Contains(t, string(out), output)
		}
	}
}
