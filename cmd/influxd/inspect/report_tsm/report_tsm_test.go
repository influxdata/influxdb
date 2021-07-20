package report_tsm

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"strconv"
	"testing"

	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/stretchr/testify/require"
)

func Test_Invalid_NotDir(t *testing.T) {
	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	file, err := os.CreateTemp(dir, "")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	runCommand(t, testInfo{
		dir:       file.Name(),
		expectOut: []string{"Files: 0"},
	})
}

func Test_Invalid_EmptyDir(t *testing.T) {
	var info dirInfo
	dir := makeTempDir(t, "", info)
	defer os.RemoveAll(dir)

	runCommand(t, testInfo{
		dir:       dir,
		expectOut: []string{"Files: 0"},
	})
}

func Test_Invalid_NotTSMFile(t *testing.T) {
	info := dirInfo{
		numFiles: 1,
		tsm: tsmInfo{
			withFile: true,
			emptyTSM: true,
		},
	}
	dir := makeTempDir(t, "", info)
	defer os.RemoveAll(dir)

	runCommand(t, testInfo{
		dir:       dir,
		expectOut: []string{"Files: 0"},
	})
}

func Test_Invalid_EmptyFile(t *testing.T) {
	info := dirInfo{
		numFiles: 1,
		tsm: tsmInfo{
			withTSMFile: true,
			emptyTSM:    true,
		},
	}
	dir := makeTempDir(t, "", info)
	defer os.RemoveAll(dir)

	runCommand(t, testInfo{
		dir:       dir,
		expectOut: []string{"error reading magic number of file"},
	})
}

func Test_Invalid_BadFile(t *testing.T) {
	info := dirInfo{
		numFiles: 1,
		tsm: tsmInfo{
			withTSMFile: true,
			invalidTSM:  true,
		},
	}
	dir := makeTempDir(t, "", info)
	defer os.RemoveAll(dir)

	runCommand(t, testInfo{
		dir:       dir,
		expectOut: []string{"can only read from tsm file"},
	})
}

func Test_Invalid_BadFile_WithGoodFiles(t *testing.T) {
	info := dirInfo{
		numFiles: 3,
		tsm: tsmInfo{
			withTSMFile: true,
			invalidTSM:  true,
		},
	}
	dir := makeTempDir(t, "", info)
	defer os.RemoveAll(dir)

	runCommand(t, testInfo{
		dir: dir,
		expectOut: []string{
			"can only read from tsm file", // bad file
			"Files: 2",                    // 2 other good files
		},
	})
}

func Test_Valid_SingleFile(t *testing.T) {
	info := dirInfo{
		numFiles: 1,
		tsm: tsmInfo{
			withTSMFile: true,
		},
	}
	dir := makeTempDir(t, "", info)

	runCommand(t, testInfo{
		dir:       dir,
		expectOut: []string{"Files: 1"},
	})
}

func Test_Valid_MultipleFiles_SingleDir(t *testing.T) {
	info := dirInfo{
		numFiles: 3,
		tsm: tsmInfo{
			withTSMFile: true,
		},
	}
	dir := makeTempDir(t, "", info)
	defer os.RemoveAll(dir)

	runCommand(t, testInfo{
		dir:       dir,
		expectOut: []string{"Files: 3"},
	})
}

func Test_Valid_MultipleFiles_MultipleDirs(t *testing.T) {
	info := dirInfo{
		numFiles: 3,
		subDirs:  3,
		tsm: tsmInfo{
			withTSMFile: true,
		},
	}
	dir := makeTempDir(t, "", info)
	defer os.RemoveAll(dir)

	runCommand(t, testInfo{
		dir:       dir,
		expectOut: []string{"Files: 12"},
	})
}

type dirInfo struct {
	tsm      tsmInfo
	numFiles int
	subDirs  int

	subDirIndex int // Used for recursion only
}

type tsmInfo struct {
	withFile    bool
	withTSMFile bool

	emptyTSM   bool
	invalidTSM bool
}

type testInfo struct {
	dir       string
	expectOut []string
}

func runCommand(t *testing.T, info testInfo) {
	cmd := NewReportTSMCommand()
	cmd.SetArgs([]string{"--data-dir", info.dir})

	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cmd.SetErr(b)

	require.NoError(t, cmd.Execute())

	out, err := io.ReadAll(b)
	require.NoError(t, err)
	for _, entry := range info.expectOut {
		require.Contains(t, string(out), entry)
	}
}

// makeTempDir returns the path to the root temporary directory
func makeTempDir(t *testing.T, startDir string, info dirInfo) string {
	t.Helper()

	dir, err := os.MkdirTemp(startDir, strconv.Itoa(info.subDirIndex))
	require.NoError(t, err)

	// Make subdirectories
	if info.subDirIndex == 0 {
		for i := 0; i < info.subDirs; i++ {
			info.subDirIndex += 1
			makeTempDir(t, dir, info)
		}
	}

	// Make TSM files
	for i := 0; i < info.numFiles; i++ {
		makeTempTSM(t, dir, info.tsm)
		info.tsm.invalidTSM = false // only do 1 max invalid TSM file, as the tests desire
	}
	return dir
}

func makeTempTSM(t *testing.T, dir string, info tsmInfo) {
	t.Helper()

	if info.withFile || info.withTSMFile {
		var ext string
		if info.withTSMFile {
			ext = tsm1.TSMFileExtension
		} else {
			ext = "txt"
		}
		file, err := os.CreateTemp(dir, "reporttsm*."+ext)
		require.NoError(t, err)

		if !info.emptyTSM {
			w, err := tsm1.NewTSMWriter(file)
			require.NoError(t, err)
			defer w.Close()

			values := []tsm1.Value{tsm1.NewValue(0, 1.0)}
			require.NoError(t, w.Write([]byte("cpu"), values))

			if info.invalidTSM {
				require.NoError(t, binary.Write(file, binary.BigEndian, []byte("foobar\n")))
			}

			require.NoError(t, w.WriteIndex())
		}
	}
}
