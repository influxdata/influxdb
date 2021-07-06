package verify_tombstone

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"os"
	"testing"

	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/stretchr/testify/require"
)

// Tombstone file headers for different versions.
// Treated as v1 without a header.
const (
	v2header = 0x1502
	v3header = 0x1503
	v4header = 0x1504
)

// Run tests on a directory with no Tombstone files
func TestVerifies_InvalidFileType(t *testing.T) {
	path, err := ioutil.TempDir("", "verify-tombstone")
	require.NoError(t, err)

	_, err = ioutil.TempFile(path, "verifytombstonetest*"+".txt")
	require.NoError(t, err)
	defer os.RemoveAll(path)

	verify := NewVerifyTombstoneCommand()
	verify.SetArgs([]string{"--dir", path})

	b := bytes.NewBufferString("")
	verify.SetOut(b)
	require.NoError(t, verify.Execute())

	out, err := ioutil.ReadAll(b)
	require.NoError(t, err)
	require.Contains(t, string(out), "No tombstone files found")
}

// Run tests on an empty Tombstone file (treated as v1)
func TestVerifies_InvalidEmptyFile(t *testing.T) {
	path, _ := NewTempTombstone(t)
	defer os.RemoveAll(path)

	verify := NewVerifyTombstoneCommand()
	verify.SetArgs([]string{"--dir", path})

	b := bytes.NewBufferString("")
	verify.SetOut(b)
	require.NoError(t, verify.Execute())

	out, err := ioutil.ReadAll(b)
	require.NoError(t, err)
	require.Contains(t, string(out), "has no tombstone entries")
}

// Runs tests on an invalid V2 Tombstone File
func TestVerifies_InvalidV2(t *testing.T) {
	path, file := NewTempTombstone(t)
	defer os.RemoveAll(path)

	WriteTombstoneHeader(t, file, v2header)
	WriteBadData(t, file)

	verify := NewVerifyTombstoneCommand()
	verify.SetArgs([]string{"--dir", path})
	verify.SetOut(bytes.NewBufferString(""))

	require.Error(t, verify.Execute())
}

func TestVerifies_ValidTS(t *testing.T) {
	path, file := NewTempTombstone(t)
	defer os.RemoveAll(path)

	ts := tsm1.NewTombstoner(file.Name(), nil)
	require.NoError(t, ts.Add([][]byte{[]byte("foobar")}))
	require.NoError(t, ts.Flush())

	verify := NewVerifyTombstoneCommand()
	verify.SetArgs([]string{"--dir", path, "--vv"})
	verify.SetOut(bytes.NewBufferString(""))

	require.NoError(t, verify.Execute())
}

// Runs tests on an invalid V3 Tombstone File
func TestVerifies_InvalidV3(t *testing.T) {
	path, file := NewTempTombstone(t)
	defer os.RemoveAll(path)

	WriteTombstoneHeader(t, file, v3header)
	WriteBadData(t, file)

	verify := NewVerifyTombstoneCommand()
	verify.SetArgs([]string{"--dir", path})
	verify.SetOut(bytes.NewBufferString(""))

	require.Error(t, verify.Execute())
}

// Runs tests on an invalid V4 Tombstone File
func TestVerifies_InvalidV4(t *testing.T) {
	path, file := NewTempTombstone(t)
	defer os.RemoveAll(path)

	WriteTombstoneHeader(t, file, v4header)
	WriteBadData(t, file)

	verify := NewVerifyTombstoneCommand()
	verify.SetArgs([]string{"--dir", path})
	verify.SetOut(bytes.NewBufferString(""))

	require.Error(t, verify.Execute())
}

// Ensures "--vvv" flag will not error as it
// is not needed, but was part of old command.
func TestTombstone_VeryVeryVerbose(t *testing.T) {
	path, file := NewTempTombstone(t)
	defer os.RemoveAll(path)

	WriteTombstoneHeader(t, file, v4header)
	WriteBadData(t, file)

	verify := NewVerifyTombstoneCommand()
	verify.SetArgs([]string{"--dir", path, "--vvv"})
	verify.SetOut(bytes.NewBufferString(""))

	require.Error(t, verify.Execute())
}

func NewTempTombstone(t *testing.T) (string, *os.File) {
	t.Helper()

	dir, err := ioutil.TempDir("", "verify-tombstone")
	require.NoError(t, err)

	file, err := ioutil.TempFile(dir, "verifytombstonetest*"+"."+tsm1.TombstoneFileExtension)
	require.NoError(t, err)

	return dir, file
}

func WriteTombstoneHeader(t *testing.T, file *os.File, header uint32) {
	t.Helper()

	writer, err := os.OpenFile(file.Name(), os.O_RDWR, 0)
	require.NoError(t, err)
	defer writer.Close()

	var b [4]byte
	binary.BigEndian.PutUint32(b[:], header)
	_, err = writer.Write(b[:])
	require.NoError(t, err)
}

func WriteBadData(t *testing.T, file *os.File) {
	t.Helper()

	writer, err := os.OpenFile(file.Name(), os.O_APPEND|os.O_WRONLY, 0644)
	require.NoError(t, err)
	defer writer.Close()

	written, err := writer.Write([]byte("foobar"))
	require.NoError(t, err)
	require.Equal(t, 6, written)
}
