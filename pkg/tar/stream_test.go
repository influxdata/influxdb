package tar_test

import (
	"archive/tar"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	pkgtar "github.com/influxdata/influxdb/pkg/tar"
)

func TestStreamRenameWithBufSize(t *testing.T) {
	dir := t.TempDir()
	testFile := filepath.Join(dir, "test2.txt.tar")
	testData := []byte("test data for buffer size")

	testFileRename := "test.txt.tar"

	require.NoError(t, os.WriteFile(testFile, testData, 0644))

	var buf bytes.Buffer
	bufSize := uint64(1024 * 1024)

	tw := tar.NewWriter(&buf)

	f, err := os.Open(testFile)
	require.NoError(t, err, "error opening testFile")
	info, err := f.Stat()
	require.NoError(t, err, "error stat testFile")

	require.NoError(t, pkgtar.StreamRenameFile(info, testFileRename, "", testFile, tw, bufSize))
	require.NoError(t, tw.Close())

	tr := tar.NewReader(&buf)
	hdr, err := tr.Next()
	require.NoError(t, err)
	require.Equal(t, testFileRename, hdr.Name)

	content, err := io.ReadAll(tr)
	require.NoError(t, err)
	require.Equal(t, testData, content)
}
