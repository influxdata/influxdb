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

func TestStreamWithBufSize(t *testing.T) {
	dir := t.TempDir()
	testFile := filepath.Join(dir, "test.txt")
	testData := []byte("test data for buffer size")

	require.NoError(t, os.WriteFile(testFile, testData, 0644))

	var buf bytes.Buffer
	bufSize := uint64(8192)

	require.NoError(t, pkgtar.Stream(&buf, dir, "shard", bufSize, nil))
	require.True(t, buf.Len() > 0)

	tr := tar.NewReader(&buf)
	hdr, err := tr.Next()
	require.NoError(t, err)
	require.Equal(t, "shard/test.txt", hdr.Name)

	content, err := io.ReadAll(tr)
	require.NoError(t, err)
	require.Equal(t, testData, content)
}
