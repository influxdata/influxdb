package upgrade

import (
	"bytes"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCopyDirAndDirSize(t *testing.T) {
	tmpdir := t.TempDir()

	err := os.MkdirAll(filepath.Join(tmpdir, "1", "1", "1"), 0700)
	if err != nil {
		t.Fatal(err)
	}
	err = os.MkdirAll(filepath.Join(tmpdir, "1", "2", "1"), 0770)
	if err != nil {
		t.Fatal(err)
	}
	err = os.MkdirAll(filepath.Join(tmpdir, "1", "2", "skip"), 0770)
	if err != nil {
		t.Fatal(err)
	}
	err = os.MkdirAll(filepath.Join(tmpdir, "2", "1", "1"), 0777)
	if err != nil {
		t.Fatal(err)
	}

	bin11Mode := mustCreateFile(t, filepath.Join(tmpdir, "1", "1.bin"), 300, 0600)
	bin1111Mode := mustCreateFile(t, filepath.Join(tmpdir, "1", "1", "1", "1.bin"), 250, 0600)
	bin1112Mode := mustCreateFile(t, filepath.Join(tmpdir, "1", "1", "1", "2.bin"), 350, 0400)
	bin1211Mode := mustCreateFile(t, filepath.Join(tmpdir, "1", "2", "1", "1.bin"), 200, 0640)
	_ = mustCreateFile(t, filepath.Join(tmpdir, "1", "2", "skip", "1.bin"), 200, 0640)
	bin2111Mode := mustCreateFile(t, filepath.Join(tmpdir, "2", "1", "1", "1.bin"), 200, 0644)
	bin2112Mode := mustCreateFile(t, filepath.Join(tmpdir, "2", "1", "1", "2.bin"), 100, 0640)

	size, err := DirSize(tmpdir)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, uint64(1600), size)

	targetDir := t.TempDir()
	targetDir = filepath.Join(targetDir, "x")
	err = CopyDir(tmpdir, targetDir, nil, func(path string) bool {
		base := filepath.Base(path)
		return base == "skip"
	},
		nil)
	if err != nil {
		t.Fatal(err)
	}
	assetFileExistHasSizeAndPerm(t, filepath.Join(targetDir, "1", "1.bin"), 300, bin11Mode)
	assetFileExistHasSizeAndPerm(t, filepath.Join(targetDir, "1", "1", "1", "1.bin"), 250, bin1111Mode)
	assetFileExistHasSizeAndPerm(t, filepath.Join(targetDir, "1", "1", "1", "2.bin"), 350, bin1112Mode)
	assetFileExistHasSizeAndPerm(t, filepath.Join(targetDir, "1", "2", "1", "1.bin"), 200, bin1211Mode)
	assert.NoFileExists(t, filepath.Join(targetDir, "1", "2", "skip", "1.bin"))
	assetFileExistHasSizeAndPerm(t, filepath.Join(targetDir, "2", "1", "1", "1.bin"), 200, bin2111Mode)
	assetFileExistHasSizeAndPerm(t, filepath.Join(targetDir, "2", "1", "1", "2.bin"), 100, bin2112Mode)
}

func assetFileExistHasSizeAndPerm(t *testing.T, path string, size int, mode os.FileMode) {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Error(err)
	} else {
		assert.Equal(t, int64(size), fi.Size(), path)
		assert.Equal(t, mode, fi.Mode()&0xFFF, path)
	}
}

func mustCreateFile(t *testing.T, path string, size int, mode os.FileMode) os.FileMode {
	t.Helper()
	var buff bytes.Buffer

	for i := 0; i < size; i++ {
		b := byte(rand.Int31n(256))
		buff.Write([]byte{b})
	}
	require.NoError(t, os.WriteFile(path, buff.Bytes(), mode))
	// Windows doesn't preserve the full FileMode, so we check the value that was
	// actually persisted by the OS and return it here so we can assert that it
	// remains unchanged later.
	fi, err := os.Stat(path)
	require.NoError(t, err)
	return fi.Mode()
}
