package upgrade

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCopyDirAndDirSize(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "tcd")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	err = os.MkdirAll(filepath.Join(tmpdir, "1", "1", "1"), 0700)
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
	mustCreateFile(t, filepath.Join(tmpdir, "1", "1.bin"), 300, 0600)
	mustCreateFile(t, filepath.Join(tmpdir, "1", "1", "1", "1.bin"), 250, 0600)
	mustCreateFile(t, filepath.Join(tmpdir, "1", "1", "1", "2.bin"), 350, 0400)
	mustCreateFile(t, filepath.Join(tmpdir, "1", "2", "1", "1.bin"), 200, 0640)
	mustCreateFile(t, filepath.Join(tmpdir, "1", "2", "skip", "1.bin"), 200, 0640)
	mustCreateFile(t, filepath.Join(tmpdir, "2", "1", "1", "1.bin"), 200, 0644)
	mustCreateFile(t, filepath.Join(tmpdir, "2", "1", "1", "2.bin"), 100, 0640)

	size, err := DirSize(tmpdir)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, uint64(1600), size)

	targetDir, err := ioutil.TempDir("", "tcd")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(targetDir)
	targetDir = filepath.Join(targetDir, "x")
	err = CopyDir(tmpdir, targetDir, nil, func(path string) bool {
		base := filepath.Base(path)
		return base == "skip"
	},
		nil)
	if err != nil {
		t.Fatal(err)
	}
	assetFileExistHasSizeAndPerm(t, filepath.Join(targetDir, "1", "1.bin"), 300, 0600)
	assetFileExistHasSizeAndPerm(t, filepath.Join(targetDir, "1", "1", "1", "1.bin"), 250, 0600)
	assetFileExistHasSizeAndPerm(t, filepath.Join(targetDir, "1", "1", "1", "2.bin"), 350, 0400)
	assetFileExistHasSizeAndPerm(t, filepath.Join(targetDir, "1", "2", "1", "1.bin"), 200, 0640)
	assert.NoFileExists(t, filepath.Join(targetDir, "1", "2", "skip", "1.bin"))
	assetFileExistHasSizeAndPerm(t, filepath.Join(targetDir, "2", "1", "1", "1.bin"), 200, 0644)
	assetFileExistHasSizeAndPerm(t, filepath.Join(targetDir, "2", "1", "1", "2.bin"), 100, 0640)
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

func mustCreateFile(t *testing.T, path string, size int, mode os.FileMode) {
	t.Helper()
	var buff bytes.Buffer

	for i := 0; i < size; i++ {
		b := byte(rand.Int31n(256))
		buff.Write([]byte{b})
	}
	err := ioutil.WriteFile(path, buff.Bytes(), mode)
	if err != nil {
		t.Fatal(err)
	}
}
