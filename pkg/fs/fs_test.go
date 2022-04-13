package fs_test

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/influxdata/influxdb/v2/pkg/fs"
)

func TestRenameFileWithReplacement(t *testing.T) {
	testFileMoveOrRename(t, "rename", fs.RenameFileWithReplacement)
}

func TestMoveFileWithReplacement(t *testing.T) {
	testFileMoveOrRename(t, "move", fs.MoveFileWithReplacement)
}

func testFileMoveOrRename(t *testing.T, name string, testFunc func(src string, dst string) error) {
	// sample data for loading into files
	sampleData1 := "this is some data"
	sampleData2 := "we got some more data"

	t.Run("exists", func(t *testing.T) {
		oldpath := MustCreateTempFile(t, sampleData1)
		newpath := MustCreateTempFile(t, sampleData2)
		defer MustRemoveAll(oldpath)
		defer MustRemoveAll(newpath)

		oldContents := MustReadAllFile(oldpath)
		newContents := MustReadAllFile(newpath)

		if got, exp := oldContents, sampleData1; got != exp {
			t.Fatalf("got contents %q, expected %q", got, exp)
		} else if got, exp := newContents, sampleData2; got != exp {
			t.Fatalf("got contents %q, expected %q", got, exp)
		}

		if err := testFunc(oldpath, newpath); err != nil {
			t.Fatalf("%s returned an error: %s", name, err)
		}

		if err := fs.SyncDir(filepath.Dir(oldpath)); err != nil {
			panic(err)
		}

		// Contents of newpath will now be equivalent to oldpath' contents.
		newContents = MustReadAllFile(newpath)
		if newContents != oldContents {
			t.Fatalf("contents for files differ: %q versus %q", newContents, oldContents)
		}

		// oldpath will be removed.
		if MustFileExists(oldpath) {
			t.Fatalf("file %q still exists, but it shouldn't", oldpath)
		}
	})

	t.Run("not exists", func(t *testing.T) {
		oldpath := MustCreateTempFile(t, sampleData1)
		defer MustRemoveAll(oldpath)

		oldContents := MustReadAllFile(oldpath)
		if got, exp := oldContents, sampleData1; got != exp {
			t.Fatalf("got contents %q, expected %q", got, exp)
		}

		root := filepath.Dir(oldpath)
		newpath := filepath.Join(root, "foo")

		if err := testFunc(oldpath, newpath); err != nil {
			t.Fatalf("%s returned an error: %s", name, err)
		}

		if err := fs.SyncDir(filepath.Dir(oldpath)); err != nil {
			panic(err)
		}

		// Contents of newpath will now be equivalent to oldpath's contents.
		newContents := MustReadAllFile(newpath)
		if newContents != oldContents {
			t.Fatalf("contents for files differ: %q versus %q", newContents, oldContents)
		}

		// oldpath will be removed.
		if MustFileExists(oldpath) {
			t.Fatalf("file %q still exists, but it shouldn't", oldpath)
		}
	})
}

// CreateTempFileOrFail creates a temporary file returning the path to the file.
func MustCreateTempFile(t testing.TB, data string) string {
	t.Helper()

	f, err := os.CreateTemp("", "fs-test")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	} else if _, err := f.WriteString(data); err != nil {
		t.Fatal(err)
	} else if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return f.Name()
}

func MustRemoveAll(path string) {
	if err := os.RemoveAll(path); err != nil {
		panic(err)
	}
}

// MustFileExists determines if a file exists, panicking if any error
// (other than one associated with the file not existing) is returned.
func MustFileExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	} else if os.IsNotExist(err) {
		return false
	}
	panic(err)
}

// MustReadAllFile reads the contents of path, panicking if there is an error.
func MustReadAllFile(path string) string {
	fd, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer fd.Close()

	data, err := io.ReadAll(fd)
	if err != nil {
		panic(err)
	}
	return string(data)
}
