package fs_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/influxdata/influxdb/pkg/fs"
)

func TestRenameFileWithReplacement(t *testing.T) {
	t.Run("exists", func(t *testing.T) {
		oldpath := MustCreateTempFile()
		newpath := MustCreateTempFile()
		defer MustRemoveAll(oldpath)
		defer MustRemoveAll(newpath)

		oldContents := MustReadAllFile(oldpath)
		newContents := MustReadAllFile(newpath)

		if got, exp := oldContents, oldpath; got != exp {
			t.Fatalf("got contents %q, expected %q", got, exp)
		} else if got, exp := newContents, newpath; got != exp {
			t.Fatalf("got contents %q, expected %q", got, exp)
		}

		if err := fs.RenameFileWithReplacement(oldpath, newpath); err != nil {
			t.Fatalf("ReplaceFileIfExists returned an error: %s", err)
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
		oldpath := MustCreateTempFile()
		defer MustRemoveAll(oldpath)

		oldContents := MustReadAllFile(oldpath)
		if got, exp := oldContents, oldpath; got != exp {
			t.Fatalf("got contents %q, expected %q", got, exp)
		}

		root := filepath.Dir(oldpath)
		newpath := filepath.Join(root, "foo")
		if err := fs.RenameFileWithReplacement(oldpath, newpath); err != nil {
			t.Fatalf("ReplaceFileIfExists returned an error: %s", err)
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

// MustCreateTempFile creates a temporary file returning the path to the file.
//
// MustCreateTempFile writes the absolute path to the file into the file itself.
// It panics if there is an error.
func MustCreateTempFile() string {
	f, err := ioutil.TempFile("", "fs-test")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp file: %v", err))
	}

	name := f.Name()
	f.WriteString(name)
	if err := f.Close(); err != nil {
		panic(err)
	}
	return name
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

	data, err := ioutil.ReadAll(fd)
	if err != nil {
		panic(err)
	}
	return string(data)
}
