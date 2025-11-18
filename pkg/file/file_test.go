package file_test

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/influxdata/influxdb/pkg/file"
	"github.com/stretchr/testify/require"
)

func TestRenameFileWithReplacement(t *testing.T) {
	testFileMoveOrRename(t, "rename", file.RenameFileWithReplacement)
}

func TestMoveFileWithReplacement(t *testing.T) {
	testFileMoveOrRename(t, "move", file.MoveFileWithReplacement)
}

func testFileMoveOrRename(t *testing.T, name string, testFunc func(src string, dst string) error) {
	// sample data for loading into files
	sampleData1 := "this is some data"
	sampleData2 := "we got some more data"

	t.Run("exists", func(t *testing.T) {
		oldPath := MustCreateTempFile(t, sampleData1)
		newPath := MustCreateTempFile(t, sampleData2)
		defer MustRemoveAll(oldPath)
		defer MustRemoveAll(newPath)

		oldContents := MustReadAllFile(oldPath)
		newContents := MustReadAllFile(newPath)

		if got, exp := oldContents, sampleData1; got != exp {
			t.Fatalf("got contents %q, expected %q", got, exp)
		} else if got, exp := newContents, sampleData2; got != exp {
			t.Fatalf("got contents %q, expected %q", got, exp)
		}

		if err := testFunc(oldPath, newPath); err != nil {
			t.Fatalf("%s returned an error: %s", name, err)
		}

		if err := file.SyncDir(filepath.Dir(oldPath)); err != nil {
			panic(err)
		}

		// Contents of newpath will now be equivalent to oldpath' contents.
		newContents = MustReadAllFile(newPath)
		if newContents != oldContents {
			t.Fatalf("contents for files differ: %q versus %q", newContents, oldContents)
		}

		// oldpath will be removed.
		if MustFileExists(oldPath) {
			t.Fatalf("file %q still exists, but it shouldn't", oldPath)
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

		if err := file.SyncDir(filepath.Dir(oldpath)); err != nil {
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
	defer func() {
		if err = fd.Close(); err != nil {
			panic(err)
		}
	}()
	data, err := io.ReadAll(fd)
	if err != nil {
		panic(err)
	}
	return string(data)
}

func TestVerifyFilePermissivenessF(t *testing.T) {
	t.Run("nil file pointer", func(t *testing.T) {
		err := file.VerifyFilePermissivenessF(nil, 0644)
		require.Error(t, err)
		require.ErrorIs(t, err, file.ErrNilParam)
	})

	t.Run("valid file with acceptable permissions", func(t *testing.T) {
		tmpFile := MustCreateTempFile(t, "test data")
		defer MustRemoveAll(tmpFile)

		require.NoError(t, os.Chmod(tmpFile, 0644))

		f, err := os.Open(tmpFile)
		require.NoError(t, err)
		defer f.Close()

		err = file.VerifyFilePermissivenessF(f, 0644)
		require.NoError(t, err)
	})

	t.Run("valid file with too open permissions", func(t *testing.T) {
		tmpFile := MustCreateTempFile(t, "test data")
		defer MustRemoveAll(tmpFile)

		require.NoError(t, os.Chmod(tmpFile, 0666))

		f, err := os.Open(tmpFile)
		require.NoError(t, err)
		defer f.Close()

		err = file.VerifyFilePermissivenessF(f, 0644)
		require.Error(t, err)
		require.ErrorIs(t, err, file.ErrPermissionsTooOpen)
		require.Contains(t, err.Error(), "maximum is 0644")
		require.Contains(t, err.Error(), "but found 0666")
		require.Contains(t, err.Error(), "extra permissions: 0022")
	})

	t.Run("valid file with restrictive permissions", func(t *testing.T) {
		tmpFile := MustCreateTempFile(t, "test data")
		defer MustRemoveAll(tmpFile)

		require.NoError(t, os.Chmod(tmpFile, 0400))

		f, err := os.Open(tmpFile)
		require.NoError(t, err)
		defer f.Close()

		err = file.VerifyFilePermissivenessF(f, 0644)
		require.NoError(t, err)
	})

	t.Run("check exact permission boundary 0600", func(t *testing.T) {
		tmpFile := MustCreateTempFile(t, "test data")
		defer MustRemoveAll(tmpFile)

		require.NoError(t, os.Chmod(tmpFile, 0600))

		f, err := os.Open(tmpFile)
		require.NoError(t, err)
		defer f.Close()

		// Should pass with 0600 max
		err = file.VerifyFilePermissivenessF(f, 0600)
		require.NoError(t, err)

		// Should fail with 0400 max
		err = file.VerifyFilePermissivenessF(f, 0400)
		require.Error(t, err)
		require.ErrorIs(t, err, file.ErrPermissionsTooOpen)
		require.Contains(t, err.Error(), "maximum is 0400")
		require.Contains(t, err.Error(), "but found 0600")
		require.Contains(t, err.Error(), "extra permissions: 0200")
	})
}

func TestVerifyFileInfoPermissiveness(t *testing.T) {
	t.Run("nil FileInfo", func(t *testing.T) {
		err := file.VerifyFileInfoPermissiveness(nil, 0644, "/some/path")
		require.Error(t, err)
		require.ErrorIs(t, err, file.ErrNilParam)
	})

	t.Run("permissions exactly at maximum", func(t *testing.T) {
		tmpFile := MustCreateTempFile(t, "test data")
		defer MustRemoveAll(tmpFile)

		require.NoError(t, os.Chmod(tmpFile, 0644))

		info, err := os.Stat(tmpFile)
		require.NoError(t, err)

		err = file.VerifyFileInfoPermissiveness(info, 0644, tmpFile)
		require.NoError(t, err)
	})

	t.Run("permissions more restrictive", func(t *testing.T) {
		tmpFile := MustCreateTempFile(t, "test data")
		defer MustRemoveAll(tmpFile)

		require.NoError(t, os.Chmod(tmpFile, 0400))

		info, err := os.Stat(tmpFile)
		require.NoError(t, err)

		err = file.VerifyFileInfoPermissiveness(info, 0644, tmpFile)
		require.NoError(t, err)
	})

	t.Run("permissions too open", func(t *testing.T) {
		tmpFile := MustCreateTempFile(t, "test data")
		defer MustRemoveAll(tmpFile)

		require.NoError(t, os.Chmod(tmpFile, 0777))

		info, err := os.Stat(tmpFile)
		require.NoError(t, err)

		err = file.VerifyFileInfoPermissiveness(info, 0644, tmpFile)
		require.Error(t, err)
		require.ErrorIs(t, err, file.ErrPermissionsTooOpen)
		require.Contains(t, err.Error(), `for "`+tmpFile+`"`)
		require.Contains(t, err.Error(), "maximum is 0644")
		require.Contains(t, err.Error(), "but found 0777")
		// Extra permissions: 0777 & ^0644 = 0133
		require.Contains(t, err.Error(), "extra permissions: 0133")
	})

	t.Run("various permission scenarios", func(t *testing.T) {
		testCases := []struct {
			name        string
			filePerms   os.FileMode
			maxPerms    os.FileMode
			shouldError bool
		}{
			{"0600 vs 0600", 0600, 0600, false},
			{"0600 vs 0644", 0600, 0644, false},
			{"0644 vs 0600", 0644, 0600, true},
			{"0400 vs 0600", 0400, 0600, false},
			{"0000 vs 0644", 0000, 0644, false},
			{"0755 vs 0644", 0755, 0644, true},
			{"0444 vs 0644", 0444, 0644, false},
			{"0666 vs 0644", 0666, 0644, true},
			{"0640 vs 0644", 0640, 0644, false},
			{"0660 vs 0644", 0660, 0644, true},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				tmpFile := MustCreateTempFile(t, "test data")
				defer MustRemoveAll(tmpFile)

				require.NoError(t, os.Chmod(tmpFile, tc.filePerms))

				info, err := os.Stat(tmpFile)
				require.NoError(t, err)

				err = file.VerifyFileInfoPermissiveness(info, tc.maxPerms, tmpFile)
				if tc.shouldError {
					require.Error(t, err, "expected error for %04o vs max %04o", tc.filePerms, tc.maxPerms)
				} else {
					require.NoError(t, err, "expected no error for %04o vs max %04o", tc.filePerms, tc.maxPerms)
				}
			})
		}
	})

	t.Run("error message format", func(t *testing.T) {
		tmpFile := MustCreateTempFile(t, "test data")
		defer MustRemoveAll(tmpFile)

		require.NoError(t, os.Chmod(tmpFile, 0666))

		info, err := os.Stat(tmpFile)
		require.NoError(t, err)

		err = file.VerifyFileInfoPermissiveness(info, 0644, tmpFile)
		require.Error(t, err)
		require.ErrorIs(t, err, file.ErrPermissionsTooOpen)

		errStr := err.Error()
		// Error should contain full path with quotes
		require.Contains(t, errStr, `for "`+tmpFile+`"`)
		// Error should contain properly formatted permission messages
		require.Contains(t, errStr, "maximum is 0644")
		require.Contains(t, errStr, "but found 0666")
		require.Contains(t, errStr, "extra permissions: 0022")
		// Error should contain both numeric and symbolic representations
		require.Contains(t, errStr, "rw-")
	})

	t.Run("empty path falls back to basename", func(t *testing.T) {
		tmpFile := MustCreateTempFile(t, "test data")
		defer MustRemoveAll(tmpFile)

		require.NoError(t, os.Chmod(tmpFile, 0666))

		info, err := os.Stat(tmpFile)
		require.NoError(t, err)

		// Pass empty path to test fallback to info.Name()
		err = file.VerifyFileInfoPermissiveness(info, 0644, "")
		require.Error(t, err)
		require.ErrorIs(t, err, file.ErrPermissionsTooOpen)
		require.Contains(t, err.Error(), `for "`+filepath.Base(info.Name())+`"`)
	})
}
