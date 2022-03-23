package upgrade

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// DirSize returns total size in bytes of containing files
func DirSize(path string) (uint64, error) {
	var size uint64
	err := filepath.WalkDir(path, func(_ string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			size += uint64(info.Size())
		}
		return err
	})
	return size, err
}

// CopyFile copies the contents of the file named src to the file named
// by dst. The file will be created if it does not already exist. If the
// destination file exists, all it's contents will be replaced by the contents
// of the source file. The file mode will be copied from the source and
// the copied data is synced/flushed to stable storage.
func CopyFile(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return
	}
	defer in.Close()

	si, err := os.Stat(src)
	if err != nil {
		return
	}

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE, si.Mode())
	//out, err := os.Create(dst)
	if err != nil {
		return
	}
	defer func() {
		if e := out.Close(); e != nil {
			err = e
		}
	}()

	_, err = io.Copy(out, in)
	if err != nil {
		return
	}

	err = out.Sync()
	if err != nil {
		return
	}

	return
}

// CopyDir recursively copies a directory tree, attempting to preserve permissions.
// Source directory must exist, destination directory must *not* exist.
// Symlinks are ignored and skipped.
// dirRenameFunc is a mapping function that transforms path to a new name. Returning the path specifies the directory should not be renamed.
// dirFilterFunc ignores all directories where dirFilterFunc(path) is true. Passing nil for dirFilterFunc includes all directories.
// fileFilterFunc ignores all files where fileFilterFunc(path) is true. Passing nil for fileFilterFunc includes all files.
func CopyDir(src string, dst string, dirRenameFunc func(path string) string, dirFilterFunc func(path string) bool, fileFilterFunc func(path string) bool) (err error) {
	src = filepath.Clean(src)
	dst = filepath.Clean(dst)

	if dirFilterFunc != nil && dirFilterFunc(src) {
		return
	}
	si, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !si.IsDir() {
		return fmt.Errorf("source is not a directory")
	}

	_, err = os.Stat(dst)
	if err != nil && !os.IsNotExist(err) {
		return
	}
	if err == nil {
		return fmt.Errorf("destination '%s' already exists", dst)
	}

	err = os.MkdirAll(dst, si.Mode())
	if err != nil {
		return
	}

	entries, err := os.ReadDir(src)
	if err != nil {
		return
	}

	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		entryName := entry.Name()
		if dirRenameFunc != nil {
			entryName = dirRenameFunc(entryName)
		}
		dstPath := filepath.Join(dst, entryName)

		if entry.IsDir() {
			err = CopyDir(srcPath, dstPath, dirRenameFunc, dirFilterFunc, fileFilterFunc)
			if err != nil {
				return
			}
		} else {
			// Skip symlinks.
			if entry.Type().Perm()&os.ModeSymlink != 0 {
				continue
			}
			if fileFilterFunc != nil && fileFilterFunc(src) {
				continue
			}
			err = CopyFile(srcPath, dstPath)
			if err != nil {
				return
			}
		}
	}

	return
}
