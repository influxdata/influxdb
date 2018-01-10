// +build !windows

package tar

import "os"

func syncDir(dirName string) error {
	// fsync the dir to flush the rename
	dir, err := os.OpenFile(dirName, os.O_RDONLY, os.ModeDir)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// renameFile renames the file at oldpath to newpath.
func renameFile(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}
