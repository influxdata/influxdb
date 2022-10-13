package file

import "os"

func SyncDir(dirName string) error {
	return nil
}

// RenameFile will rename the source to target using os function. If target exists it will be removed before renaming.
func RenameFile(oldpath, newpath string) error {
	if _, err := os.Stat(newpath); err == nil {
		if err = os.Remove(newpath); nil != err {
			return err
		}
	}

	return os.Rename(oldpath, newpath)
}

// RenameFileWithReplacement will replace any existing file at newpath with the contents
// of oldpath.
//
// If no file already exists at newpath, newpath will be created using the contents
// of oldpath. If this function returns successfully, the contents of newpath will
// be identical to oldpath, and oldpath will be removed.
func RenameFileWithReplacement(oldpath, newpath string) error {
	if _, err := os.Stat(newpath); err == nil {
		if err = os.Remove(newpath); nil != err {
			return err
		}
	}

	return os.Rename(oldpath, newpath)
}
