package fs

import "os"

func SyncDir(dirName string) error {
	return nil
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

// RenameFile renames oldpath to newpath, returning an error if newpath already
// exists. If this function returns successfully, the contents of newpath will
// be identical to oldpath, and oldpath will be removed.
func RenameFile(oldpath, newpath string) error {
	if _, err := os.Stat(newpath); err == nil {
		// os.Rename on Windows will return an error if the file exists, but it's
		// preferable to keep the errors the same across platforms.
		return newFileExistsError(newpath)
	}

	return os.Rename(oldpath, newpath)
}

// CreateFileWithReplacement will create a new file at any path, removing the
// contents of the old file
func CreateFileWithReplacement(newpath string) (*os.File, error) {
	return os.Create(newpath)
}

// CreateFile creates a new file at newpath, returning an error if newpath already
// exists
func CreateFile(newpath string) (*os.File, error) {
	if _, err := os.Stat(newpath); err == nil {
		return nil, newFileExistsError(newpath)
	}

	return os.Create(newpath)
}
