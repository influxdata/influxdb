package tar

import "os"

func syncDir(dirName string) error {
	return nil
}

// renameFile renames the file at oldpath to newpath.
// If newpath already exists, it will be removed before renaming.
func renameFile(oldpath, newpath string) error {
	if _, err := os.Stat(newpath); err == nil {
		if err = os.Remove(newpath); nil != err {
			return err
		}
	}

	return os.Rename(oldpath, newpath)
}
