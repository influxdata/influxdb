package meta

import (
	"os"
	"path/filepath"
)

// snapshot will save the current meta data to disk
func snapshot(path string, data *Data) error {
	file := filepath.Join(path, metaFile)
	tmpFile := file + "tmp"

	f, err := os.Create(tmpFile)
	if err != nil {
		return err
	}
	defer f.Close()

	var d []byte
	if b, err := data.MarshalBinary(); err != nil {
		return err
	} else {
		d = b
	}

	if _, err := f.Write(d); err != nil {
		return err
	}

	if err = f.Close(); nil != err {
		return err
	}

	if _, err := os.Stat(file); err == nil {
		if err = os.Remove(file); nil != err {
			return err
		}
	}

	return os.Rename(tmpFile, file)
}
