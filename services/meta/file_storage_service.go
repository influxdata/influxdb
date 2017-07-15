package meta

import (
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
)

const (
	metaFile = "meta.db"
)

type fileStorageService struct {
	path string
}

func NewStorageService(config *Config) (StorageService, error) {
	if config.StorageType == Etcd {
		return NewEtcdStorageService(config)
	} else {
		return newFileStorageService(config)
	}
}

func newFileStorageService(config *Config) (*fileStorageService, error) {
	return &fileStorageService{
		path: config.Dir,
	}, nil
}

// Load loads the current meta data from disk.
func (s *fileStorageService) Load() (*Data, error) {
	file := filepath.Join(s.path, metaFile)

	// FIXME for this init
	cacheData := &Data{
		ClusterID: uint64(rand.Int63()),
		Index:     1,
	}

	f, err := os.Open(file)
	if err != nil {
		if os.IsNotExist(err) {
			return cacheData, nil
		}
		return nil, err
	}
	defer f.Close()

	data, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	if err := cacheData.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	return cacheData, nil
}

// snapshot saves the current meta data to disk.
func (s *fileStorageService) Snapshot(data *Data) error {
	file := filepath.Join(s.path, metaFile)
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

	if err = f.Sync(); err != nil {
		return err
	}

	//close file handle before renaming to support Windows
	if err = f.Close(); err != nil {
		return err
	}

	return renameFile(tmpFile, file)
}
