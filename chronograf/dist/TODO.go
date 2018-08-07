package dist

import (
	"errors"
	"os"
)

// The functions defined in this file are placeholders
// until we decide how to get the finalized Chronograf assets in platform.

var errTODO = errors.New("TODO: decide how to handle chronograf assets in platform")

func Asset(string) ([]byte, error) {
	return nil, errTODO
}

func AssetInfo(name string) (os.FileInfo, error) {
	return nil, errTODO
}

func AssetDir(name string) ([]string, error) {
	return nil, errTODO
}