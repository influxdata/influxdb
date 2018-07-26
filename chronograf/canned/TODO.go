package canned

import "errors"

// The functions defined in this file are placeholders
// until we decide how to get the finalized Chronograf assets in platform.

var errTODO = errors.New("TODO: decide how to handle chronograf assets in platform")

func Asset(string) ([]byte, error) {
	return nil, errTODO
}

func AssetNames() []string {
	return nil
}
