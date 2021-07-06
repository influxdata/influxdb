package canned

import "errors"

// The functions defined in this file are placeholders when the binary is compiled
// without assets.

// Asset returns an error stating no assets were included in the binary.
func Asset(string) ([]byte, error) {
	return nil, errors.New("no assets included in binary")
}

// AssetNames returns nil because there are no assets included in the binary.
func AssetNames() []string {
	return nil
}
