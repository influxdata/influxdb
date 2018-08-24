package server

import (
	"errors"
)

// The functions defined in this file are placeholders
// until we decide how to get the finalized Chronograf assets in platform.

var errTODO = errors.New("You didn't generate assets for the chronograf/server folder, using placeholders")

func Asset(string) ([]byte, error) {
	return nil, errTODO
}
