package jsonparser

import (
	"github.com/buger/jsonparser"
	"github.com/influxdata/influxdb/v2/kit/platform"
)

// GetID returns an influxdb.ID for the specified keys path or an error if
// the value cannot be decoded or does not exist.
func GetID(data []byte, keys ...string) (val platform.ID, err error) {
	v, _, _, err := jsonparser.Get(data, keys...)
	if err != nil {
		return 0, err
	}

	var id platform.ID
	err = id.Decode(v)
	if err != nil {
		return 0, err
	}

	return id, nil
}

// GetOptionalID returns an influxdb.ID for the specified keys path or an error if
// the value cannot be decoded. The value of exists will be false if the keys path
// does not exist.
func GetOptionalID(data []byte, keys ...string) (val platform.ID, exists bool, err error) {
	v, typ, _, err := jsonparser.Get(data, keys...)
	if typ == jsonparser.NotExist {
		return 0, false, nil
	}

	if err != nil {
		return 0, false, err
	}

	var id platform.ID
	err = id.Decode(v)
	if err != nil {
		return 0, false, err
	}

	return id, true, nil
}
