package kv

import (
	"errors"
	"strings"

	"github.com/influxdata/influxdb/v2/kit/platform"
)

// EncodeFn returns an encoding when called. Closures are your friend here.
type EncodeFn func() ([]byte, error)

// Encode concatenates a list of encodings together.
func Encode(encodings ...EncodeFn) EncodeFn {
	return func() ([]byte, error) {
		var key []byte
		for _, enc := range encodings {
			part, err := enc()
			if err != nil {
				return key, err
			}
			key = append(key, part...)
		}
		return key, nil
	}
}

// EncString encodes a string.
func EncString(str string) EncodeFn {
	return func() ([]byte, error) {
		return []byte(str), nil
	}
}

// EncStringCaseInsensitive encodes a string and makes it case insensitive by lower casing
// everything.
func EncStringCaseInsensitive(str string) EncodeFn {
	return EncString(strings.ToLower(str))
}

// EncID encodes an influx ID.
func EncID(id platform.ID) EncodeFn {
	return func() ([]byte, error) {
		if id == 0 {
			return nil, errors.New("no ID was provided")
		}
		return id.Encode()
	}
}

// EncBytes is a basic pass through for providing raw bytes.
func EncBytes(b []byte) EncodeFn {
	return func() ([]byte, error) {
		return b, nil
	}
}
