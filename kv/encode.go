package kv

import (
	"errors"
	"strings"

	"github.com/influxdata/influxdb"
)

// EncodeFn returns an encoding when called. Closures are your friend here.
type EncodeFn func() ([]byte, error)

type IDPK influxdb.ID

func (i IDPK) PrimaryKey() ([]byte, error) {
	if i == 0 {
		return nil, errors.New("no ID was provided")
	}
	return influxdb.ID(i).Encode()
}

// Encode2 concatenates a list of encodings together.
func Encode2(encodings ...EncodeFn) ([]byte, error) {
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
func EncID(id influxdb.ID) EncodeFn {
	return func() ([]byte, error) {
		if id == 0 {
			return nil, errors.New("no ID was provided")
		}
		return id.Encode()
	}
}
