package storage

import "bytes"

func DefaultPredicate(last []byte) func(key []byte) bool {
	return func(key []byte) bool {
		return bytes.Compare(key, last) < 1
	}
}
