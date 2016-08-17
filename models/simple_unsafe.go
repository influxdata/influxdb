package models

import (
	"reflect"
	"unsafe"
)

// unsafeBytesToString converts a []byte to a string without a heap allocation.
//
// It is unsafe, and is intended to prepare input to short-lived functions
// that require strings.
func unsafeBytesToString(in []byte) string {
	src := *(*reflect.SliceHeader)(unsafe.Pointer(&in))
	dst := reflect.StringHeader{
		Data: src.Data,
		Len:  src.Len,
	}
	s := *(*string)(unsafe.Pointer(&dst))
	return s
}

// unsafeStringToBytes converts a string to a []byte without a heap allocation.
//
// It is unsafe, and is intended to prepare input to short-lived functions
// that require byte slices.
func unsafeStringToBytes(in string) []byte {
	src := *(*reflect.StringHeader)(unsafe.Pointer(&in))
	dst := reflect.SliceHeader{
		Data: src.Data,
		Len:  src.Len,
		Cap:  src.Len,
	}
	s := *(*[]byte)(unsafe.Pointer(&dst))
	return s
}
