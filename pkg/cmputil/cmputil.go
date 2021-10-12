// Package cmputil provides helper utilities for the go-cmp package.
package cmputil

import (
	"reflect"
	"unicode"
	"unicode/utf8"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
)

func IgnoreProtobufUnexported() cmp.Option {
	return cmp.FilterPath(filterProtobufUnexported, cmp.Ignore())
}

func filterProtobufUnexported(p cmp.Path) bool {
	// Determine if the path is pointing to a struct field.
	sf, ok := p.Index(-1).(cmp.StructField)
	if !ok {
		return false
	}

	// Return true if it is a proto.Message and the field is unexported.
	return implementsProtoMessage(p.Index(-2).Type()) && !isExported(sf.Name())
}

// isExported reports whether the identifier is exported.
func isExported(id string) bool {
	r, _ := utf8.DecodeRuneInString(id)
	return unicode.IsUpper(r)
}

var messageType = reflect.TypeOf((*proto.Message)(nil)).Elem()

func implementsProtoMessage(t reflect.Type) bool {
	return t.Implements(messageType) || reflect.PtrTo(t).Implements(messageType)
}
