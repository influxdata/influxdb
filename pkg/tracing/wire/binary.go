/*
Package wire is used to serialize a trace.

*/
package wire

//go:generate sh -c "protoc -I$(go list -f '{{ .Dir }}' -m github.com/gogo/protobuf) -I. --gogofaster_out=Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types:. binary.proto"
