/*
Package wire is used to serialize a trace.

*/
package wire

//go:generate protoc -I$GOPATH/src -I. --gogofaster_out=Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types:. binary.proto
