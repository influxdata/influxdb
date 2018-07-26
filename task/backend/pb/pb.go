// Package pb contains protobuf types that are helpful for implementations of task Stores.
// These types are not intended for use by consumers of tasks.
package pb

// The tooling needed to correctly run go generate is managed by the Makefile.
// Run `make` from the project root to ensure these generate commands execute correctly.
//go:generate protoc -I ../../../vendor -I . --plugin ../../../bin/${GOOS}/protoc-gen-gogofaster --gogofaster_out=plugins=grpc:. ./tasks.proto
