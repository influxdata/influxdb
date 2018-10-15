package datatypes

//go:generate protoc -I ../../../internal -I . --plugin ../../../scripts/protoc-gen-gogofaster --gogofaster_out=Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,plugins=grpc:. storage_common.proto predicate.proto
