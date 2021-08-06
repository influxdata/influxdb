package datatypes

//go:generate sh -c "protoc -I$(../../../scripts/gogo-path.sh) -I. --plugin ../../../scripts/protoc-gen-gogofaster --gogofaster_out=Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,plugins=grpc:. storage_common.proto predicate.proto"
