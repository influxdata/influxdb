package datatypes

//go:generate sh -c "protoc -I$(go list -f '{{ .Dir }}' -m github.com/gogo/protobuf) -I. --plugin ../../../scripts/protoc-gen-gogofaster --gogofaster_out=Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,plugins=grpc:. storage_common.proto predicate.proto"
