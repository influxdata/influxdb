package storage

//go:generate sh -c "protoc -I$(go list -f '{{ .Dir }}' -m github.com/gogo/protobuf) -I. --gogofaster_out=. source.proto"
