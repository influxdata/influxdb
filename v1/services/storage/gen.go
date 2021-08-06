package storage

//go:generate sh -c "protoc -I$(../../../scripts/gogo-path.sh) -I. --gogofaster_out=. source.proto"
