package wal

import (
	"encoding/binary"
	"io"
)

type entryHeader struct {
	requestNumber uint32
	shardId       uint32
	length        uint32
}

func (self *entryHeader) Write(w io.Writer) (int, error) {
	size := 0

	for _, n := range []uint32{self.requestNumber, self.shardId, self.length} {
		if err := binary.Write(w, binary.BigEndian, n); err != nil {
			return size, err
		}
		size += 4
	}
	return size, nil
}

func (self *entryHeader) Read(r io.Reader) (int, error) {
	size := 0

	for _, n := range []*uint32{&self.requestNumber, &self.shardId, &self.length} {
		if err := binary.Read(r, binary.BigEndian, n); err != nil {
			return size, err
		}
		size += 4
	}
	return size, nil
}
