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

func (hdr *entryHeader) Write(w io.Writer) error {
	for _, n := range []uint32{hdr.requestNumber, hdr.shardId, hdr.length} {
		if err := binary.Write(w, binary.BigEndian, n); err != nil {
			return err
		}
	}
	return nil
}

func (hdr *entryHeader) Read(r io.Reader) error {
	for _, n := range []*uint32{&hdr.requestNumber, &hdr.shardId, &hdr.length} {
		if err := binary.Read(r, binary.BigEndian, n); err != nil {
			return err
		}
	}
	return nil
}
