package datastore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"code.google.com/p/log4go"

	"github.com/influxdb/influxdb/common"
)

const maxSeqNumber = (1 << 64) - 1

// storageKey is the key that we use to store values in our key/value
// store engine. The key contains the field id, timestamp and sequence
// number of the value being stored.
type storageKey struct {
	bytesBuf  []byte
	id        uint64
	timestamp int64
	seq       uint64
}

// Create a new storageKey.
//    timestamp: the timestamp in microseconds. timestamp can be negative.
func newStorageKey(id uint64, timestamp int64, seq uint64) storageKey {
	return storageKey{
		bytesBuf:  nil,
		id:        id,
		timestamp: timestamp,
		seq:       seq,
	}
}

// Parse the given byte slice into a storageKey
func parseKey(b []byte) (storageKey, error) {
	if len(b) != 8*3 {
		return storageKey{}, fmt.Errorf("Expected %d fields, found %d", 8*3, len(b))
	}

	sk := storageKey{}
	buf := bytes.NewBuffer(b)
	binary.Read(buf, binary.BigEndian, &sk.id)
	var t uint64
	binary.Read(buf, binary.BigEndian, &t)
	sk.timestamp = convertUintTimestampToInt64(t)
	binary.Read(buf, binary.BigEndian, &sk.seq)
	sk.bytesBuf = b
	log4go.Debug("Parsed %v to %v", b, sk)
	return sk, nil
}

// Return a byte representation of the storage key. If the given byte
// representation was to be lexicographic sorted, then b1 < b2 iff
// id1 < id2 (b1 is a byte representation of a storageKey with a smaller
// id) or id1 == id2 and t1 < t2, or id1 == id2 and t1 == t2 and
// seq1 < seq2. This means that the byte representation has the same
// sort properties as the tuple (id, time, sequence)
func (sk storageKey) bytes() []byte {
	if sk.bytesBuf != nil {
		return sk.bytesBuf
	}

	buf := bytes.NewBuffer(nil)
	binary.Write(buf, binary.BigEndian, sk.id)
	t := convertTimestampToUint(sk.timestamp)
	binary.Write(buf, binary.BigEndian, t)
	binary.Write(buf, binary.BigEndian, sk.seq)
	sk.bytesBuf = buf.Bytes()
	return sk.bytesBuf
}

func (sk storageKey) time() time.Time {
	return common.TimeFromMicroseconds(sk.timestamp)
}

// utility functions only used in this file

func convertTimestampToUint(t int64) uint64 {
	if t < 0 {
		return uint64(math.MaxInt64 + t + 1)
	}
	return uint64(t) + uint64(math.MaxInt64) + uint64(1)
}

func convertUintTimestampToInt64(t uint64) int64 {
	if t > uint64(math.MaxInt64) {
		return int64(t-math.MaxInt64) - int64(1)
	}
	return int64(t) - math.MaxInt64 - int64(1)
}
