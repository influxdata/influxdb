package execute

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

type PartitionLookup struct {
	partitions map[uint64][]partitionEntry
}

type partitionEntry struct {
	key   PartitionKey
	value interface{}
}

func NewPartitionLookup() *PartitionLookup {
	return &PartitionLookup{
		partitions: make(map[uint64][]partitionEntry),
	}
}

func (l *PartitionLookup) Lookup(key PartitionKey) (interface{}, bool) {
	if key == nil {
		return nil, false
	}
	h := key.Hash()
	entries := l.partitions[h]
	if len(entries) == 1 {
		return entries[0].value, true
	}
	for _, entry := range entries {
		if entry.key.Equal(key) {
			return entry.value, true
		}
	}

	return nil, false
}

func (l *PartitionLookup) Set(key PartitionKey, value interface{}) {
	h := key.Hash()
	entries := l.partitions[h]
	l.partitions[h] = append(entries, partitionEntry{
		key:   key,
		value: value,
	})
}

func (l *PartitionLookup) Delete(key PartitionKey) (interface{}, bool) {
	if key == nil {
		return nil, false
	}
	h := key.Hash()
	entries := l.partitions[h]
	if len(entries) == 1 {
		delete(l.partitions, h)
		return entries[0].value, true
	}
	for i, entry := range entries {
		if entry.key.Equal(key) {
			l.partitions[h] = append(entries[:i+1], entries[i+1:]...)
			return entry.value, true
		}
	}
	return nil, false
}

func (l *PartitionLookup) Range(f func(key PartitionKey, value interface{})) {
	for _, entries := range l.partitions {
		for _, entry := range entries {
			f(entry.key, entry.value)
		}
	}
}

func computeKeyHash(key PartitionKey) uint64 {
	h := fnv.New64()
	for j, c := range key.Cols() {
		h.Write([]byte(c.Label))
		switch c.Type {
		case TBool:
			if key.ValueBool(j) {
				h.Write([]byte{1})
			} else {
				h.Write([]byte{0})
			}
		case TInt:
			binary.Write(h, binary.BigEndian, key.ValueInt(j))
		case TUInt:
			binary.Write(h, binary.BigEndian, key.ValueUInt(j))
		case TFloat:
			binary.Write(h, binary.BigEndian, math.Float64bits(key.ValueFloat(j)))
		case TString:
			h.Write([]byte(key.ValueString(j)))
		case TTime:
			binary.Write(h, binary.BigEndian, uint64(key.ValueTime(j)))
		}
	}
	return h.Sum64()
}
