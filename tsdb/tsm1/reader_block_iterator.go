package tsm1

// BlockIterator allows iterating over each block in a TSM file in order.  It provides
// raw access to the block bytes without decoding them.
type BlockIterator struct {
	r       *TSMReader
	iter    *TSMIndexIterator
	entries []IndexEntry
}

// PeekNext returns the next key to be iterated or an empty string.
func (b *BlockIterator) PeekNext() []byte {
	return b.iter.Peek()
}

// Next returns true if there are more blocks to iterate through.
func (b *BlockIterator) Next() bool {
	if b.iter.Err() != nil {
		return false
	}

	if len(b.entries) > 0 {
		b.entries = b.entries[1:]
		if len(b.entries) > 0 {
			return true
		}
	}

	if !b.iter.Next() {
		return false
	}
	b.entries = b.iter.Entries()

	return len(b.entries) > 0
}

// Read reads information about the next block to be iterated.
func (b *BlockIterator) Read() (key []byte, minTime int64, maxTime int64, typ byte, checksum uint32, buf []byte, err error) {
	if err := b.iter.Err(); err != nil {
		return nil, 0, 0, 0, 0, nil, err
	}
	checksum, buf, err = b.r.ReadBytes(&b.entries[0], nil)
	if err != nil {
		return nil, 0, 0, 0, 0, nil, err
	}
	return b.iter.Key(), b.entries[0].MinTime, b.entries[0].MaxTime, b.iter.Type(), checksum, buf, err
}

// Err returns any errors encounter during iteration.
func (b *BlockIterator) Err() error {
	return b.iter.Err()
}
