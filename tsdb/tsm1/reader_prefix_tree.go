package tsm1

import "unsafe"

type prefixTreeKey [8]byte

const prefixTreeKeySize = len(prefixTreeKey{})

type prefixTree struct {
	values []TimeRange
	short  map[byte]*prefixTree
	long   map[prefixTreeKey]*prefixTree
}

func newPrefixTree() *prefixTree {
	return &prefixTree{
		short: make(map[byte]*prefixTree),
		long:  make(map[prefixTreeKey]*prefixTree),
	}
}

func (p *prefixTree) Append(prefix []byte, values ...TimeRange) {
	if len(prefix) >= prefixTreeKeySize {
		lookup := *(*prefixTreeKey)(unsafe.Pointer(&prefix[0]))
		ch, ok := p.long[lookup]
		if !ok {
			ch = newPrefixTree()
			p.long[lookup] = ch
		}
		ch.Append(prefix[prefixTreeKeySize:], values...)

	} else if len(prefix) > 0 {
		ch, ok := p.short[prefix[0]]
		if !ok {
			ch = newPrefixTree()
			p.short[prefix[0]] = ch
		}
		ch.Append(prefix[1:], values...)

	} else {
		p.values = append(p.values, values...)
	}
}

func (p *prefixTree) Search(key []byte, buf []TimeRange) []TimeRange {
	buf = append(buf, p.values...)

	if len(key) > 0 {
		if ch, ok := p.short[key[0]]; ok {
			buf = ch.Search(key[1:], buf)
		}
	}

	if len(key) >= prefixTreeKeySize {
		lookup := *(*prefixTreeKey)(unsafe.Pointer(&key[0]))
		if ch, ok := p.long[lookup]; ok {
			buf = ch.Search(key[prefixTreeKeySize:], buf)
		}
	}

	return buf
}

func (p *prefixTree) checkOverlap(key []byte, ts int64) bool {
	for _, t := range p.values {
		if t.Min <= ts && ts <= t.Max {
			return true
		}
	}

	if len(key) > 0 {
		if ch, ok := p.short[key[0]]; ok && ch.checkOverlap(key[1:], ts) {
			return true
		}
	}

	if len(key) >= prefixTreeKeySize {
		lookup := *(*prefixTreeKey)(unsafe.Pointer(&key[0]))
		if ch, ok := p.long[lookup]; ok && ch.checkOverlap(key[prefixTreeKeySize:], ts) {
			return true
		}
	}

	return false
}
