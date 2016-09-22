package rhh

import (
	"bytes"

	"github.com/influxdata/influxdb/pkg/murmur3"
)

// HashMap represents a hash map that implements Robin Hood Hashing.
// https://cs.uwaterloo.ca/research/tr/1986/CS-86-14.pdf
type HashMap struct {
	hashes []uint32
	elems  []hashElem

	n          int
	capacity   int
	threshold  int
	mask       uint32
	loadFactor int
}

func NewHashMap(opt Options) *HashMap {
	m := &HashMap{
		capacity:   pow2(opt.Capacity),
		loadFactor: opt.LoadFactor,
	}
	m.alloc()
	return m
}

func (m *HashMap) Get(key []byte) interface{} {
	i := m.index(key)
	if i == -1 {
		return nil
	}
	return m.elems[i].value
}

func (m *HashMap) Put(key []byte, val interface{}) {
	// Grow the map if we've run out of slots.
	m.n++
	if m.n > m.threshold {
		m.grow()
	}

	// If the key was overwritten then decrement the size.
	overwritten := m.insert(m.hashKey(key), key, val)
	if overwritten {
		m.n--
	}
}

func (m *HashMap) insert(hash uint32, key []byte, val interface{}) (overwritten bool) {
	pos := int(hash & m.mask)
	dist := 0

	// Continue searching until we find an empty slot or lower probe distance.
	for {
		// Empty slot found or matching key, insert and exit.
		if m.hashes[pos] == 0 {
			m.hashes[pos] = hash
			m.elems[pos] = hashElem{hash: hash, key: key, value: val}
			return false
		} else if bytes.Equal(m.elems[pos].key, key) {
			m.hashes[pos] = hash
			m.elems[pos] = hashElem{hash: hash, key: key, value: val}
			return true
		}

		// If the existing elem has probed less than us, then swap places with
		// existing elem, and keep going to find another slot for that elem.
		elemDist := m.dist(m.hashes[pos], pos)
		if elemDist < dist {
			// Swap with current position.
			e := &m.elems[pos]
			hash, m.hashes[pos] = m.hashes[pos], hash
			key, e.key = e.key, key
			val, e.value = e.value, val

			// Update current distance.
			dist = elemDist
		}

		// Increment position, wrap around on overflow.
		pos = int(uint32(pos+1) & m.mask)
		dist++
	}
}

// alloc elems according to currently set capacity.
func (m *HashMap) alloc() {
	m.elems = make([]hashElem, m.capacity)
	m.hashes = make([]uint32, m.capacity)
	m.threshold = (m.capacity * m.loadFactor) / 100
	m.mask = uint32(m.capacity - 1)
}

// grow doubles the capacity and reinserts all existing hashes & elements.
func (m *HashMap) grow() {
	// Copy old elements and hashes.
	elems, hashes := m.elems, m.hashes
	capacity := m.capacity

	// Double capacity & reallocate.
	m.capacity *= 2
	m.alloc()

	// Copy old elements to new hash/elem list.
	for i := 0; i < capacity; i++ {
		elem, hash := &elems[i], hashes[i]
		if hash == 0 {
			continue
		}
		m.insert(hash, elem.key, elem.value)
	}
}

// index returns the position of key in the hash map.
func (m *HashMap) index(key []byte) int {
	hash := m.hashKey(key)
	pos := int(hash & m.mask)

	dist := 0
	for {
		if m.hashes[pos] == 0 {
			return -1
		} else if dist > m.dist(m.hashes[pos], pos) {
			return -1
		} else if m.hashes[pos] == hash && bytes.Equal(m.elems[pos].key, key) {
			return pos
		}

		pos = int(uint32(pos+1) & m.mask)
		dist++
	}
}

// hashKey computes a hash of key. Hash is always non-zero.
func (m *HashMap) hashKey(key []byte) uint32 {
	h := murmur3.Sum32(key)
	if h == 0 {
		h = 1
	}
	return h
}

// Elem returns the i-th key/value pair of the hash map.
func (m *HashMap) Elem(i int) (key []byte, value interface{}) {
	if i >= len(m.elems) {
		return nil, nil
	}

	e := &m.elems[i]
	return e.key, e.value
}

// Len returns the number of key/values set in map.
func (m *HashMap) Len() int { return m.n }

// Cap returns the number of key/values set in map.
func (m *HashMap) Cap() int { return m.capacity }

// AverageProbeCount returns the average number of probes for each element.
func (m *HashMap) AverageProbeCount() float64 {
	var sum float64
	for i := 0; i < m.capacity; i++ {
		hash := m.hashes[i]
		if hash == 0 {
			continue
		}
		sum += float64(m.dist(hash, i))
	}
	return sum/float64(m.n) + 1.0
}

// dist returns the probe distance for a hash in a slot index.
func (m *HashMap) dist(hash uint32, i int) int {
	return int(uint32(i+m.capacity-int(hash&m.mask)) & m.mask)
}

type hashElem struct {
	key   []byte
	value interface{}
	hash  uint32
}

// Options represents initialization options that are passed to NewHashMap().
type Options struct {
	Capacity   int
	LoadFactor int
}

// DefaultOptions represents a default set of options to pass to NewHashMap().
var DefaultOptions = Options{
	Capacity:   256,
	LoadFactor: 90,
}

// pow2 returns the number that is the next highest power of 2.
// Returns v if it is a power of 2.
func pow2(v int) int {
	for i := 2; i < 1<<32; i *= 2 {
		if i >= v {
			return i
		}
	}
	panic("unreachable")
}
