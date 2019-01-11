package rhh

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"sort"
	"time"

	"github.com/cespare/xxhash"
	"github.com/prometheus/client_golang/prometheus"
)

// HashMap represents a hash map that implements Robin Hood Hashing.
// https://cs.uwaterloo.ca/research/tr/1986/CS-86-14.pdf
type HashMap struct {
	hashes []int64
	elems  []hashElem

	n          int64
	capacity   int64
	threshold  int64
	mask       int64
	loadFactor int

	tmpKey []byte

	tracker *rhhTracker
}

// NewHashMap initialises a new Hashmap with the provided options.
func NewHashMap(opt Options) *HashMap {
	if opt.Metrics == nil {
		opt.Metrics = NewMetrics("", "", nil)
	}

	m := &HashMap{
		capacity:   pow2(opt.Capacity), // Limited to 2^64.
		loadFactor: opt.LoadFactor,
		tracker:    newRHHTracker(opt.Metrics, opt.Labels),
	}
	m.tracker.enabled = opt.MetricsEnabled

	m.alloc()
	return m
}

// Reset clears the values in the map without deallocating the space.
func (m *HashMap) Reset() {
	for i := int64(0); i < m.capacity; i++ {
		m.hashes[i] = 0
		m.elems[i].reset()
	}
	m.n = 0
	m.tracker.SetSize(0)
}

// Get returns the value for a key from the Hashmap, or nil if no key exists.
func (m *HashMap) Get(key []byte) interface{} {
	var now time.Time
	var sample bool
	if rand.Float64() < 0.1 {
		now = time.Now()
		sample = true
	}

	i := m.index(key)

	if sample {
		m.tracker.ObserveGet(time.Since(now))
	}

	if i == -1 {
		m.tracker.IncGetMiss()
		return nil
	}
	m.tracker.IncGetHit()
	return m.elems[i].value
}

func (m *HashMap) put(key []byte, val interface{}, instrument bool) {
	var now time.Time
	var samplePut bool

	if instrument && rand.Float64() < 0.1 {
		now = time.Now()
		samplePut = true
	}

	// Grow the map if we've run out of slots.
	m.n++
	if m.n > m.threshold {
		m.grow()
	}

	// If the key was overwritten then decrement the size.
	overwritten := m.insert(HashKey(key), key, val)
	if instrument && samplePut {
		m.tracker.ObservePut(time.Since(now))
	}

	if overwritten {
		m.n--
		if instrument {
			m.tracker.IncPutHit()
		}
	} else if instrument {
		m.tracker.SetSize(uint64(m.n))
		m.tracker.SetLoadFactor(float64(m.n) / float64(m.capacity) * 100.0)
		m.tracker.IncPutMiss()
	}
}

// Put stores the value at key in the Hashmap, overwriting an existing value if
// one exists. If the maximum load of the Hashmap is reached, the Hashmap will
// first resize itself.
func (m *HashMap) Put(key []byte, val interface{}) {
	m.put(key, val, true)
}

// PutQuiet is equivalent to Put, but no instrumentation code is executed. It can
// be faster when many keys are being inserted into the Hashmap.
func (m *HashMap) PutQuiet(key []byte, val interface{}) {
	m.put(key, val, false)
}

func (m *HashMap) insert(hash int64, key []byte, val interface{}) (overwritten bool) {
	pos := hash & m.mask
	var dist int64

	var copied bool
	searchKey := key

	// Continue searching until we find an empty slot or lower probe distance.
	for {
		e := &m.elems[pos]

		// Empty slot found or matching key, insert and exit.
		match := bytes.Equal(m.elems[pos].key, searchKey)
		if m.hashes[pos] == 0 || match {
			m.hashes[pos] = hash
			e.hash, e.value = hash, val
			e.setKey(searchKey)
			return match
		}

		// If the existing elem has probed less than us, then swap places with
		// existing elem, and keep going to find another slot for that elem.
		elemDist := Dist(m.hashes[pos], pos, m.capacity)
		if elemDist < dist {
			// Swap with current position.
			hash, m.hashes[pos] = m.hashes[pos], hash
			val, e.value = e.value, val

			m.tmpKey = assign(m.tmpKey, e.key)
			e.setKey(searchKey)

			if !copied {
				searchKey = make([]byte, len(key))
				copy(searchKey, key)
				copied = true
			}

			searchKey = assign(searchKey, m.tmpKey)

			// Update current distance.
			dist = elemDist
		}

		// Increment position, wrap around on overflow.
		pos = (pos + 1) & m.mask
		dist++
	}
}

// alloc elems according to currently set capacity.
func (m *HashMap) alloc() {
	m.elems = make([]hashElem, m.capacity)
	m.hashes = make([]int64, m.capacity)
	m.threshold = (m.capacity * int64(m.loadFactor)) / 100
	m.mask = int64(m.capacity - 1)
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
	for i := int64(0); i < capacity; i++ {
		elem, hash := &elems[i], hashes[i]
		if hash == 0 {
			continue
		}
		m.insert(hash, elem.key, elem.value)
	}
}

// index returns the position of key in the hash map.
func (m *HashMap) index(key []byte) int64 {
	hash := HashKey(key)
	pos := hash & m.mask

	var dist int64
	for {
		if m.hashes[pos] == 0 {
			return -1
		} else if dist > Dist(m.hashes[pos], pos, m.capacity) {
			return -1
		} else if m.hashes[pos] == hash && bytes.Equal(m.elems[pos].key, key) {
			return pos
		}

		pos = (pos + 1) & m.mask
		dist++
	}
}

// Elem returns the i-th key/value pair of the hash map.
func (m *HashMap) Elem(i int64) (key []byte, value interface{}) {
	if i >= int64(len(m.elems)) {
		return nil, nil
	}

	e := &m.elems[i]
	return e.key, e.value
}

// Len returns the number of key/values set in map.
func (m *HashMap) Len() int64 { return m.n }

// Cap returns the number of key/values set in map.
func (m *HashMap) Cap() int64 { return m.capacity }

// AverageProbeCount returns the average number of probes for each element.
func (m *HashMap) AverageProbeCount() float64 {
	var sum float64
	for i := int64(0); i < m.capacity; i++ {
		hash := m.hashes[i]
		if hash == 0 {
			continue
		}
		sum += float64(Dist(hash, i, m.capacity))
	}
	return sum / (float64(m.n) + 1.0)
}

// Keys returns a list of sorted keys.
func (m *HashMap) Keys() [][]byte {
	a := make([][]byte, 0, m.Len())
	for i := int64(0); i < m.Cap(); i++ {
		k, v := m.Elem(i)
		if v == nil {
			continue
		}
		a = append(a, k)
	}
	sort.Sort(byteSlices(a))
	return a
}

// PrometheusCollectors returns the metrics associated with this hashmap.
func (m *HashMap) PrometheusCollectors() []prometheus.Collector {
	return m.tracker.metrics.PrometheusCollectors()
}

type rhhTracker struct {
	metrics *Metrics
	labels  prometheus.Labels
	enabled bool
}

// Labels returns a copy of the default labels used by the tracker's metrics.
// The returned map is safe for modification.
func (t *rhhTracker) Labels() prometheus.Labels {
	labels := make(prometheus.Labels, len(t.labels))
	for k, v := range t.labels {
		labels[k] = v
	}
	return labels
}

func newRHHTracker(metrics *Metrics, defaultLabels prometheus.Labels) *rhhTracker {
	return &rhhTracker{metrics: metrics, labels: defaultLabels, enabled: true}
}

func (t *rhhTracker) SetLoadFactor(load float64) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	t.metrics.LoadFactor.With(labels).Set(load)
}

func (t *rhhTracker) SetSize(sz uint64) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	t.metrics.Size.With(labels).Set(float64(sz))
}

func (t *rhhTracker) ObserveGet(d time.Duration) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	t.metrics.GetDuration.With(labels).Observe(float64(d.Nanoseconds()))
	t.metrics.LastGetDuration.With(labels).Set(float64(d.Nanoseconds()))
}

func (t *rhhTracker) ObservePut(d time.Duration) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	t.metrics.InsertDuration.With(labels).Observe(float64(d.Nanoseconds()))
	t.metrics.LastInsertDuration.With(labels).Set(float64(d.Nanoseconds()))
}

func (t *rhhTracker) SetGrowDuration(d time.Duration) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	t.metrics.LastGrowDuration.With(labels).Set(d.Seconds())
}

// TODO(edd): currently no safe way to calculate this concurrently.
func (t *rhhTracker) SetProbeCount(length float64) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	t.metrics.MeanProbeCount.With(labels).Set(length)
}

func (t *rhhTracker) incGet(status string) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	labels["status"] = status
	t.metrics.Gets.With(labels).Inc()
}

func (t *rhhTracker) IncGetHit()  { t.incGet("hit") }
func (t *rhhTracker) IncGetMiss() { t.incGet("miss") }

func (t *rhhTracker) incPut(status string) {
	if !t.enabled {
		return
	}

	labels := t.Labels()
	labels["status"] = status
	t.metrics.Puts.With(labels).Inc()
}

func (t *rhhTracker) IncPutHit()  { t.incPut("hit") }
func (t *rhhTracker) IncPutMiss() { t.incPut("miss") }

type hashElem struct {
	key   []byte
	value interface{}
	hash  int64
}

// reset clears the values in the element.
func (e *hashElem) reset() {
	e.key = e.key[:0]
	e.value = nil
	e.hash = 0
}

// setKey copies v to a key on e.
func (e *hashElem) setKey(v []byte) {
	e.key = assign(e.key, v)
}

// Options represents initialization options that are passed to NewHashMap().
type Options struct {
	Capacity       int64
	LoadFactor     int
	MetricsEnabled bool
	Metrics        *Metrics
	Labels         prometheus.Labels
}

// DefaultOptions represents a default set of options to pass to NewHashMap().
var DefaultOptions = Options{
	Capacity:       256,
	LoadFactor:     90,
	MetricsEnabled: true,
}

// HashKey computes a hash of key. Hash is always non-zero.
func HashKey(key []byte) int64 {
	h := int64(xxhash.Sum64(key))
	if h == 0 {
		h = 1
	} else if h < 0 {
		h = 0 - h
	}
	return h
}

// HashUint64 computes a hash of an int64. Hash is always non-zero.
func HashUint64(key uint64) int64 {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, key)
	return HashKey(buf)
}

// Dist returns the probe distance for a hash in a slot index.
// NOTE: Capacity must be a power of 2.
func Dist(hash, i, capacity int64) int64 {
	mask := capacity - 1
	dist := (i + capacity - (hash & mask)) & mask
	return dist
}

// pow2 returns the number that is the next highest power of 2.
// Returns v if it is a power of 2.
func pow2(v int64) int64 {
	for i := int64(2); i < 1<<62; i *= 2 {
		if i >= v {
			return i
		}
	}
	panic("unreachable")
}

func assign(x, v []byte) []byte {
	if cap(x) < len(v) {
		x = make([]byte, len(v))
	}
	x = x[:len(v)]
	copy(x, v)
	return x
}

type byteSlices [][]byte

func (a byteSlices) Len() int           { return len(a) }
func (a byteSlices) Less(i, j int) bool { return bytes.Compare(a[i], a[j]) == -1 }
func (a byteSlices) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
