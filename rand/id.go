package rand

import (
	"encoding/binary"
	"math/rand"
	"sync"

	"github.com/influxdata/influxdb/v2/kit/platform"
)

var _ platform.IDGenerator = (*OrgBucketID)(nil)

// OrgBucketID creates an id that does not have ascii
// backslash, commas, or spaces.  Used to create IDs for organizations
// and buckets.
//
// It is implemented without those characters because orgbucket
// pairs are placed in the old measurement field.  Measurement
// was interpreted as a string delimited with commas.  Therefore,
// to continue to use the underlying storage engine we need to
// sanitize ids.
//
// Safe for concurrent use by multiple goroutines.
type OrgBucketID struct {
	m   sync.Mutex
	src *rand.Rand
}

// NewOrgBucketID creates an influxdb.IDGenerator that creates
// random numbers seeded with seed.  Ascii backslash, comma,
// and space are manipulated by incrementing.
//
// Typically, seed with `time.Now().UnixNano()`
func NewOrgBucketID(seed int64) *OrgBucketID {
	return &OrgBucketID{
		src: rand.New(rand.NewSource(seed)),
	}
}

// Seed allows one to override the current seed.
// Typically, this override is done for tests.
func (r *OrgBucketID) Seed(seed int64) {
	r.m.Lock()
	r.src = rand.New(rand.NewSource(seed))
	r.m.Unlock()
}

// ID generates an ID that does not have backslashes, commas, or spaces.
func (r *OrgBucketID) ID() platform.ID {
	r.m.Lock()
	n := r.src.Uint64()
	r.m.Unlock()

	n = sanitize(n)
	return platform.ID(n)
}

func sanitize(n uint64) uint64 {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, n)
	for i := range b {
		switch b[i] {
		// these bytes must be remove here to prevent the need
		// to escape/unescape.  See the models package for
		// additional detail.
		//    \     ,     " "
		case 0x5C, 0x2C, 0x20:
			b[i] = b[i] + 1
		}
	}
	return binary.BigEndian.Uint64(b)
}
