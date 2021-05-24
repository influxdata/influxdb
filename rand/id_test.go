package rand

import (
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/platform"
)

func TestOrgBucketID_ID(t *testing.T) {
	tests := []struct {
		name string
		seed int64
		want platform.ID
	}{
		{
			name: "when seeded with 6 the first random number contains characters",
			seed: 6,
			want: platform.ID(0xaddff35d7fe88f15),
		},
		{
			name: "when seeded with 1234567890 we get a random number without any bad chars",
			seed: 1234567890,
			want: platform.ID(0x8a95c1bf40518fee),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewOrgBucketID(tt.seed)
			if got := r.ID(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("OrgBucketID.ID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOrgBucketID_ID_sanitized(t *testing.T) {
	r := NewOrgBucketID(42)
	b := make([]byte, 8)
	for i := 0; i < 1000; i++ {
		id := r.ID()
		binary.LittleEndian.PutUint64(b, uint64(id))
		for j := range b {
			switch b[j] {
			case 0x5C, 0x2C, 0x20:
				t.Fatalf("unexpected bytes found in IDs")
			}
		}
	}
}
