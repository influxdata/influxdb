package tsm1

import (
	"testing"
)

func Test_NewCache(t *testing.T) {
	c := NewCache(100)
	if c == nil {
		t.Fatalf("failed to create new cache")
	}

	if c.MaxSize() != 100 {
		t.Fatalf("new cache max size not correct")
	}
	if c.Size() != 0 {
		t.Fatalf("new cache size not correct")
	}
	if c.Checkpoint() != 0 {
		t.Fatalf("new checkpoint not correct")
	}
	return
}
