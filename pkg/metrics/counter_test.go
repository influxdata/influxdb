package metrics

import (
	"testing"
)

func TestCounter_Add(t *testing.T) {
	c := Counter{}
	c.Add(5)
	c.Add(5)
	if exp, got := int64(10), c.Value(); exp != got {
		t.Errorf("unexpected value; exp=%d, got=%d", exp, got)
	}
}
