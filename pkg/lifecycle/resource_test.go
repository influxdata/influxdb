package lifecycle

import (
	"runtime"
	"testing"
)

// TestReferenceLeak is only useful to test printing with the debug build tag.
func TestReferenceLeak(t *testing.T) {
	var res Resource
	res.Open()
	res.Acquire()
	runtime.GC()
}
