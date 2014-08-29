package heap

import (
	"math/rand"
	"sort"
	"testing"
	"time"

	"launchpad.net/gocheck"
)

type HeapSuite struct{}

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) {
	gocheck.TestingT(t)
}

var _ = gocheck.Suite(&HeapSuite{})

func (_ *HeapSuite) SetUpSuite(c *gocheck.C) {
	rand.Seed(time.Now().Unix())
}

func randomInts(bubbleUp bool) []int {
	is := make([]int, 0, 100)
	for i := 0; i < 100; i++ {
		is = is[:i+1]
		is[i] = rand.Intn(1000)
		if bubbleUp {
			BubbleUp(sort.IntSlice(is), i)
		}
	}
	return is
}

func (_ *HeapSuite) TestHeapSort(c *gocheck.C) {
	for _, bubble := range []bool{true, false} {
		is := randomInts(bubble)
		temp := make([]int, len(is))
		copy(temp, is)

		if !bubble {
			Initialize(sort.IntSlice(is))
		}

		sorted := make([]int, len(is))
		for i := range sorted {
			sorted[i] = is[0]
			l := len(is)
			is, is[0] = is[:l-1], is[l-1]
			BubbleDown(sort.IntSlice(is), 0)
		}

		sort.Ints(temp)
		c.Assert(sorted, gocheck.DeepEquals, temp)
	}
}
