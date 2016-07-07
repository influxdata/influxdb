package stats

// Collect collects all the statistics in the View.
//
// If close is true, then the View is closed before the function returns.
//
// For example:
//
//     var stats []stats.Statistics = stats.Collect(stats.Root.Open(), true)
//
func Collect(v View, close bool) Collection {
	return (&collector{}).collect(v, close)
}

type collector struct {
	collected Collection
}

func (c *collector) collect(v View, close bool) Collection {
	c.collected = make(Collection, 0)
	v.Do(c.onEach)
	if close {
		v.Close()
	}
	return c.collected
}

func (c *collector) onEach(s Statistics) {
	c.collected = append(c.collected, s)
}
