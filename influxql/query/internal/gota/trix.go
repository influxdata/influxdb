package gota

// Trix - TRIple Exponential average (http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:trix)
type TRIX struct {
	ema1  EMA
	ema2  EMA
	ema3  EMA
	last  float64
	count int
}

// NewTRIX constructs a new TRIX.
func NewTRIX(inTimePeriod int, warmType WarmupType) *TRIX {
	ema1 := NewEMA(inTimePeriod, warmType)
	ema2 := NewEMA(inTimePeriod, warmType)
	ema3 := NewEMA(inTimePeriod, warmType)
	return &TRIX{
		ema1: *ema1,
		ema2: *ema2,
		ema3: *ema3,
	}
}

// Add adds a new sample value to the algorithm and returns the computed value.
func (trix *TRIX) Add(v float64) float64 {
	cur := trix.ema1.Add(v)
	if trix.ema1.Warmed() || trix.ema1.warmType == WarmEMA {
		cur = trix.ema2.Add(cur)
		if trix.ema2.Warmed() || trix.ema2.warmType == WarmEMA {
			cur = trix.ema3.Add(cur)
		}
	}

	rate := ((cur / trix.last) - 1) * 100
	trix.last = cur
	if !trix.Warmed() && trix.ema3.Warmed() {
		trix.count++
	}
	return rate
}

// WarmCount returns the number of samples that must be provided for the algorithm to be fully "warmed".
func (trix *TRIX) WarmCount() int {
	if trix.ema1.warmType == WarmEMA {
		return trix.ema1.WarmCount() + 1
	}
	return trix.ema1.WarmCount()*3 + 1
}

// Warmed indicates whether the algorithm has enough data to generate accurate results.
func (trix *TRIX) Warmed() bool {
	return trix.count == 2
}
