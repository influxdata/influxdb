package gota

// RSI - Relative Strength Index (http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:relative_strength_index_rsi)
type RSI struct {
	emaUp   EMA
	emaDown EMA
	lastV   float64
}

// NewRSI constructs a new RSI.
func NewRSI(inTimePeriod int, warmType WarmupType) *RSI {
	ema := NewEMA(inTimePeriod+1, warmType)
	ema.alpha = float64(1) / float64(inTimePeriod)
	return &RSI{
		emaUp:   *ema,
		emaDown: *ema,
	}
}

// WarmCount returns the number of samples that must be provided for the algorithm to be fully "warmed".
func (rsi RSI) WarmCount() int {
	return rsi.emaUp.WarmCount()
}

// Warmed indicates whether the algorithm has enough data to generate accurate results.
func (rsi RSI) Warmed() bool {
	return rsi.emaUp.Warmed()
}

// Last returns the last output value.
func (rsi RSI) Last() float64 {
	return 100 - (100 / (1 + rsi.emaUp.Last()/rsi.emaDown.Last()))
}

// Add adds a new sample value to the algorithm and returns the computed value.
func (rsi *RSI) Add(v float64) float64 {
	var up float64
	var down float64
	if v > rsi.lastV {
		up = v - rsi.lastV
	} else if v < rsi.lastV {
		down = rsi.lastV - v
	}
	rsi.emaUp.Add(up)
	rsi.emaDown.Add(down)
	rsi.lastV = v
	return rsi.Last()
}
