package gota

// CMO - Chande Momentum Oscillator (https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/cmo)
type CMO struct {
	points  []cmoPoint
	sumUp   float64
	sumDown float64
	count   int
	idx     int // index of newest point
}

type cmoPoint struct {
	price float64
	diff  float64
}

// NewCMO constructs a new CMO.
func NewCMO(inTimePeriod int) *CMO {
	return &CMO{
		points: make([]cmoPoint, inTimePeriod-1),
	}
}

// WarmCount returns the number of samples that must be provided for the algorithm to be fully "warmed".
func (cmo *CMO) WarmCount() int {
	return len(cmo.points)
}

// Add adds a new sample value to the algorithm and returns the computed value.
func (cmo *CMO) Add(v float64) float64 {
	idxOldest := cmo.idx + 1
	if idxOldest == len(cmo.points) {
		idxOldest = 0
	}

	var diff float64
	if cmo.count != 0 {
		prev := cmo.points[cmo.idx]
		diff = v - prev.price
		if diff > 0 {
			cmo.sumUp += diff
		} else if diff < 0 {
			cmo.sumDown -= diff
		}
	}

	var outV float64
	if cmo.sumUp != 0 || cmo.sumDown != 0 {
		outV = 100.0 * ((cmo.sumUp - cmo.sumDown) / (cmo.sumUp + cmo.sumDown))
	}

	oldest := cmo.points[idxOldest]
	//NOTE: because we're just adding and subtracting the difference, and not recalculating sumUp/sumDown using cmo.points[].price, it's possible for imprecision to creep in over time. Not sure how significant this is going to be, but if we want to fix it, we could recalculate it from scratch every N points.
	if oldest.diff > 0 {
		cmo.sumUp -= oldest.diff
	} else if oldest.diff < 0 {
		cmo.sumDown += oldest.diff
	}

	p := cmoPoint{
		price: v,
		diff:  diff,
	}
	cmo.points[idxOldest] = p
	cmo.idx = idxOldest

	if !cmo.Warmed() {
		cmo.count++
	}

	return outV
}

// Warmed indicates whether the algorithm has enough data to generate accurate results.
func (cmo *CMO) Warmed() bool {
	return cmo.count == len(cmo.points)+2
}

// CMOS is a smoothed version of the Chande Momentum Oscillator.
// This is the version of CMO utilized by ta-lib.
type CMOS struct {
	emaUp   EMA
	emaDown EMA
	lastV   float64
}

// NewCMOS constructs a new CMOS.
func NewCMOS(inTimePeriod int, warmType WarmupType) *CMOS {
	ema := NewEMA(inTimePeriod+1, warmType)
	ema.alpha = float64(1) / float64(inTimePeriod)
	return &CMOS{
		emaUp:   *ema,
		emaDown: *ema,
	}
}

// WarmCount returns the number of samples that must be provided for the algorithm to be fully "warmed".
func (cmos CMOS) WarmCount() int {
	return cmos.emaUp.WarmCount()
}

// Warmed indicates whether the algorithm has enough data to generate accurate results.
func (cmos CMOS) Warmed() bool {
	return cmos.emaUp.Warmed()
}

// Last returns the last output value.
func (cmos CMOS) Last() float64 {
	up := cmos.emaUp.Last()
	down := cmos.emaDown.Last()
	return 100.0 * ((up - down) / (up + down))
}

// Add adds a new sample value to the algorithm and returns the computed value.
func (cmos *CMOS) Add(v float64) float64 {
	var up float64
	var down float64
	if v > cmos.lastV {
		up = v - cmos.lastV
	} else if v < cmos.lastV {
		down = cmos.lastV - v
	}
	cmos.emaUp.Add(up)
	cmos.emaDown.Add(down)
	cmos.lastV = v
	return cmos.Last()
}
