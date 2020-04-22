package gota

import (
	"math"
)

// KER - Kaufman's Efficiency Ratio (http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:kaufman_s_adaptive_moving_average#efficiency_ratio_er)
type KER struct {
	points []kerPoint
	noise  float64
	count  int
	idx    int // index of newest point
}

type kerPoint struct {
	price float64
	diff  float64
}

// NewKER constructs a new KER.
func NewKER(inTimePeriod int) *KER {
	return &KER{
		points: make([]kerPoint, inTimePeriod),
	}
}

// WarmCount returns the number of samples that must be provided for the algorithm to be fully "warmed".
func (ker *KER) WarmCount() int {
	return len(ker.points)
}

// Add adds a new sample value to the algorithm and returns the computed value.
func (ker *KER) Add(v float64) float64 {
	//TODO this does not return a sensible value if not warmed.
	n := len(ker.points)
	idxOldest := ker.idx + 1
	if idxOldest >= n {
		idxOldest = 0
	}

	signal := math.Abs(v - ker.points[idxOldest].price)

	kp := kerPoint{
		price: v,
		diff:  math.Abs(v - ker.points[ker.idx].price),
	}
	ker.noise -= ker.points[idxOldest].diff
	ker.noise += kp.diff
	noise := ker.noise

	ker.idx = idxOldest
	ker.points[ker.idx] = kp

	if !ker.Warmed() {
		ker.count++
	}

	if signal == 0 || noise == 0 {
		return 0
	}
	return signal / noise
}

// Warmed indicates whether the algorithm has enough data to generate accurate results.
func (ker *KER) Warmed() bool {
	return ker.count == len(ker.points)+1
}

// KAMA - Kaufman's Adaptive Moving Average (http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:kaufman_s_adaptive_moving_average)
type KAMA struct {
	ker  KER
	last float64
}

// NewKAMA constructs a new KAMA.
func NewKAMA(inTimePeriod int) *KAMA {
	ker := NewKER(inTimePeriod)
	return &KAMA{
		ker: *ker,
	}
}

// WarmCount returns the number of samples that must be provided for the algorithm to be fully "warmed".
func (kama *KAMA) WarmCount() int {
	return kama.ker.WarmCount()
}

// Add adds a new sample value to the algorithm and returns the computed value.
func (kama *KAMA) Add(v float64) float64 {
	if !kama.Warmed() {
		/*
			// initialize with a simple moving average
			kama.last = 0
			for _, v := range kama.ker.points[:kama.ker.count] {
				kama.last += v
			}
			kama.last /= float64(kama.ker.count + 1)
		*/
		// initialize with the last value
		kama.last = kama.ker.points[kama.ker.idx].price
	}

	er := kama.ker.Add(v)
	sc := math.Pow(er*(2.0/(2.0+1.0)-2.0/(30.0+1.0))+2.0/(30.0+1.0), 2)

	kama.last = kama.last + sc*(v-kama.last)
	return kama.last
}

// Warmed indicates whether the algorithm has enough data to generate accurate results.
func (kama *KAMA) Warmed() bool {
	return kama.ker.Warmed()
}
