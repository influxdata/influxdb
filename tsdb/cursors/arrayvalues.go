package cursors

// MeanCountArray is too different to codegen easily

type MeanCountArray struct {
	Timestamps []int64
	Values0    []float64
	Values1    []int64
}

func NewMeanCountArrayLen(sz int) *MeanCountArray {
	return &MeanCountArray{
		Timestamps: make([]int64, sz),
		Values0:    make([]float64, sz),
		Values1:    make([]int64, sz),
	}
}

func (a *MeanCountArray) MinTime() int64 {
	return a.Timestamps[0]
}

func (a *MeanCountArray) MaxTime() int64 {
	return a.Timestamps[len(a.Timestamps)-1]
}

func (a *MeanCountArray) Len() int {
	return len(a.Timestamps)
}

// Sizes for each type are different and not codegen-able

func (a *MeanCountArray) Size() int {
	// size of timestamps + values
	return len(a.Timestamps)*8 + len(a.Values0)*8 + len(a.Values1)*8
}

func (a *FloatArray) Size() int {
	// size of timestamps + values
	return len(a.Timestamps)*8 + len(a.Values)*8
}

func (a *IntegerArray) Size() int {
	// size of timestamps + values
	return len(a.Timestamps)*8 + len(a.Values)*8
}

func (a *UnsignedArray) Size() int {
	// size of timestamps + values
	return len(a.Timestamps)*8 + len(a.Values)*8
}

func (a *StringArray) Size() int {
	sz := len(a.Timestamps) * 8
	for _, s := range a.Values {
		sz += len(s)
	}
	return sz
}

func (a *BooleanArray) Size() int {
	// size of timestamps + values
	return len(a.Timestamps)*8 + len(a.Values)
}
