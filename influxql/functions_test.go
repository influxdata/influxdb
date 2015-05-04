package influxql

import "testing"
import "sort"

type point struct {
	seriesID  uint64
	timestamp int64
	value     interface{}
}

type testIterator struct {
	values []point
}

func (t *testIterator) Next() (seriesID uint64, timestamp int64, value interface{}) {
	if len(t.values) > 0 {
		v := t.values[0]
		t.values = t.values[1:]
		return v.seriesID, v.timestamp, v.value
	}
	return 0, 0, nil
}

func TestMapMeanNoValues(t *testing.T) {
	iter := &testIterator{}
	if got := MapMean(iter); got != nil {
		t.Errorf("output mismatch: exp nil got %v", got)
	}
}

func TestMapMean(t *testing.T) {

	tests := []struct {
		input  []point
		output *meanMapOutput
	}{
		{ // Single point
			input: []point{
				point{0, 1, 1.0},
			},
			output: &meanMapOutput{1, 1},
		},
		{ // Two points
			input: []point{
				point{0, 1, 2.0},
				point{0, 2, 8.0},
			},
			output: &meanMapOutput{2, 5.0},
		},
	}

	for _, test := range tests {
		iter := &testIterator{
			values: test.input,
		}

		got := MapMean(iter)
		if got == nil {
			t.Fatalf("MapMean(%v): output mismatch: exp %v got %v", test.input, test.output, got)
		}

		if got.(*meanMapOutput).Count != test.output.Count || got.(*meanMapOutput).Mean != test.output.Mean {
			t.Errorf("output mismatch: exp %v got %v", test.output, got)
		}

	}
}
func TestInitializeMapFuncPercentile(t *testing.T) {
	// No args
	c := &Call{
		Name: "percentile",
		Args: []Expr{},
	}
	_, err := InitializeMapFunc(c)
	if err == nil {
		t.Errorf("InitializeMapFunc(%v) expected error. got nil", c)
	}

	if exp := "expected two arguments for percentile()"; err.Error() != exp {
		t.Errorf("InitializeMapFunc(%v) mismatch. exp %v got %v", c, exp, err.Error())
	}

	// No percentile arg
	c = &Call{
		Name: "percentile",
		Args: []Expr{
			&VarRef{Val: "field1"},
		},
	}

	_, err = InitializeMapFunc(c)
	if err == nil {
		t.Errorf("InitializeMapFunc(%v) expected error. got nil", c)
	}

	if exp := "expected two arguments for percentile()"; err.Error() != exp {
		t.Errorf("InitializeMapFunc(%v) mismatch. exp %v got %v", c, exp, err.Error())
	}
}

func TestInitializeReduceFuncPercentile(t *testing.T) {
	// No args
	c := &Call{
		Name: "percentile",
		Args: []Expr{},
	}
	_, err := InitializeReduceFunc(c)
	if err == nil {
		t.Errorf("InitializedReduceFunc(%v) expected error. got nil", c)
	}

	if exp := "expected float argument in percentile()"; err.Error() != exp {
		t.Errorf("InitializedReduceFunc(%v) mismatch. exp %v got %v", c, exp, err.Error())
	}

	// No percentile arg
	c = &Call{
		Name: "percentile",
		Args: []Expr{
			&VarRef{Val: "field1"},
		},
	}

	_, err = InitializeReduceFunc(c)
	if err == nil {
		t.Errorf("InitializedReduceFunc(%v) expected error. got nil", c)
	}

	if exp := "expected float argument in percentile()"; err.Error() != exp {
		t.Errorf("InitializedReduceFunc(%v) mismatch. exp %v got %v", c, exp, err.Error())
	}
}

func TestReducePercentileNil(t *testing.T) {

	// ReducePercentile should ignore nil values when calculating the percentile
	fn := ReducePercentile(100)
	input := []interface{}{
		nil,
	}

	got := fn(input)
	if got != nil {
		t.Fatalf("ReducePercentile(100) returned wrong type. exp nil got %v", got)
	}
}

var getSortedRangeData = []float64{
	60, 61, 62, 63, 64, 65, 66, 67, 68, 69,
	20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
	10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
	50, 51, 52, 53, 54, 55, 56, 57, 58, 59,
	30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
}

var getSortedRangeTests = []struct {
	name     string
	data     []float64
	start    int
	count    int
	expected []float64
}{
	{"first 5", getSortedRangeData, 0, 5, []float64{0, 1, 2, 3, 4}},
	{"0 length", getSortedRangeData, 8, 0, []float64{}},
	{"past end of data", getSortedRangeData, len(getSortedRangeData) - 3, 5, []float64{67, 68, 69}},
}

func TestGetSortedRange(t *testing.T) {
	for _, tt := range getSortedRangeTests {
		results := getSortedRange(tt.data, tt.start, tt.count)
		if len(results) != len(tt.expected) {
			t.Errorf("Test %s failed.  Expected getSortedRange to return %v but got %v", tt.name, tt.expected, results)
		}
		for i, point := range tt.expected {
			if point != results[i] {
				t.Errorf("Test %s failed. getSortedRange returned wrong result for index %v.  Expected %v but got %v", tt.name, i, point, results[i])
			}
		}
	}
}

var benchGetSortedRangeResults []float64

func BenchmarkGetSortedRangeByPivot(b *testing.B) {
	data := make([]float64, len(getSortedRangeData))
	var results []float64
	for i := 0; i < b.N; i++ {
		copy(data, getSortedRangeData)
		results = getSortedRange(data, 8, 15)
	}
	benchGetSortedRangeResults = results
}

func BenchmarkGetSortedRangeBySort(b *testing.B) {
	data := make([]float64, len(getSortedRangeData))
	var results []float64
	for i := 0; i < b.N; i++ {
		copy(data, getSortedRangeData)
		sort.Float64s(data)
		results = data[8:23]
	}
	benchGetSortedRangeResults = results
}
