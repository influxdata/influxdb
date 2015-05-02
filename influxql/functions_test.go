package influxql

import "testing"

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
