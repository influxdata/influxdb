package tsm1

import (
	"bytes"
	"fmt"
	"testing"
)

func TestMergeSeriesKey_Single(t *testing.T) {
	a := make(chan seriesKey, 5)
	for i := 0; i < cap(a); i++ {
		a <- seriesKey{key: []byte(fmt.Sprintf("%d", i))}
	}

	merged := merge(a)
	close(a)

	exp := []string{"0", "1", "2", "3", "4"}
	for v := range merged {
		if got, exp := v, exp[0]; !bytes.Equal(got.key, []byte(exp)) {
			t.Fatalf("value mismatch: got %v, exp %v", got, exp)
		}
		exp = exp[1:]
	}

	if len(exp) > 0 {
		t.Fatalf("missed values: %v", exp)
	}
}

func TestMergeSeriesKey_Nil(t *testing.T) {
	merged := merge(nil)

	for v := range merged {
		t.Fatalf("value mismatch: got %v, exp nil", v)
	}

	merged = merge(nil, nil)
	for v := range merged {
		t.Fatalf("value mismatch: got %v, exp nil", v)
	}

}

func TestMergeSeriesKey_Duplicates(t *testing.T) {
	a := make(chan seriesKey, 5)
	b := make(chan seriesKey, 5)

	for i := 0; i < cap(a); i++ {
		a <- seriesKey{key: []byte(fmt.Sprintf("%d", i))}
		b <- seriesKey{key: []byte(fmt.Sprintf("%d", i))}
	}

	merged := merge(a, b)
	close(a)
	close(b)

	exp := []string{"0", "1", "2", "3", "4"}
	for v := range merged {
		if len(exp) == 0 {
			t.Fatalf("more values than expected: got %v", v)
		}

		if got, exp := v, exp[0]; !bytes.Equal(got.key, []byte(exp)) {
			t.Fatalf("value mismatch: got %v, exp %v", got, exp)
		}

		exp = exp[1:]
	}

	if len(exp) > 0 {
		t.Fatalf("missed values: %v", exp)
	}
}

func TestMergeSeriesKey_Alternating(t *testing.T) {
	a := make(chan seriesKey, 2)
	b := make(chan seriesKey, 2)

	for i := 0; i < cap(a); i++ {
		a <- seriesKey{key: []byte(fmt.Sprintf("%d", i*2))}
		b <- seriesKey{key: []byte(fmt.Sprintf("%d", i*2+1))}
	}

	merged := merge(a, b)
	close(a)
	close(b)

	exp := []string{"0", "1", "2", "3"}
	for v := range merged {
		if len(exp) == 0 {
			t.Fatalf("more values than expected: got %v", v)
		}

		if got, exp := v, exp[0]; !bytes.Equal(got.key, []byte(exp)) {
			t.Fatalf("value mismatch: got %v, exp %v", string(got.key), exp)
		}
		exp = exp[1:]
	}

	if len(exp) > 0 {
		t.Fatalf("missed values: %v", exp)
	}
}

func TestMergeSeriesKey_AlternatingDuplicates(t *testing.T) {
	a := make(chan seriesKey, 2)
	b := make(chan seriesKey, 2)
	c := make(chan seriesKey, 2)

	for i := 0; i < cap(a); i++ {
		a <- seriesKey{key: []byte(fmt.Sprintf("%d", i*2))}
		b <- seriesKey{key: []byte(fmt.Sprintf("%d", i*2+1))}
		c <- seriesKey{key: []byte(fmt.Sprintf("%d", i*2))}
	}

	merged := merge(a, b, c)
	close(a)
	close(b)
	close(c)

	exp := []string{"0", "1", "2", "3"}
	for v := range merged {
		if len(exp) == 0 {
			t.Fatalf("more values than expected: got %v", v)
		}

		if got, exp := v, exp[0]; !bytes.Equal(got.key, []byte(exp)) {
			t.Fatalf("value mismatch: got %v, exp %v", string(got.key), exp)
		}
		exp = exp[1:]
	}

	if len(exp) > 0 {
		t.Fatalf("missed values: %v", exp)
	}
}

func TestMergeSeriesKey_Unbuffered(t *testing.T) {
	a := make(chan seriesKey)
	b := make(chan seriesKey)

	go func() {
		for i := 0; i < 2; i++ {
			a <- seriesKey{key: []byte(fmt.Sprintf("%d", i*2))}
		}
		close(a)
	}()

	go func() {
		for i := 0; i < 2; i++ {
			b <- seriesKey{key: []byte(fmt.Sprintf("%d", i*2+1))}
		}
		close(b)
	}()

	merged := merge(a, b)

	exp := []string{"0", "1", "2", "3"}
	for v := range merged {
		if len(exp) == 0 {
			t.Fatalf("more values than expected: got %v", v)
		}

		if got, exp := v, exp[0]; !bytes.Equal(got.key, []byte(exp)) {
			t.Fatalf("value mismatch: got %v, exp %v", string(got.key), exp)
		}
		exp = exp[1:]
	}

	if len(exp) > 0 {
		t.Fatalf("missed values: %v", exp)
	}
}

func TestMergeSeriesKey_OneEmpty(t *testing.T) {
	a := make(chan seriesKey)
	b := make(chan seriesKey)

	go func() {
		for i := 0; i < 2; i++ {
			a <- seriesKey{key: []byte(fmt.Sprintf("%d", i*2))}
		}
		close(a)
	}()

	close(b)
	merged := merge(a, b)

	exp := []string{"0", "2"}
	for v := range merged {
		if len(exp) == 0 {
			t.Fatalf("more values than expected: got %v", v)
		}

		if got, exp := v, exp[0]; !bytes.Equal(got.key, []byte(exp)) {
			t.Fatalf("value mismatch: got %v, exp %v", got, exp)
		}
		exp = exp[1:]
	}

	if len(exp) > 0 {
		t.Fatalf("missed values: %v", exp)
	}
}

func TestMergeSeriesKey_Overlapping(t *testing.T) {
	a := make(chan seriesKey)
	b := make(chan seriesKey)
	c := make(chan seriesKey)

	go func() {
		for i := 0; i < 3; i++ {
			a <- seriesKey{key: []byte(fmt.Sprintf("%d", i))}
		}
		close(a)
	}()

	go func() {
		for i := 4; i < 7; i++ {
			b <- seriesKey{key: []byte(fmt.Sprintf("%d", i))}
		}
		close(b)
	}()

	go func() {
		for i := 0; i < 9; i++ {
			c <- seriesKey{key: []byte(fmt.Sprintf("%d", i))}
		}
		close(c)
	}()
	merged := merge(a, b, c)

	exp := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8"}
	for v := range merged {
		if len(exp) == 0 {
			t.Fatalf("more values than expected: got %v", v)
		}

		if got, exp := v, exp[0]; !bytes.Equal(got.key, []byte(exp)) {
			t.Fatalf("value mismatch: got %v, exp %v", string(got.key), exp)
		}
		exp = exp[1:]
	}

	if len(exp) > 0 {
		t.Fatalf("missed values: %v", exp)
	}
}
