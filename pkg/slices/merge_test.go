package slices_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/pkg/slices"
)

func TestMergeSortedBytes(t *testing.T) {
	cases := []struct {
		Inputs [][][]byte
		Out    [][]byte
	}{
		{Inputs: [][][]byte{}},
		{Inputs: [][][]byte{toBytes(0)}, Out: toBytes(0)},
		{
			Inputs: [][][]byte{toBytes(2), [][]byte(nil), toBytes(2)},
			Out:    toBytes(2),
		},
		{
			Inputs: [][][]byte{toBytes(9), toBytes(1, 16, 16), toBytes(5, 10)},
			Out:    toBytes(1, 5, 9, 10, 16),
		},
		{
			Inputs: [][][]byte{toBytes(20), toBytes(16), toBytes(10)},
			Out:    toBytes(10, 16, 20),
		},
		{
			Inputs: [][][]byte{toBytes(2, 2, 2, 2, 2, 2, 2, 2)},
			Out:    toBytes(2),
		},
		{
			Inputs: [][][]byte{toBytes(2, 2, 2, 2, 2, 2, 2, 2), [][]byte(nil), [][]byte(nil), [][]byte(nil)},
			Out:    toBytes(2),
		},
		{
			Inputs: [][][]byte{toBytes(1, 2, 3, 4, 5), toBytes(1, 2, 3, 4, 5), toBytes(1, 2, 3, 4, 5)},
			Out:    toBytes(1, 2, 3, 4, 5),
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("Example %d", i+1), func(t *testing.T) {
			if got, exp := slices.MergeSortedBytes(c.Inputs...), c.Out; !reflect.DeepEqual(got, exp) {
				t.Fatalf("got %v, expected %v", got, exp)
			}
		})
	}
}

func toBytes(a ...int) [][]byte {
	var result [][]byte
	for _, v := range a {
		result = append(result, []byte{byte(v)})
	}
	return result
}

func TestMergeSortedInts(t *testing.T) {
	cases := []struct {
		Inputs [][]int64
		Out    []int64
	}{
		{Inputs: [][]int64{}},
		{Inputs: [][]int64{[]int64{0}}, Out: []int64{0}},
		{
			Inputs: [][]int64{[]int64{2}, []int64(nil), []int64{2}},
			Out:    []int64{2},
		},
		{
			Inputs: [][]int64{[]int64{9}, []int64{1, 16, 16}, []int64{5, 10}},
			Out:    []int64{1, 5, 9, 10, 16},
		},
		{
			Inputs: [][]int64{[]int64{20}, []int64{16}, []int64{10}},
			Out:    []int64{10, 16, 20},
		},
		{
			Inputs: [][]int64{[]int64{2, 2, 2, 2, 2, 2, 2, 2}},
			Out:    []int64{2},
		},
		{
			Inputs: [][]int64{[]int64{2, 2, 2, 2, 2, 2, 2, 2}, []int64(nil), []int64(nil), []int64(nil)},
			Out:    []int64{2},
		},
		{
			Inputs: [][]int64{[]int64{1, 2, 3, 4, 5}, []int64{1, 2, 3, 4, 5}, []int64{1, 2, 3, 4, 5}},
			Out:    []int64{1, 2, 3, 4, 5},
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("Example %d", i+1), func(t *testing.T) {
			if got, exp := slices.MergeSortedInts(c.Inputs...), c.Out; !reflect.DeepEqual(got, exp) {
				t.Fatalf("got %v, expected %v", got, exp)
			}
		})
	}
}
