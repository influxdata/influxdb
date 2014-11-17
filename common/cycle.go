package common

//import (
//	"github.com/deckarep/golang-set"
//)

func CycleNextOne(cycle []interface{}, start uint) (value interface{}, nextIdx uint) {
	length := uint(len(cycle))
	if start >= length {
		start = start % length
	}
	value = cycle[start]

	nextIdx = start + 1
	if nextIdx >= length {
		nextIdx = nextIdx % length
	}
	return
}

func CycleNextMany(cycle []interface{}, start, n uint) (value []interface{}, nextIdx uint) {
	for i := uint(0); i < n; i++ {
		res, newIdx := CycleNextOne(cycle, start+i)
		value = append(value, res)
		nextIdx = newIdx
	}
	return
}

//func CycleNextManySet(cycle []interface{}, start, n uint) (set mapset.Set, nextIdx uint) {
//	values := []interface{}{}
//	for i := uint(0); i < n; i++ {
//		res, newIdx := CycleNextOne(cycle, start+i)
//		values = append(values, res)
//		nextIdx = newIdx
//	}
//	set = mapset.NewSetFromSlice(values)
//	return
//}
