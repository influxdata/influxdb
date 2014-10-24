package common

import (
	. "launchpad.net/gocheck"
)

type CycleSuite struct{}

var _ = Suite(&CycleSuite{})

func sumIntAsInterface(values []interface{}) (res int) {
	res = 0
	for i := 0; i < len(values); i++ {
		res += values[i].(int)
	}
	return
}

// general Cycle -------------

func (self *CycleSuite) TestCycleNextOne_0(c *C) {
	// test get value from start index

	list := []interface{}{1, 2, 3}
	res, idx := CycleNextOne(list, 0)
	c.Log(res, idx, list)
	c.Assert(res, Equals, 1)
}

func (self *CycleSuite) TestCycleNextOne_1(c *C) {
	// test cycle  and returned idx never greater than size of cycle

	list := []interface{}{1, 2, 3}
	resList := []interface{}{}
	for i := uint(0); i < 6; i++ {
		res, idx := CycleNextOne(list, i)
		c.Log("IntCycleNextOne", i, res, idx)
		resList = append(resList, res.(int))

		// Why! no greater or lesser ?
		greater := idx > uint(len(list))
		c.Assert(greater, Equals, false)
	}
	c.Assert(resList[2], Equals, 3)
	c.Assert(resList[3], Equals, 1)

	sumRes := sumIntAsInterface(resList)
	c.Assert(sumRes, Equals, 12)
}

func (self *CycleSuite) TestCycleNextOne_2(c *C) {
	// test string value
	list := []interface{}{"string a", "string b", "string c"}
	res, idx := CycleNextOne(list, 0)
	c.Log(res, idx, list)
	c.Assert(res, Equals, "string a")
}

func (self *CycleSuite) TestCycleNextMany_0(c *C) {
	// test get multiple values at a time.

	list := []interface{}{1, 2, 3}
	sumRes, idx := 0, uint(0)
	for i := uint(0); i < 3; i++ {
		res, newIdx := CycleNextMany(list, idx, 2)
		idx = newIdx
		sumRes += sumIntAsInterface(res)
		c.Log(i, idx, sumRes, res)
	}
	c.Assert(sumRes, Equals, 12)
}

func (self *CycleSuite) TestCycleNextMany_1(c *C) {
	// test get multiple values in cycle

	list := []interface{}{1}
	sumRes, idx := 0, uint(0)
	allList := []interface{}{}

	for i := uint(0); i < 3; i++ {
		res, newIdx := CycleNextMany(list, idx, 2)
		idx = newIdx
		sumRes += sumIntAsInterface(res)
		c.Log(i, idx, sumRes, res)
		allList = append(allList, res...)
	}
	c.Log(allList)
	c.Assert(sumRes, Equals, 6)

}

//func (self *CycleSuite) TestCycleNextManySet_1(c *C) {
//	// test single value set
//
//	list := []interface{}{1}
//	sumRes, idx := 0, uint(0)
//	allList := []interface{}{}
//
//	for i := uint(0); i < 3; i++ {
//		res, newIdx := CycleNextManySet(list, idx, 5)
//		idx = newIdx
//		for v := range res.Iter() {
//			sumRes += v.(int)
//			allList = append(allList, v)
//		}
//		c.Log(i, idx, sumRes, res)
//	}
//	c.Log(allList)
//	c.Assert(sumRes, Equals, 3)
//}
