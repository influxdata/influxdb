package engine

type Group interface {
	HasTimestamp() bool
	GetTimestamp() int64
	GetValue(int) interface{}
	WithoutTimestamp() Group
	WithTimestamp(int64) Group
}

type Group0 struct {
	Group
}

var ALL_GROUP_IDENTIFIER = Group0{}

func (self Group0) HasTimestamp() bool {
	return false
}

func (self Group0) GetTimestamp() int64 {
	return 0
}

func (self Group0) GetValue(idx int) interface{} {
	panic("invalid index")
}

func (self Group0) WithoutTimestamp() Group {
	return self
}

func (self Group0) WithTimestamp(timestamp int64) Group {
	return createGroup1(true, timestamp)
}

type Group1 struct {
	hasTimestamp bool
	value        interface{}
}

func createGroup1(hasTimestamp bool, value interface{}) Group1 {
	return Group1{hasTimestamp, value}
}

func (self Group1) HasTimestamp() bool {
	return self.hasTimestamp
}

func (self Group1) GetTimestamp() int64 {
	if self.hasTimestamp {
		return self.value.(int64)
	}

	return 0
}

func (self Group1) GetValue(idx int) interface{} {
	if idx != 0 {
		panic("invalid index")
	}
	return self.value
}

func (self Group1) WithoutTimestamp() Group {
	if self.hasTimestamp {
		return ALL_GROUP_IDENTIFIER
	}

	return self
}

func (self Group1) WithTimestamp(timestamp int64) Group {
	if self.hasTimestamp {
		panic("This group has timestamp already")
	}

	return createGroup2(true, timestamp, self.value)
}

type Group2 struct {
	hasTimestamp bool
	value0       interface{}
	value1       interface{}
}

func createGroup2(hasTimestamp bool, value0, value1 interface{}) Group2 {
	return Group2{hasTimestamp, value0, value1}
}

func (self Group2) HasTimestamp() bool {
	return self.hasTimestamp
}

func (self Group2) GetTimestamp() int64 {
	if self.hasTimestamp {
		return self.value0.(int64)
	}

	return 0
}

func (self Group2) WithoutTimestamp() Group {
	if self.hasTimestamp {
		return createGroup1(false, self.value1)
	}

	return self
}

func (self Group2) WithTimestamp(timestamp int64) Group {
	if self.hasTimestamp {
		panic("This group has timestamp already")
	}

	return createGroup3(true, timestamp, self.value0, self.value1)
}

func (self Group2) GetValue(idx int) interface{} {
	switch idx {
	case 0:
		return self.value0
	case 1:
		return self.value1
	default:
		panic("invalid index")
	}
}

type Group3 struct {
	hasTimestamp bool
	value0       interface{}
	value1       interface{}
	value2       interface{}
}

func createGroup3(hasTimestamp bool, value0, value1, value2 interface{}) Group3 {
	return Group3{hasTimestamp, value0, value1, value2}
}

func (self Group3) HasTimestamp() bool {
	return self.hasTimestamp
}

func (self Group3) GetTimestamp() int64 {
	if self.hasTimestamp {
		return self.value0.(int64)
	}

	return 0
}

func (self Group3) WithoutTimestamp() Group {
	if self.hasTimestamp {
		return createGroup1(false, self.value1)
	}

	return self
}

func (self Group3) WithTimestamp(timestamp int64) Group {
	if self.hasTimestamp {
		panic("This group has timestamp already")
	}

	panic("group with 4 columns isn't supported yet")
}

func (self Group3) GetValue(idx int) interface{} {
	switch idx {
	case 0:
		return self.value0
	case 1:
		return self.value1
	case 2:
		return self.value2
	default:
		panic("invalid index")
	}
}
