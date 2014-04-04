package engine

type Group interface {
	HasTimestamp() bool
	GetTimestamp() int64
	GetValue(int) interface{}
	WithoutTimestamp() Group
	WithTimestamp(int64) Group
}

type NullGroup struct {
	Group
}

var ALL_GROUP_IDENTIFIER Group = NullGroup{}

func (self NullGroup) HasTimestamp() bool {
	return false
}

func (self NullGroup) GetTimestamp() int64 {
	return 0
}

func (self NullGroup) GetValue(idx int) interface{} {
	panic("invalid index")
}

func (self NullGroup) WithoutTimestamp() Group {
	return self
}

func (self NullGroup) WithTimestamp(timestamp int64) Group {
	return createGroup2(true, timestamp, self)
}

type Group2 struct {
	hasTimestamp bool
	value0       interface{}
	value1       Group
}

func createGroup2(hasTimestamp bool, value0 interface{}, value1 Group) Group {
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
		return self.value1
	}

	return self
}

func (self Group2) WithTimestamp(timestamp int64) Group {
	if self.hasTimestamp {
		panic("This group has timestamp already")
	}

	return createGroup2(true, timestamp, self)
}

func (self Group2) GetValue(idx int) interface{} {
	if idx == 0 {
		return self.value0
	}

	return self.value1.GetValue(idx - 1)
}
