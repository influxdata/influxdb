package protocol

import (
	"code.google.com/p/goprotobuf/proto"
)

func UnmarshalPoint(data []byte) (point *Point, err error) {
	point = &Point{}
	err = proto.Unmarshal(data, point)
	return
}

func MarshalPoint(point *Point) (data []byte, err error) {
	return proto.Marshal(point)
}

func (self *Point) GetTimestampInMicroseconds() *int64 {
	return self.Timestamp
}

func (self *Point) SetTimestampInMicroseconds(t int64) {
	self.Timestamp = &t
}

func (self *FieldValue) GetValue() interface{} {
	if self.StringValue != nil {
		return *self.StringValue
	}

	if self.DoubleValue != nil {
		return *self.DoubleValue
	}

	if self.Int64Value != nil {
		return *self.Int64Value
	}

	if self.BoolValue != nil {
		return *self.BoolValue
	}

	// TODO: should we do something here ?
	return nil
}

func (self *Point) GetFieldValue(idx int) interface{} {
	return self.Values[idx].GetValue()
}
