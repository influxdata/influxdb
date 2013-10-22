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
