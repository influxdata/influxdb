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
