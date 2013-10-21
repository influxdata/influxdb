package protocol

import (
	"code.google.com/p/goprotobuf/proto"
	"regexp"
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

func NewMatcher(isRegex bool, name string) *Matcher {
	return &Matcher{
		Name:    &name,
		IsRegex: &isRegex,
	}
}

func (self *Matcher) Matches(name string) bool {
	if self.GetIsRegex() {
		matches, _ := regexp.MatchString(self.GetName(), name)
		return matches
	}

	return self.GetName() == name
}
