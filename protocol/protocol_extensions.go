package protocol

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"

	"math"

	"code.google.com/p/goprotobuf/proto"
)

var String = proto.String
var Float64 = proto.Float64
var Int64 = proto.Int64

func DecodePoint(buff *bytes.Buffer) (point *Point, err error) {
	point = &Point{}
	err = proto.Unmarshal(buff.Bytes(), point)
	return
}

func (point *Point) Encode() (data []byte, err error) {
	return proto.Marshal(point)
}

func (self *Point) GetTimestampInMicroseconds() *int64 {
	return self.Timestamp
}

func (self *Point) SetTimestampInMicroseconds(t int64) {
	self.Timestamp = &t
}

// Returns the value as an interface and true for all values, except
// for infinite and NaN values
func (self *FieldValue) GetValue() (interface{}, bool) {
	if self.StringValue != nil {
		return *self.StringValue, true
	}

	if self.DoubleValue != nil {
		fv := *self.DoubleValue
		if math.IsNaN(fv) || math.IsInf(fv, 0) {
			return 0, false
		}
		return fv, true
	}

	if self.Int64Value != nil {
		return *self.Int64Value, true
	}

	if self.BoolValue != nil {
		return *self.BoolValue, true
	}

	// TODO: should we do something here ?
	return nil, true
}

func (self *Point) GetFieldValue(idx int) interface{} {
	v := self.Values[idx]
	// issue #27
	if v == nil {
		return nil
	}
	iv, _ := v.GetValue()
	return iv
}

func (self *Point) GetFieldValueAsString(idx int) string {
	if idx < 0 {
		return ""
	} else {
		pointValue := self.GetFieldValue(idx)

		switch value := pointValue.(type) {
		case int64:
			return strconv.FormatInt(value, 10)
		case float64:
			return strconv.FormatFloat(value, 'f', -1, 64)
		case string:
			return value
		default:
			return ""
		}
	}
}

func (self *Request) Size() int {
	return proto.Size(self)
}

func DecodeRequest(buff *bytes.Buffer) (request *Request, err error) {
	request = &Request{}
	err = proto.Unmarshal(buff.Bytes(), request)
	return
}

func (self *Request) GetDescription() string {
	switch t := self.GetType(); t {
	case Request_QUERY:
		return fmt.Sprintf("%s:%d [%s]", t, self.GetRequestNumber(), self.GetQuery())
	default:
		return fmt.Sprintf("%s:%d", t, self.GetRequestNumber())
	}
}

func (self *Request) Encode() (data []byte, err error) {
	return proto.Marshal(self)
}

func (self *Request) Decode(data []byte) error {
	return proto.Unmarshal(data, self)
}

func (self *Response) Size() int {
	return proto.Size(self)
}

func DecodeResponse(buff *bytes.Buffer) (response *Response, err error) {
	response = &Response{}
	err = proto.Unmarshal(buff.Bytes(), response)
	return
}

func (self *Response) Encode() (data []byte, err error) {
	return proto.Marshal(self)
}

type PointsCollection []*Point

func (s PointsCollection) Len() int      { return len(s) }
func (s PointsCollection) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

type ByPointTimeDesc struct{ PointsCollection }
type ByPointTimeAsc struct{ PointsCollection }

func (s ByPointTimeAsc) Less(i, j int) bool {
	if s.PointsCollection[i] != nil && s.PointsCollection[j] != nil {
		return *s.PointsCollection[i].Timestamp < *s.PointsCollection[j].Timestamp
	}
	return false
}
func (s ByPointTimeDesc) Less(i, j int) bool {
	if s.PointsCollection[i] != nil && s.PointsCollection[j] != nil {
		return *s.PointsCollection[i].Timestamp > *s.PointsCollection[j].Timestamp
	}
	return false
}

func (self *Series) GetFieldIndex(fieldName string) int {
	for index, field := range self.Fields {
		if field == fieldName {
			return index
		}
	}

	return -1
}

func (self *Series) SortPointsTimeAscending() {
	sort.Sort(ByPointTimeAsc{self.Points})
}

func (self *Series) SortPointsTimeDescending() {
	if self.Points != nil {
		sort.Sort(ByPointTimeDesc{self.Points})
	}
}

func (self *FieldValue) Equals(other *FieldValue) bool {
	if self.BoolValue != nil {
		if other.BoolValue == nil {
			return false
		}
		return *other.BoolValue == *self.BoolValue
	}
	if self.Int64Value != nil {
		if other.Int64Value == nil {
			return false
		}
		return *other.Int64Value == *self.Int64Value
	}
	if self.DoubleValue != nil {
		if other.DoubleValue == nil {
			return false
		}
		return *other.DoubleValue == *self.DoubleValue
	}
	if self.StringValue != nil {
		if other.StringValue == nil {
			return false
		}
		return *other.StringValue == *self.StringValue
	}
	return other.GetIsNull()
}

// defines total order on FieldValue, the following is true
// nil > false > true > ordered(int64) > ordered(float64) > ordered(string)
// where order is the total natural order of the given set of values
func (self *FieldValue) GreaterOrEqual(other *FieldValue) bool {
	if self.BoolValue != nil {
		if other.BoolValue == nil {
			return false
		}
		// true >= false, false >= false
		return *self.BoolValue || !*other.BoolValue
	}
	if self.Int64Value != nil {
		if other.Int64Value == nil {
			return other.BoolValue != nil
		}
		return *self.Int64Value >= *other.Int64Value
	}
	if self.DoubleValue != nil {
		if other.DoubleValue == nil {
			return other.BoolValue != nil || other.Int64Value != nil
		}
		return *self.DoubleValue >= *other.DoubleValue
	}
	if self.StringValue != nil {
		if other.StringValue == nil {
			return other.BoolValue != nil || other.Int64Value != nil || other.DoubleValue != nil
		}
		return *self.StringValue >= *other.StringValue
	}
	return true
}
