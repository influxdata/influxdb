// Code generated by protoc-gen-gogo.
// source: snapshot_request.proto
// DO NOT EDIT!

package protobuf

import proto "github.com/gogo/protobuf/proto"
import math "math"

// discarding unused import gogoproto "code.google.com/p/gogoprotobuf/gogoproto/gogo.pb"

import io7 "io"
import fmt28 "fmt"
import code_google_com_p_gogoprotobuf_proto14 "github.com/gogo/protobuf/proto"

import fmt29 "fmt"
import strings14 "strings"
import reflect14 "reflect"

import fmt30 "fmt"
import strings15 "strings"
import code_google_com_p_gogoprotobuf_proto15 "github.com/gogo/protobuf/proto"
import sort7 "sort"
import strconv7 "strconv"
import reflect15 "reflect"

import fmt31 "fmt"
import bytes7 "bytes"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = math.Inf

type SnapshotRequest struct {
	LeaderName       *string `protobuf:"bytes,1,req" json:"LeaderName,omitempty"`
	LastIndex        *uint64 `protobuf:"varint,2,req" json:"LastIndex,omitempty"`
	LastTerm         *uint64 `protobuf:"varint,3,req" json:"LastTerm,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *SnapshotRequest) Reset()      { *m = SnapshotRequest{} }
func (*SnapshotRequest) ProtoMessage() {}

func (m *SnapshotRequest) GetLeaderName() string {
	if m != nil && m.LeaderName != nil {
		return *m.LeaderName
	}
	return ""
}

func (m *SnapshotRequest) GetLastIndex() uint64 {
	if m != nil && m.LastIndex != nil {
		return *m.LastIndex
	}
	return 0
}

func (m *SnapshotRequest) GetLastTerm() uint64 {
	if m != nil && m.LastTerm != nil {
		return *m.LastTerm
	}
	return 0
}

func init() {
}
func (m *SnapshotRequest) Unmarshal(data []byte) error {
	l := len(data)
	index := 0
	for index < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if index >= l {
				return io7.ErrUnexpectedEOF
			}
			b := data[index]
			index++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt28.Errorf("proto: wrong wireType = %d for field LeaderName", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io7.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := index + int(stringLen)
			if postIndex > l {
				return io7.ErrUnexpectedEOF
			}
			s := string(data[index:postIndex])
			m.LeaderName = &s
			index = postIndex
		case 2:
			if wireType != 0 {
				return fmt28.Errorf("proto: wrong wireType = %d for field LastIndex", wireType)
			}
			var v uint64
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io7.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				v |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.LastIndex = &v
		case 3:
			if wireType != 0 {
				return fmt28.Errorf("proto: wrong wireType = %d for field LastTerm", wireType)
			}
			var v uint64
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io7.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				v |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.LastTerm = &v
		default:
			var sizeOfWire int
			for {
				sizeOfWire++
				wire >>= 7
				if wire == 0 {
					break
				}
			}
			index -= sizeOfWire
			skippy, err := code_google_com_p_gogoprotobuf_proto14.Skip(data[index:])
			if err != nil {
				return err
			}
			if (index + skippy) > l {
				return io7.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, data[index:index+skippy]...)
			index += skippy
		}
	}
	return nil
}
func (this *SnapshotRequest) String() string {
	if this == nil {
		return "nil"
	}
	s := strings14.Join([]string{`&SnapshotRequest{`,
		`LeaderName:` + valueToStringSnapshotRequest(this.LeaderName) + `,`,
		`LastIndex:` + valueToStringSnapshotRequest(this.LastIndex) + `,`,
		`LastTerm:` + valueToStringSnapshotRequest(this.LastTerm) + `,`,
		`XXX_unrecognized:` + fmt29.Sprintf("%v", this.XXX_unrecognized) + `,`,
		`}`,
	}, "")
	return s
}
func valueToStringSnapshotRequest(v interface{}) string {
	rv := reflect14.ValueOf(v)
	if rv.IsNil() {
		return "nil"
	}
	pv := reflect14.Indirect(rv).Interface()
	return fmt29.Sprintf("*%v", pv)
}
func (m *SnapshotRequest) Size() (n int) {
	var l int
	_ = l
	if m.LeaderName != nil {
		l = len(*m.LeaderName)
		n += 1 + l + sovSnapshotRequest(uint64(l))
	}
	if m.LastIndex != nil {
		n += 1 + sovSnapshotRequest(uint64(*m.LastIndex))
	}
	if m.LastTerm != nil {
		n += 1 + sovSnapshotRequest(uint64(*m.LastTerm))
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func sovSnapshotRequest(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozSnapshotRequest(x uint64) (n int) {
	return sovSnapshotRequest(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func NewPopulatedSnapshotRequest(r randySnapshotRequest, easy bool) *SnapshotRequest {
	this := &SnapshotRequest{}
	v1 := randStringSnapshotRequest(r)
	this.LeaderName = &v1
	v2 := uint64(r.Uint32())
	this.LastIndex = &v2
	v3 := uint64(r.Uint32())
	this.LastTerm = &v3
	if !easy && r.Intn(10) != 0 {
		this.XXX_unrecognized = randUnrecognizedSnapshotRequest(r, 4)
	}
	return this
}

type randySnapshotRequest interface {
	Float32() float32
	Float64() float64
	Int63() int64
	Int31() int32
	Uint32() uint32
	Intn(n int) int
}

func randUTF8RuneSnapshotRequest(r randySnapshotRequest) rune {
	res := rune(r.Uint32() % 1112064)
	if 55296 <= res {
		res += 2047
	}
	return res
}
func randStringSnapshotRequest(r randySnapshotRequest) string {
	v4 := r.Intn(100)
	tmps := make([]rune, v4)
	for i := 0; i < v4; i++ {
		tmps[i] = randUTF8RuneSnapshotRequest(r)
	}
	return string(tmps)
}
func randUnrecognizedSnapshotRequest(r randySnapshotRequest, maxFieldNumber int) (data []byte) {
	l := r.Intn(5)
	for i := 0; i < l; i++ {
		wire := r.Intn(4)
		if wire == 3 {
			wire = 5
		}
		fieldNumber := maxFieldNumber + r.Intn(100)
		data = randFieldSnapshotRequest(data, r, fieldNumber, wire)
	}
	return data
}
func randFieldSnapshotRequest(data []byte, r randySnapshotRequest, fieldNumber int, wire int) []byte {
	key := uint32(fieldNumber)<<3 | uint32(wire)
	switch wire {
	case 0:
		data = encodeVarintPopulateSnapshotRequest(data, uint64(key))
		v5 := r.Int63()
		if r.Intn(2) == 0 {
			v5 *= -1
		}
		data = encodeVarintPopulateSnapshotRequest(data, uint64(v5))
	case 1:
		data = encodeVarintPopulateSnapshotRequest(data, uint64(key))
		data = append(data, byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)))
	case 2:
		data = encodeVarintPopulateSnapshotRequest(data, uint64(key))
		ll := r.Intn(100)
		data = encodeVarintPopulateSnapshotRequest(data, uint64(ll))
		for j := 0; j < ll; j++ {
			data = append(data, byte(r.Intn(256)))
		}
	default:
		data = encodeVarintPopulateSnapshotRequest(data, uint64(key))
		data = append(data, byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)), byte(r.Intn(256)))
	}
	return data
}
func encodeVarintPopulateSnapshotRequest(data []byte, v uint64) []byte {
	for v >= 1<<7 {
		data = append(data, uint8(uint64(v)&0x7f|0x80))
		v >>= 7
	}
	data = append(data, uint8(v))
	return data
}
func (m *SnapshotRequest) Marshal() (data []byte, err error) {
	size := m.Size()
	data = make([]byte, size)
	n, err := m.MarshalTo(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (m *SnapshotRequest) MarshalTo(data []byte) (n int, err error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.LeaderName != nil {
		data[i] = 0xa
		i++
		i = encodeVarintSnapshotRequest(data, i, uint64(len(*m.LeaderName)))
		i += copy(data[i:], *m.LeaderName)
	}
	if m.LastIndex != nil {
		data[i] = 0x10
		i++
		i = encodeVarintSnapshotRequest(data, i, uint64(*m.LastIndex))
	}
	if m.LastTerm != nil {
		data[i] = 0x18
		i++
		i = encodeVarintSnapshotRequest(data, i, uint64(*m.LastTerm))
	}
	if m.XXX_unrecognized != nil {
		i += copy(data[i:], m.XXX_unrecognized)
	}
	return i, nil
}
func encodeFixed64SnapshotRequest(data []byte, offset int, v uint64) int {
	data[offset] = uint8(v)
	data[offset+1] = uint8(v >> 8)
	data[offset+2] = uint8(v >> 16)
	data[offset+3] = uint8(v >> 24)
	data[offset+4] = uint8(v >> 32)
	data[offset+5] = uint8(v >> 40)
	data[offset+6] = uint8(v >> 48)
	data[offset+7] = uint8(v >> 56)
	return offset + 8
}
func encodeFixed32SnapshotRequest(data []byte, offset int, v uint32) int {
	data[offset] = uint8(v)
	data[offset+1] = uint8(v >> 8)
	data[offset+2] = uint8(v >> 16)
	data[offset+3] = uint8(v >> 24)
	return offset + 4
}
func encodeVarintSnapshotRequest(data []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		data[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	data[offset] = uint8(v)
	return offset + 1
}
func (this *SnapshotRequest) GoString() string {
	if this == nil {
		return "nil"
	}
	s := strings15.Join([]string{`&protobuf.SnapshotRequest{` + `LeaderName:` + valueToGoStringSnapshotRequest(this.LeaderName, "string"), `LastIndex:` + valueToGoStringSnapshotRequest(this.LastIndex, "uint64"), `LastTerm:` + valueToGoStringSnapshotRequest(this.LastTerm, "uint64"), `XXX_unrecognized:` + fmt30.Sprintf("%#v", this.XXX_unrecognized) + `}`}, ", ")
	return s
}
func valueToGoStringSnapshotRequest(v interface{}, typ string) string {
	rv := reflect15.ValueOf(v)
	if rv.IsNil() {
		return "nil"
	}
	pv := reflect15.Indirect(rv).Interface()
	return fmt30.Sprintf("func(v %v) *%v { return &v } ( %#v )", typ, typ, pv)
}
func extensionToGoStringSnapshotRequest(e map[int32]code_google_com_p_gogoprotobuf_proto15.Extension) string {
	if e == nil {
		return "nil"
	}
	s := "map[int32]proto.Extension{"
	keys := make([]int, 0, len(e))
	for k := range e {
		keys = append(keys, int(k))
	}
	sort7.Ints(keys)
	ss := []string{}
	for _, k := range keys {
		ss = append(ss, strconv7.Itoa(k)+": "+e[int32(k)].GoString())
	}
	s += strings15.Join(ss, ",") + "}"
	return s
}
func (this *SnapshotRequest) VerboseEqual(that interface{}) error {
	if that == nil {
		if this == nil {
			return nil
		}
		return fmt31.Errorf("that == nil && this != nil")
	}

	that1, ok := that.(*SnapshotRequest)
	if !ok {
		return fmt31.Errorf("that is not of type *SnapshotRequest")
	}
	if that1 == nil {
		if this == nil {
			return nil
		}
		return fmt31.Errorf("that is type *SnapshotRequest but is nil && this != nil")
	} else if this == nil {
		return fmt31.Errorf("that is type *SnapshotRequestbut is not nil && this == nil")
	}
	if this.LeaderName != nil && that1.LeaderName != nil {
		if *this.LeaderName != *that1.LeaderName {
			return fmt31.Errorf("LeaderName this(%v) Not Equal that(%v)", *this.LeaderName, *that1.LeaderName)
		}
	} else if this.LeaderName != nil {
		return fmt31.Errorf("this.LeaderName == nil && that.LeaderName != nil")
	} else if that1.LeaderName != nil {
		return fmt31.Errorf("LeaderName this(%v) Not Equal that(%v)", this.LeaderName, that1.LeaderName)
	}
	if this.LastIndex != nil && that1.LastIndex != nil {
		if *this.LastIndex != *that1.LastIndex {
			return fmt31.Errorf("LastIndex this(%v) Not Equal that(%v)", *this.LastIndex, *that1.LastIndex)
		}
	} else if this.LastIndex != nil {
		return fmt31.Errorf("this.LastIndex == nil && that.LastIndex != nil")
	} else if that1.LastIndex != nil {
		return fmt31.Errorf("LastIndex this(%v) Not Equal that(%v)", this.LastIndex, that1.LastIndex)
	}
	if this.LastTerm != nil && that1.LastTerm != nil {
		if *this.LastTerm != *that1.LastTerm {
			return fmt31.Errorf("LastTerm this(%v) Not Equal that(%v)", *this.LastTerm, *that1.LastTerm)
		}
	} else if this.LastTerm != nil {
		return fmt31.Errorf("this.LastTerm == nil && that.LastTerm != nil")
	} else if that1.LastTerm != nil {
		return fmt31.Errorf("LastTerm this(%v) Not Equal that(%v)", this.LastTerm, that1.LastTerm)
	}
	if !bytes7.Equal(this.XXX_unrecognized, that1.XXX_unrecognized) {
		return fmt31.Errorf("XXX_unrecognized this(%v) Not Equal that(%v)", this.XXX_unrecognized, that1.XXX_unrecognized)
	}
	return nil
}
func (this *SnapshotRequest) Equal(that interface{}) bool {
	if that == nil {
		if this == nil {
			return true
		}
		return false
	}

	that1, ok := that.(*SnapshotRequest)
	if !ok {
		return false
	}
	if that1 == nil {
		if this == nil {
			return true
		}
		return false
	} else if this == nil {
		return false
	}
	if this.LeaderName != nil && that1.LeaderName != nil {
		if *this.LeaderName != *that1.LeaderName {
			return false
		}
	} else if this.LeaderName != nil {
		return false
	} else if that1.LeaderName != nil {
		return false
	}
	if this.LastIndex != nil && that1.LastIndex != nil {
		if *this.LastIndex != *that1.LastIndex {
			return false
		}
	} else if this.LastIndex != nil {
		return false
	} else if that1.LastIndex != nil {
		return false
	}
	if this.LastTerm != nil && that1.LastTerm != nil {
		if *this.LastTerm != *that1.LastTerm {
			return false
		}
	} else if this.LastTerm != nil {
		return false
	} else if that1.LastTerm != nil {
		return false
	}
	if !bytes7.Equal(this.XXX_unrecognized, that1.XXX_unrecognized) {
		return false
	}
	return true
}
