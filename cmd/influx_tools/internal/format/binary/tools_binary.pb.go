// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.34.1
// 	protoc        v5.29.2
// source: tools_binary.proto

package binary

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type FieldType int32

const (
	FieldType_FloatFieldType    FieldType = 0
	FieldType_IntegerFieldType  FieldType = 1
	FieldType_UnsignedFieldType FieldType = 2
	FieldType_BooleanFieldType  FieldType = 3
	FieldType_StringFieldType   FieldType = 4
)

// Enum value maps for FieldType.
var (
	FieldType_name = map[int32]string{
		0: "FloatFieldType",
		1: "IntegerFieldType",
		2: "UnsignedFieldType",
		3: "BooleanFieldType",
		4: "StringFieldType",
	}
	FieldType_value = map[string]int32{
		"FloatFieldType":    0,
		"IntegerFieldType":  1,
		"UnsignedFieldType": 2,
		"BooleanFieldType":  3,
		"StringFieldType":   4,
	}
)

func (x FieldType) Enum() *FieldType {
	p := new(FieldType)
	*p = x
	return p
}

func (x FieldType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (FieldType) Descriptor() protoreflect.EnumDescriptor {
	return file_tools_binary_proto_enumTypes[0].Descriptor()
}

func (FieldType) Type() protoreflect.EnumType {
	return &file_tools_binary_proto_enumTypes[0]
}

func (x FieldType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use FieldType.Descriptor instead.
func (FieldType) EnumDescriptor() ([]byte, []int) {
	return file_tools_binary_proto_rawDescGZIP(), []int{0}
}

type Header_Version int32

const (
	Header_Version0 Header_Version = 0
)

// Enum value maps for Header_Version.
var (
	Header_Version_name = map[int32]string{
		0: "Version0",
	}
	Header_Version_value = map[string]int32{
		"Version0": 0,
	}
)

func (x Header_Version) Enum() *Header_Version {
	p := new(Header_Version)
	*p = x
	return p
}

func (x Header_Version) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Header_Version) Descriptor() protoreflect.EnumDescriptor {
	return file_tools_binary_proto_enumTypes[1].Descriptor()
}

func (Header_Version) Type() protoreflect.EnumType {
	return &file_tools_binary_proto_enumTypes[1]
}

func (x Header_Version) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Header_Version.Descriptor instead.
func (Header_Version) EnumDescriptor() ([]byte, []int) {
	return file_tools_binary_proto_rawDescGZIP(), []int{0, 0}
}

type Header struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Version         Header_Version `protobuf:"varint,1,opt,name=version,proto3,enum=binary.Header_Version" json:"version,omitempty"`
	Database        string         `protobuf:"bytes,2,opt,name=database,proto3" json:"database,omitempty"`
	RetentionPolicy string         `protobuf:"bytes,3,opt,name=retention_policy,json=retentionPolicy,proto3" json:"retention_policy,omitempty"`
	ShardDuration   int64          `protobuf:"varint,4,opt,name=shard_duration,json=shardDuration,proto3" json:"shard_duration,omitempty"`
}

func (x *Header) Reset() {
	*x = Header{}
	if protoimpl.UnsafeEnabled {
		mi := &file_tools_binary_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Header) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Header) ProtoMessage() {}

func (x *Header) ProtoReflect() protoreflect.Message {
	mi := &file_tools_binary_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Header.ProtoReflect.Descriptor instead.
func (*Header) Descriptor() ([]byte, []int) {
	return file_tools_binary_proto_rawDescGZIP(), []int{0}
}

func (x *Header) GetVersion() Header_Version {
	if x != nil {
		return x.Version
	}
	return Header_Version0
}

func (x *Header) GetDatabase() string {
	if x != nil {
		return x.Database
	}
	return ""
}

func (x *Header) GetRetentionPolicy() string {
	if x != nil {
		return x.RetentionPolicy
	}
	return ""
}

func (x *Header) GetShardDuration() int64 {
	if x != nil {
		return x.ShardDuration
	}
	return 0
}

type BucketHeader struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Start int64 `protobuf:"fixed64,1,opt,name=start,proto3" json:"start,omitempty"`
	End   int64 `protobuf:"fixed64,2,opt,name=end,proto3" json:"end,omitempty"`
}

func (x *BucketHeader) Reset() {
	*x = BucketHeader{}
	if protoimpl.UnsafeEnabled {
		mi := &file_tools_binary_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BucketHeader) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BucketHeader) ProtoMessage() {}

func (x *BucketHeader) ProtoReflect() protoreflect.Message {
	mi := &file_tools_binary_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BucketHeader.ProtoReflect.Descriptor instead.
func (*BucketHeader) Descriptor() ([]byte, []int) {
	return file_tools_binary_proto_rawDescGZIP(), []int{1}
}

func (x *BucketHeader) GetStart() int64 {
	if x != nil {
		return x.Start
	}
	return 0
}

func (x *BucketHeader) GetEnd() int64 {
	if x != nil {
		return x.End
	}
	return 0
}

type BucketFooter struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *BucketFooter) Reset() {
	*x = BucketFooter{}
	if protoimpl.UnsafeEnabled {
		mi := &file_tools_binary_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BucketFooter) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BucketFooter) ProtoMessage() {}

func (x *BucketFooter) ProtoReflect() protoreflect.Message {
	mi := &file_tools_binary_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BucketFooter.ProtoReflect.Descriptor instead.
func (*BucketFooter) Descriptor() ([]byte, []int) {
	return file_tools_binary_proto_rawDescGZIP(), []int{2}
}

type FloatPoints struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Timestamps []int64   `protobuf:"fixed64,1,rep,packed,name=timestamps,proto3" json:"timestamps,omitempty"`
	Values     []float64 `protobuf:"fixed64,2,rep,packed,name=values,proto3" json:"values,omitempty"`
}

func (x *FloatPoints) Reset() {
	*x = FloatPoints{}
	if protoimpl.UnsafeEnabled {
		mi := &file_tools_binary_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *FloatPoints) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FloatPoints) ProtoMessage() {}

func (x *FloatPoints) ProtoReflect() protoreflect.Message {
	mi := &file_tools_binary_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FloatPoints.ProtoReflect.Descriptor instead.
func (*FloatPoints) Descriptor() ([]byte, []int) {
	return file_tools_binary_proto_rawDescGZIP(), []int{3}
}

func (x *FloatPoints) GetTimestamps() []int64 {
	if x != nil {
		return x.Timestamps
	}
	return nil
}

func (x *FloatPoints) GetValues() []float64 {
	if x != nil {
		return x.Values
	}
	return nil
}

type IntegerPoints struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Timestamps []int64 `protobuf:"fixed64,1,rep,packed,name=timestamps,proto3" json:"timestamps,omitempty"`
	Values     []int64 `protobuf:"varint,2,rep,packed,name=values,proto3" json:"values,omitempty"`
}

func (x *IntegerPoints) Reset() {
	*x = IntegerPoints{}
	if protoimpl.UnsafeEnabled {
		mi := &file_tools_binary_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *IntegerPoints) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*IntegerPoints) ProtoMessage() {}

func (x *IntegerPoints) ProtoReflect() protoreflect.Message {
	mi := &file_tools_binary_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use IntegerPoints.ProtoReflect.Descriptor instead.
func (*IntegerPoints) Descriptor() ([]byte, []int) {
	return file_tools_binary_proto_rawDescGZIP(), []int{4}
}

func (x *IntegerPoints) GetTimestamps() []int64 {
	if x != nil {
		return x.Timestamps
	}
	return nil
}

func (x *IntegerPoints) GetValues() []int64 {
	if x != nil {
		return x.Values
	}
	return nil
}

type UnsignedPoints struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Timestamps []int64  `protobuf:"fixed64,1,rep,packed,name=timestamps,proto3" json:"timestamps,omitempty"`
	Values     []uint64 `protobuf:"varint,2,rep,packed,name=values,proto3" json:"values,omitempty"`
}

func (x *UnsignedPoints) Reset() {
	*x = UnsignedPoints{}
	if protoimpl.UnsafeEnabled {
		mi := &file_tools_binary_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *UnsignedPoints) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UnsignedPoints) ProtoMessage() {}

func (x *UnsignedPoints) ProtoReflect() protoreflect.Message {
	mi := &file_tools_binary_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UnsignedPoints.ProtoReflect.Descriptor instead.
func (*UnsignedPoints) Descriptor() ([]byte, []int) {
	return file_tools_binary_proto_rawDescGZIP(), []int{5}
}

func (x *UnsignedPoints) GetTimestamps() []int64 {
	if x != nil {
		return x.Timestamps
	}
	return nil
}

func (x *UnsignedPoints) GetValues() []uint64 {
	if x != nil {
		return x.Values
	}
	return nil
}

type BooleanPoints struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Timestamps []int64 `protobuf:"fixed64,1,rep,packed,name=timestamps,proto3" json:"timestamps,omitempty"`
	Values     []bool  `protobuf:"varint,2,rep,packed,name=values,proto3" json:"values,omitempty"`
}

func (x *BooleanPoints) Reset() {
	*x = BooleanPoints{}
	if protoimpl.UnsafeEnabled {
		mi := &file_tools_binary_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BooleanPoints) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BooleanPoints) ProtoMessage() {}

func (x *BooleanPoints) ProtoReflect() protoreflect.Message {
	mi := &file_tools_binary_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BooleanPoints.ProtoReflect.Descriptor instead.
func (*BooleanPoints) Descriptor() ([]byte, []int) {
	return file_tools_binary_proto_rawDescGZIP(), []int{6}
}

func (x *BooleanPoints) GetTimestamps() []int64 {
	if x != nil {
		return x.Timestamps
	}
	return nil
}

func (x *BooleanPoints) GetValues() []bool {
	if x != nil {
		return x.Values
	}
	return nil
}

type StringPoints struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Timestamps []int64  `protobuf:"fixed64,1,rep,packed,name=timestamps,proto3" json:"timestamps,omitempty"`
	Values     []string `protobuf:"bytes,2,rep,name=values,proto3" json:"values,omitempty"`
}

func (x *StringPoints) Reset() {
	*x = StringPoints{}
	if protoimpl.UnsafeEnabled {
		mi := &file_tools_binary_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StringPoints) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StringPoints) ProtoMessage() {}

func (x *StringPoints) ProtoReflect() protoreflect.Message {
	mi := &file_tools_binary_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StringPoints.ProtoReflect.Descriptor instead.
func (*StringPoints) Descriptor() ([]byte, []int) {
	return file_tools_binary_proto_rawDescGZIP(), []int{7}
}

func (x *StringPoints) GetTimestamps() []int64 {
	if x != nil {
		return x.Timestamps
	}
	return nil
}

func (x *StringPoints) GetValues() []string {
	if x != nil {
		return x.Values
	}
	return nil
}

type SeriesHeader struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	FieldType FieldType `protobuf:"varint,1,opt,name=field_type,json=fieldType,proto3,enum=binary.FieldType" json:"field_type,omitempty"`
	SeriesKey []byte    `protobuf:"bytes,2,opt,name=series_key,json=seriesKey,proto3" json:"series_key,omitempty"`
	Field     []byte    `protobuf:"bytes,3,opt,name=field,proto3" json:"field,omitempty"`
}

func (x *SeriesHeader) Reset() {
	*x = SeriesHeader{}
	if protoimpl.UnsafeEnabled {
		mi := &file_tools_binary_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SeriesHeader) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SeriesHeader) ProtoMessage() {}

func (x *SeriesHeader) ProtoReflect() protoreflect.Message {
	mi := &file_tools_binary_proto_msgTypes[8]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SeriesHeader.ProtoReflect.Descriptor instead.
func (*SeriesHeader) Descriptor() ([]byte, []int) {
	return file_tools_binary_proto_rawDescGZIP(), []int{8}
}

func (x *SeriesHeader) GetFieldType() FieldType {
	if x != nil {
		return x.FieldType
	}
	return FieldType_FloatFieldType
}

func (x *SeriesHeader) GetSeriesKey() []byte {
	if x != nil {
		return x.SeriesKey
	}
	return nil
}

func (x *SeriesHeader) GetField() []byte {
	if x != nil {
		return x.Field
	}
	return nil
}

type SeriesFooter struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *SeriesFooter) Reset() {
	*x = SeriesFooter{}
	if protoimpl.UnsafeEnabled {
		mi := &file_tools_binary_proto_msgTypes[9]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SeriesFooter) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SeriesFooter) ProtoMessage() {}

func (x *SeriesFooter) ProtoReflect() protoreflect.Message {
	mi := &file_tools_binary_proto_msgTypes[9]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SeriesFooter.ProtoReflect.Descriptor instead.
func (*SeriesFooter) Descriptor() ([]byte, []int) {
	return file_tools_binary_proto_rawDescGZIP(), []int{9}
}

var File_tools_binary_proto protoreflect.FileDescriptor

var file_tools_binary_proto_rawDesc = []byte{
	0x0a, 0x12, 0x74, 0x6f, 0x6f, 0x6c, 0x73, 0x5f, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x12, 0x06, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x22, 0xc1, 0x01, 0x0a,
	0x06, 0x48, 0x65, 0x61, 0x64, 0x65, 0x72, 0x12, 0x30, 0x0a, 0x07, 0x76, 0x65, 0x72, 0x73, 0x69,
	0x6f, 0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x16, 0x2e, 0x62, 0x69, 0x6e, 0x61, 0x72,
	0x79, 0x2e, 0x48, 0x65, 0x61, 0x64, 0x65, 0x72, 0x2e, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e,
	0x52, 0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x1a, 0x0a, 0x08, 0x64, 0x61, 0x74,
	0x61, 0x62, 0x61, 0x73, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x64, 0x61, 0x74,
	0x61, 0x62, 0x61, 0x73, 0x65, 0x12, 0x29, 0x0a, 0x10, 0x72, 0x65, 0x74, 0x65, 0x6e, 0x74, 0x69,
	0x6f, 0x6e, 0x5f, 0x70, 0x6f, 0x6c, 0x69, 0x63, 0x79, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x0f, 0x72, 0x65, 0x74, 0x65, 0x6e, 0x74, 0x69, 0x6f, 0x6e, 0x50, 0x6f, 0x6c, 0x69, 0x63, 0x79,
	0x12, 0x25, 0x0a, 0x0e, 0x73, 0x68, 0x61, 0x72, 0x64, 0x5f, 0x64, 0x75, 0x72, 0x61, 0x74, 0x69,
	0x6f, 0x6e, 0x18, 0x04, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0d, 0x73, 0x68, 0x61, 0x72, 0x64, 0x44,
	0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x22, 0x17, 0x0a, 0x07, 0x56, 0x65, 0x72, 0x73, 0x69,
	0x6f, 0x6e, 0x12, 0x0c, 0x0a, 0x08, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x30, 0x10, 0x00,
	0x22, 0x36, 0x0a, 0x0c, 0x42, 0x75, 0x63, 0x6b, 0x65, 0x74, 0x48, 0x65, 0x61, 0x64, 0x65, 0x72,
	0x12, 0x14, 0x0a, 0x05, 0x73, 0x74, 0x61, 0x72, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x10, 0x52,
	0x05, 0x73, 0x74, 0x61, 0x72, 0x74, 0x12, 0x10, 0x0a, 0x03, 0x65, 0x6e, 0x64, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x10, 0x52, 0x03, 0x65, 0x6e, 0x64, 0x22, 0x0e, 0x0a, 0x0c, 0x42, 0x75, 0x63, 0x6b,
	0x65, 0x74, 0x46, 0x6f, 0x6f, 0x74, 0x65, 0x72, 0x22, 0x45, 0x0a, 0x0b, 0x46, 0x6c, 0x6f, 0x61,
	0x74, 0x50, 0x6f, 0x69, 0x6e, 0x74, 0x73, 0x12, 0x1e, 0x0a, 0x0a, 0x74, 0x69, 0x6d, 0x65, 0x73,
	0x74, 0x61, 0x6d, 0x70, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x10, 0x52, 0x0a, 0x74, 0x69, 0x6d,
	0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x73, 0x12, 0x16, 0x0a, 0x06, 0x76, 0x61, 0x6c, 0x75, 0x65,
	0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x01, 0x52, 0x06, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x22,
	0x47, 0x0a, 0x0d, 0x49, 0x6e, 0x74, 0x65, 0x67, 0x65, 0x72, 0x50, 0x6f, 0x69, 0x6e, 0x74, 0x73,
	0x12, 0x1e, 0x0a, 0x0a, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x73, 0x18, 0x01,
	0x20, 0x03, 0x28, 0x10, 0x52, 0x0a, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x73,
	0x12, 0x16, 0x0a, 0x06, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x03,
	0x52, 0x06, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x22, 0x48, 0x0a, 0x0e, 0x55, 0x6e, 0x73, 0x69,
	0x67, 0x6e, 0x65, 0x64, 0x50, 0x6f, 0x69, 0x6e, 0x74, 0x73, 0x12, 0x1e, 0x0a, 0x0a, 0x74, 0x69,
	0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x10, 0x52, 0x0a,
	0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x73, 0x12, 0x16, 0x0a, 0x06, 0x76, 0x61,
	0x6c, 0x75, 0x65, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x04, 0x52, 0x06, 0x76, 0x61, 0x6c, 0x75,
	0x65, 0x73, 0x22, 0x47, 0x0a, 0x0d, 0x42, 0x6f, 0x6f, 0x6c, 0x65, 0x61, 0x6e, 0x50, 0x6f, 0x69,
	0x6e, 0x74, 0x73, 0x12, 0x1e, 0x0a, 0x0a, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70,
	0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x10, 0x52, 0x0a, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61,
	0x6d, 0x70, 0x73, 0x12, 0x16, 0x0a, 0x06, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x18, 0x02, 0x20,
	0x03, 0x28, 0x08, 0x52, 0x06, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x22, 0x46, 0x0a, 0x0c, 0x53,
	0x74, 0x72, 0x69, 0x6e, 0x67, 0x50, 0x6f, 0x69, 0x6e, 0x74, 0x73, 0x12, 0x1e, 0x0a, 0x0a, 0x74,
	0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x10, 0x52,
	0x0a, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x73, 0x12, 0x16, 0x0a, 0x06, 0x76,
	0x61, 0x6c, 0x75, 0x65, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x09, 0x52, 0x06, 0x76, 0x61, 0x6c,
	0x75, 0x65, 0x73, 0x22, 0x75, 0x0a, 0x0c, 0x53, 0x65, 0x72, 0x69, 0x65, 0x73, 0x48, 0x65, 0x61,
	0x64, 0x65, 0x72, 0x12, 0x30, 0x0a, 0x0a, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x5f, 0x74, 0x79, 0x70,
	0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x11, 0x2e, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79,
	0x2e, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x54, 0x79, 0x70, 0x65, 0x52, 0x09, 0x66, 0x69, 0x65, 0x6c,
	0x64, 0x54, 0x79, 0x70, 0x65, 0x12, 0x1d, 0x0a, 0x0a, 0x73, 0x65, 0x72, 0x69, 0x65, 0x73, 0x5f,
	0x6b, 0x65, 0x79, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x09, 0x73, 0x65, 0x72, 0x69, 0x65,
	0x73, 0x4b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x18, 0x03, 0x20,
	0x01, 0x28, 0x0c, 0x52, 0x05, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x22, 0x0e, 0x0a, 0x0c, 0x53, 0x65,
	0x72, 0x69, 0x65, 0x73, 0x46, 0x6f, 0x6f, 0x74, 0x65, 0x72, 0x2a, 0x77, 0x0a, 0x09, 0x46, 0x69,
	0x65, 0x6c, 0x64, 0x54, 0x79, 0x70, 0x65, 0x12, 0x12, 0x0a, 0x0e, 0x46, 0x6c, 0x6f, 0x61, 0x74,
	0x46, 0x69, 0x65, 0x6c, 0x64, 0x54, 0x79, 0x70, 0x65, 0x10, 0x00, 0x12, 0x14, 0x0a, 0x10, 0x49,
	0x6e, 0x74, 0x65, 0x67, 0x65, 0x72, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x54, 0x79, 0x70, 0x65, 0x10,
	0x01, 0x12, 0x15, 0x0a, 0x11, 0x55, 0x6e, 0x73, 0x69, 0x67, 0x6e, 0x65, 0x64, 0x46, 0x69, 0x65,
	0x6c, 0x64, 0x54, 0x79, 0x70, 0x65, 0x10, 0x02, 0x12, 0x14, 0x0a, 0x10, 0x42, 0x6f, 0x6f, 0x6c,
	0x65, 0x61, 0x6e, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x54, 0x79, 0x70, 0x65, 0x10, 0x03, 0x12, 0x13,
	0x0a, 0x0f, 0x53, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x54, 0x79, 0x70,
	0x65, 0x10, 0x04, 0x42, 0x0a, 0x5a, 0x08, 0x2e, 0x3b, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x62,
	0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_tools_binary_proto_rawDescOnce sync.Once
	file_tools_binary_proto_rawDescData = file_tools_binary_proto_rawDesc
)

func file_tools_binary_proto_rawDescGZIP() []byte {
	file_tools_binary_proto_rawDescOnce.Do(func() {
		file_tools_binary_proto_rawDescData = protoimpl.X.CompressGZIP(file_tools_binary_proto_rawDescData)
	})
	return file_tools_binary_proto_rawDescData
}

var file_tools_binary_proto_enumTypes = make([]protoimpl.EnumInfo, 2)
var file_tools_binary_proto_msgTypes = make([]protoimpl.MessageInfo, 10)
var file_tools_binary_proto_goTypes = []interface{}{
	(FieldType)(0),         // 0: binary.FieldType
	(Header_Version)(0),    // 1: binary.Header.Version
	(*Header)(nil),         // 2: binary.Header
	(*BucketHeader)(nil),   // 3: binary.BucketHeader
	(*BucketFooter)(nil),   // 4: binary.BucketFooter
	(*FloatPoints)(nil),    // 5: binary.FloatPoints
	(*IntegerPoints)(nil),  // 6: binary.IntegerPoints
	(*UnsignedPoints)(nil), // 7: binary.UnsignedPoints
	(*BooleanPoints)(nil),  // 8: binary.BooleanPoints
	(*StringPoints)(nil),   // 9: binary.StringPoints
	(*SeriesHeader)(nil),   // 10: binary.SeriesHeader
	(*SeriesFooter)(nil),   // 11: binary.SeriesFooter
}
var file_tools_binary_proto_depIdxs = []int32{
	1, // 0: binary.Header.version:type_name -> binary.Header.Version
	0, // 1: binary.SeriesHeader.field_type:type_name -> binary.FieldType
	2, // [2:2] is the sub-list for method output_type
	2, // [2:2] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_tools_binary_proto_init() }
func file_tools_binary_proto_init() {
	if File_tools_binary_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_tools_binary_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Header); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_tools_binary_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BucketHeader); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_tools_binary_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BucketFooter); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_tools_binary_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*FloatPoints); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_tools_binary_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*IntegerPoints); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_tools_binary_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*UnsignedPoints); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_tools_binary_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BooleanPoints); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_tools_binary_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*StringPoints); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_tools_binary_proto_msgTypes[8].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SeriesHeader); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_tools_binary_proto_msgTypes[9].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SeriesFooter); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_tools_binary_proto_rawDesc,
			NumEnums:      2,
			NumMessages:   10,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_tools_binary_proto_goTypes,
		DependencyIndexes: file_tools_binary_proto_depIdxs,
		EnumInfos:         file_tools_binary_proto_enumTypes,
		MessageInfos:      file_tools_binary_proto_msgTypes,
	}.Build()
	File_tools_binary_proto = out.File
	file_tools_binary_proto_rawDesc = nil
	file_tools_binary_proto_goTypes = nil
	file_tools_binary_proto_depIdxs = nil
}
